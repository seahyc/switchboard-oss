// Package lark implements the switchboard Connector for Lark/Feishu using the
// official github.com/larksuite/oapi-sdk-go SDK.
//
// Design (per SPEC + lead):
//   - Live events arrive over the SDK websocket long-connection (no public
//     webhook): im.message.receive, recalled, reaction.created, and
//     card.action.trigger (interactive card / button taps).
//   - The websocket authenticates as the app/bot, so it only sees chats the bot
//     is in. To capture the user's own chats we ALSO run a poll loop that lists
//     the user's chats + messages with the USER access token (full read).
//   - Backfill pages im/v1/messages per chat with the user token.
//   - Send delivers text as the user (send_as_user) and interactive cards as the
//     bot (Lark's rich-UI equivalent of inline buttons). React adds an emoji.
//
// All capture is pure — no LLM, no pay-per-token API.
package lark

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"

	"github.com/seahyc/switchboard-oss/internal/connectors"
	"github.com/seahyc/switchboard-oss/internal/event"
)

// pollInterval is how often the user-token poll loop checks for new messages in
// chats the bot cannot see over the websocket.
const pollInterval = 30 * time.Second

// listMsgInterPageDelay paces successive message-list pages within one chat's backfill,
// mirroring the Telegram fix (#42): a deep from-scratch backfill pages Im.Message.List
// back-to-back (50 msgs/page), and across the breadth sweep's 25 chats/fire that burst
// risks tripping Lark's API rate limit. ~1s is lighter than Telegram's 1.5s — Lark's
// page is smaller and its per-app limits are more forgiving than TG's account-global
// FLOOD_WAIT — while still keeping a multi-page chat comfortably paced.
const listMsgInterPageDelay = 1 * time.Second

// Connector is the Lark implementation of connectors.Connector.
type Connector struct {
	cfg    Config
	client *lark.Client
	tokens *tokenStore
	now    func() time.Time // injectable clock (tests)

	mu        sync.Mutex
	connected bool
	// watermarks tracks the last seen message create-time per raw chat id for the
	// poll loop, so overlapping live/poll/backfill stays idempotent on id anyway.
	watermarks map[string]time.Time
	// discovered holds p2p (DM) chats found via message-search (#31). Im.Chat.List
	// returns ONLY group chats, so the user's DMs are otherwise invisible to backfill
	// and the poll loop; discovery seeds them in. Keyed by raw chat id.
	discovered map[string]chatRef
	// enricher is the memoized contact enricher (shared cache across the projector
	// adapter and ChatMetadata so repeated open_ids are fetched once).
	enricher *Enricher
}

// New builds a Lark connector from the given config dir (default if "").
func New(configDir string) (*Connector, error) {
	if configDir == "" {
		configDir = DefaultConfigDir()
	}
	cfg, err := LoadConfig(configDir)
	if err != nil {
		return nil, err
	}
	tokens, err := newTokenStore(cfg)
	if err != nil {
		return nil, err
	}
	// Point REST at the tenant's domain (Lark international vs Feishu CN). The
	// websocket needs the same domain explicitly (see Connect).
	client := lark.NewClient(cfg.AppID, cfg.AppSecret, lark.WithOpenBaseUrl(cfg.Domain))
	return &Connector{
		cfg:        cfg,
		client:     client,
		tokens:     tokens,
		now:        time.Now,
		watermarks: map[string]time.Time{},
		discovered: map[string]chatRef{},
	}, nil
}

// Name implements connectors.Connector.
func (c *Connector) Name() string { return event.PlatformLark }

// userOpt returns a request option carrying the (refreshed) user access token.
func (c *Connector) userOpt(ctx context.Context) (larkcore.RequestOptionFunc, error) {
	tok, err := c.tokens.AccessToken(ctx, c.client, c.now().UTC())
	if err != nil {
		return nil, err
	}
	return larkcore.WithUserAccessToken(tok), nil
}

// Connect streams live events to out until ctx is cancelled. It starts both the
// websocket long-connection (bot events + card actions) and the user-token poll
// loop, then blocks until ctx is done.
func (c *Connector) Connect(ctx context.Context, out chan<- event.Message) error {
	// Ensure we have a USABLE user token without forcing a refresh: a valid
	// access token must be used as-is (refresh only when actually expired). A
	// force-refresh here would throw away a perfectly good token and, worse,
	// fail the whole connector if the refresh chain hiccups.
	if _, err := c.tokens.AccessToken(ctx, c.client, c.now().UTC()); err != nil {
		return fmt.Errorf("lark connect: %w", err)
	}
	// Learn the user's own open_id (drives is_self/@me) via user_info using the
	// current token. Best-effort — capture proceeds even if this fails.
	_ = c.tokens.ensureIdentity(ctx, c.client, c.now().UTC())

	c.mu.Lock()
	c.connected = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.connected = false
		c.mu.Unlock()
	}()

	handler := c.eventDispatcher(ctx, out)
	wsClient := larkws.NewClient(c.cfg.AppID, c.cfg.AppSecret,
		larkws.WithEventHandler(handler),
		larkws.WithAutoReconnect(true),
		larkws.WithLogLevel(larkcore.LogLevelInfo),
		// Without this the SDK dials the Feishu CN endpoint and the larksuite
		// tenant rejects it: "1000040351: Incorrect domain name".
		larkws.WithDomain(c.cfg.Domain),
	)

	errCh := make(chan error, 1)
	go func() { errCh <- wsClient.Start(ctx) }()

	go c.pollLoop(ctx, out)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("lark websocket: %w", err)
		}
		return nil
	}
}

// eventDispatcher wires the SDK event handlers to canonical-event emission.
func (c *Connector) eventDispatcher(ctx context.Context, out chan<- event.Message) *dispatcher.EventDispatcher {
	self := c.tokens.selfOpenID()
	return dispatcher.NewEventDispatcher("", "").
		OnP2MessageReceiveV1(func(_ context.Context, ev *larkim.P2MessageReceiveV1) error {
			if m, ok := mapMessageEvent(ev, self, c.now().UTC()); ok {
				// Register a live p2p chat so backfill + poll cover its history too
				// (#31): DMs never appear in Im.Chat.List, so a live message is often
				// the first time we learn the chat exists.
				if m.ChatType == event.ChatDM {
					target := ""
					if !m.Sender.IsSelf {
						target = m.Sender.PlatformUserID
					}
					if c.registerDiscovered(stripPrefix(m.ChatID), target) {
						// First sight of this DM — pull its history once in the
						// background so we get more than just this live message.
						go c.backfillDiscovered(ctx, stripPrefix(m.ChatID), out)
					}
				}
				c.emit(ctx, out, m)
			}
			return nil
		}).
		OnP2MessageRecalledV1(func(_ context.Context, ev *larkim.P2MessageRecalledV1) error {
			if m, ok := mapRecalled(ev, self, c.now().UTC()); ok {
				c.emit(ctx, out, m)
			}
			return nil
		}).
		OnP2MessageReactionCreatedV1(func(_ context.Context, ev *larkim.P2MessageReactionCreatedV1) error {
			if m, ok := mapReactionCreated(ev, self, c.now().UTC()); ok {
				c.emit(ctx, out, m)
			}
			return nil
		}).
		OnP2MessageReactionDeletedV1(func(_ context.Context, _ *larkim.P2MessageReactionDeletedV1) error {
			// Acknowledge so the SDK doesn't log "not found handler" (#31). The frozen
			// canonical schema has no representation for a reaction REMOVAL (Reaction
			// is {emoji,by} with no removed flag), so we intentionally don't emit one —
			// re-mapping it as KindReaction would falsely look like a new reaction.
			return nil
		}).
		OnP2CardActionTrigger(func(_ context.Context, ev *callback.CardActionTriggerEvent) (*callback.CardActionTriggerResponse, error) {
			if m, ok := mapCardAction(ev, self, c.now().UTC()); ok {
				c.emit(ctx, out, m)
			}
			// Acknowledge without mutating the card.
			return &callback.CardActionTriggerResponse{}, nil
		})
}

// emit sends a canonical event to out, respecting ctx cancellation.
func (c *Connector) emit(ctx context.Context, out chan<- event.Message, m event.Message) {
	select {
	case out <- m:
	case <-ctx.Done():
	}
}

// pollLoop periodically lists the user's chats and any new messages since the
// per-chat watermark, emitting them as live events. This covers chats the bot
// (websocket identity) is not a member of. Idempotency is by event id.
func (c *Connector) pollLoop(ctx context.Context, out chan<- event.Message) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.pollOnce(ctx, out); err != nil {
				// Transient (e.g. token blip); next tick retries.
				continue
			}
		}
	}
}

func (c *Connector) pollOnce(ctx context.Context, out chan<- event.Message) error {
	opt, err := c.userOpt(ctx)
	if err != nil {
		return err
	}
	chats, err := c.allChats(ctx, opt)
	if err != nil {
		return err
	}
	self := c.tokens.selfOpenID()
	for _, ch := range chats {
		c.mu.Lock()
		since := c.watermarks[ch.id]
		c.mu.Unlock()
		// On first sight of a chat, start from now-ish to avoid a full re-pull
		// (Backfill is the explicit history path).
		if since.IsZero() {
			since = c.now().Add(-pollInterval)
		}
		latest := since
		emitFn := func(m event.Message) {
			c.emit(ctx, out, m)
			if m.TS.After(latest) {
				latest = m.TS
			}
		}
		if err := c.listMessages(ctx, opt, ch.id, ch.chatType, since, event.SourceLive, self, emitFn); err != nil {
			continue
		}
		c.mu.Lock()
		c.watermarks[ch.id] = latest
		c.mu.Unlock()
	}
	return nil
}

// chatRef is a chat descriptor from the chat list. id + chatType drive the poll
// loop / backfill; name/labels/p2pTargetID feed ChatMetadata.
type chatRef struct {
	id          string
	chatType    string // canonical
	name        string
	labels      []string
	p2pTargetID string // counterpart open_id for p2p chats (for display-name enrich)
}

func (c *Connector) listChats(ctx context.Context, opt larkcore.RequestOptionFunc) ([]chatRef, error) {
	var out []chatRef
	pageToken := ""
	for {
		b := larkim.NewListChatReqBuilder().PageSize(100)
		if pageToken != "" {
			b = b.PageToken(pageToken)
		}
		resp, err := c.client.Im.Chat.List(ctx, b.Build(), opt)
		if err != nil {
			return nil, err
		}
		if !resp.Success() {
			return nil, fmt.Errorf("lark list chats: code=%d msg=%s", resp.Code, resp.Msg)
		}
		for _, ch := range resp.Data.Items {
			out = append(out, chatRef{
				id:          deref(ch.ChatId),
				chatType:    chatTypeFor(deref(ch.ChatMode)),
				name:        deref(ch.Name),
				labels:      ch.Labels,
				p2pTargetID: deref(ch.P2pTargetId),
			})
		}
		if resp.Data.HasMore == nil || !*resp.Data.HasMore || resp.Data.PageToken == nil {
			break
		}
		pageToken = *resp.Data.PageToken
	}
	return out, nil
}

// ChatMetadata enumerates the user's chats with human-legible titles + the org
// signals Lark exposes (task #11). The daemon polls this and upserts so
// list_chats shows real names instead of "lark:oc_...". For p2p chats the title
// is the counterpart's display name (via the cached enricher); for groups it's
// the group name. Lark's user API does not surface per-chat mute/pin/archive in
// the chat list, so those are left at their defaults.
func (c *Connector) ChatMetadata(ctx context.Context) ([]connectors.ChatMeta, error) {
	opt, err := c.userOpt(ctx)
	if err != nil {
		return nil, err
	}
	chats, err := c.listChats(ctx, opt)
	if err != nil {
		return nil, err
	}

	// Resolve display names for p2p counterparts in one batched, cached call.
	var p2pIDs []string
	for _, ch := range chats {
		if ch.chatType == event.ChatDM && ch.p2pTargetID != "" {
			p2pIDs = append(p2pIDs, ch.p2pTargetID)
		}
	}
	var profiles map[string]Profile
	if len(p2pIDs) > 0 {
		profiles, _ = c.Enricher().Enrich(ctx, p2pIDs) // best-effort; falls back to name
	}

	return chatMetasFrom(chats, profiles), nil
}

// chatMetasFrom is the pure mapping of chat list + resolved profiles to canonical
// ChatMeta (unit-tested). Title prefers a p2p counterpart's display name, else
// the group/chat name.
func chatMetasFrom(chats []chatRef, profiles map[string]Profile) []connectors.ChatMeta {
	out := make([]connectors.ChatMeta, 0, len(chats))
	for _, ch := range chats {
		title := ch.name
		if ch.chatType == event.ChatDM && ch.p2pTargetID != "" {
			if p, ok := profiles[ch.p2pTargetID]; ok && p.DisplayName != "" {
				title = p.DisplayName
			}
		}
		out = append(out, connectors.ChatMeta{
			ChatID: canonChatID(ch.id),
			Title:  title,
			Labels: ch.labels,
			// Muted/Pinned/Archived/Importance: not exposed by Lark's user chat
			// list API — left at defaults.
		})
	}
	return out
}

// listMessages pages im/v1/messages for a chat since `since`, mapping + emitting
// each through emitFn. Shared by Backfill and the poll loop.
// listMessages pages a chat's history NEWEST-FIRST (ByCreateTimeDesc) and emits
// each message. Newest-first is deliberate (#31): for a message bus the dominant
// query is "latest from X", so backfill must surface recent context immediately
// and fill older history behind it — paging oldest-first buried the most-wanted
// recent message under months of backlog. The store is idempotent + cursored, so
// emit order doesn't affect correctness, only which messages become queryable first.
func (c *Connector) listMessages(ctx context.Context, opt larkcore.RequestOptionFunc, chatID, chatType string, since time.Time, source, self string, emitFn func(event.Message)) error {
	pageToken := ""
	for {
		b := larkim.NewListMessageReqBuilder().
			ContainerIdType("chat").
			ContainerId(chatID).
			SortType("ByCreateTimeDesc").
			PageSize(50)
		if !since.IsZero() {
			b = b.StartTime(strconv.FormatInt(since.Unix(), 10))
		}
		if pageToken != "" {
			b = b.PageToken(pageToken)
		}
		resp, err := c.client.Im.Message.List(ctx, b.Build(), opt)
		if err != nil {
			return err
		}
		if !resp.Success() {
			return fmt.Errorf("lark list messages: code=%d msg=%s", resp.Code, resp.Msg)
		}
		for _, msg := range resp.Data.Items {
			m, ok := mapListedMessage(msg, chatType, self, c.now().UTC())
			if !ok {
				continue
			}
			m.Source = source
			emitFn(m)
		}
		if resp.Data.HasMore == nil || !*resp.Data.HasMore || resp.Data.PageToken == nil {
			break
		}
		pageToken = *resp.Data.PageToken
		// Pace before the next page so a deep multi-page backfill doesn't burst the
		// message-list API into a rate limit (#42, mirrors the TG fix). ctx-aware so a
		// cancel / per-chat timeout returns promptly rather than sleeping out the delay.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(listMsgInterPageDelay):
		}
	}
	return nil
}

// Backfill drives history-sync for a chat (or all chats when chatID == "").
func (c *Connector) Backfill(ctx context.Context, chatID string, since time.Time, out chan<- event.Message) error {
	opt, err := c.userOpt(ctx)
	if err != nil {
		return err
	}
	self := c.tokens.selfOpenID()
	emitFn := func(m event.Message) { c.emit(ctx, out, m) }

	if chatID == "" {
		chats, err := c.allChats(ctx, opt)
		if err != nil {
			return err
		}
		for _, ch := range chats {
			if err := c.listMessages(ctx, opt, ch.id, ch.chatType, since, event.SourceBackfill, self, emitFn); err != nil {
				return err
			}
		}
		return nil
	}

	raw := stripPrefix(chatID)
	chatType := c.chatTypeForChat(ctx, opt, raw)
	return c.listMessages(ctx, opt, raw, chatType, since, event.SourceBackfill, self, emitFn)
}

// chatTypeForChat resolves a chat's canonical type by scanning the chat list;
// defaults to group if unknown.
func (c *Connector) chatTypeForChat(ctx context.Context, opt larkcore.RequestOptionFunc, rawChatID string) string {
	chats, err := c.listChats(ctx, opt)
	if err != nil {
		return event.ChatGroup
	}
	for _, ch := range chats {
		if ch.id == rawChatID {
			return ch.chatType
		}
	}
	return event.ChatGroup
}

// larkReceiveIDType infers Lark's receive_id_type from a (prefix-stripped) target
// id (#35): so a send to a person's open_id (ou_…) routes to their p2p DM instead
// of being rejected as an invalid chat_id. resolve_person hands the agent an
// open_id; this lets `send` use it directly. Unprefixed ids fall back to chat_id
// (the prior default — most targets are oc_… chats).
func larkReceiveIDType(id string) string {
	switch {
	case strings.HasPrefix(id, "ou_"):
		return "open_id"
	case strings.HasPrefix(id, "oc_"):
		return "chat_id"
	case strings.HasPrefix(id, "on_"):
		return "union_id"
	case strings.Contains(id, "@"):
		return "email"
	default:
		return "chat_id"
	}
}

// Send delivers an outbound message and returns the canonical sent id.
// Text is sent as the user (send_as_user); interactive cards/buttons are sent as
// the bot (Lark's rich-UI channel). Replies use the reply endpoint.
func (c *Connector) Send(ctx context.Context, req connectors.SendRequest) (string, error) {
	rawChat := stripPrefix(req.ChatID)

	// Interactive card / buttons → send as the bot (app token).
	if req.Card != nil || req.Buttons != nil {
		card := req.Card
		if card == nil {
			card = req.Buttons
		}
		content, err := json.Marshal(card)
		if err != nil {
			return "", fmt.Errorf("lark send card: marshal: %w", err)
		}
		body := larkim.NewCreateMessageReqBodyBuilder().
			ReceiveId(rawChat).
			MsgType("interactive").
			Content(string(content)).
			Build()
		mreq := larkim.NewCreateMessageReqBuilder().
			ReceiveIdType(larkReceiveIDType(rawChat)).
			Body(body).
			Build()
		resp, err := c.client.Im.Message.Create(ctx, mreq)
		if err != nil {
			return "", err
		}
		if !resp.Success() {
			return "", fmt.Errorf("lark send card: code=%d msg=%s", resp.Code, resp.Msg)
		}
		return canonMsgID(deref(resp.Data.MessageId)), nil
	}

	// Text → send as the user.
	opt, err := c.userOpt(ctx)
	if err != nil {
		return "", err
	}
	content, _ := json.Marshal(map[string]string{"text": req.Text})

	if req.ReplyTo != "" {
		parent := stripPrefix(req.ReplyTo)
		body := larkim.NewReplyMessageReqBodyBuilder().
			MsgType("text").
			Content(string(content)).
			Build()
		rreq := larkim.NewReplyMessageReqBuilder().
			MessageId(parent).
			Body(body).
			Build()
		resp, err := c.client.Im.Message.Reply(ctx, rreq, opt)
		if err != nil {
			return "", err
		}
		if !resp.Success() {
			return "", fmt.Errorf("lark reply: code=%d msg=%s", resp.Code, resp.Msg)
		}
		return canonMsgID(deref(resp.Data.MessageId)), nil
	}

	body := larkim.NewCreateMessageReqBodyBuilder().
		ReceiveId(rawChat).
		MsgType("text").
		Content(string(content)).
		Build()
	mreq := larkim.NewCreateMessageReqBuilder().
		ReceiveIdType(larkReceiveIDType(rawChat)).
		Body(body).
		Build()
	resp, err := c.client.Im.Message.Create(ctx, mreq, opt)
	if err != nil {
		return "", err
	}
	if !resp.Success() {
		return "", fmt.Errorf("lark send text: code=%d msg=%s", resp.Code, resp.Msg)
	}
	return canonMsgID(deref(resp.Data.MessageId)), nil
}

// React adds an emoji reaction to a message (as the user). The emoji argument is
// a Lark emoji_type code (e.g. "THUMBSUP", "HEART", "SMILE").
func (c *Connector) React(ctx context.Context, chatID, msgID, emoji string) error {
	opt, err := c.userOpt(ctx)
	if err != nil {
		return err
	}
	emojiType := larkim.NewEmojiBuilder().EmojiType(strings.ToUpper(emoji)).Build()
	body := larkim.NewCreateMessageReactionReqBodyBuilder().ReactionType(emojiType).Build()
	rreq := larkim.NewCreateMessageReactionReqBuilder().
		MessageId(stripPrefix(msgID)).
		Body(body).
		Build()
	resp, err := c.client.Im.MessageReaction.Create(ctx, rreq, opt)
	if err != nil {
		return err
	}
	if !resp.Success() {
		return fmt.Errorf("lark react: code=%d msg=%s", resp.Code, resp.Msg)
	}
	return nil
}

// Login drives token refresh (the only interactive step Lark needs, and only if
// the refresh_token itself has expired — then the user must re-authorize).
func (c *Connector) Login(ctx context.Context) (connectors.LoginStatus, error) {
	// Validate the token (lazy refresh only if expired) rather than force a
	// refresh — a fresh token should make Login succeed without consuming the
	// single-use refresh_token.
	if _, err := c.tokens.AccessToken(ctx, c.client, c.now().UTC()); err != nil {
		return connectors.LoginStatus{
			NeedsAuth: true,
			Detail:    fmt.Sprintf("lark token invalid; re-authorize via `lark auth login`: %v", err),
		}, err
	}
	_ = c.tokens.ensureIdentity(ctx, c.client, c.now().UTC())
	return connectors.LoginStatus{
		Connected: true,
		Detail:    "lark user token valid; ready",
	}, nil
}

// Status reports auth/connection state without driving login.
func (c *Connector) Status(_ context.Context) (connectors.LoginStatus, error) {
	c.mu.Lock()
	connected := c.connected
	c.mu.Unlock()

	if c.tokens.fresh(c.now().UTC()) {
		return connectors.LoginStatus{
			Connected: connected,
			Detail:    "lark user token valid",
		}, nil
	}
	return connectors.LoginStatus{
		Connected: connected,
		NeedsAuth: false, // a refresh_token is on file; refresh on demand
		Detail:    "lark user token expired; will refresh on next use",
	}, nil
}

// stripPrefix removes a leading "lark:" platform prefix from a canonical id,
// yielding the raw Lark id the SDK expects.
func stripPrefix(id string) string {
	return strings.TrimPrefix(id, event.PlatformLark+":")
}

// compile-time assertions that *Connector satisfies the base interface and the
// optional chat-metadata capability.
var (
	_ connectors.Connector            = (*Connector)(nil)
	_ connectors.ChatMetadataProvider = (*Connector)(nil)
)
