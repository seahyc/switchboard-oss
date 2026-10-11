package telegram

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/tg"

	"github.com/seahyc/switchboard-oss/internal/connectors"
	"github.com/seahyc/switchboard-oss/internal/event"
)

// ErrNoCredentials is returned when Connect/Login is attempted without app_id /
// app_hash (TG_APP_ID / TG_APP_HASH). The connector still builds and unit-tests
// without them — credentials are only needed to actually authenticate.
var ErrNoCredentials = errors.New("telegram: missing app_id/app_hash (set TG_APP_ID and TG_APP_HASH)")

// AuthPhase is a typed, machine-readable signal of where interactive login is.
// The daemon's auth driver can type-assert *Connector to PhaseReporter and
// branch on this enum to decide which Submit* call to make next — far more
// robust than parsing the free-text LoginStatus.Detail string.
type AuthPhase int

const (
	AuthIdle          AuthPhase = iota // not started / not connecting
	AuthAwaitingPhone                  // login flow needs the phone number
	AuthAwaitingCode                   // login flow needs the SMS/app login code
	AuthAwaiting2FA                    // login flow needs the 2FA cloud password
	AuthConnected                      // authenticated and live
)

// String renders the phase for logs/UX.
func (p AuthPhase) String() string {
	switch p {
	case AuthAwaitingPhone:
		return "awaiting_phone"
	case AuthAwaitingCode:
		return "awaiting_code"
	case AuthAwaiting2FA:
		return "awaiting_2fa"
	case AuthConnected:
		return "connected"
	default:
		return "idle"
	}
}

// PhaseReporter is the optional capability the daemon type-asserts to read the
// structured auth phase. *Connector implements it.
type PhaseReporter interface {
	AuthPhase() AuthPhase
}

// Connector implements connectors.Connector for Telegram. It runs as a USER
// account (userbot) for reading personal DMs/groups, and uses the @example_bot
// Bot API only for outbound rich inline-keyboard UI, unified behind Send.
type Connector struct {
	cfg     Config
	storage *stringSessionStorage
	peers   *peerCache
	bot     *botClient // nil if no bot token

	dispatcher tg.UpdateDispatcher
	gaps       *updates.Manager
	client     *telegram.Client

	initOnce sync.Once
	initErr  error

	mu     sync.RWMutex
	api    *tg.Client
	self   *tg.User
	out    chan<- event.Message
	detail string
	phase  AuthPhase

	ready     chan struct{}
	readyOnce sync.Once

	// interactive auth plumbing — the daemon drives these with the user.
	phoneCh chan string
	codeCh  chan string
	pwCh    chan string
}

var (
	_ connectors.Connector            = (*Connector)(nil)
	_ connectors.ButtonTapper         = (*Connector)(nil)
	_ connectors.PollVoter            = (*Connector)(nil)
	_ connectors.ChatMetadataProvider = (*Connector)(nil)
	_ connectors.InteractiveAuth      = (*Connector)(nil)
)

// New builds a Telegram connector from cfg. It never dials the network — that
// happens in Connect/Login — so it is safe to construct without credentials.
func New(cfg Config) *Connector {
	var bot *botClient
	if cfg.BotToken != "" {
		bot = newBotClient(cfg.BotToken)
	}
	return &Connector{
		cfg:     cfg,
		storage: newStringSessionStorage(cfg.SessionPath),
		peers:   newPeerCache(),
		bot:     bot,
		ready:   make(chan struct{}),
		phoneCh: make(chan string),
		codeCh:  make(chan string),
		pwCh:    make(chan string),
		detail:  "not connected",
	}
}

// Name returns the platform identifier.
func (c *Connector) Name() string { return event.PlatformTelegram }

// ---- lifecycle -------------------------------------------------------------

// initClient lazily constructs the gotd client, update dispatcher and gaps
// manager. Requires credentials.
func (c *Connector) initClient() error {
	c.initOnce.Do(func() {
		if !c.cfg.HasCredentials() {
			c.initErr = ErrNoCredentials
			return
		}
		c.dispatcher = tg.NewUpdateDispatcher()
		c.registerHandlers()
		c.gaps = updates.New(updates.Config{Handler: c.dispatcher})
		c.client = telegram.NewClient(c.cfg.AppID, c.cfg.AppHash, telegram.Options{
			SessionStorage: c.storage,
			UpdateHandler:  c.gaps,
		})
	})
	return c.initErr
}

// Connect authenticates if needed, then streams live events to out until ctx is
// cancelled. It blocks for the connection's lifetime (run it in a goroutine).
func (c *Connector) Connect(ctx context.Context, out chan<- event.Message) error {
	if err := c.initClient(); err != nil {
		return err
	}
	c.setOut(out)

	return c.client.Run(ctx, func(ctx context.Context) error {
		status, err := c.client.Auth().Status(ctx)
		if err != nil {
			return fmt.Errorf("telegram: auth status: %w", err)
		}
		if !status.Authorized {
			c.setDetail("authenticating (awaiting phone/code/2FA)")
			flow := auth.NewFlow(userAuthenticator{c}, auth.SendCodeOptions{})
			if err := flow.Run(ctx, c.client.Auth()); err != nil {
				return fmt.Errorf("telegram: login flow: %w", err)
			}
		}

		self, err := c.client.Self(ctx)
		if err != nil {
			return fmt.Errorf("telegram: self: %w", err)
		}
		// #43: warm the peer access-hash cache BEFORE marking send-ready. The cache is
		// in-memory + repopulated by the periodic metadata refresher's dialog enumeration
		// (startup+20s, then 10min), so without this there's a window after every restart/
		// deploy where a send to a not-yet-re-observed peer 502s "no cached access hash".
		// Enumerating dialogs here (getDialogs is light, not flood-prone) caches every
		// dialog's access_hash up front, closing that window for ALL peers. Best-effort:
		// a warm failure must NOT block the connector coming up — the refresher retries,
		// and a still-cold peer just falls back to the same 502 it had before.
		c.setDetail("warming peer cache")
		if _, werr := c.enumerateDialogs(ctx, c.client.API()); werr != nil {
			c.log("telegram: startup peer-cache warm failed (relying on meta refresher): %v", werr)
		}
		c.markReady(self)
		c.setDetail("connected as @" + self.Username)

		// Block here, processing live updates and recovering gaps, until ctx ends.
		return c.gaps.Run(ctx, c.client.API(), self.ID, updates.AuthOptions{
			OnStart: func(context.Context) { c.setDetail("live") },
		})
	})
}

// awaitReady blocks until the client is authorized and the API is available, so
// outbound calls (Send/React/Backfill) don't race connection startup.
func (c *Connector) awaitReady(ctx context.Context) (*tg.Client, error) {
	c.mu.RLock()
	api := c.api
	c.mu.RUnlock()
	if api != nil {
		return api, nil
	}
	select {
	case <-c.ready:
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.api, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Connector) markReady(self *tg.User) {
	c.mu.Lock()
	c.self = self
	c.api = c.client.API()
	c.phase = AuthConnected
	c.mu.Unlock()
	c.readyOnce.Do(func() { close(c.ready) })
}

func (c *Connector) selfID() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.self == nil {
		return 0
	}
	return c.self.ID
}

func (c *Connector) setOut(out chan<- event.Message) {
	c.mu.Lock()
	c.out = out
	c.mu.Unlock()
}

// log emits a connector-level diagnostic via the default slog logger. Kept
// dependency-free so New() needs no logger argument.
func (c *Connector) log(format string, args ...any) {
	slog.Info(fmt.Sprintf(format, args...))
}

func (c *Connector) setDetail(d string) {
	c.mu.Lock()
	c.detail = d
	c.mu.Unlock()
}

// setPhase records the structured auth phase and a matching human detail string
// in one shot, so PhaseReporter and LoginStatus.Detail never disagree.
func (c *Connector) setPhase(p AuthPhase, detail string) {
	c.mu.Lock()
	c.phase = p
	c.detail = detail
	c.mu.Unlock()
}

// AuthPhase returns the current interactive-login phase (PhaseReporter).
func (c *Connector) AuthPhase() AuthPhase {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.phase
}

// emit sends a normalized event downstream without blocking the live socket
// indefinitely — it respects ctx so shutdown can't deadlock.
func (c *Connector) emit(ctx context.Context, m event.Message) {
	c.mu.RLock()
	out := c.out
	c.mu.RUnlock()
	if out == nil {
		return
	}
	select {
	case out <- m:
	case <-ctx.Done():
	}
}

// ---- live update handlers --------------------------------------------------

func (c *Connector) registerHandlers() {
	c.dispatcher.OnNewMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
		c.handleMessage(ctx, e, u.Message, event.SourceLive, false)
		return nil
	})
	c.dispatcher.OnNewChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewChannelMessage) error {
		c.handleMessage(ctx, e, u.Message, event.SourceLive, false)
		return nil
	})
	c.dispatcher.OnEditMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateEditMessage) error {
		c.handleMessage(ctx, e, u.Message, event.SourceLive, true)
		return nil
	})
	c.dispatcher.OnEditChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateEditChannelMessage) error {
		c.handleMessage(ctx, e, u.Message, event.SourceLive, true)
		return nil
	})
	c.dispatcher.OnDeleteChannelMessages(func(ctx context.Context, e tg.Entities, u *tg.UpdateDeleteChannelMessages) error {
		peer := &tg.PeerChannel{ChannelID: u.ChannelID}
		for _, m := range mapDelete(peer, u.Messages, c.selfID(), time.Now()) {
			c.emit(ctx, m)
		}
		return nil
	})
	// NOTE: plain UpdateDeleteMessages carries no peer (Telegram quirk), so we
	// cannot scope those ids to a chat reliably; they are intentionally not
	// emitted here. Channel deletes (above) are handled precisely.
}

func (c *Connector) handleMessage(ctx context.Context, e tg.Entities, mc tg.MessageClass, source string, isEdit bool) {
	c.peers.learnFromEntities(e.Users, e.Chats, e.Channels)
	msg, ok := mc.(*tg.Message)
	if !ok {
		return // MessageService / MessageEmpty: skip non-content updates for now
	}
	ix := newEntityIndex(e, c.selfID(), c.peers)
	// Also cache the message's own peer so replies/sends can target it.
	c.learnMessagePeer(ix, msg.PeerID)

	ev := mapMessage(msg, ix, source, time.Now())
	if isEdit {
		ev.Kind = event.KindEdit
	}
	c.emit(ctx, ev)
}

// learnMessagePeer ensures the message's dialog peer is resolvable for outbound
// calls, deriving an InputPeer from the entity set.
func (c *Connector) learnMessagePeer(ix entityIndex, peer tg.PeerClass) {
	switch v := peer.(type) {
	case *tg.PeerUser:
		if u, ok := ix.users[v.UserID]; ok {
			c.peers.put(canonicalChatID(peer), u.AsInputPeer())
		}
	case *tg.PeerChat:
		c.peers.put(canonicalChatID(peer), &tg.InputPeerChat{ChatID: v.ChatID})
	case *tg.PeerChannel:
		if ch, ok := ix.channels[v.ChannelID]; ok {
			c.peers.put(canonicalChatID(peer), ch.AsInputPeer())
		}
	}
}

// ---- Backfill --------------------------------------------------------------

// dialogPageLimit is the per-request page size for both getDialogs and
// getHistory. 100 is the Telegram max for these methods.
const dialogPageLimit = 100

// backfillInterPageDelay paces successive getHistory pages WITHIN one chat's backfill.
// A from-scratch deep backfill of a long chat pages getHistory back-to-back (100 msgs
// each); across the breadth sweep's 25 chats/fire that burst is exactly what trips
// Telegram's account-global FLOOD_WAIT (#42). The sweep's 8s delay only spaces BETWEEN
// chats, not between pages — so pace the page loop too. ~1.5s keeps a multi-page chat
// well under the flood threshold while staying within the per-chat backfill timeout.
const backfillInterPageDelay = 1500 * time.Millisecond

// maxDialogPages caps dialog enumeration so a runaway never loops forever
// (100 pages * 100 = 10k dialogs, far above any real account).
const maxDialogPages = 200

// Backfill pulls history through the SAME normalize pipeline as live updates,
// emitting events tagged source=backfill.
//
//   - chatID == "" (or "all"): enumerate EVERY dialog (DMs, groups, channels)
//     via messages.getDialogs, then page back through each one's history.
//   - chatID set: backfill just that chat.
//
// `since` bounds how far back to go; a zero `since` pulls full history.
func (c *Connector) Backfill(ctx context.Context, chatID string, since time.Time, out chan<- event.Message) error {
	c.log("telegram: Backfill invoked chatID=%q since=%v", chatID, since)
	if err := c.initClient(); err != nil {
		c.log("telegram: Backfill initClient err: %v", err)
		return err
	}
	api, err := c.awaitReady(ctx)
	if err != nil {
		c.log("telegram: Backfill awaitReady err: %v", err)
		return err
	}

	if chatID == "" || chatID == "all" {
		return c.backfillAll(ctx, api, since, out)
	}

	peer, err := c.peers.resolve(chatID)
	if err != nil {
		return err
	}
	_, err = c.backfillChat(ctx, api, chatID, peer, since, out)
	return err
}

// backfillAll enumerates the full dialog list and backfills each dialog. A
// failure on one chat is logged and skipped so one bad peer can't abort the
// whole sync.
func (c *Connector) backfillAll(ctx context.Context, api *tg.Client, since time.Time, out chan<- event.Message) error {
	dialogs, err := c.enumerateDialogs(ctx, api)
	if err != nil {
		return err
	}
	c.log("telegram: backfill enumerated %d dialogs", len(dialogs))
	total := 0
	for _, d := range dialogs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, err := c.backfillChat(ctx, api, d.chatID, d.peer, since, out)
		if err != nil {
			c.log("telegram: backfill chat %s failed: %v", d.chatID, err)
			continue
		}
		total += n
		if n > 0 {
			c.log("telegram: backfill chat %s pulled %d", d.chatID, n)
		}
	}
	c.log("telegram: backfill complete — %d dialogs, %d messages total", len(dialogs), total)
	return nil
}

// dialogRef pairs a canonical chat id with its resolved input peer.
type dialogRef struct {
	chatID string
	peer   tg.InputPeerClass
}

// enumerateDialogs returns the full ordered dialog list across BOTH the main
// folder (0) and the archive folder (1), caching every peer's access hash. This
// is what makes "backfill all" cover the user's whole Telegram — including
// archived chats — not just chats seen live.
func (c *Connector) enumerateDialogs(ctx context.Context, api *tg.Client) ([]dialogRef, error) {
	var out []dialogRef
	seen := map[string]bool{}
	// Folder 0 = main inbox, folder 1 = archived. Enumerate both.
	for _, folder := range []int{0, 1} {
		refs, err := c.enumerateFolder(ctx, api, folder, seen)
		if err != nil {
			// Return what we have plus the error; the caller logs and proceeds.
			return append(out, refs...), err
		}
		out = append(out, refs...)
	}
	return out, nil
}

// enumerateFolder pages messages.getDialogs for a single folder to completion.
func (c *Connector) enumerateFolder(ctx context.Context, api *tg.Client, folder int, seen map[string]bool) ([]dialogRef, error) {
	var (
		out        []dialogRef
		offsetDate int
		offsetID   int
		offsetPeer tg.InputPeerClass = &tg.InputPeerEmpty{}
	)

	for page := 0; page < maxDialogPages; page++ {
		req := &tg.MessagesGetDialogsRequest{
			OffsetDate: offsetDate,
			OffsetID:   offsetID,
			OffsetPeer: offsetPeer,
			Limit:      dialogPageLimit,
		}
		if folder != 0 {
			req.SetFolderID(folder)
		}
		res, err := api.MessagesGetDialogs(ctx, req)
		if err != nil {
			return out, fmt.Errorf("telegram: getDialogs(folder=%d): %w", folder, err)
		}

		dialogs, messages, users, chats, isSlice := extractDialogs(res)
		if len(dialogs) == 0 {
			break
		}
		c.peers.learnFromClasses(users, chats)
		usersIdx := indexUsers(users)
		chatsIdx := indexChats(chats)
		chanIdx := indexChannels(chats)
		dateIdx := messageDateIndex(messages)

		var lastPeer tg.PeerClass
		var lastTop int
		for _, dc := range dialogs {
			d, ok := dc.(*tg.Dialog)
			if !ok {
				continue
			}
			cid := canonicalChatID(d.Peer)
			if ip := dialogInputPeer(d.Peer, usersIdx, chatsIdx, chanIdx); ip != nil {
				c.peers.put(cid, ip)
				if !seen[cid] {
					seen[cid] = true
					out = append(out, dialogRef{chatID: cid, peer: ip})
				}
			}
			lastPeer = d.Peer
			lastTop = d.TopMessage
		}

		// A non-slice result means the full list fit in one response → done.
		if !isSlice || len(dialogs) < dialogPageLimit {
			break
		}
		// Page from the last dialog: its top message's date/id + its peer.
		offsetID = lastTop
		offsetDate = dateIdx[peerKey(lastPeer)+":"+strconv.Itoa(lastTop)]
		if ip := dialogInputPeer(lastPeer, usersIdx, chatsIdx, chanIdx); ip != nil {
			offsetPeer = ip
		}
		if offsetDate == 0 {
			break // cannot advance the cursor reliably; stop rather than loop
		}
	}
	return out, nil
}

// backfillChat pages messages.getHistory for one chat from newest to oldest,
// stopping at `since` (or the start of history).
func (c *Connector) backfillChat(ctx context.Context, api *tg.Client, chatID string, peer tg.InputPeerClass, since time.Time, out chan<- event.Message) (int, error) {
	if peer == nil {
		c.log("telegram: backfillChat %s SKIPPED — nil peer", chatID)
		return 0, nil
	}
	offsetID := 0
	emitted := 0
	pages := 0
	for {
		if ctx.Err() != nil {
			return emitted, ctx.Err()
		}
		res, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:     peer,
			OffsetID: offsetID,
			Limit:    dialogPageLimit,
		})
		if err != nil {
			return emitted, fmt.Errorf("telegram: getHistory: %w", err)
		}
		msgs, users, chats := extractMessages(res)
		pages++
		if pages == 1 {
			c.log("telegram: backfillChat %s page1 getHistory returned %d raw msgs (type %T)", chatID, len(msgs), res)
		}
		if len(msgs) == 0 {
			return emitted, nil
		}
		c.peers.learnFromClasses(users, chats)
		ix := entityIndex{
			selfID:   c.selfID(),
			users:    indexUsers(users),
			chats:    indexChats(chats),
			channels: indexChannels(chats),
			cache:    c.peers, // #54: name fallback
		}

		reachedSince := false
		for _, mc := range msgs {
			m, ok := mc.(*tg.Message)
			if !ok {
				continue
			}
			ts := time.Unix(int64(m.Date), 0).UTC()
			if !since.IsZero() && ts.Before(since) {
				reachedSince = true
				continue
			}
			ev := mapMessage(m, ix, event.SourceBackfill, time.Now())
			select {
			case out <- ev:
				emitted++
			case <-ctx.Done():
				return emitted, ctx.Err()
			}
		}
		if reachedSince {
			return emitted, nil
		}
		// getHistory returns newest→oldest; the smallest id is the next offset.
		next := smallestID(msgs)
		if next == 0 || next == offsetID {
			return emitted, nil
		}
		offsetID = next
		// Pace before the next page so a deep multi-page backfill doesn't burst
		// getHistory into an account-global FLOOD_WAIT (#42). ctx-aware so a cancel /
		// per-chat-timeout returns promptly rather than sleeping out the delay.
		select {
		case <-ctx.Done():
			return emitted, ctx.Err()
		case <-time.After(backfillInterPageDelay):
		}
	}
}

// Contacts implements connectors.ContactsProvider — the user's full Telegram
// address book via contacts.getContacts, so everyone they know gets a named
// person even without message history. UserID matches event.Sender.Platform-
// UserID (the bare numeric user id).
func (c *Connector) Contacts(ctx context.Context) ([]connectors.Contact, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	api, err := c.awaitReady(ctx)
	if err != nil {
		return nil, err
	}
	res, err := api.ContactsGetContacts(ctx, 0)
	if err != nil {
		return nil, fmt.Errorf("telegram: getContacts: %w", err)
	}
	cc, ok := res.(*tg.ContactsContacts)
	if !ok { // ContactsContactsNotModified — nothing to map
		return nil, nil
	}
	out := make([]connectors.Contact, 0, len(cc.Users))
	for _, uc := range cc.Users {
		u, ok := uc.(*tg.User)
		if !ok {
			continue
		}
		ct := connectors.Contact{
			UserID:    strconv.FormatInt(u.ID, 10),
			Handle:    u.Username,
			FirstName: u.FirstName,
			LastName:  u.LastName,
			IsSelf:    u.Self,
		}
		if u.Phone != "" {
			ct.Phone = "+" + u.Phone // Telegram returns bare digits
		}
		out = append(out, ct)
	}
	return out, nil
}

// ---- Send / React ----------------------------------------------------------

// Send delivers an outbound message. Rich inline-keyboard sends (Buttons set)
// route through the @example_bot Bot API because a user account cannot send
// inline keyboards; everything else goes through the userbot. Both paths return
// a canonical sent id.
func (c *Connector) Send(ctx context.Context, req connectors.SendRequest) (string, error) {
	// Rich-UI path: inline buttons → bot identity.
	if req.Buttons != nil {
		if c.bot == nil {
			return "", errors.New("telegram: inline-keyboard send requested but no bot token (set SWITCHBOARD_TG_BOT_TOKEN)")
		}
		markup, err := buttonsToMarkup(req.Buttons)
		if err != nil {
			return "", err
		}
		chatID, err := botChatID(req.ChatID)
		if err != nil {
			return "", err
		}
		mid, err := c.bot.sendMessage(ctx, chatID, req.Text, replyToMsgID(req.ReplyTo), markup)
		if err != nil {
			return "", err
		}
		return botSentID(req.ChatID, mid), nil
	}

	// Normal path: userbot.
	api, err := c.awaitReady(ctx)
	if err != nil {
		return "", err
	}
	peer, err := c.peers.resolve(req.ChatID)
	if err != nil {
		return "", err
	}
	randomID, err := randInt64()
	if err != nil {
		return "", err
	}
	sendReq := &tg.MessagesSendMessageRequest{
		Peer:     peer,
		Message:  req.Text,
		RandomID: randomID,
	}
	if rid := replyToMsgID(req.ReplyTo); rid != 0 {
		sendReq.SetReplyTo(&tg.InputReplyToMessage{ReplyToMsgID: rid})
	}
	updResp, err := api.MessagesSendMessage(ctx, sendReq)
	if err != nil {
		return "", fmt.Errorf("telegram: sendMessage: %w", err)
	}
	if id, ok := sentMessageID(req.ChatID, updResp); ok {
		return id, nil
	}
	// Fall back to a deterministic-ish id if the update shape is unexpected.
	return "", nil
}

// React adds an emoji reaction to a message via the userbot.
func (c *Connector) React(ctx context.Context, chatID, msgID, emoji string) error {
	api, err := c.awaitReady(ctx)
	if err != nil {
		return err
	}
	peer, err := c.peers.resolve(chatID)
	if err != nil {
		return err
	}
	mid, err := localMsgID(msgID)
	if err != nil {
		return err
	}
	req := &tg.MessagesSendReactionRequest{Peer: peer, MsgID: mid}
	req.SetReaction([]tg.ReactionClass{&tg.ReactionEmoji{Emoticon: emoji}})
	if _, err := api.MessagesSendReaction(ctx, req); err != nil {
		return fmt.Errorf("telegram: sendReaction: %w", err)
	}
	return nil
}

// ---- Media download (#48) --------------------------------------------------

// DownloadMedia implements connectors.MediaDownloader: it fetches an attachment's
// bytes from the rich tgMediaRef recorded at ingest (id/access_hash/file_reference
// + photo size). The gotd userbot had NO downloader before — it only recorded a thin
// "telegram-photo:<id>" ref with no location, so every Telegram media row failed
// "no downloader or empty ref". file_references are volatile, so on an expired one we
// re-fetch the message once for a fresh reference and retry (backlog media fetched
// long after ingest would otherwise fail permanently).
func (c *Connector) DownloadMedia(ctx context.Context, ref string) (io.ReadCloser, int64, error) {
	r, ok := decodeTGMediaRef(ref)
	if !ok {
		return nil, 0, fmt.Errorf("telegram: legacy/empty media ref %q — re-backfill the chat to gain a downloadable ref", clip(ref, 48))
	}
	api, err := c.awaitReady(ctx)
	if err != nil {
		return nil, 0, err
	}
	data, derr := downloadLocation(ctx, api, r.location())
	if derr != nil && isFileRefExpired(derr) && r.Chat != "" && r.Msg != 0 {
		// Volatile file_reference expired — re-fetch the message for a fresh one, retry once.
		if fresh, ferr := c.refreshMediaRef(ctx, api, r); ferr == nil {
			data, derr = downloadLocation(ctx, api, fresh.location())
		}
	}
	if derr != nil {
		return nil, 0, fmt.Errorf("telegram download: %w", derr)
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

// downloadLocation streams a file location's bytes via gotd's downloader (which
// handles DC redirection + chunking).
func downloadLocation(ctx context.Context, api *tg.Client, loc tg.InputFileLocationClass) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := downloader.NewDownloader().Download(api, loc).Stream(ctx, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func isFileRefExpired(err error) bool {
	return strings.Contains(strings.ToUpper(err.Error()), "FILE_REFERENCE")
}

// refreshMediaRef re-fetches the source message to obtain a fresh file_reference for
// an expired ref: resolve the peer, pull the single message, re-extract its media ref.
func (c *Connector) refreshMediaRef(ctx context.Context, api *tg.Client, r tgMediaRef) (tgMediaRef, error) {
	peer, err := c.peers.resolve(r.Chat)
	if err != nil {
		return r, err
	}
	msg, err := fetchOneMessage(ctx, api, peer, r.Msg)
	if err != nil {
		return r, err
	}
	atts := attachmentsOf(msg)
	if len(atts) == 0 {
		return r, fmt.Errorf("telegram: refreshed message %d has no media", r.Msg)
	}
	fresh, ok := decodeTGMediaRef(atts[0].Ref)
	if !ok {
		return r, fmt.Errorf("telegram: refreshed ref undecodable")
	}
	return fresh, nil
}

// fetchOneMessage pulls a single message by id, routing channels through
// channels.getMessages (needs the InputChannel) and users/basic-chats through
// messages.getMessages.
func fetchOneMessage(ctx context.Context, api *tg.Client, peer tg.InputPeerClass, msgID int) (*tg.Message, error) {
	ids := []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}}
	var res tg.MessagesMessagesClass
	var err error
	if ch, ok := peer.(*tg.InputPeerChannel); ok {
		res, err = api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
			Channel: &tg.InputChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash},
			ID:      ids,
		})
	} else {
		res, err = api.MessagesGetMessages(ctx, ids)
	}
	if err != nil {
		return nil, err
	}
	var msgs []tg.MessageClass
	switch m := res.(type) {
	case *tg.MessagesMessages:
		msgs = m.Messages
	case *tg.MessagesMessagesSlice:
		msgs = m.Messages
	case *tg.MessagesChannelMessages:
		msgs = m.Messages
	}
	for _, mc := range msgs {
		if msg, ok := mc.(*tg.Message); ok && msg.ID == msgID {
			return msg, nil
		}
	}
	return nil, fmt.Errorf("telegram: message %d not found on refetch", msgID)
}

// clip truncates a string for safe inclusion in an error message.
func clip(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// TapButton presses an inline keyboard button by invoking
// messages.getBotCallbackAnswer with the button's callback data. buttonID is the
// base64 of the callback data, as emitted in the canonical UI.buttons payload.
func (c *Connector) TapButton(ctx context.Context, chatID, msgID, buttonID string) (string, error) {
	api, err := c.awaitReady(ctx)
	if err != nil {
		return "", err
	}
	peer, err := c.peers.resolve(chatID)
	if err != nil {
		return "", err
	}
	mid, err := localMsgID(msgID)
	if err != nil {
		return "", err
	}
	data, err := base64.StdEncoding.DecodeString(buttonID)
	if err != nil {
		return "", fmt.Errorf("telegram: button id is not base64 callback data: %w", err)
	}
	req := &tg.MessagesGetBotCallbackAnswerRequest{Peer: peer, MsgID: mid}
	req.SetData(data)
	ans, err := api.MessagesGetBotCallbackAnswer(ctx, req)
	if err != nil {
		return "", fmt.Errorf("telegram: getBotCallbackAnswer: %w", err)
	}
	return ans.Message, nil
}

// VotePoll casts a vote in a poll via messages.sendVote. option is the base64 of
// the poll option bytes, as emitted in the canonical UI.poll payload.
func (c *Connector) VotePoll(ctx context.Context, chatID, msgID, option string) error {
	api, err := c.awaitReady(ctx)
	if err != nil {
		return err
	}
	peer, err := c.peers.resolve(chatID)
	if err != nil {
		return err
	}
	mid, err := localMsgID(msgID)
	if err != nil {
		return err
	}
	opt, err := base64.StdEncoding.DecodeString(option)
	if err != nil {
		return fmt.Errorf("telegram: poll option is not base64: %w", err)
	}
	if _, err := api.MessagesSendVote(ctx, &tg.MessagesSendVoteRequest{
		Peer:    peer,
		MsgID:   mid,
		Options: [][]byte{opt},
	}); err != nil {
		return fmt.Errorf("telegram: sendVote: %w", err)
	}
	return nil
}

// ---- Auth (interactive, daemon-driven) -------------------------------------

// Login reports the auth state and the next step the daemon must drive. The
// actual interactive flow runs inside Connect; the daemon feeds values via
// SubmitPhone/SubmitCode/SubmitPassword. Login itself does not block.
func (c *Connector) Login(ctx context.Context) (connectors.LoginStatus, error) {
	if !c.cfg.HasCredentials() {
		return connectors.LoginStatus{
			NeedsAuth: true,
			Detail:    "set TG_APP_ID and TG_APP_HASH (from https://my.telegram.org) before login",
		}, ErrNoCredentials
	}
	return c.Status(ctx)
}

// Status reports current auth/connection state without driving login.
func (c *Connector) Status(_ context.Context) (connectors.LoginStatus, error) {
	c.mu.RLock()
	connected := c.api != nil
	detail := c.detail
	c.mu.RUnlock()

	st := connectors.LoginStatus{Connected: connected, Detail: detail}
	if !connected {
		// Needs auth unless a persisted session exists (then Connect re-uses it).
		st.NeedsAuth = !c.storage.Exists()
		if st.NeedsAuth && detail == "not connected" {
			st.Detail = "awaiting login: phone + code + optional 2FA"
		}
	}
	return st, nil
}

// SubmitPhone supplies the login phone number to an in-progress login flow.
func (c *Connector) SubmitPhone(ctx context.Context, phone string) error {
	return c.submit(ctx, c.phoneCh, phone)
}

// SubmitCode supplies the login code Telegram sent to the user.
func (c *Connector) SubmitCode(ctx context.Context, code string) error {
	return c.submit(ctx, c.codeCh, code)
}

// SubmitPassword supplies the 2FA password when the account has one.
func (c *Connector) SubmitPassword(ctx context.Context, password string) error {
	return c.submit(ctx, c.pwCh, password)
}

func (c *Connector) submit(ctx context.Context, ch chan string, v string) error {
	select {
	case ch <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SessionString returns the persisted portable MTProto session string (empty if
// not yet authenticated). Useful for backup/migration.
func (c *Connector) SessionString() string { return c.storage.String() }

// userAuthenticator adapts the connector's interactive Submit* plumbing to
// gotd's auth.UserAuthenticator. Each method blocks until the daemon supplies a
// value or ctx is cancelled.
type userAuthenticator struct{ c *Connector }

var _ auth.UserAuthenticator = userAuthenticator{}

func (a userAuthenticator) Phone(ctx context.Context) (string, error) {
	if a.c.cfg.Phone != "" {
		return a.c.cfg.Phone, nil
	}
	a.c.setPhase(AuthAwaitingPhone, "awaiting phone number")
	select {
	case p := <-a.c.phoneCh:
		return p, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (a userAuthenticator) Code(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
	a.c.setPhase(AuthAwaitingCode, "awaiting login code")
	select {
	case code := <-a.c.codeCh:
		return code, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (a userAuthenticator) Password(ctx context.Context) (string, error) {
	a.c.setPhase(AuthAwaiting2FA, "awaiting 2FA password")
	select {
	case pw := <-a.c.pwCh:
		return pw, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (a userAuthenticator) AcceptTermsOfService(context.Context, tg.HelpTermsOfService) error {
	return nil
}

func (a userAuthenticator) SignUp(context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, errors.New("telegram: account sign-up not supported; the account must already exist")
}

// ---- small helpers ---------------------------------------------------------

// replyToMsgID extracts the numeric Telegram message id from a canonical
// reply-to event id ("telegram:<kind>:<peer>:<msgid>"), or 0 if absent/invalid.
func replyToMsgID(canonicalID string) int {
	if canonicalID == "" {
		return 0
	}
	mid, err := localMsgID(canonicalID)
	if err != nil {
		return 0
	}
	return mid
}

// localMsgID pulls the trailing Telegram message id out of a canonical id. It
// accepts the full id "telegram:user:123:456" or a bare "456".
func localMsgID(id string) (int, error) {
	// The Telegram message id is the final ":"-separated segment.
	last := id
	for i := len(id) - 1; i >= 0; i-- {
		if id[i] == ':' {
			last = id[i+1:]
			break
		}
	}
	n, err := strconv.Atoi(last)
	if err != nil {
		return 0, fmt.Errorf("telegram: cannot parse message id from %q: %w", id, err)
	}
	return n, nil
}

func randInt64() (int64, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(0).SetUint64(^uint64(0)>>1))
	if err != nil {
		return 0, err
	}
	return n.Int64(), nil
}

// sentMessageID best-effort extracts the new message id from a send response.
func sentMessageID(chatID string, u tg.UpdatesClass) (string, bool) {
	var found int
	extract := func(updates []tg.UpdateClass) {
		for _, up := range updates {
			switch v := up.(type) {
			case *tg.UpdateMessageID:
				found = v.ID
			case *tg.UpdateNewMessage:
				if m, ok := v.Message.(*tg.Message); ok && found == 0 {
					found = m.ID
				}
			case *tg.UpdateNewChannelMessage:
				if m, ok := v.Message.(*tg.Message); ok && found == 0 {
					found = m.ID
				}
			}
		}
	}
	switch up := u.(type) {
	case *tg.Updates:
		extract(up.Updates)
	case *tg.UpdatesCombined:
		extract(up.Updates)
	case *tg.UpdateShortSentMessage:
		found = up.ID
	}
	if found == 0 {
		return "", false
	}
	kind, id, err := parseChatID(chatID)
	if err != nil {
		return "", false
	}
	return "telegram:" + kind + ":" + strconv.FormatInt(id, 10) + ":" + strconv.Itoa(found), true
}

// extractMessages pulls messages + entity lists from a messages.getHistory result.
func extractMessages(res tg.MessagesMessagesClass) (msgs []tg.MessageClass, users []tg.UserClass, chats []tg.ChatClass) {
	if m, ok := res.AsModified(); ok {
		return m.GetMessages(), m.GetUsers(), m.GetChats()
	}
	return nil, nil, nil
}

// extractDialogs pulls dialogs + entities from a messages.getDialogs result.
// isSlice reports whether the result was paginated (more pages may follow).
func extractDialogs(res tg.MessagesDialogsClass) (dialogs []tg.DialogClass, messages []tg.MessageClass, users []tg.UserClass, chats []tg.ChatClass, isSlice bool) {
	switch v := res.(type) {
	case *tg.MessagesDialogs:
		return v.Dialogs, v.Messages, v.Users, v.Chats, false
	case *tg.MessagesDialogsSlice:
		return v.Dialogs, v.Messages, v.Users, v.Chats, true
	default:
		return nil, nil, nil, nil, false
	}
}

// dialogInputPeer builds the InputPeer (with access hash) for a dialog peer from
// the entity maps returned alongside the dialog list.
func dialogInputPeer(peer tg.PeerClass, users map[int64]*tg.User, chats map[int64]*tg.Chat, channels map[int64]*tg.Channel) tg.InputPeerClass {
	switch v := peer.(type) {
	case *tg.PeerUser:
		if u, ok := users[v.UserID]; ok {
			return u.AsInputPeer()
		}
	case *tg.PeerChat:
		return &tg.InputPeerChat{ChatID: v.ChatID}
	case *tg.PeerChannel:
		if ch, ok := channels[v.ChannelID]; ok {
			return ch.AsInputPeer()
		}
	}
	return nil
}

// messageDateIndex maps "<peerKey>:<msgID>" → unix date for the messages in a
// dialog response, so dialog pagination can read a dialog's top-message date.
func messageDateIndex(messages []tg.MessageClass) map[string]int {
	out := make(map[string]int, len(messages))
	for _, mc := range messages {
		switch m := mc.(type) {
		case *tg.Message:
			out[peerKey(m.PeerID)+":"+strconv.Itoa(m.ID)] = m.Date
		case *tg.MessageService:
			out[peerKey(m.PeerID)+":"+strconv.Itoa(m.ID)] = m.Date
		}
	}
	return out
}

func indexUsers(users []tg.UserClass) map[int64]*tg.User {
	out := make(map[int64]*tg.User, len(users))
	for _, uc := range users {
		if u, ok := uc.(*tg.User); ok {
			out[u.ID] = u
		}
	}
	return out
}

func indexChats(chats []tg.ChatClass) map[int64]*tg.Chat {
	out := make(map[int64]*tg.Chat)
	for _, cc := range chats {
		if c, ok := cc.(*tg.Chat); ok {
			out[c.ID] = c
		}
	}
	return out
}

func indexChannels(chats []tg.ChatClass) map[int64]*tg.Channel {
	out := make(map[int64]*tg.Channel)
	for _, cc := range chats {
		if c, ok := cc.(*tg.Channel); ok {
			out[c.ID] = c
		}
	}
	return out
}

func smallestID(msgs []tg.MessageClass) int {
	smallest := 0
	for _, mc := range msgs {
		if m, ok := mc.(*tg.Message); ok {
			if smallest == 0 || m.ID < smallest {
				smallest = m.ID
			}
		}
	}
	return smallest
}
