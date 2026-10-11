package lark

import (
	"context"
	"fmt"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larksearch "github.com/larksuite/oapi-sdk-go/v3/service/search/v2"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// allChats returns the union of group chats (from Im.Chat.List) and p2p chats
// discovered via message search (#31), deduped by raw id. This is the chat set the
// poll loop and full backfill iterate — so the user's DMs, which Im.Chat.List omits
// entirely, are covered once discovered.
func (c *Connector) allChats(ctx context.Context, opt larkcore.RequestOptionFunc) ([]chatRef, error) {
	chats, err := c.listChats(ctx, opt)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(chats))
	for _, ch := range chats {
		seen[ch.id] = true
	}
	c.mu.Lock()
	for id, ch := range c.discovered {
		if !seen[id] {
			chats = append(chats, ch)
			seen[id] = true
		}
	}
	c.mu.Unlock()
	return chats, nil
}

// registerDiscovered records a p2p chat found by discovery (or a live p2p event) so
// the poll loop + full backfill cover it. Returns true if it was newly added (a chat
// already known via the chat list or a prior discovery is a no-op). chatID is raw
// (no platform prefix).
func (c *Connector) registerDiscovered(chatID, p2pTargetID string) bool {
	if chatID == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.discovered[chatID]; ok {
		return false
	}
	c.discovered[chatID] = chatRef{id: chatID, chatType: event.ChatDM, p2pTargetID: p2pTargetID}
	return true
}

// backfillDiscovered pulls a newly-discovered p2p chat's full history once, emitting
// to out (#31). Used when a DM is first seen live: the live event captures only that
// one message, but the chat was previously invisible, so its backbacklog has never
// been ingested. Newest-first (listMessages), best-effort, runs in its own goroutine.
func (c *Connector) backfillDiscovered(ctx context.Context, rawChatID string, out chan<- event.Message) {
	opt, err := c.userOpt(ctx)
	if err != nil {
		return
	}
	self := c.tokens.selfOpenID()
	_ = c.listMessages(ctx, opt, rawChatID, event.ChatDM, time.Time{}, event.SourceBackfill, self,
		func(m event.Message) { c.emit(ctx, out, m) })
}

// maxResolvePerRun bounds the Im.Message.Get fan-out across one discovery run, so a
// broad query can't trigger thousands of per-message lookups (the hang guard, #31).
const maxResolvePerRun = 400

// DiscoverP2PChats finds the user's p2p (DM) chats — which Im.Chat.List omits — via
// the message-search API (POST /open-apis/search/v2/message, user token), so they
// become backfillable + pollable (#31). search/v2/message REQUIRES a non-empty query
// (an empty/absent one is the 99992402 "field validation failed"), so the caller
// passes a set of common-word queries; this unions p2p message hits across them,
// narrowed by sender (fromOpenIDs) + chat_type=p2p_chat, resolves each hit to its
// chat id (Im.Message.Get), and registers the distinct p2p chats. Bounded by a per-run
// resolve cap. Returns the newly-registered raw chat ids; best-effort on errors.
func (c *Connector) DiscoverP2PChats(ctx context.Context, fromOpenIDs, queries []string) ([]string, error) {
	opt, err := c.userOpt(ctx)
	if err != nil {
		return nil, err
	}
	seenMsg := map[string]bool{}
	seenChat := map[string]bool{}
	var newChats []string
	resolved := 0
	for _, q := range queries {
		if q == "" {
			continue // query is mandatory — skip empties rather than 99992402
		}
		msgIDs, serr := c.searchP2PMessageIDs(ctx, opt, fromOpenIDs, q)
		if serr != nil {
			return newChats, serr
		}
		for _, mid := range msgIDs {
			if seenMsg[mid] {
				continue
			}
			seenMsg[mid] = true
			if resolved >= maxResolvePerRun {
				return newChats, nil // hit the per-run cap — stop early, best-effort
			}
			resolved++
			chatID, target, ok := c.resolveMessageChat(ctx, opt, mid)
			if !ok || seenChat[chatID] {
				continue
			}
			seenChat[chatID] = true
			if c.registerDiscovered(chatID, target) {
				newChats = append(newChats, chatID)
			}
		}
	}
	return newChats, nil
}

// searchP2PMessageIDs pages POST /open-apis/search/v2/message for p2p-chat messages,
// returning the matched message ids. fromOpenIDs (open_id sender filter) and query
// (keyword) are both optional — Lark accepts a sender-only search, which lets us
// enumerate a known person's DM without a keyword. Caps pages defensively.
func (c *Connector) searchP2PMessageIDs(ctx context.Context, opt larkcore.RequestOptionFunc, fromOpenIDs []string, query string) ([]string, error) {
	var ids []string
	pageToken := ""
	for range 6 { // per-query page cap — discovery iterates many queries, so keep each shallow
		bodyB := larksearch.NewCreateMessageReqBodyBuilder().ChatType("p2p_chat")
		if query != "" {
			bodyB = bodyB.Query(query)
		}
		if len(fromOpenIDs) > 0 {
			bodyB = bodyB.FromIds(fromOpenIDs)
		}
		rb := larksearch.NewCreateMessageReqBuilder().
			UserIdType("open_id").
			PageSize(50).
			Body(bodyB.Build())
		if pageToken != "" {
			rb = rb.PageToken(pageToken)
		}
		resp, err := c.client.Search.Message.Create(ctx, rb.Build(), opt)
		if err != nil {
			return ids, err
		}
		if !resp.Success() {
			return ids, fmt.Errorf("lark search message: code=%d msg=%s", resp.Code, resp.Msg)
		}
		if resp.Data != nil {
			ids = append(ids, resp.Data.Items...)
		}
		if resp.Data == nil || resp.Data.HasMore == nil || !*resp.Data.HasMore ||
			resp.Data.PageToken == nil || *resp.Data.PageToken == "" {
			break
		}
		pageToken = *resp.Data.PageToken
	}
	return ids, nil
}

// resolveMessageChat resolves a message id to its (chat_id, p2p counterpart open_id).
// The counterpart is the message's sender when it is NOT self — for a p2p chat that
// IS the other party, which feeds the chat's display-name enrich. Returns ok=false on
// any miss so discovery simply skips that id.
func (c *Connector) resolveMessageChat(ctx context.Context, opt larkcore.RequestOptionFunc, msgID string) (chatID, p2pTarget string, ok bool) {
	if msgID == "" {
		return "", "", false
	}
	req := larkim.NewGetMessageReqBuilder().MessageId(msgID).Build()
	resp, err := c.client.Im.Message.Get(ctx, req, opt)
	if err != nil || !resp.Success() || resp.Data == nil || len(resp.Data.Items) == 0 {
		return "", "", false
	}
	msg := resp.Data.Items[0]
	chatID = deref(msg.ChatId)
	if chatID == "" {
		return "", "", false
	}
	self := c.tokens.selfOpenID()
	if msg.Sender != nil {
		if sid := deref(msg.Sender.Id); sid != "" && sid != self {
			p2pTarget = sid
		}
	}
	return chatID, p2pTarget, true
}
