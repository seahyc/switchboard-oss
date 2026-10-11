package lark

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// platform constant + canonical id helpers --------------------------------

// chatTypeFor maps Lark chat semantics to the canonical chat_type enum.
// Lark message events use chat_type p2p|group|topic_group; the chat list uses
// chat_mode p2p|group|topic. Both collapse to the same canonical set.
func chatTypeFor(larkChatType string) string {
	switch larkChatType {
	case "p2p":
		return event.ChatDM
	case "group", "topic_group", "topic":
		return event.ChatGroup
	default:
		return event.ChatGroup
	}
}

// canonChatID / canonMsgID wrap raw Lark ids with the platform prefix.
func canonChatID(chatID string) string { return event.PlatformLark + ":" + chatID }
func canonMsgID(msgID string) string   { return event.MakeID(event.PlatformLark, msgID) }

// parseMillis converts a Lark millisecond-epoch string to a UTC time.
func parseMillis(ms string) time.Time {
	if ms == "" {
		return time.Time{}
	}
	n, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(n).UTC()
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// msgCore is the platform-neutral subset shared by live message events and
// backfilled (listed) messages, so both paths normalize through one builder.
type msgCore struct {
	msgID        string
	chatID       string
	chatType     string // canonical (already mapped)
	msgType      string
	content      string
	createMillis string
	updateMillis string
	rootID       string
	parentID     string
	threadID     string
	senderOpenID string
	mentions     []string // canonical: platform user ids (open_id)
	deleted      bool
	updated      bool
}

// build assembles a canonical event.Message from the shared core fields.
func (c msgCore) build(selfOpenID, source string, now time.Time, raw json.RawMessage) event.Message {
	parsed := parseContent(c.msgType, c.content)
	// #48: upgrade bare attachment keys to the rich downloadable ref (msg_id + key +
	// resource type) now that we have the message id — so the media worker can fetch them.
	enrichMediaRefs(c.msgID, parsed.Attachments)

	m := event.Message{
		ID:        canonMsgID(c.msgID),
		Platform:  event.PlatformLark,
		AccountID: event.PlatformLark + ":" + selfOpenID,
		ChatID:    canonChatID(c.chatID),
		ChatType:  c.chatType,
		Sender: event.Sender{
			PlatformUserID: c.senderOpenID,
			IsSelf:         selfOpenID != "" && c.senderOpenID == selfOpenID,
		},
		TS:          parseMillis(c.createMillis),
		Kind:        kindForMsgType(c.msgType),
		Text:        parsed.Text,
		ThreadID:    c.threadID,
		Mentions:    c.mentions,
		Attachments: parsed.Attachments,
		Deleted:     c.deleted,
		Source:      source,
		IngestedAt:  now,
		Raw:         raw,
	}

	// reply_to: Lark uses parent_id for direct replies, root_id for the thread
	// root. Prefer the direct parent; fall back to the root.
	if c.parentID != "" {
		m.ReplyTo = canonMsgID(c.parentID)
	} else if c.rootID != "" {
		m.ReplyTo = canonMsgID(c.rootID)
	}

	// Guard: a json.RawMessage MUST hold valid JSON or event marshaling fails on
	// ingest. parseContent already vets this, but double-check at the seam.
	if parsed.Card != nil && json.Valid(parsed.Card) {
		m.UI.Card = parsed.Card
	}

	// An edited message: surface edited_ts from update_time.
	if c.updated && c.updateMillis != "" {
		ts := parseMillis(c.updateMillis)
		m.EditedTS = &ts
	}

	return m
}

// mapMessageEvent normalizes a live im.message.receive_v1 websocket event.
func mapMessageEvent(ev *larkim.P2MessageReceiveV1, selfOpenID string, now time.Time) (event.Message, bool) {
	if ev == nil || ev.Event == nil || ev.Event.Message == nil {
		return event.Message{}, false
	}
	msg := ev.Event.Message

	core := msgCore{
		msgID:        deref(msg.MessageId),
		chatID:       deref(msg.ChatId),
		chatType:     chatTypeFor(deref(msg.ChatType)),
		msgType:      deref(msg.MessageType),
		content:      deref(msg.Content),
		createMillis: deref(msg.CreateTime),
		updateMillis: deref(msg.UpdateTime),
		rootID:       deref(msg.RootId),
		parentID:     deref(msg.ParentId),
		threadID:     deref(msg.ThreadId),
	}
	if ev.Event.Sender != nil && ev.Event.Sender.SenderId != nil {
		core.senderOpenID = deref(ev.Event.Sender.SenderId.OpenId)
	}
	for _, mt := range msg.Mentions {
		if mt != nil && mt.Id != nil {
			if oid := deref(mt.Id.OpenId); oid != "" {
				core.mentions = append(core.mentions, oid)
			}
		}
	}

	raw, _ := json.Marshal(ev.Event)
	return core.build(selfOpenID, event.SourceLive, now, raw), true
}

// mapListedMessage normalizes a message returned by im/v1/messages list during
// backfill. chatType must be supplied by the caller (the List Message struct
// does not carry chat_type — the backfiller knows it from the chat it lists).
func mapListedMessage(msg *larkim.Message, chatType, selfOpenID string, now time.Time) (event.Message, bool) {
	if msg == nil {
		return event.Message{}, false
	}
	core := msgCore{
		msgID:        deref(msg.MessageId),
		chatID:       deref(msg.ChatId),
		chatType:     chatType,
		msgType:      deref(msg.MsgType),
		createMillis: deref(msg.CreateTime),
		updateMillis: deref(msg.UpdateTime),
		rootID:       deref(msg.RootId),
		parentID:     deref(msg.ParentId),
		threadID:     deref(msg.ThreadId),
		deleted:      msg.Deleted != nil && *msg.Deleted,
		updated:      msg.Updated != nil && *msg.Updated,
	}
	if msg.Body != nil {
		core.content = deref(msg.Body.Content)
	}
	if msg.Sender != nil {
		core.senderOpenID = deref(msg.Sender.Id)
	}
	for _, mt := range msg.Mentions {
		if mt != nil {
			if oid := deref(mt.Id); oid != "" {
				core.mentions = append(core.mentions, oid)
			}
		}
	}

	raw, _ := json.Marshal(msg)
	return core.build(selfOpenID, event.SourceBackfill, now, raw), true
}

// mapReactionCreated normalizes an im.message.reaction.created_v1 event into a
// canonical reaction event. The reaction payload carries no chat_id, so ChatID
// is left empty; ReplyTo links to the target message for the projector to join.
func mapReactionCreated(ev *larkim.P2MessageReactionCreatedV1, selfOpenID string, now time.Time) (event.Message, bool) {
	if ev == nil || ev.Event == nil {
		return event.Message{}, false
	}
	d := ev.Event
	msgID := deref(d.MessageId)
	emoji := ""
	if d.ReactionType != nil {
		emoji = deref(d.ReactionType.EmojiType)
	}
	by := ""
	if d.UserId != nil {
		by = deref(d.UserId.OpenId)
	}
	actionTime := deref(d.ActionTime)

	raw, _ := json.Marshal(d)
	m := event.Message{
		// Synthetic, idempotent id: a reaction has no message id of its own.
		ID:        event.MakeID(event.PlatformLark, msgID+":rx:"+by+":"+emoji+":"+actionTime),
		Platform:  event.PlatformLark,
		AccountID: event.PlatformLark + ":" + selfOpenID,
		ChatType:  event.ChatGroup,
		Sender: event.Sender{
			PlatformUserID: by,
			IsSelf:         selfOpenID != "" && by == selfOpenID,
		},
		TS:         parseMillis(actionTime),
		Kind:       event.KindReaction,
		ReplyTo:    canonMsgID(msgID), // the message that was reacted to
		Reactions:  []event.Reaction{{Emoji: emoji, By: by}},
		Source:     event.SourceLive,
		IngestedAt: now,
		Raw:        raw,
	}
	return m, true
}

// mapRecalled normalizes an im.message.recalled_v1 event into a delete event.
func mapRecalled(ev *larkim.P2MessageRecalledV1, selfOpenID string, now time.Time) (event.Message, bool) {
	if ev == nil || ev.Event == nil {
		return event.Message{}, false
	}
	d := ev.Event
	raw, _ := json.Marshal(d)
	m := event.Message{
		ID:         canonMsgID(deref(d.MessageId)),
		Platform:   event.PlatformLark,
		AccountID:  event.PlatformLark + ":" + selfOpenID,
		ChatID:     canonChatID(deref(d.ChatId)),
		ChatType:   event.ChatGroup,
		TS:         parseMillis(deref(d.RecallTime)),
		Kind:       event.KindDelete,
		Deleted:    true,
		Source:     event.SourceLive,
		IngestedAt: now,
		Raw:        raw,
	}
	return m, true
}

// mapCardAction normalizes an interactive card / button action (card.action.trigger)
// into a canonical card event. This is Lark's equivalent of an inline-button tap.
func mapCardAction(ev *callback.CardActionTriggerEvent, selfOpenID string, now time.Time) (event.Message, bool) {
	if ev == nil || ev.Event == nil {
		return event.Message{}, false
	}
	d := ev.Event
	var by, msgID, chatID string
	if d.Operator != nil {
		by = d.Operator.OpenID
	}
	if d.Context != nil {
		msgID = d.Context.OpenMessageID
		chatID = d.Context.OpenChatID
	}

	raw, _ := json.Marshal(d)
	m := event.Message{
		ID:        event.MakeID(event.PlatformLark, msgID+":action:"+by),
		Platform:  event.PlatformLark,
		AccountID: event.PlatformLark + ":" + selfOpenID,
		ChatID:    canonChatID(chatID),
		ChatType:  event.ChatGroup,
		Sender: event.Sender{
			PlatformUserID: by,
			IsSelf:         selfOpenID != "" && by == selfOpenID,
		},
		TS:         now,
		Kind:       event.KindCard,
		ReplyTo:    canonMsgID(msgID),
		Source:     event.SourceLive,
		IngestedAt: now,
		Raw:        raw,
	}
	// Preserve the action payload (button value/name/form input) under ui.buttons
	// so the agent can see exactly which control fired.
	if d.Action != nil {
		if b, err := json.Marshal(d.Action); err == nil {
			m.UI.Buttons = []json.RawMessage{b}
		}
	}
	return m, true
}
