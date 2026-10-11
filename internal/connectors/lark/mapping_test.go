package lark

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/seahyc/switchboard-oss/internal/event"
)

func ptr(s string) *string { return &s }

// fixed clock + identity used across mapping tests.
var (
	testNow  = time.Date(2026, 6, 9, 8, 0, 1, 0, time.UTC)
	selfOpen = "ou_self"
)

func TestLarkReceiveIDType(t *testing.T) {
	cases := map[string]string{
		"ou_ce4cc5bec5ed06fdc72fea5af0fb6d6e": "open_id", // person → p2p DM (#35)
		"oc_3c2b5daa39abf08f41d9784aaffe9c1e": "chat_id", // a chat
		"on_abc123":                           "union_id",
		"alice@example.com":                    "email",
		"7300123456789":                       "chat_id", // unprefixed → prior default
		"":                                    "chat_id",
	}
	for id, want := range cases {
		if got := larkReceiveIDType(id); got != want {
			t.Errorf("larkReceiveIDType(%q) = %q, want %q", id, got, want)
		}
	}
}

func TestChatTypeFor(t *testing.T) {
	cases := map[string]string{
		"p2p":         event.ChatDM,
		"group":       event.ChatGroup,
		"topic_group": event.ChatGroup,
		"topic":       event.ChatGroup,
		"weird":       event.ChatGroup,
	}
	for in, want := range cases {
		if got := chatTypeFor(in); got != want {
			t.Errorf("chatTypeFor(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestKindForMsgType(t *testing.T) {
	cases := map[string]string{
		"text":        event.KindText,
		"post":        event.KindText,
		"image":       event.KindMedia,
		"file":        event.KindMedia,
		"audio":       event.KindMedia,
		"media":       event.KindMedia,
		"sticker":     event.KindSticker,
		"interactive": event.KindCard,
		"system":      event.KindSystem,
		"share_chat":  event.KindSystem,
	}
	for in, want := range cases {
		if got := kindForMsgType(in); got != want {
			t.Errorf("kindForMsgType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseMillis(t *testing.T) {
	got := parseMillis("1749456001000")
	want := time.UnixMilli(1749456001000).UTC()
	if !got.Equal(want) {
		t.Errorf("parseMillis = %v, want %v", got, want)
	}
	if !parseMillis("").IsZero() {
		t.Errorf("parseMillis(\"\") should be zero")
	}
	if !parseMillis("notanumber").IsZero() {
		t.Errorf("parseMillis(bad) should be zero")
	}
}

func TestParseContent(t *testing.T) {
	t.Run("text", func(t *testing.T) {
		p := parseContent("text", `{"text":"hello world"}`)
		if p.Text != "hello world" {
			t.Errorf("text = %q", p.Text)
		}
	})
	t.Run("post rich", func(t *testing.T) {
		raw := `{"title":"T","content":[[{"tag":"text","text":"hi "},{"tag":"at","user_name":"Bob","user_id":"ou_b"},{"tag":"a","text":"link","href":"http://x"}]]}`
		p := parseContent("post", raw)
		want := "T\nhi @Boblink"
		if p.Text != want {
			t.Errorf("post text = %q, want %q", p.Text, want)
		}
	})
	t.Run("image", func(t *testing.T) {
		p := parseContent("image", `{"image_key":"img_v2_abc"}`)
		if len(p.Attachments) != 1 || p.Attachments[0].Ref != "img_v2_abc" || p.Attachments[0].Type != "image" {
			t.Errorf("image attachment = %+v", p.Attachments)
		}
	})
	t.Run("file", func(t *testing.T) {
		p := parseContent("file", `{"file_key":"file_xyz","file_name":"a.pdf"}`)
		if len(p.Attachments) != 1 || p.Attachments[0].Name != "a.pdf" || p.Attachments[0].Ref != "file_xyz" {
			t.Errorf("file attachment = %+v", p.Attachments)
		}
	})
	t.Run("interactive card", func(t *testing.T) {
		raw := `{"header":{"title":{"content":"Approve?"}},"elements":[]}`
		p := parseContent("interactive", raw)
		if p.Text != "Approve?" {
			t.Errorf("card title = %q", p.Text)
		}
		if string(p.Card) != raw {
			t.Errorf("card raw not preserved: %s", p.Card)
		}
	})
	t.Run("malformed falls back to raw text", func(t *testing.T) {
		p := parseContent("text", `{not json`)
		if p.Text != `{not json` {
			t.Errorf("fallback text = %q", p.Text)
		}
	})
}

func TestMapMessageEvent(t *testing.T) {
	ev := &larkim.P2MessageReceiveV1{
		Event: &larkim.P2MessageReceiveV1Data{
			Sender: &larkim.EventSender{
				SenderId: &larkim.UserId{OpenId: ptr("ou_bob")},
			},
			Message: &larkim.EventMessage{
				MessageId:   ptr("om_123"),
				ChatId:      ptr("oc_chat"),
				ChatType:    ptr("group"),
				MessageType: ptr("text"),
				Content:     ptr(`{"text":"hey @me"}`),
				CreateTime:  ptr("1749456000000"),
				ParentId:    ptr("om_parent"),
				ThreadId:    ptr("omt_thread"),
				Mentions: []*larkim.MentionEvent{
					{Id: &larkim.UserId{OpenId: ptr("ou_self")}},
				},
			},
		},
	}
	m, ok := mapMessageEvent(ev, selfOpen, testNow)
	if !ok {
		t.Fatal("mapMessageEvent returned !ok")
	}
	if m.ID != "lark:om_123" {
		t.Errorf("ID = %q", m.ID)
	}
	if m.Platform != event.PlatformLark {
		t.Errorf("Platform = %q", m.Platform)
	}
	if m.AccountID != "lark:ou_self" {
		t.Errorf("AccountID = %q", m.AccountID)
	}
	if m.ChatID != "lark:oc_chat" {
		t.Errorf("ChatID = %q", m.ChatID)
	}
	if m.ChatType != event.ChatGroup {
		t.Errorf("ChatType = %q", m.ChatType)
	}
	if m.Sender.PlatformUserID != "ou_bob" || m.Sender.IsSelf {
		t.Errorf("Sender = %+v", m.Sender)
	}
	if m.Kind != event.KindText || m.Text != "hey @me" {
		t.Errorf("Kind/Text = %q/%q", m.Kind, m.Text)
	}
	if m.ReplyTo != "lark:om_parent" {
		t.Errorf("ReplyTo = %q", m.ReplyTo)
	}
	if m.ThreadID != "omt_thread" {
		t.Errorf("ThreadID = %q", m.ThreadID)
	}
	if len(m.Mentions) != 1 || m.Mentions[0] != "ou_self" {
		t.Errorf("Mentions = %v", m.Mentions)
	}
	if !m.TS.Equal(time.UnixMilli(1749456000000).UTC()) {
		t.Errorf("TS = %v", m.TS)
	}
	if m.Source != event.SourceLive || !m.IngestedAt.Equal(testNow) {
		t.Errorf("Source/IngestedAt = %q/%v", m.Source, m.IngestedAt)
	}
	if len(m.Raw) == 0 {
		t.Error("Raw should be populated")
	}
}

func TestMapMessageEventSelfSender(t *testing.T) {
	ev := &larkim.P2MessageReceiveV1{
		Event: &larkim.P2MessageReceiveV1Data{
			Sender:  &larkim.EventSender{SenderId: &larkim.UserId{OpenId: ptr("ou_self")}},
			Message: &larkim.EventMessage{MessageId: ptr("om_1"), ChatType: ptr("p2p"), MessageType: ptr("text"), Content: ptr(`{"text":"hi"}`), CreateTime: ptr("1749456000000")},
		},
	}
	m, _ := mapMessageEvent(ev, selfOpen, testNow)
	if !m.Sender.IsSelf {
		t.Error("expected IsSelf true for own message")
	}
	if m.ChatType != event.ChatDM {
		t.Errorf("p2p should map to dm, got %q", m.ChatType)
	}
}

func TestMapMessageEventNilSafe(t *testing.T) {
	if _, ok := mapMessageEvent(nil, selfOpen, testNow); ok {
		t.Error("nil event should return !ok")
	}
	if _, ok := mapMessageEvent(&larkim.P2MessageReceiveV1{}, selfOpen, testNow); ok {
		t.Error("empty event should return !ok")
	}
}

func TestMapListedMessage(t *testing.T) {
	deleted := false
	updated := true
	msg := &larkim.Message{
		MessageId:  ptr("om_999"),
		ChatId:     ptr("oc_chat"),
		MsgType:    ptr("text"),
		CreateTime: ptr("1749456000000"),
		UpdateTime: ptr("1749456900000"),
		RootId:     ptr("om_root"),
		Deleted:    &deleted,
		Updated:    &updated,
		Sender:     &larkim.Sender{Id: ptr("ou_alice"), IdType: ptr("open_id")},
		Body:       &larkim.MessageBody{Content: ptr(`{"text":"backfilled"}`)},
		Mentions:   []*larkim.Mention{{Id: ptr("ou_self")}},
	}
	m, ok := mapListedMessage(msg, event.ChatGroup, selfOpen, testNow)
	if !ok {
		t.Fatal("!ok")
	}
	if m.ID != "lark:om_999" || m.Source != event.SourceBackfill {
		t.Errorf("ID/Source = %q/%q", m.ID, m.Source)
	}
	if m.Text != "backfilled" {
		t.Errorf("Text = %q", m.Text)
	}
	if m.ReplyTo != "lark:om_root" {
		t.Errorf("ReplyTo should fall back to root, got %q", m.ReplyTo)
	}
	if m.Sender.PlatformUserID != "ou_alice" {
		t.Errorf("Sender = %+v", m.Sender)
	}
	if m.EditedTS == nil || !m.EditedTS.Equal(time.UnixMilli(1749456900000).UTC()) {
		t.Errorf("EditedTS = %v", m.EditedTS)
	}
	if len(m.Mentions) != 1 || m.Mentions[0] != "ou_self" {
		t.Errorf("Mentions = %v", m.Mentions)
	}
}

func TestMapReactionCreated(t *testing.T) {
	ev := &larkim.P2MessageReactionCreatedV1{
		Event: &larkim.P2MessageReactionCreatedV1Data{
			MessageId:    ptr("om_target"),
			ReactionType: &larkim.Emoji{EmojiType: ptr("THUMBSUP")},
			UserId:       &larkim.UserId{OpenId: ptr("ou_bob")},
			ActionTime:   ptr("1749456000000"),
		},
	}
	m, ok := mapReactionCreated(ev, selfOpen, testNow)
	if !ok {
		t.Fatal("!ok")
	}
	if m.Kind != event.KindReaction {
		t.Errorf("Kind = %q", m.Kind)
	}
	if m.ReplyTo != "lark:om_target" {
		t.Errorf("ReplyTo = %q", m.ReplyTo)
	}
	if len(m.Reactions) != 1 || m.Reactions[0].Emoji != "THUMBSUP" || m.Reactions[0].By != "ou_bob" {
		t.Errorf("Reactions = %+v", m.Reactions)
	}
	if m.ID != "lark:om_target:rx:ou_bob:THUMBSUP:1749456000000" {
		t.Errorf("synthetic ID = %q", m.ID)
	}
}

func TestMapRecalled(t *testing.T) {
	ev := &larkim.P2MessageRecalledV1{
		Event: &larkim.P2MessageRecalledV1Data{
			MessageId:  ptr("om_gone"),
			ChatId:     ptr("oc_chat"),
			RecallTime: ptr("1749456000000"),
		},
	}
	m, ok := mapRecalled(ev, selfOpen, testNow)
	if !ok {
		t.Fatal("!ok")
	}
	if m.Kind != event.KindDelete || !m.Deleted {
		t.Errorf("Kind/Deleted = %q/%v", m.Kind, m.Deleted)
	}
	if m.ID != "lark:om_gone" || m.ChatID != "lark:oc_chat" {
		t.Errorf("ID/ChatID = %q/%q", m.ID, m.ChatID)
	}
}

func TestMapCardAction(t *testing.T) {
	ev := &callback.CardActionTriggerEvent{
		Event: &callback.CardActionTriggerRequest{
			Operator: &callback.Operator{OpenID: "ou_bob"},
			Action: &callback.CallBackAction{
				Tag:   "button",
				Name:  "approve",
				Value: map[string]interface{}{"action": "approve"},
			},
			Context: &callback.Context{
				OpenMessageID: "om_card",
				OpenChatID:    "oc_chat",
			},
		},
	}
	m, ok := mapCardAction(ev, selfOpen, testNow)
	if !ok {
		t.Fatal("!ok")
	}
	if m.Kind != event.KindCard {
		t.Errorf("Kind = %q", m.Kind)
	}
	if m.ReplyTo != "lark:om_card" || m.ChatID != "lark:oc_chat" {
		t.Errorf("ReplyTo/ChatID = %q/%q", m.ReplyTo, m.ChatID)
	}
	if m.Sender.PlatformUserID != "ou_bob" {
		t.Errorf("Sender = %+v", m.Sender)
	}
	if len(m.UI.Buttons) != 1 {
		t.Fatalf("expected 1 button payload, got %d", len(m.UI.Buttons))
	}
	var action map[string]interface{}
	if err := json.Unmarshal(m.UI.Buttons[0], &action); err != nil {
		t.Fatalf("button payload not JSON: %v", err)
	}
	if action["name"] != "approve" {
		t.Errorf("button name = %v", action["name"])
	}
}

// TestInteractiveRecalledContentIsMarshalable is a regression test for the live
// ingest crash: Lark returns msg_type=interactive with a PLAIN-STRING content
// ("This message was recalled") for recalled cards. That is not valid JSON, so
// stuffing it into UI.Card (json.RawMessage) made the whole event fail to
// marshal: "invalid character 'T' looking for beginning of value". The mapped
// event MUST marshal cleanly, with Card unset and the string kept as Text.
func TestInteractiveRecalledContentIsMarshalable(t *testing.T) {
	// Real payload from message om_x100b802ee0afd4180f66ff676f24d6e.
	msg := &larkim.Message{
		MessageId:  ptr("om_x100b802ee0afd4180f66ff676f24d6e"),
		ChatId:     ptr("oc_chat"),
		MsgType:    ptr("interactive"),
		CreateTime: ptr("1749456000000"),
		Body:       &larkim.MessageBody{Content: ptr("This message was recalled")},
	}
	m, ok := mapListedMessage(msg, event.ChatGroup, selfOpen, testNow)
	if !ok {
		t.Fatal("!ok")
	}
	if m.UI.Card != nil {
		t.Errorf("Card must be nil for non-JSON interactive content, got %s", m.UI.Card)
	}
	if m.Text != "This message was recalled" {
		t.Errorf("Text = %q", m.Text)
	}
	// The actual bug: the whole event must marshal without error.
	if _, err := json.Marshal(m); err != nil {
		t.Fatalf("event must marshal cleanly: %v", err)
	}
}

// A well-formed interactive card still round-trips into UI.Card and marshals.
func TestInteractiveValidCardPreserved(t *testing.T) {
	card := `{"header":{"title":{"content":"Approve"}},"elements":[]}`
	msg := &larkim.Message{
		MessageId:  ptr("om_card1"),
		ChatId:     ptr("oc_chat"),
		MsgType:    ptr("interactive"),
		CreateTime: ptr("1749456000000"),
		Body:       &larkim.MessageBody{Content: ptr(card)},
	}
	m, _ := mapListedMessage(msg, event.ChatGroup, selfOpen, testNow)
	if string(m.UI.Card) != card {
		t.Errorf("card not preserved: %s", m.UI.Card)
	}
	if m.Text != "Approve" {
		t.Errorf("Text = %q", m.Text)
	}
	if _, err := json.Marshal(m); err != nil {
		t.Fatalf("event must marshal cleanly: %v", err)
	}
}

func TestChatMetasFrom(t *testing.T) {
	chats := []chatRef{
		{id: "oc_group", chatType: event.ChatGroup, name: "Eng Team", labels: []string{"work"}},
		{id: "oc_dm", chatType: event.ChatDM, name: "", p2pTargetID: "ou_bob"},
		{id: "oc_dm_noprofile", chatType: event.ChatDM, name: "Fallback Name", p2pTargetID: "ou_unknown"},
	}
	profiles := map[string]Profile{
		"ou_bob": {OpenID: "ou_bob", DisplayName: "Bob Tan"},
	}
	got := chatMetasFrom(chats, profiles)
	if len(got) != 3 {
		t.Fatalf("want 3, got %d", len(got))
	}
	// Group: title from name, labels preserved, canonical id.
	if got[0].ChatID != "lark:oc_group" || got[0].Title != "Eng Team" || len(got[0].Labels) != 1 || got[0].Labels[0] != "work" {
		t.Errorf("group meta = %+v", got[0])
	}
	// p2p with resolved profile: title is counterpart display name.
	if got[1].Title != "Bob Tan" {
		t.Errorf("p2p title = %q, want Bob Tan", got[1].Title)
	}
	// p2p without a resolved profile: falls back to the chat name.
	if got[2].Title != "Fallback Name" {
		t.Errorf("p2p fallback title = %q", got[2].Title)
	}
	// Org signals Lark doesn't expose stay default.
	if got[0].Muted || got[0].Pinned || got[0].Archived {
		t.Errorf("unexposed org signals should be default-false: %+v", got[0])
	}
}

// Guard: every mapper must produce a canonical id with the lark: prefix and a
// valid kind, so the pipeline's idempotency key is always well-formed.
func TestCanonicalIDPrefix(t *testing.T) {
	if canonMsgID("om_x") != "lark:om_x" {
		t.Errorf("canonMsgID = %q", canonMsgID("om_x"))
	}
	if canonChatID("oc_x") != "lark:oc_x" {
		t.Errorf("canonChatID = %q", canonChatID("oc_x"))
	}
}
