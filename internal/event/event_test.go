package event

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestMakeID(t *testing.T) {
	if got := MakeID(PlatformWhatsApp, "3EB0ABC"); got != "whatsapp:3EB0ABC" {
		t.Fatalf("MakeID = %q, want whatsapp:3EB0ABC", got)
	}
	m := &Message{Platform: PlatformTelegram}
	m.SetID("12345")
	if m.ID != "telegram:12345" {
		t.Fatalf("SetID -> %q, want telegram:12345", m.ID)
	}
}

func TestRoundTrip(t *testing.T) {
	ts := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	edited := ts.Add(time.Minute)
	orig := Message{
		ID:        "whatsapp:3EB0",
		Platform:  PlatformWhatsApp,
		AccountID: "wa:15555550123",
		ChatID:    "whatsapp:123@s.whatsapp.net",
		ChatType:  ChatDM,
		Sender: Sender{
			PlatformUserID: "123@s.whatsapp.net",
			Handle:         "alice",
			Phone:          "+6591234567",
			Email:          "alice@example.com",
			DisplayName:    "Alice",
			IsSelf:         false,
		},
		TS:       ts,
		Kind:     KindText,
		Text:     "hello world",
		ReplyTo:  "whatsapp:3EB0PARENT",
		ThreadID: "thread-1",
		Mentions: []string{"self@s.whatsapp.net"},
		Attachments: []Attachment{
			{Type: "image", MIME: "image/jpeg", Ref: "blob://x", Name: "p.jpg", Size: 1024},
		},
		UI: UI{
			Buttons: []json.RawMessage{json.RawMessage(`{"id":"yes","label":"Yes"}`)},
			Poll:    json.RawMessage(`{"q":"pick"}`),
			Card:    json.RawMessage(`null`),
		},
		Reactions:  []Reaction{{Emoji: "👍", By: "123@s.whatsapp.net"}},
		EditedTS:   &edited,
		Deleted:    false,
		Source:     SourceLive,
		IngestedAt: ts.Add(time.Second),
		Raw:        json.RawMessage(`{"original":true}`),
	}

	b, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got Message
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !reflect.DeepEqual(orig, got) {
		t.Fatalf("round-trip mismatch:\n orig=%+v\n got =%+v", orig, got)
	}
}

// TestJSONContract pins the exact wire field names from the SPEC. If any of
// these break, the frozen contract changed and all connectors are affected.
func TestJSONContract(t *testing.T) {
	b, err := json.Marshal(Message{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := []string{
		"id", "platform", "account_id", "chat_id", "chat_type", "sender",
		"ts", "kind", "text", "reply_to", "thread_id", "mentions",
		"attachments", "ui", "reactions", "edited_ts", "deleted", "source",
		"ingested_at", "raw",
	}
	for _, k := range want {
		if _, ok := m[k]; !ok {
			t.Errorf("missing top-level field %q in JSON contract", k)
		}
	}
	// sender sub-fields
	var s map[string]json.RawMessage
	if err := json.Unmarshal(m["sender"], &s); err != nil {
		t.Fatalf("unmarshal sender: %v", err)
	}
	for _, k := range []string{"platform_user_id", "handle", "phone", "email", "display_name", "is_self"} {
		if _, ok := s[k]; !ok {
			t.Errorf("missing sender field %q", k)
		}
	}
}
