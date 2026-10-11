package gmail

import (
	"encoding/base64"
	"testing"
	"time"

	gmailapi "google.golang.org/api/gmail/v1"

	"github.com/seahyc/switchboard-oss/internal/event"
)

func b64url(s string) string { return base64.URLEncoding.EncodeToString([]byte(s)) }

// sampleMessage is a representative inbound email: a reply, with To+Cc (one of
// which is the user), a plaintext + html body, and an attachment.
func sampleMessage() *gmailapi.Message {
	return &gmailapi.Message{
		Id:           "18febaadf00dcafe",
		ThreadId:     "18thread99",
		InternalDate: time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC).UnixMilli(),
		Payload: &gmailapi.MessagePart{
			MimeType: "multipart/mixed",
			Headers: []*gmailapi.MessagePartHeader{
				{Name: "From", Value: "Alice Smith <alice@example.com>"},
				{Name: "To", Value: "owner@example.com, Bob <bob@other.com>"},
				{Name: "Cc", Value: "carol@example.org"},
				{Name: "Subject", Value: "Lunch Friday?"},
				{Name: "Message-ID", Value: "<msg-abc-123@example.com>"},
				{Name: "In-Reply-To", Value: "<parent-xyz-789@example.com>"},
				{Name: "Date", Value: "Tue, 09 Jun 2026 08:00:00 +0000"},
			},
			Parts: []*gmailapi.MessagePart{
				{
					MimeType: "text/plain",
					Body:     &gmailapi.MessagePartBody{Data: b64url("Are we still on for lunch?")},
				},
				{
					MimeType: "text/html",
					Body:     &gmailapi.MessagePartBody{Data: b64url("<p>Are we still on for lunch?</p>")},
				},
				{
					MimeType: "application/pdf",
					Filename: "menu.pdf",
					Body:     &gmailapi.MessagePartBody{AttachmentId: "att-1", Size: 2048},
				},
			},
		},
	}
}

func TestMapMessage(t *testing.T) {
	now := time.Date(2026, 6, 9, 8, 0, 1, 0, time.UTC)
	m, ok := mapMessage(sampleMessage(), "owner@example.com", event.SourceLive, now)
	if !ok {
		t.Fatal("mapMessage returned ok=false")
	}

	if m.Platform != event.PlatformGmail {
		t.Errorf("platform = %q, want %q", m.Platform, event.PlatformGmail)
	}
	if m.ChatType != event.ChatEmail {
		t.Errorf("chat_type = %q, want %q", m.ChatType, event.ChatEmail)
	}
	// id is the canonical RFC822 Message-ID (angle brackets stripped).
	if want := "gmail:msg-abc-123@example.com"; m.ID != want {
		t.Errorf("id = %q, want %q", m.ID, want)
	}
	if want := "gmail:owner@example.com"; m.AccountID != want {
		t.Errorf("account_id = %q, want %q", m.AccountID, want)
	}
	// chat_id encodes account + threadId so threads route back to the mailbox.
	if want := "gmail:owner@example.com:18thread99"; m.ChatID != want {
		t.Errorf("chat_id = %q, want %q", m.ChatID, want)
	}
	if m.ThreadID != "18thread99" {
		t.Errorf("thread_id = %q, want 18thread99", m.ThreadID)
	}

	// sender = From; email is the strong gbrain alias.
	if m.Sender.Email != "alice@example.com" {
		t.Errorf("sender.email = %q, want alice@example.com", m.Sender.Email)
	}
	if m.Sender.PlatformUserID != "alice@example.com" {
		t.Errorf("sender.platform_user_id = %q", m.Sender.PlatformUserID)
	}
	if m.Sender.DisplayName != "Alice Smith" {
		t.Errorf("sender.display_name = %q, want Alice Smith", m.Sender.DisplayName)
	}
	if m.Sender.IsSelf {
		t.Error("sender.is_self should be false (Alice is not the account)")
	}

	// mentions = To + Cc; the user's own address is among them (=> addressed to me).
	wantMentions := map[string]bool{
		"owner@example.com": true,
		"bob@other.com":     true,
		"carol@example.org": true,
	}
	if len(m.Mentions) != len(wantMentions) {
		t.Errorf("mentions = %v, want %d entries", m.Mentions, len(wantMentions))
	}
	for _, mn := range m.Mentions {
		if !wantMentions[mn] {
			t.Errorf("unexpected mention %q", mn)
		}
	}

	// text = subject + plaintext body (html ignored for text, kept in raw).
	if want := "Lunch Friday?\n\nAre we still on for lunch?"; m.Text != want {
		t.Errorf("text = %q, want %q", m.Text, want)
	}

	// reply_to via In-Reply-To header.
	if want := "gmail:parent-xyz-789@example.com"; m.ReplyTo != want {
		t.Errorf("reply_to = %q, want %q", m.ReplyTo, want)
	}

	if !m.TS.Equal(time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)) {
		t.Errorf("ts = %v, want 2026-06-09T08:00:00Z", m.TS)
	}
	if m.Source != event.SourceLive {
		t.Errorf("source = %q, want live", m.Source)
	}

	if len(m.Attachments) != 1 {
		t.Fatalf("attachments = %d, want 1", len(m.Attachments))
	}
	att := m.Attachments[0]
	if att.Name != "menu.pdf" || att.MIME != "application/pdf" || att.Ref != "att-1" || att.Size != 2048 {
		t.Errorf("attachment = %+v", att)
	}
}

func TestMapMessageIsSelf(t *testing.T) {
	msg := sampleMessage()
	// Make the account the sender => is_self.
	msg.Payload.Headers[0].Value = "Me <owner@example.com>"
	m, _ := mapMessage(msg, "owner@example.com", event.SourceBackfill, time.Now())
	if !m.Sender.IsSelf {
		t.Error("sender.is_self should be true when From == account")
	}
	if m.Source != event.SourceBackfill {
		t.Errorf("source = %q, want backfill", m.Source)
	}
}

func TestMapMessageFallbackID(t *testing.T) {
	msg := sampleMessage()
	// Drop the Message-ID header => fall back to the Gmail message id.
	msg.Payload.Headers[4].Value = ""
	m, _ := mapMessage(msg, "owner@example.com", event.SourceLive, time.Now())
	if want := "gmail:18febaadf00dcafe"; m.ID != want {
		t.Errorf("fallback id = %q, want %q", m.ID, want)
	}
}

func TestParseChatID(t *testing.T) {
	acc, thread := parseChatID("gmail:owner@example.com:18thread99")
	if acc != "owner@example.com" || thread != "18thread99" {
		t.Errorf("parseChatID = (%q,%q)", acc, thread)
	}
	// Bare form (no account) tolerated.
	acc, thread = parseChatID("gmail:justthread")
	if acc != "" || thread != "justthread" {
		t.Errorf("parseChatID bare = (%q,%q)", acc, thread)
	}
}

func TestEnsureReSubject(t *testing.T) {
	cases := map[string]string{
		"Hello":     "Re: Hello",
		"Re: Hello": "Re: Hello",
		"RE: Hello": "RE: Hello",
		"":          "",
	}
	for in, want := range cases {
		if got := ensureReSubject(in); got != want {
			t.Errorf("ensureReSubject(%q) = %q, want %q", in, got, want)
		}
	}
}
