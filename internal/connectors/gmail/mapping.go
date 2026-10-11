package gmail

import (
	"encoding/base64"
	"encoding/json"
	"net/mail"
	"strings"
	"time"

	gmailapi "google.golang.org/api/gmail/v1"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// canonChatID encodes a Gmail thread as a canonical chat id. The account is part
// of the id so the same thread in two different mailboxes stays distinct and so
// Send/Backfill can route back to the right account from the chat id alone:
// "gmail:<account>:<threadId>".
func canonChatID(account, threadID string) string {
	return event.PlatformGmail + ":" + account + ":" + threadID
}

// parseChatID splits a canonical Gmail chat id back into (account, threadID).
// Accepts the canonical "gmail:<account>:<threadId>" form; tolerates a bare
// "gmail:<threadId>" (account empty) for callers that don't know the account.
func parseChatID(chatID string) (account, threadID string) {
	rest := strings.TrimPrefix(chatID, event.PlatformGmail+":")
	if i := strings.LastIndexByte(rest, ':'); i >= 0 {
		return rest[:i], rest[i+1:]
	}
	return "", rest
}

// canonMsgID builds the canonical/idempotency id for a Gmail message. The RFC822
// Message-ID is global + stable across accounts/replays; fall back to the Gmail
// message id when a header is missing.
func canonMsgID(rfc822ID, gmailID string) string {
	id := strings.Trim(strings.TrimSpace(rfc822ID), "<>")
	if id == "" {
		id = gmailID
	}
	return event.MakeID(event.PlatformGmail, id)
}

// header returns the (first) value of a header on a MIME part, case-insensitively.
func header(p *gmailapi.MessagePart, name string) string {
	if p == nil {
		return ""
	}
	for _, h := range p.Headers {
		if strings.EqualFold(h.Name, name) {
			return h.Value
		}
	}
	return ""
}

// addrEmail lowercases + extracts the bare address from a header value.
func addrEmail(a *mail.Address) string {
	return strings.ToLower(strings.TrimSpace(a.Address))
}

// parseAddrList parses a To/Cc style header into bare lowercased addresses.
func parseAddrList(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	addrs, err := mail.ParseAddressList(v)
	if err != nil || len(addrs) == 0 {
		return nil
	}
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if e := addrEmail(a); e != "" {
			out = append(out, e)
		}
	}
	return out
}

// decodeBody base64url-decodes a MIME part body (Gmail uses web-safe base64,
// sometimes unpadded).
func decodeBody(data string) string {
	if data == "" {
		return ""
	}
	if b, err := base64.URLEncoding.DecodeString(data); err == nil {
		return string(b)
	}
	if b, err := base64.RawURLEncoding.DecodeString(data); err == nil {
		return string(b)
	}
	return ""
}

// extractBody walks the MIME tree and returns the first text/plain body. Falls
// back to text/html (tags left intact) when no plaintext part exists. The full
// HTML/raw remains available in the event's Raw.
func extractBody(p *gmailapi.MessagePart) string {
	if p == nil {
		return ""
	}
	if plain := findPart(p, "text/plain"); plain != "" {
		return plain
	}
	return findPart(p, "text/html")
}

func findPart(p *gmailapi.MessagePart, mime string) string {
	if p == nil {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(p.MimeType), mime) && p.Body != nil && p.Body.Data != "" {
		return decodeBody(p.Body.Data)
	}
	for _, sub := range p.Parts {
		if got := findPart(sub, mime); got != "" {
			return got
		}
	}
	return ""
}

// extractAttachments lists file attachments (parts with a filename), referencing
// the Gmail attachmentId so the bytes can be fetched on demand.
func extractAttachments(p *gmailapi.MessagePart) []event.Attachment {
	if p == nil {
		return nil
	}
	var out []event.Attachment
	var walk func(part *gmailapi.MessagePart)
	walk = func(part *gmailapi.MessagePart) {
		if part == nil {
			return
		}
		if part.Filename != "" && part.Body != nil {
			out = append(out, event.Attachment{
				Type: "file",
				MIME: part.MimeType,
				Ref:  part.Body.AttachmentId,
				Name: part.Filename,
				Size: part.Body.Size,
			})
		}
		for _, sub := range part.Parts {
			walk(sub)
		}
	}
	walk(p)
	return out
}

// mapMessage normalizes a Gmail API message (format=full or metadata+body) into
// the FROZEN canonical event. account is the mailbox address this message was
// fetched from; it drives account_id, chat_id routing, and is_self/@me. source is
// event.SourceLive or SourceBackfill. Returns ok=false only for a nil message.
//
// Mapping (coordinated with core):
//   - account_id = "gmail:<account>"; chat_id = "gmail:<account>:<threadId>"
//     (a Gmail thread is the chat); chat_type = "email".
//   - sender  = From header (email is a STRONG gbrain alias); is_self when From
//     is the account's own address.
//   - mentions = To + Cc addresses (the email analogue of @-mentions); the user's
//     own address among them => "addressed to me".
//   - text = "Subject\n\n<plaintext body>"; reply_to via In-Reply-To.
//   - id = canonical RFC822 Message-ID (fallback Gmail id) for idempotency.
func mapMessage(msg *gmailapi.Message, account, src string, now time.Time) (event.Message, bool) {
	if msg == nil {
		return event.Message{}, false
	}
	p := msg.Payload

	fromRaw := header(p, "From")
	var senderEmail, senderName string
	if a, err := mail.ParseAddress(fromRaw); err == nil {
		senderEmail = addrEmail(a)
		senderName = a.Name
	} else {
		senderEmail = strings.ToLower(strings.TrimSpace(fromRaw))
	}
	accountLower := strings.ToLower(account)

	mentions := append(parseAddrList(header(p, "To")), parseAddrList(header(p, "Cc"))...)

	subject := header(p, "Subject")
	body := extractBody(p)
	text := subject
	if body != "" {
		if text != "" {
			text += "\n\n"
		}
		text += body
	}

	ts := time.Time{}
	if msg.InternalDate != 0 {
		ts = time.UnixMilli(msg.InternalDate).UTC()
	} else if d := header(p, "Date"); d != "" {
		if parsed, err := mail.ParseDate(d); err == nil {
			ts = parsed.UTC()
		}
	}

	raw, _ := json.Marshal(msg)

	m := event.Message{
		ID:        canonMsgID(header(p, "Message-ID"), msg.Id),
		Platform:  event.PlatformGmail,
		AccountID: event.PlatformGmail + ":" + accountLower,
		ChatID:    canonChatID(accountLower, msg.ThreadId),
		ChatType:  event.ChatEmail,
		Sender: event.Sender{
			PlatformUserID: senderEmail,
			Email:          senderEmail,
			DisplayName:    senderName,
			IsSelf:         senderEmail != "" && senderEmail == accountLower,
		},
		TS:          ts,
		Kind:        event.KindText,
		Text:        text,
		ThreadID:    msg.ThreadId,
		Mentions:    mentions,
		Attachments: extractAttachments(p),
		Source:      src,
		IngestedAt:  now,
		Raw:         raw,
	}

	// reply_to: In-Reply-To points at the parent's RFC822 Message-ID.
	if irt := header(p, "In-Reply-To"); irt != "" {
		m.ReplyTo = canonMsgID(irt, "")
	}

	return m, true
}
