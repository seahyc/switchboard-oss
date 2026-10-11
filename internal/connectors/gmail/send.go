package gmail

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// rfc822 is the minimal set of fields we render into an outbound MIME message.
type rfc822 struct {
	From       string
	To         string
	Subject    string
	Body       string
	InReplyTo  string
	References string
}

// buildRFC822 renders a plain-text email and returns it base64url-encoded, ready
// for gmail.Message.Raw. Threading headers (In-Reply-To/References) are included
// when set so replies land in the original thread on the recipient side too.
func buildRFC822(m rfc822) string {
	var b strings.Builder
	if m.From != "" {
		fmt.Fprintf(&b, "From: %s\r\n", m.From)
	}
	if m.To != "" {
		fmt.Fprintf(&b, "To: %s\r\n", m.To)
	}
	fmt.Fprintf(&b, "Subject: %s\r\n", m.Subject)
	if m.InReplyTo != "" {
		fmt.Fprintf(&b, "In-Reply-To: %s\r\n", strings.TrimSpace(m.InReplyTo))
	}
	if m.References != "" {
		fmt.Fprintf(&b, "References: %s\r\n", strings.TrimSpace(m.References))
	}
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=\"UTF-8\"\r\n")
	b.WriteString("\r\n")
	b.WriteString(m.Body)
	return base64.URLEncoding.EncodeToString([]byte(b.String()))
}

// ensureReSubject prefixes "Re: " to a subject for replies (idempotently).
func ensureReSubject(subject string) string {
	s := strings.TrimSpace(subject)
	if s == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(s), "re:") {
		return s
	}
	return "Re: " + s
}
