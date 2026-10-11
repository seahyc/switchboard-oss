package lark

import (
	"encoding/json"
	"strings"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// parsedContent is the normalized result of decoding a Lark message `content`
// JSON blob for a given msg_type. It is the bridge between Lark's many
// per-type content shapes and the canonical event fields.
type parsedContent struct {
	Text        string
	Attachments []event.Attachment
	Card        json.RawMessage // set only for interactive (card) messages
}

// kindForMsgType maps a Lark message type to the canonical event.Kind.
// Lark msg_type values: text, post, image, file, audio, media, sticker,
// interactive, share_chat, share_user, system, location, todo, calendar ...
func kindForMsgType(msgType string) string {
	switch msgType {
	case "text", "post":
		return event.KindText
	case "image", "file", "audio", "media", "video":
		return event.KindMedia
	case "sticker":
		return event.KindSticker
	case "interactive":
		return event.KindCard
	case "system":
		return event.KindSystem
	default:
		// share_chat, share_user, location, todo, calendar, etc. — preserved as
		// system-ish text; the raw payload is always retained on the event.
		return event.KindSystem
	}
}

// parseContent decodes a Lark message content blob into canonical fields.
// It never errors fatally: on malformed JSON it falls back to the raw string as
// text so capture is lossless (the original is also kept in event.Raw).
func parseContent(msgType, content string) parsedContent {
	content = strings.TrimSpace(content)
	if content == "" {
		return parsedContent{}
	}
	switch msgType {
	case "text":
		var c struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal([]byte(content), &c); err != nil {
			return parsedContent{Text: content}
		}
		return parsedContent{Text: c.Text}

	case "post":
		return parsedContent{Text: parsePostText(content)}

	case "image":
		var c struct {
			ImageKey string `json:"image_key"`
		}
		_ = json.Unmarshal([]byte(content), &c)
		return parsedContent{Attachments: []event.Attachment{{
			Type: "image",
			Ref:  c.ImageKey,
		}}}

	case "file":
		var c struct {
			FileKey  string `json:"file_key"`
			FileName string `json:"file_name"`
		}
		_ = json.Unmarshal([]byte(content), &c)
		return parsedContent{Attachments: []event.Attachment{{
			Type: "file",
			Ref:  c.FileKey,
			Name: c.FileName,
		}}}

	case "audio":
		var c struct {
			FileKey string `json:"file_key"`
		}
		_ = json.Unmarshal([]byte(content), &c)
		return parsedContent{Attachments: []event.Attachment{{
			Type: "audio",
			Ref:  c.FileKey,
		}}}

	case "media", "video":
		var c struct {
			FileKey  string `json:"file_key"`
			ImageKey string `json:"image_key"`
			FileName string `json:"file_name"`
		}
		_ = json.Unmarshal([]byte(content), &c)
		return parsedContent{Attachments: []event.Attachment{{
			Type: "video",
			Ref:  c.FileKey,
			Name: c.FileName,
		}}}

	case "sticker":
		var c struct {
			FileKey string `json:"file_key"`
		}
		_ = json.Unmarshal([]byte(content), &c)
		return parsedContent{Attachments: []event.Attachment{{
			Type: "sticker",
			Ref:  c.FileKey,
		}}}

	case "interactive":
		// The whole content blob IS the card definition. Preserve it verbatim so
		// the agent can re-render/act on it; surface any header title as text.
		// BUT Lark returns a PLAIN-STRING content for some interactive messages
		// (e.g. a recalled card → "This message was recalled"), which is NOT
		// valid JSON. Never stuff a non-JSON string into a json.RawMessage — that
		// breaks event marshaling/ingest. Only set Card when content is real JSON.
		if json.Valid([]byte(content)) {
			return parsedContent{
				Text: cardTitle(content),
				Card: json.RawMessage(content),
			}
		}
		return parsedContent{Text: content}

	case "share_chat":
		var c struct {
			ChatID string `json:"chat_id"`
		}
		_ = json.Unmarshal([]byte(content), &c)
		return parsedContent{Text: "[shared chat " + c.ChatID + "]"}

	case "share_user":
		var c struct {
			UserID string `json:"user_id"`
		}
		_ = json.Unmarshal([]byte(content), &c)
		return parsedContent{Text: "[shared user " + c.UserID + "]"}

	default:
		return parsedContent{}
	}
}

// parsePostText flattens a Lark rich-text "post" body into plain text.
// Shape: {"title":"..","content":[[{"tag":"text","text":".."}, ...], ...]}.
func parsePostText(content string) string {
	var p struct {
		Title   string `json:"title"`
		Content [][]struct {
			Tag      string `json:"tag"`
			Text     string `json:"text"`
			UserID   string `json:"user_id"`
			UserName string `json:"user_name"`
			Href     string `json:"href"`
		} `json:"content"`
	}
	if err := json.Unmarshal([]byte(content), &p); err != nil {
		return content
	}
	var b strings.Builder
	if p.Title != "" {
		b.WriteString(p.Title)
		b.WriteString("\n")
	}
	for _, line := range p.Content {
		for _, el := range line {
			switch el.Tag {
			case "text":
				b.WriteString(el.Text)
			case "a":
				if el.Text != "" {
					b.WriteString(el.Text)
				} else {
					b.WriteString(el.Href)
				}
			case "at":
				name := el.UserName
				if name == "" {
					name = el.UserID
				}
				b.WriteString("@" + name)
			case "img", "media":
				// non-text element; skip in the text flattening.
			default:
				b.WriteString(el.Text)
			}
		}
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

// cardTitle best-effort extracts the header title text of an interactive card
// so the canonical Text has something human-readable; the full card stays in UI.
func cardTitle(content string) string {
	var c struct {
		Header struct {
			Title struct {
				Content string `json:"content"`
			} `json:"title"`
		} `json:"header"`
	}
	if err := json.Unmarshal([]byte(content), &c); err != nil {
		return ""
	}
	return c.Header.Title.Content
}
