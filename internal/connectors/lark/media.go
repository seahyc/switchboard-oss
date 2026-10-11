package lark

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// larkMediaRef is the rich, self-contained handle for downloading a Lark attachment's
// bytes later (#48). Lark's resource-download API (Im.Message.Resource.Get) needs the
// MESSAGE id + the file/image key + a resource type ("image"|"file") — but the connector
// historically stored only the bare key on Attachment.Ref, so every Lark media row failed
// "no downloader or empty ref". This JSON ref carries all three. (Lark keys don't expire,
// so no refresh dance is needed — unlike Telegram's file_reference.)
type larkMediaRef struct {
	MsgID string `json:"m"`
	Key   string `json:"k"`
	Type  string `json:"t"` // "image" | "file" (Lark resource type)
}

func (r larkMediaRef) encode() string {
	b, _ := json.Marshal(r)
	return string(b)
}

func decodeLarkMediaRef(s string) (larkMediaRef, bool) {
	var r larkMediaRef
	if err := json.Unmarshal([]byte(s), &r); err != nil || r.Key == "" || r.MsgID == "" {
		return larkMediaRef{}, false
	}
	return r, true
}

// larkResourceType maps a canonical attachment type to Lark's resource Get `type` param.
// image_key resources are "image"; everything else (file/audio/video/sticker) is "file".
func larkResourceType(attType string) string {
	if attType == "image" {
		return "image"
	}
	return "file"
}

// enrichMediaRefs upgrades freshly-parsed attachment refs (bare key) to the rich
// downloadable larkMediaRef, given the message id. Called at ingest (build).
func enrichMediaRefs(msgID string, atts []event.Attachment) {
	for i := range atts {
		if atts[i].Ref == "" || msgID == "" {
			continue
		}
		atts[i].Ref = larkMediaRef{MsgID: msgID, Key: atts[i].Ref, Type: larkResourceType(atts[i].Type)}.encode()
	}
}

// DownloadMedia implements connectors.MediaDownloader: fetch an attachment's bytes via
// Lark's message-resource API using the rich ref recorded at ingest. The Lark connector
// had NO downloader before — only the bare key was stored — so all Lark media failed
// "no downloader or empty ref" (#48).
func (c *Connector) DownloadMedia(ctx context.Context, ref string) (io.ReadCloser, int64, error) {
	r, ok := decodeLarkMediaRef(ref)
	if !ok {
		return nil, 0, fmt.Errorf("lark: legacy/bare media ref %q — re-ingest the chat to gain a downloadable ref", clip(ref, 48))
	}
	opt, err := c.userOpt(ctx)
	if err != nil {
		return nil, 0, err
	}
	req := larkim.NewGetMessageResourceReqBuilder().MessageId(r.MsgID).FileKey(r.Key).Type(r.Type).Build()
	resp, err := c.client.Im.MessageResource.Get(ctx, req, opt)
	if err != nil {
		return nil, 0, fmt.Errorf("lark resource get: %w", err)
	}
	if !resp.Success() {
		return nil, 0, fmt.Errorf("lark resource get: code=%d msg=%s", resp.Code, resp.Msg)
	}
	if resp.File == nil {
		return nil, 0, fmt.Errorf("lark resource get: empty body")
	}
	data, err := io.ReadAll(resp.File)
	if err != nil {
		return nil, 0, fmt.Errorf("lark resource read: %w", err)
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

// ReextractMediaRef implements connectors.MediaRefReextractor: rebuild the rich ref from a
// stored raw Lark Message payload (#48 recovery for the 452 rows that hold only the bare
// key). Parses message_id + msg_type + body.content (the key lives in the content JSON).
// Returns "" if the raw carries no downloadable attachment. (chatID is unused — Lark's
// download key is the message id, which is in the raw.)
func (c *Connector) ReextractMediaRef(raw []byte, _ string) string {
	var msg struct {
		MessageID string `json:"message_id"`
		MsgType   string `json:"msg_type"`
		Body      struct {
			Content string `json:"content"`
		} `json:"body"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil || msg.MessageID == "" || msg.Body.Content == "" {
		return ""
	}
	parsed := parseContent(msg.MsgType, msg.Body.Content)
	if len(parsed.Attachments) == 0 || parsed.Attachments[0].Ref == "" {
		return ""
	}
	a := parsed.Attachments[0]
	return larkMediaRef{MsgID: msg.MessageID, Key: a.Ref, Type: larkResourceType(a.Type)}.encode()
}

// clip truncates a string for safe inclusion in an error message.
func clip(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
