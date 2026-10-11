package whatsapp

import (
	"encoding/json"
	"time"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// chatIDForJID builds the canonical chat id "whatsapp:<jid>".
func chatIDForJID(jid types.JID) string {
	return event.MakeID(event.PlatformWhatsApp, jid.String())
}

// userIDForJID is the canonical per-user platform id: the address without the
// device suffix, e.g. "15555550123@s.whatsapp.net" or "<lid>@lid".
func userIDForJID(jid types.JID) string {
	if jid.IsEmpty() {
		return ""
	}
	return jid.ToNonAD().String()
}

// chatTypeForJID classifies a chat from its JID server.
func chatTypeForJID(jid types.JID, isGroup bool) string {
	switch jid.Server {
	case types.GroupServer:
		return event.ChatGroup
	case types.NewsletterServer:
		return event.ChatChannel
	case types.BroadcastServer:
		// Broadcast lists behave like a fan-out channel for our purposes.
		return event.ChatChannel
	default:
		if isGroup {
			return event.ChatGroup
		}
		return event.ChatDM
	}
}

// mapSender extracts the canonical sender identity plus alias signals (phone)
// for the gbrain entity registry. In LID-addressing mode the phone lives on the
// alternative address, so we check both.
func mapSender(info types.MessageInfo) event.Sender {
	s := event.Sender{
		PlatformUserID: userIDForJID(info.Sender),
		Handle:         info.PushName,
		DisplayName:    info.PushName,
		IsSelf:         info.IsFromMe,
	}
	if info.Sender.Server == types.DefaultUserServer && info.Sender.User != "" {
		s.Phone = info.Sender.User
	} else if info.SenderAlt.Server == types.DefaultUserServer && info.SenderAlt.User != "" {
		s.Phone = info.SenderAlt.User
	}
	return s
}

// LIDPhoneFromRaw recovers a sender's phone from a stored Raw event (which is
// json.Marshal of the whatsmeow events.Message). Used by the #33 one-time
// re-extraction to backfill sender_phone for @lid messages ingested BEFORE the
// SenderAlt capture in mapSender — the phone was in Raw all along (same replay-safe
// pattern as RefFromRaw for media keys). Returns ("", false) when Raw is missing/
// unparseable or carries no phone (e.g. a true @lid with no SenderAlt).
func LIDPhoneFromRaw(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var ev events.Message
	if err := json.Unmarshal(raw, &ev); err != nil {
		return "", false
	}
	if p := mapSender(ev.Info).Phone; p != "" {
		return p, true
	}
	return "", false
}

// applyContextInfo folds reply target + mentions from a ContextInfo onto m.
func applyContextInfo(m *event.Message, ci *waE2E.ContextInfo) {
	if ci == nil {
		return
	}
	if id := ci.GetStanzaID(); id != "" {
		m.ReplyTo = event.MakeID(event.PlatformWhatsApp, id)
	}
	for _, j := range ci.GetMentionedJID() {
		if jid, err := types.ParseJID(j); err == nil {
			m.Mentions = append(m.Mentions, userIDForJID(jid))
		} else if j != "" {
			m.Mentions = append(m.Mentions, j)
		}
	}
}

// canonicalIDFromKey converts a referenced message key into a canonical event id.
func canonicalIDFromKey(key *waCommon.MessageKey) string {
	if key == nil || key.GetID() == "" {
		return ""
	}
	return event.MakeID(event.PlatformWhatsApp, key.GetID())
}

// classify inspects the (already unwrapped) waE2E.Message and fills kind, text,
// attachments, mentions, reply, reactions, edit/delete flags on m.
func classify(m *event.Message, msg *waE2E.Message, mediaKey mediaRefMessageKey) {
	if msg == nil {
		m.Kind = event.KindSystem
		return
	}

	switch {
	case msg.GetReactionMessage() != nil:
		r := msg.GetReactionMessage()
		m.Kind = event.KindReaction
		m.Text = r.GetText()
		m.ReplyTo = canonicalIDFromKey(r.GetKey())
		m.Reactions = append(m.Reactions, event.Reaction{
			Emoji: r.GetText(),
			By:    m.Sender.PlatformUserID,
		})

	case msg.GetProtocolMessage() != nil:
		p := msg.GetProtocolMessage()
		switch p.GetType() {
		case waE2E.ProtocolMessage_REVOKE:
			m.Kind = event.KindDelete
			m.Deleted = true
			m.ReplyTo = canonicalIDFromKey(p.GetKey())
		case waE2E.ProtocolMessage_MESSAGE_EDIT:
			m.Kind = event.KindEdit
			ts := m.TS
			m.EditedTS = &ts
			m.ReplyTo = canonicalIDFromKey(p.GetKey())
			if edited := p.GetEditedMessage(); edited != nil {
				m.Text = extractText(edited)
				applyContextInfo(m, edited.GetExtendedTextMessage().GetContextInfo())
			}
		default:
			m.Kind = event.KindSystem
		}

	case msg.GetConversation() != "":
		m.Kind = event.KindText
		m.Text = msg.GetConversation()

	case msg.GetExtendedTextMessage() != nil:
		ext := msg.GetExtendedTextMessage()
		m.Kind = event.KindText
		m.Text = ext.GetText()
		applyContextInfo(m, ext.GetContextInfo())

	case msg.GetImageMessage() != nil:
		im := msg.GetImageMessage()
		m.Kind = event.KindMedia
		m.Text = im.GetCaption()
		m.Attachments = append(m.Attachments, event.Attachment{
			Type: "image",
			MIME: im.GetMimetype(),
			Ref:  encodeMediaRef(im.GetDirectPath(), im.GetURL(), im.GetMediaKey(), im.GetFileEncSHA256(), im.GetFileSHA256(), "image", mediaKey),
			Size: int64(im.GetFileLength()),
		})
		applyContextInfo(m, im.GetContextInfo())

	case msg.GetVideoMessage() != nil:
		vm := msg.GetVideoMessage()
		m.Kind = event.KindMedia
		m.Text = vm.GetCaption()
		m.Attachments = append(m.Attachments, event.Attachment{
			Type: "video",
			MIME: vm.GetMimetype(),
			Ref:  encodeMediaRef(vm.GetDirectPath(), vm.GetURL(), vm.GetMediaKey(), vm.GetFileEncSHA256(), vm.GetFileSHA256(), "video", mediaKey),
			Size: int64(vm.GetFileLength()),
		})
		applyContextInfo(m, vm.GetContextInfo())

	case msg.GetAudioMessage() != nil:
		am := msg.GetAudioMessage()
		m.Kind = event.KindMedia
		typ := "audio"
		if am.GetPTT() {
			typ = "voice"
		}
		m.Attachments = append(m.Attachments, event.Attachment{
			Type: typ,
			MIME: am.GetMimetype(),
			Ref:  encodeMediaRef(am.GetDirectPath(), am.GetURL(), am.GetMediaKey(), am.GetFileEncSHA256(), am.GetFileSHA256(), "audio", mediaKey),
			Size: int64(am.GetFileLength()),
		})

	case msg.GetDocumentMessage() != nil:
		dm := msg.GetDocumentMessage()
		m.Kind = event.KindMedia
		m.Text = dm.GetCaption()
		name := dm.GetFileName()
		if name == "" {
			name = dm.GetTitle()
		}
		m.Attachments = append(m.Attachments, event.Attachment{
			Type: "document",
			MIME: dm.GetMimetype(),
			Ref:  encodeMediaRef(dm.GetDirectPath(), dm.GetURL(), dm.GetMediaKey(), dm.GetFileEncSHA256(), dm.GetFileSHA256(), "document", mediaKey),
			Name: name,
			Size: int64(dm.GetFileLength()),
		})

	case msg.GetStickerMessage() != nil:
		sm := msg.GetStickerMessage()
		m.Kind = event.KindSticker
		m.Attachments = append(m.Attachments, event.Attachment{
			Type: "sticker",
			MIME: sm.GetMimetype(),
			Ref:  encodeMediaRef(sm.GetDirectPath(), sm.GetURL(), sm.GetMediaKey(), sm.GetFileEncSHA256(), sm.GetFileSHA256(), "sticker", mediaKey),
			Size: int64(sm.GetFileLength()),
		})

	case msg.GetPollCreationMessage() != nil:
		pc := msg.GetPollCreationMessage()
		m.Kind = event.KindPoll
		m.Text = pc.GetName()
		if raw, err := json.Marshal(pc); err == nil {
			m.UI.Poll = raw
		}

	default:
		m.Kind = event.KindSystem
	}
}

// extractText pulls the best-effort text body from a message proto.
func extractText(msg *waE2E.Message) string {
	if msg == nil {
		return ""
	}
	if c := msg.GetConversation(); c != "" {
		return c
	}
	if ext := msg.GetExtendedTextMessage(); ext != nil {
		return ext.GetText()
	}
	return ""
}

// mediaRefV1 is the serialized handle the media store later uses to re-download
// an attachment's bytes. WhatsApp media is end-to-end encrypted, so the direct
// path alone is NOT enough — DownloadMediaWithPath needs the media key + the two
// content hashes + the media type. We carry all of them in Attachment.Ref so the
// (gated, off-path) media worker can fetch on demand. Byte slices base64-encode
// automatically through JSON.
type mediaRefV1 struct {
	V    int    `json:"v"` // schema version
	Path string `json:"p"`
	Key  []byte `json:"k,omitempty"` // mediaKey
	Enc  []byte `json:"e,omitempty"` // fileEncSHA256
	SHA  []byte `json:"s,omitempty"` // fileSHA256
	MT   string `json:"t"`           // image|video|audio|voice|document|sticker
	URL  string `json:"u,omitempty"` // fallback (unencrypted / legacy)

	// Message key metadata lets DownloadMedia request a fresh direct path via
	// SendMediaRetryReceipt when old CDN paths expire. Older refs omit these
	// fields and keep the old best-effort behavior.
	Chat        string `json:"c,omitempty"`
	ID          string `json:"id,omitempty"`
	FromMe      bool   `json:"fm,omitempty"`
	IsGroup     bool   `json:"g,omitempty"`
	Participant string `json:"pt,omitempty"`
}

type mediaRefMessageKey struct {
	Chat        string
	ID          string
	FromMe      bool
	IsGroup     bool
	Participant string
}

func mediaRefMessageKeyFromInfo(info types.MessageInfo) mediaRefMessageKey {
	key := mediaRefMessageKey{
		Chat:    info.Chat.String(),
		ID:      string(info.ID),
		FromMe:  info.IsFromMe,
		IsGroup: info.IsGroup,
	}
	if info.IsGroup && !info.Sender.IsEmpty() {
		key.Participant = info.Sender.String()
	}
	return key
}

// RefFromRaw re-derives the full media-download Ref tuple from a stored raw
// whatsmeow event payload (event.Message.Raw = json.Marshal of the events.Message,
// which embeds the waE2E proto with the media keys). This is the replay-safe
// recovery path: attachments ingested before the connector serialized the full
// tuple carry a path-only Ref, but the keys were preserved in raw all along — so
// the media-enqueue path can rebuild a complete, decryptable Ref from raw with no
// extra WhatsApp metadata calls. Mirrors classify's media handling. Returns ""
// when the raw has no direct downloadable media.
func RefFromRaw(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var w struct {
		Info    types.MessageInfo `json:"Info"`
		Message *waE2E.Message    `json:"Message"`
	}
	if err := json.Unmarshal(raw, &w); err != nil || w.Message == nil {
		return ""
	}
	msg := w.Message
	key := mediaRefMessageKeyFromInfo(w.Info)
	switch {
	case msg.GetImageMessage() != nil:
		im := msg.GetImageMessage()
		return encodeMediaRef(im.GetDirectPath(), im.GetURL(), im.GetMediaKey(), im.GetFileEncSHA256(), im.GetFileSHA256(), "image", key)
	case msg.GetVideoMessage() != nil:
		vm := msg.GetVideoMessage()
		return encodeMediaRef(vm.GetDirectPath(), vm.GetURL(), vm.GetMediaKey(), vm.GetFileEncSHA256(), vm.GetFileSHA256(), "video", key)
	case msg.GetAudioMessage() != nil:
		am := msg.GetAudioMessage()
		t := "audio"
		if am.GetPTT() {
			t = "voice"
		}
		return encodeMediaRef(am.GetDirectPath(), am.GetURL(), am.GetMediaKey(), am.GetFileEncSHA256(), am.GetFileSHA256(), t, key)
	case msg.GetDocumentMessage() != nil:
		dm := msg.GetDocumentMessage()
		return encodeMediaRef(dm.GetDirectPath(), dm.GetURL(), dm.GetMediaKey(), dm.GetFileEncSHA256(), dm.GetFileSHA256(), "document", key)
	case msg.GetStickerMessage() != nil:
		sm := msg.GetStickerMessage()
		return encodeMediaRef(sm.GetDirectPath(), sm.GetURL(), sm.GetMediaKey(), sm.GetFileEncSHA256(), sm.GetFileSHA256(), "sticker", key)
	}
	return ""
}

// encodeMediaRef serializes the full download tuple into Attachment.Ref. Returns
// "" when there's nothing fetchable (no path and no url).
func encodeMediaRef(directPath, url string, key, enc, sha []byte, mt string, msgKey mediaRefMessageKey) string {
	if directPath == "" && url == "" {
		return ""
	}
	b, err := json.Marshal(mediaRefV1{
		V:           1,
		Path:        directPath,
		Key:         key,
		Enc:         enc,
		SHA:         sha,
		MT:          mt,
		URL:         url,
		Chat:        msgKey.Chat,
		ID:          msgKey.ID,
		FromMe:      msgKey.FromMe,
		IsGroup:     msgKey.IsGroup,
		Participant: msgKey.Participant,
	})
	if err != nil {
		return directPath // degrade to path-only rather than drop the ref
	}
	return string(b)
}

// mapMessage converts a whatsmeow message event into the FROZEN canonical
// event.Message. It is pure (no client/network) so it is fully unit-testable.
//
//	accountID — the user's account id, e.g. "wa:15555550123".
//	source    — event.SourceLive or event.SourceBackfill.
//	ingestedAt — when the pipeline received it (RFC3339 stamp).
func mapMessage(evt *events.Message, accountID, source string, ingestedAt time.Time) event.Message {
	info := evt.Info

	m := event.Message{
		Platform:    event.PlatformWhatsApp,
		AccountID:   accountID,
		ChatID:      chatIDForJID(info.Chat),
		ChatType:    chatTypeForJID(info.Chat, info.IsGroup),
		Sender:      mapSender(info),
		TS:          info.Timestamp,
		Mentions:    []string{},
		Attachments: []event.Attachment{},
		Reactions:   []event.Reaction{},
		Source:      source,
		IngestedAt:  ingestedAt,
	}
	m.SetID(info.ID)

	// Prefer the unwrapped message; fall back to the raw wrapper.
	body := evt.Message
	if body == nil {
		body = evt.RawMessage
	}
	classify(&m, body, mediaRefMessageKeyFromInfo(info))

	// Preserve the original platform payload untouched.
	if raw, err := json.Marshal(evt); err == nil {
		m.Raw = raw
	}

	return m
}
