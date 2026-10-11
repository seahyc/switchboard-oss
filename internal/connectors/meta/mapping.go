// Package meta is the switchboard connector for Instagram DMs and Facebook
// Messenger. It is a Matrix CLIENT of a self-hosted mautrix-meta bridge +
// Conduit homeserver running on the VM (see docs/META_CONNECTOR.md). The bridge
// is a SEPARATE process; we talk to it only over the standard Matrix
// client-server API, so none of its AGPL code links into this binary.
//
// One shared Client owns the single Matrix session + sync loop; two thin
// Connector facades (Name() "instagram" / "messenger") expose it to the daemon
// so canonical events carry distinct platforms (people-graph fit) while Send /
// React route cleanly via platformOf(chat_id).
//
// This file is the PURE, dependency-free mapping core: a normalized MatrixEvent
// (decoded by the mautrix glue in backend_mautrix.go) → the FROZEN canonical
// event.Message. It imports no Matrix library so it is fully unit-testable
// against hand-built events (see mapping_test.go) — mirroring how the WhatsApp /
// beeper connectors keep their mappers pure.
package meta

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// Platform strings this connector emits. Defined locally (not in the frozen
// event package) so we don't touch core's schema file; values match the brief's
// people-graph requirement.
const (
	PlatformInstagram = "instagram"
	PlatformMessenger = "messenger"
)

// Networks is the set of platforms this connector serves.
var Networks = []string{PlatformInstagram, PlatformMessenger}

// Matrix event type tokens we handle.
const (
	evTypeMessage   = "m.room.message"
	evTypeReaction  = "m.reaction"
	evTypeRedaction = "m.room.redaction"
	evTypeMember    = "m.room.member"
)

// Matrix msgtypes.
const (
	msgText   = "m.text"
	msgImage  = "m.image"
	msgVideo  = "m.video"
	msgAudio  = "m.audio"
	msgFile   = "m.file"
	msgEmote  = "m.emote"
	msgNotice = "m.notice"
)

// MatrixEvent is our normalized, library-agnostic view of a Matrix timeline
// event — the decoded fields the mapper needs. The mautrix glue
// (backend_mautrix.go) populates it from *event.Event; tests build it directly.
type MatrixEvent struct {
	RoomID   string
	EventID  string
	Sender   string // full mxid, e.g. "@meta_17841400000:meta.local"
	Type     string // evType* above
	Network  string // instagram | messenger — which bridge owns the room (set by glue)
	TSMillis int64  // origin_server_ts

	// Redaction.
	Redacts string

	// Message fields (Type == m.room.message).
	MsgType  string
	Body     string
	URL      string // mxc:// for media
	MIME     string
	Size     int64
	FileName string
	IsVoice  bool // bridge flags voice notes (org.matrix.msc3245.voice / IG voice)

	// Relations.
	IsEdit       bool   // m.relates_to rel_type == m.replace
	EditTarget   string // event id the edit replaces
	ReplyToEvent string // m.in_reply_to event id

	// Reaction (Type == m.reaction): annotation key + target.
	ReactionKey    string
	ReactionTarget string

	// Member/profile (Type == m.room.member): the ghost's displayname.
	DisplayName string

	// Mentions: remote user ids (already stripped from ghost mxids by the glue).
	Mentions []string

	// IG-specific rich content (reel/post/story share): an external link the
	// bridge attaches. Preserved into ui.card.
	ExternalURL string

	// Bridge-provided REMOTE ids (best-effort; "" when the bridge didn't stamp
	// them — the mapper then falls back to the Matrix ids).
	RemoteMsgID    string // stable remote message id → idempotency
	RemoteThreadID string // remote thread/chat id → chat_id
	RemoteUserID   string // remote sender id (else parsed from ghost mxid)

	IsGroup bool // portal is a group thread (else DM)

	Raw json.RawMessage // the original Matrix event JSON, untouched
}

// SelfSignals carries the user's own alias signals, stamped onto is_self
// messages so the gbrain registry gets handle/phone/email for the self persona.
type SelfSignals struct {
	MXID   string
	Handle string
	Phone  string
	Email  string
}

// mapEvent converts a normalized MatrixEvent into the FROZEN canonical
// event.Message for the given network ("instagram"/"messenger"). source is
// "live" or "backfill"; ghostPrefixes are the bridge's username-template
// prefixes used to parse remote user ids from ghost mxids. Returns ok=false for
// events that produce no canonical message (e.g. our own membership churn).
func mapEvent(ev MatrixEvent, network string, self SelfSignals, ghostPrefixes []string, source string, ingestedAt time.Time) (event.Message, bool) {
	switch ev.Type {
	case evTypeMessage, evTypeReaction, evTypeRedaction:
		// mappable below
	default:
		// m.room.member etc. are not messages; the connector consumes them for
		// profile/index upkeep, not for the firehose.
		return event.Message{}, false
	}

	m := event.Message{
		Platform:   network,
		AccountID:  network + ":" + self.MXID,
		ChatID:     canonicalChatID(network, ev),
		ChatType:   chatType(ev),
		TS:         time.UnixMilli(ev.TSMillis).UTC(),
		Source:     source,
		IngestedAt: ingestedAt.UTC(),
		Raw:        ev.Raw,
	}
	m.SetID(platformMsgID(ev))

	// Sender.
	remoteUID := ev.RemoteUserID
	if remoteUID == "" {
		remoteUID = remoteUserIDFromMXID(ev.Sender, ghostPrefixes)
	}
	isSelf := ev.Sender == self.MXID
	m.Sender = event.Sender{
		PlatformUserID: remoteUID,
		DisplayName:    ev.DisplayName,
		IsSelf:         isSelf,
	}
	if isSelf {
		m.Sender.Handle = self.Handle
		m.Sender.Phone = self.Phone
		m.Sender.Email = self.Email
		if m.Sender.PlatformUserID == "" {
			m.Sender.PlatformUserID = self.MXID
		}
	} else {
		m.Sender.Handle = handleFromMXID(ev.Sender, ghostPrefixes)
	}

	// Mentions (remote ids); the daemon maps self-mention → "@me anywhere".
	if len(ev.Mentions) > 0 {
		m.Mentions = append([]string(nil), ev.Mentions...)
	}

	switch ev.Type {
	case evTypeReaction:
		m.Kind = event.KindReaction
		m.ReplyTo = mappedID(network, ev.ReactionTarget, ev.RemoteMsgID)
		m.Reactions = []event.Reaction{{Emoji: ev.ReactionKey, By: remoteUID}}
		return m, true

	case evTypeRedaction:
		// IG "unsend" / Matrix redaction → a delete tombstone for the target.
		m.Kind = event.KindDelete
		m.Deleted = true
		m.ReplyTo = mappedID(network, ev.Redacts, "")
		return m, true
	}

	// evTypeMessage from here.
	m.Kind = kindOf(ev)
	m.Text = cleanBody(ev.Body)

	if ev.IsEdit {
		m.Kind = event.KindEdit
		t := m.TS
		m.EditedTS = &t
		// The edit points at the original message.
		m.ReplyTo = mappedID(network, ev.EditTarget, "")
	} else if ev.ReplyToEvent != "" {
		m.ReplyTo = mappedID(network, ev.ReplyToEvent, "")
	}

	// Media → attachment (ref = mxc://, fetched later by the gated media worker).
	if att, ok := attachmentOf(ev); ok {
		m.Attachments = []event.Attachment{att}
	}

	// IG rich share (reel/post/story) → ui.card, link preserved.
	if ev.ExternalURL != "" {
		card, _ := json.Marshal(map[string]string{"url": ev.ExternalURL, "body": ev.Body})
		m.UI.Card = json.RawMessage(card)
	}

	return m, true
}

// canonicalChatID is "<network>:<remoteThreadID>" when the bridge stamped a
// remote id, else "<network>:room:<matrixRoomID>" (stable + reversible).
func canonicalChatID(network string, ev MatrixEvent) string {
	if ev.RemoteThreadID != "" {
		return network + ":" + ev.RemoteThreadID
	}
	return network + ":room:" + ev.RoomID
}

// platformMsgID is the idempotency suffix: the remote message id when present
// (stable across live+backfill), else "evt:<matrixEventID>" (also globally
// unique + stable, so replay/overlap still dedupe). Combined with MakeID this
// yields "<network>:<id>".
func platformMsgID(ev MatrixEvent) string {
	if ev.RemoteMsgID != "" {
		return ev.RemoteMsgID
	}
	return "evt:" + ev.EventID
}

// mappedID builds the canonical id of a referenced message (reply/reaction/edit
// target). Prefer the remote id; else the Matrix event id form. Returns "" if
// neither is known.
func mappedID(network, matrixEventID, remoteMsgID string) string {
	if remoteMsgID != "" {
		return event.MakeID(network, remoteMsgID)
	}
	if matrixEventID != "" {
		return event.MakeID(network, "evt:"+matrixEventID)
	}
	return ""
}

func chatType(ev MatrixEvent) string {
	if ev.IsGroup {
		return event.ChatGroup
	}
	return event.ChatDM
}

// kindOf maps a message event to the canonical kind.
func kindOf(ev MatrixEvent) string {
	switch ev.MsgType {
	case msgImage, msgVideo, msgFile:
		return event.KindMedia
	case msgAudio:
		return event.KindMedia
	}
	// A sticker arrives as its own event type in newer specs but the bridge may
	// send it as m.image with a flag; treat dedicated sticker msgtype if present.
	if ev.MsgType == "m.sticker" {
		return event.KindSticker
	}
	return event.KindText
}

// attachmentOf extracts a single canonical attachment from a media message.
func attachmentOf(ev MatrixEvent) (event.Attachment, bool) {
	switch ev.MsgType {
	case msgImage, msgVideo, msgAudio, msgFile, "m.sticker":
	default:
		return event.Attachment{}, false
	}
	if ev.URL == "" {
		return event.Attachment{}, false
	}
	return event.Attachment{
		Type: attachmentType(ev),
		MIME: ev.MIME,
		Ref:  ev.URL, // mxc:// — DownloadMedia fetches via the Matrix media repo
		Name: ev.FileName,
		Size: ev.Size,
	}, true
}

func attachmentType(ev MatrixEvent) string {
	switch ev.MsgType {
	case msgImage:
		return "image"
	case msgVideo:
		return "video"
	case msgAudio:
		if ev.IsVoice {
			return "voice"
		}
		return "audio"
	case "m.sticker":
		return "sticker"
	}
	if ev.MIME != "" {
		if i := strings.IndexByte(ev.MIME, '/'); i > 0 {
			return ev.MIME[:i]
		}
	}
	return "file"
}

// cleanBody strips the bridge's reply-fallback quote ("> <@user> quoted text")
// that Matrix prepends to reply bodies, leaving just the new message text.
func cleanBody(body string) string {
	if !strings.HasPrefix(body, "> ") {
		return body
	}
	lines := strings.Split(body, "\n")
	i := 0
	for i < len(lines) && strings.HasPrefix(lines[i], "> ") {
		i++
	}
	// Skip a single blank separator line after the quote block.
	if i < len(lines) && strings.TrimSpace(lines[i]) == "" {
		i++
	}
	return strings.Join(lines[i:], "\n")
}

// remoteUserIDFromMXID extracts the bridge ghost's remote user id from a mxid
// like "@meta_17841400000:meta.local" given the configured ghost prefixes
// (e.g. "meta_", "instagram_"). Returns "" when the mxid isn't a ghost (e.g.
// the user's own account).
func remoteUserIDFromMXID(mxid string, prefixes []string) string {
	local := localpart(mxid)
	if local == "" {
		return ""
	}
	for _, p := range prefixes {
		if p != "" && strings.HasPrefix(local, p) {
			return strings.TrimPrefix(local, p)
		}
	}
	return ""
}

// handleFromMXID is a best-effort handle for a non-self sender: the remote id is
// numeric, so we don't have a username here — the ghost displayname / profile
// (set on m.room.member) is the real handle source, surfaced via ContactsProvider.
// Returns "" so the mapper doesn't fabricate a handle from a numeric id.
func handleFromMXID(mxid string, prefixes []string) string {
	return ""
}

func localpart(mxid string) string {
	if !strings.HasPrefix(mxid, "@") {
		return ""
	}
	rest := mxid[1:]
	if i := strings.IndexByte(rest, ':'); i >= 0 {
		return rest[:i]
	}
	return rest
}
