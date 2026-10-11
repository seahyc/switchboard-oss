package connectors

import (
	"context"
	"io"
	"time"
)

// Optional capability interfaces. The base Connector stays minimal + frozen;
// platforms that support richer actions implement these and the daemon
// type-asserts at call time, returning a clean "unsupported on <platform>"
// error when a connector doesn't implement one. This keeps support variance
// (WhatsApp polls vs Lark card actions vs Telegram inline keyboards) out of the
// base interface.

// ButtonTapper taps an inline/card button. Returns an optional result id.
type ButtonTapper interface {
	TapButton(ctx context.Context, chatID, msgID, buttonID string) (string, error)
}

// PollVoter casts a vote in a poll.
type PollVoter interface {
	VotePoll(ctx context.Context, chatID, msgID, option string) error
}

// GroupInviteLinker returns a shareable invite link for a group chat (#52, the card's
// "open group" affordance). reset=true rotates the link (invalidates the old one).
// Implemented by connectors whose platform exposes group invite links (WhatsApp).
type GroupInviteLinker interface {
	GroupInviteLink(ctx context.Context, chatID string, reset bool) (string, error)
}

// ChatMeta is connector-supplied, human-legible + organizational metadata for a
// chat (task #11). Title is the display name (group subject / DM counterpart);
// the org signals are normalized across platforms so the agent can prioritize.
type ChatMeta struct {
	ChatID     string
	Title      string
	Archived   bool
	Muted      bool
	MutedUntil *time.Time // when the mute expires, if the platform exposes it
	Pinned     bool
	Labels     []string // folders/labels (Telegram dialog filters, Gmail labels, …)
	Importance string   // optional connector hint; core's scorer is authoritative
}

// ChatMetadataProvider is implemented by connectors that can enumerate their
// chats' names + org signals (Telegram dialogs, WhatsApp chat list, Lark chats,
// Gmail labels). The daemon polls it and upserts into the store's chat
// projection — separate from per-message ingestion, so the FROZEN event schema
// is untouched.
type ChatMetadataProvider interface {
	ChatMetadata(ctx context.Context) ([]ChatMeta, error)
}

// Contact is one entry from a platform's address book / contact store — the
// AUTHORITATIVE name source for the people graph. A message sender's display name
// is blank for many senders (e.g. WhatsApp @lid participants), so the contact
// store is where real human names come from. Connectors fill the fields they have:
//   - WhatsApp (whatsmeow contact store): FullName/FirstName/PushName/BusinessName
//   - Telegram: FirstName/LastName/Handle(username)
//   - Lark: FullName/Email/Phone from the directory
//   - Gmail: FullName + Email from "From" headers / contacts
type Contact struct {
	UserID       string // platform user id (matches event.Sender.PlatformUserID)
	Phone        string
	Email        string
	Handle       string // @username
	FullName     string // user-assigned address-book name
	FirstName    string
	LastName     string
	PushName     string // self-set profile name
	BusinessName string
	IsSelf       bool
}

// ContactsProvider is the optional capability a connector implements to expose its
// address book to the people-graph projector. It mirrors ChatMetadataProvider: the
// daemon polls it on an interval and feeds the result to the gbrain-feed projector
// (which projects EVERY contact, not just observed senders — so everyone the user
// knows gets a named person page even with no message history). Connectors that
// can't enumerate contacts simply don't implement it.
type ContactsProvider interface {
	Contacts(ctx context.Context) ([]Contact, error)
}

// MediaDownloader is implemented by connectors that can fetch the bytes of a
// previously-observed attachment. fetchRef is the connector-specific handle the
// connector placed on event.Attachment.Ref at ingest time (WhatsApp media
// keys/directpath, a Telegram file location, a Lark file_key, a Gmail
// attachment id). The media worker calls this OFF the ingest path, gated by chat
// importance + a per-file size ceiling, and content-addresses + dedups the
// result. Returns the byte stream and the content length (0 if unknown).
// Connectors without downloadable media simply don't implement it.
type MediaDownloader interface {
	DownloadMedia(ctx context.Context, fetchRef string) (io.ReadCloser, int64, error)
}

// MediaSendRequest is an outbound media send. Body contains already-resolved
// bytes from switchboard's media store; connector-specific attachment refs are
// intentionally not reused for outbound sends because those refs are download
// handles, not portable upload payloads.
type MediaSendRequest struct {
	ChatID          string
	Text            string // optional caption
	ReplyTo         string
	Type            string
	MIME            string
	Name            string
	Size            int64
	SourceMediaID   string
	SourceMessageID string
	MarkForwarded   bool
	AsDocument      bool
	Body            io.Reader
}

// MediaSender is implemented by connectors that can upload and send media
// bytes. It is deliberately separate from SendRequest.Attachments, which holds
// captured platform refs rather than outbound byte streams.
type MediaSender interface {
	SendMedia(ctx context.Context, req MediaSendRequest) (sentID string, err error)
}

// RefDeriver re-derives a complete media-download Ref from a stored raw event
// payload (event.Message.Raw). It makes media capture replay-safe: when an
// attachment's Ref is path-only/insufficient (e.g. ingested before the connector
// serialized the full decrypt tuple), the media-enqueue path rebuilds a full Ref
// from raw — which the connector preserved untouched — so reprojection always
// produces downloadable Refs without re-hitting the platform for metadata.
// Returns "" when raw has no recoverable media.
type RefDeriver interface {
	DeriveMediaRef(raw []byte) string
}

// MediaRefReextractor rebuilds a downloadable media Ref from a stored raw payload
// PLUS the canonical chat id — for connectors where the raw alone is insufficient.
// Telegram is the case: gotd's tg.Message has INTERFACE fields (Media, PeerID) that
// don't round-trip through encoding/json, so a plain RefDeriver(raw) can't recover
// the peer (needed to refresh a volatile file_reference). The chat id is supplied by
// the recovery reproject (which holds the canonical message). Returns "" when raw has
// no recoverable media. Used by the one-time #48 media-ref recovery.
type MediaRefReextractor interface {
	ReextractMediaRef(raw []byte, chatID string) string
}

// MediaLocalDecryptor decrypts already-downloaded ciphertext bytes IN PLACE using the
// key carried in the stored ref — NO network. WhatsApp implements it: media stored
// undecrypted (the path-only fallback downloaded the .enc without the mediaKey) can be
// recovered by decrypting the on-disk bytes with the mediaKey from the ref, which also
// recovers media whose CDN URL has since expired (a re-download would lose those). ref
// is the connector's Attachment.Ref; enc is the on-disk ciphertext. Returns "" -err when
// the ref lacks a key / the bytes aren't this connector's ciphertext.
type MediaLocalDecryptor interface {
	DecryptLocalMedia(ref string, enc []byte) ([]byte, error)
}

// InteractiveAuth is implemented by connectors whose login needs step-by-step
// user input (e.g. Telegram userbot: phone → code → 2FA password). The daemon
// drives it: it surfaces the connector's auth phase (via Status/Detail) and
// feeds each value back as the user replies. Implemented by *telegram.Connector.
type InteractiveAuth interface {
	SubmitPhone(ctx context.Context, phone string) error
	SubmitCode(ctx context.Context, code string) error
	SubmitPassword(ctx context.Context, password string) error
}

// ChatStarter is implemented by connectors that can START a brand-new
// conversation with a contact who has no existing chat — e.g. an Instagram
// cold-DM to a handle the user has never messaged. It resolves a
// platform-specific identifier (a @username or a numeric id) to a canonical
// chat_id, creating the underlying conversation/portal if needed, so a
// subsequent Send to that chat_id reaches the contact. resolvedName is the
// human-confirmable display name of the resolved target (so the caller can
// verify it's the right account before sending). Connectors that can't start
// cold conversations simply don't implement it.
type ChatStarter interface {
	StartChat(ctx context.Context, identifier string) (chatID, resolvedName string, err error)
}

// ParticipantReactor reacts to a message when the platform needs the target
// message's sender id (e.g. WhatsApp group reactions, where whatsmeow's
// reaction key requires the original participant JID). The daemon resolves the
// sender from the store and calls this when available; connectors that don't
// implement it fall back to the base Connector.React.
type ParticipantReactor interface {
	ReactAs(ctx context.Context, chatID, msgID, senderID, emoji string) error
}
