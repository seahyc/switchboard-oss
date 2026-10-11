// Package event defines the FROZEN canonical message event contract for
// switchboard. Every connector (WhatsApp, Telegram, Lark) MUST emit exactly
// this Message shape. Schema changes go through the lead (core) only.
//
// The JSON contract mirrors docs/SPEC.md verbatim — field names, nesting, and
// semantics are part of the frozen interface. Do not rename or drop fields.
package event

import (
	"encoding/json"
	"fmt"
	"time"
)

// Platform identifiers. The global event id is "<platform>:<platform_msg_id>".
const (
	PlatformWhatsApp  = "whatsapp"
	PlatformTelegram  = "telegram"
	PlatformLark      = "lark"
	PlatformGmail     = "gmail"
	PlatformInstagram = "instagram"
	PlatformX         = "x"
	PlatformSlack     = "slack"
)

// Chat types.
const (
	ChatDM      = "dm"
	ChatGroup   = "group"
	ChatChannel = "channel"
	// ChatEmail is the Gmail thread-as-chat type. An email thread is modeled as a
	// chat; To/Cc recipients are modeled as mentions (To/Cc of the user = "@me").
	ChatEmail = "email"
	// ChatFeed is a social-feed pseudo-chat (#49): a follow's posts/stories, NOT a
	// conversation. Bucketed per-author as "<platform>:feed:<author_id>" so feed
	// activity stays out of DM/group reads while remaining author-attributable.
	ChatFeed = "feed"
)

// Message kinds.
const (
	KindText     = "text"
	KindMedia    = "media"
	KindSticker  = "sticker"
	KindPoll     = "poll"
	KindCard     = "card"
	KindReaction = "reaction"
	KindEdit     = "edit"
	KindDelete   = "delete"
	KindSystem   = "system"
	// KindSocialActivity is a follow's feed post/story (#49) — a social-graph event
	// distinct from a message/DM. The Social field carries the feed-specific data.
	KindSocialActivity = "social_activity"
)

// Source of an event in the pipeline.
const (
	SourceLive     = "live"
	SourceBackfill = "backfill"
)

// Sender carries the per-platform identity plus alias signals that feed the
// gbrain entity registry (handle / phone / email).
type Sender struct {
	PlatformUserID string `json:"platform_user_id"`
	Handle         string `json:"handle"`
	Phone          string `json:"phone"`
	Email          string `json:"email"`
	DisplayName    string `json:"display_name"`
	IsSelf         bool   `json:"is_self"`
}

// Attachment describes a media/file payload reference (not the bytes).
type Attachment struct {
	Type string `json:"type"`
	MIME string `json:"mime"`
	Ref  string `json:"ref"`
	Name string `json:"name"`
	Size int64  `json:"size"`
	// Hash is the sha256 (hex) of the downloaded bytes — the content-addressed
	// dedup key for the media store. Empty until the media worker has fetched the
	// bytes. Additive/optional: omitted from JSON when unset so older events and
	// connectors that don't populate it are unaffected.
	Hash string `json:"hash,omitempty"`
	// Stored reports whether the bytes are currently held in the local media
	// store. false = ref-only: never downloaded (gated out), not yet fetched, or
	// evicted under the size cap — re-fetchable on demand via Ref.
	Stored bool `json:"stored,omitempty"`
}

// Reaction is a single emoji reaction by a platform user.
type Reaction struct {
	Emoji string `json:"emoji"`
	By    string `json:"by"`
}

// UI preserves rich/custom platform UI (inline buttons, polls, cards) so the
// agent can re-render or act on it. Fields are intentionally loose (raw JSON)
// because each platform's shape differs; connectors fill what they have.
type UI struct {
	Buttons []json.RawMessage `json:"buttons"`
	Poll    json.RawMessage   `json:"poll"`
	Card    json.RawMessage   `json:"card"`
}

// Message is the canonical, normalized event. FROZEN contract — see SPEC.
type Message struct {
	// ID is the GLOBAL id "<platform>:<platform_msg_id>" and the idempotency key.
	ID          string       `json:"id"`
	Platform    string       `json:"platform"`
	AccountID   string       `json:"account_id"`
	ChatID      string       `json:"chat_id"`
	ChatType    string       `json:"chat_type"`
	Sender      Sender       `json:"sender"`
	TS          time.Time    `json:"ts"`
	Kind        string       `json:"kind"`
	Text        string       `json:"text"`
	ReplyTo     string       `json:"reply_to"`
	ThreadID    string       `json:"thread_id"`
	Mentions    []string     `json:"mentions"`
	Attachments []Attachment `json:"attachments"`
	UI          UI           `json:"ui"`
	Reactions   []Reaction   `json:"reactions"`
	EditedTS    *time.Time   `json:"edited_ts"`
	Deleted     bool         `json:"deleted"`
	Source      string       `json:"source"`
	IngestedAt  time.Time    `json:"ingested_at"`
	// Social carries feed-specific fields for KindSocialActivity events (#49). Pointer
	// + omitempty → absent (and byte-identical on the wire/disk) for every message/DM
	// event, keeping the frozen envelope unchanged; only social_activity events set it.
	Social *Social `json:"social,omitempty"`
	// TargetID identifies the original message for an edit or deletion event.
	TargetID string `json:"target_id,omitempty"`
	// Raw is the original platform payload, untouched.
	Raw json.RawMessage `json:"raw"`
}

// Social is the feed-specific payload for a KindSocialActivity event (#49): a follow's
// post or story. The author is the event's Sender; the caption is Text; media is in
// Attachments. Engagement is a SNAPSHOT at first-ingest (events are idempotent on id, so
// re-ingest won't update growing like/comment counts — acceptable for v1).
type Social struct {
	Sub       string     `json:"sub"`                  // post | story | repost | comment | follow
	URL       string     `json:"url,omitempty"`        // permalink
	Likes     int        `json:"likes,omitempty"`      // snapshot at ingest
	Comments  int        `json:"comments,omitempty"`   // snapshot at ingest
	ExpiresAt *time.Time `json:"expires_at,omitempty"` // stories (~24h); nil for posts

	// Follow-identity classification signals (#49), set ONLY for Sub=="follow" — a
	// follow-list entry ingested so the followed account becomes a durable identity
	// node (independent of whether they've posted). people-layer's ClassifyFollow
	// re-derives the node type/category from these AT PROJECTION TIME (durable-by-
	// construction; node_type is intentionally not persisted), so they must travel
	// in the event. All omitempty + nil-pointer for non-follow events → byte-identical
	// on wire/disk for every existing post/story event (#1 additive-only rule).
	Mutual     *bool  `json:"mutual,omitempty"`      // they follow the user back
	IsBusiness *bool  `json:"is_business,omitempty"` // professional/creator account flag
	Followers  int    `json:"followers,omitempty"`   // follower scale (creator-vs-friend signal)
	Category   string `json:"category,omitempty"`    // IG-supplied account category hint

	// Engagement amplification metrics (#55), set for X/Twitter posts (platform=x).
	// Likes + Comments(=replies) above already cover the shared axes; these carry the
	// rest of the X engagement surface used by the briefing's virality scorer. All
	// omitempty → byte-identical on wire/disk for every existing IG/non-X event.
	Retweets int `json:"retweets,omitempty"` // reposts (amplification)
	Quotes   int `json:"quotes,omitempty"`   // quote-tweets (amplification)
	Views    int `json:"views,omitempty"`    // impressions (reach denominator)
}

// MakeID builds the canonical global/idempotency id for an event:
// "<platform>:<platform_msg_id>". This is the single source of truth for id
// construction — connectors and the pipeline MUST use it so dedupe is stable.
func MakeID(platform, platformMsgID string) string {
	return fmt.Sprintf("%s:%s", platform, platformMsgID)
}

// SetID computes and assigns m.ID from the platform + the given platform message id.
func (m *Message) SetID(platformMsgID string) {
	m.ID = MakeID(m.Platform, platformMsgID)
}
