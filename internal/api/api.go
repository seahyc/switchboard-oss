// Package api is the local HTTP boundary between the daemon (switchboardd) and
// the MCP server (switchboard-mcp). The MCP server is a thin stdio process that
// translates agent tool calls into HTTP requests against the daemon, which owns
// the store, the live connectors, and the push event stream.
//
// Transport is authenticated JSON over HTTP on a loopback address.
// Archive readers and administrators use separate bearer credentials.
package api

import (
	"context"
	"io"
	"time"

	"github.com/seahyc/switchboard-oss/internal/connectors"
	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/igweb"
	"github.com/seahyc/switchboard-oss/internal/store"
	"github.com/seahyc/switchboard-oss/internal/xweb"
)

// DefaultAddr is the loopback address the daemon binds and the MCP client dials.
const DefaultAddr = "127.0.0.1:8717"

// Backend is what the HTTP server needs from the daemon: read access to the
// store plus high-level write actions. The daemon implements these by resolving
// the right connector (by the chat id's platform prefix) and, for Backfill,
// wiring the connector's output into the shared ingestion pipeline.
type Backend interface {
	Store() *store.Store
	Send(ctx context.Context, req connectors.SendRequest) (sentID string, err error)
	SendMedia(ctx context.Context, req SendMediaBody) (sentID string, err error)
	React(ctx context.Context, chatID, msgID, emoji string) error
	Backfill(ctx context.Context, chatID string, since time.Time) error
	// Reproject rebuilds the gbrain identity registry from the durable store.
	Reproject(ctx context.Context) (int, error)
	// ReextractLIDPhones re-parses stored WhatsApp @lid message Raw to backfill the
	// sender_phone column (#33 one-time recovery for messages ingested before the
	// SenderAlt capture). Returns rows updated; follow with Reproject.
	ReextractLIDPhones(ctx context.Context) (int, error)
	// ReextractMediaRefs re-derives downloadable media refs from stored Raw for a
	// platform whose old rows hold a thin/empty ref (#48 recovery; Telegram). Resets
	// the matching failed/skipped rows to pending so the bytes can be fetched. Returns
	// rows refreshed.
	ReextractMediaRefs(ctx context.Context, platform, chatID string) (int, error)
	// RevalidateMedia re-checks stored media blobs against their declared type and
	// resets the undecrypted (ciphertext) ones for re-download (#48). Returns
	// (checked, reset).
	RevalidateMedia(ctx context.Context, platform string) (checked, decrypted, reset int, err error)
	// IngestSocialActivity pulls the IG following-feed + story tray + complete follow
	// list, writing each as a canonical social_activity event (#49): posts/stories are
	// activity; follows are lightweight identity-establishing events (Sub="follow") so
	// every followed account becomes a durable node for classify/merge even if it never
	// posted. Returns (posts, stories, follows) enqueued. On-demand.
	IngestSocialActivity(ctx context.Context, limit int) (posts, stories, follows int, err error)
	// IngestXActivity pulls the X (Twitter) following-feed and writes each post as a
	// canonical social_activity event (#55, platform=x). Ingests everything to the log;
	// the briefing filters at read time. Returns the count enqueued. On-demand.
	IngestXActivity(ctx context.Context, limit int) (int, error)
	// DiscoverLarkP2P finds the user's Lark p2p DMs — which Im.Chat.List omits, so
	// they're otherwise invisible to backfill/poll (#31) — via message search, then
	// backfills each newly-found DM. ASYNC: launches in the background (the run can
	// take minutes) and returns queued=true immediately, or queued=false if a run is
	// already in flight; the discovered-count is logged. query overrides the stopword
	// seed set; maxSeeds>0 seeds the sender filter from known Lark identities, capped.
	DiscoverLarkP2P(ctx context.Context, query string, maxSeeds int) (queued bool, err error)
	// SweepBackfill triggers a bounded, salience-ordered full-history backfill of
	// never-swept chats (#13) — the fix for chats_known >> chats_with_messages (the
	// on-demand-only backfill gap). Async: returns the preflight (candidates selected,
	// low-tier gated count+sample) immediately; the backfill runs in the background.
	SweepBackfill(ctx context.Context, platform string, maxChats int) (SweepResult, error)
	// RunDeterministicMerges applies the deterministic corroborated-exact-name
	// merge policy over the graph + drains qualifying pending proposals (no LLM).
	// dryRun=true logs the would-merge candidates without applying; returns the
	// count merged (or would-merge in dry-run).
	RunDeterministicMerges(ctx context.Context, dryRun bool) (int, error)
	// RunDeterministicProposals refreshes the deterministic propose-only queue (#19):
	// strong cross-platform pairs (shared handle / handle↔email-localpart) surfaced for
	// human review. No LLM, no auto-merge, idempotent. Returns new-proposal count.
	RunDeterministicProposals(ctx context.Context) (int, error)
	// TapButton/VotePoll are routed to the connector's optional capability
	// interfaces; they return a clean error when the platform doesn't support it.
	TapButton(ctx context.Context, chatID, msgID, buttonID string) (string, error)
	VotePoll(ctx context.Context, chatID, msgID, option string) error
	// GroupInvite returns a shareable invite link for a group chat (#52, the card's
	// "open group" affordance). reset=true rotates the link. Asks the platform for the
	// link (not an outbound message), so it's not in the sendgate set.
	GroupInvite(ctx context.Context, chatID string, reset bool) (string, error)
	// HonchoBackfill replays existing inner-tier WhatsApp DM history into Honcho (bounded,
	// idempotent via the per-session watermark). No-op when ingestion is flagged off.
	// Returns (chats, messages) processed.
	HonchoBackfill(ctx context.Context, maxChats, perChat int) (chats, msgs int, err error)
	// StartChat resolves a platform identifier (a handle/username or id) to a
	// canonical chat_id, creating the conversation if needed (e.g. an Instagram
	// cold-DM). Returns the chat_id + the resolved display name for confirmation.
	StartChat(ctx context.Context, platform, identifier string) (chatID, resolvedName string, err error)
	// IGProfile / IGStories are the READ-ONLY Instagram browser (#25): they read
	// the user's IG network via the private web API through the residential proxy
	// (egress-verified inside the client). IGProfile returns a profile card for a
	// handle; IGStories returns the current story tray.
	IGProfile(ctx context.Context, handle string) (igweb.ProfileCard, error)
	IGStories(ctx context.Context) (igweb.StoriesResult, error)
	// IGFeed reads the home feed (recent posts from people the user follows).
	// maxID is an opaque pagination cursor ("" = first page); limit (>0)
	// auto-paginates internally to return up to that many real posts.
	IGFeed(ctx context.Context, maxID string, limit int) (igweb.FeedResult, error)
	// IGFollowingFeed reads recent posts from ONLY the accounts the user follows,
	// merged chronologically, with no recommendations/ads (#47). limit (<=0 →
	// default) caps the merged result.
	IGFollowingFeed(ctx context.Context, limit int) (igweb.FeedResult, error)
	// IGFollowing returns the COMPLETE list of accounts the user follows, each
	// enriched with friend-vs-creator signals (followers/bio/is_business) for #49.
	IGFollowing(ctx context.Context) (igweb.FollowingList, error)
	// XFollowingFeed reads the chronological X "Following" timeline — recent posts
	// from accounts the user follows, newest-first, promoted/ads dropped (#50).
	XFollowingFeed(ctx context.Context, limit int) (xweb.FeedResult, error)
	// XProfile reads a read-only X profile card + the account's recent tweets.
	XProfile(ctx context.Context, handle string, limit int) (xweb.ProfileCard, error)
	// Auth drives interactive login (Telegram phone/code/2FA): AuthStatus
	// reports a connector's current login state; AuthSubmit feeds a user-supplied
	// value (field = phone|code|password) to the connector.
	AuthStatus(ctx context.Context, platform string) (connectors.LoginStatus, error)
	AuthSubmit(ctx context.Context, platform, field, value string) error
	// ConnectorHealth is a read-only health snapshot for one connector (#37): connected
	// + last_error (surfaces auth failures) + newest ingested ts + account. Backs GET
	// /connectors/{name}/health for olympus's re-auth auto-ack. No mutation.
	ConnectorHealth(ctx context.Context, name string) (ConnectorHealth, error)
	// Observability is the phase-2 read-only health snapshot: per-platform coverage,
	// ingestion lag, connector health, and last backfill-sweep convergence delta.
	// Backs GET /observability. No mutation.
	Observability(ctx context.Context) (Observability, error)
	// People review flow (human-in-the-loop canonical-persona mapping). The agent
	// lists PendingProposals and resolves each by its stable id (approve→merge,
	// reject→never re-propose); RenameCanonical applies a confirmed name to one
	// canonical person (decoupled from proposals — a rename targets a single id).
	// PendingProposals returns a page of pending proposals (limit≤0 = default
	// page; offset for paging) — the full set can be hundreds, too large to
	// render at once.
	PendingProposals(ctx context.Context, limit, offset int) ([]ProposalDTO, error)
	// ResolveProposal applies a decision; on "approve" it returns the merge
	// outcome (survivor id+name) so the agent can confirm + rename the survivor.
	// nil outcome on reject.
	ResolveProposal(ctx context.Context, id, decision string) (*ResolveOutcome, error)
	RenameCanonical(ctx context.Context, canonID, name string) error
	// MergeCanonicals force-merges two operator-chosen canonical ids (a human-
	// directed merge with no auto-proposal — e.g. a name too ambiguous for the
	// propose logic to surface). Durable: routes through the same ApproveMerge path
	// as a proposal-approve (force-merge + persist + merged_into provenance #17),
	// so it survives reproject. Returns the survivor outcome. Idempotent.
	MergeCanonicals(ctx context.Context, a, b string) (*ResolveOutcome, error)
	// BackfillSenderNames fills blank message sender_name/sender_handle for a
	// platform's existing rows by resolving each blank sender_id through the
	// people-graph (#54 historical-row backfill; the forward path is populated at
	// ingest). Idempotent (only fills blanks). Returns scan/fill/unresolved counts.
	BackfillSenderNames(ctx context.Context, platform string) (*BackfillResult, error)
	// ResolvePerson resolves a free-text name (or alias) to ranked canonical
	// person cards — the "who is this / how do I reach them" lookup. Accent- and
	// case-insensitive matching is the implementation's responsibility (callers
	// pass the query raw). Returns best-first cards; empty slice = no one found
	// (not an error). people-layer owns the graph-backed implementation (the
	// daemon adapts its projector ResolvePerson to this signature); the handler
	// wraps the cards in the {query,in_graph,hits} envelope.
	ResolvePerson(ctx context.Context, q string) ([]PersonCard, error)
	// CoreNetwork returns the user's salience-ranked people (#15) — optional tier
	// filter (inner|active|peripheral|dormant) + limit cap. Cards carry
	// salience_score/salience_tier + last_interaction. Backs GET /people/core_network
	// (people-layer's salience scorer; the daemon adapts its projector CoreNetwork).
	CoreNetwork(ctx context.Context, tier string, limit int) ([]PersonCard, error)
	// RecomputeSalience forces a fresh people-graph salience recompute (#15) — the
	// deterministic no-LLM scorer, decoupled from the shelved #19 matcher. Backs POST
	// /people/salience/recompute (calibration / post-reproject refresh). Returns the
	// number of scored people.
	RecomputeSalience(ctx context.Context) (int, error)
	// Search runs FTS over the firehose. When person != "", the daemon resolves
	// that name to its identity set (sender_ids, via ResolvePerson incl. the Lark
	// directory fallback) and filters messages by AUTHOR — not the by-design-blank
	// sender_name (#32). person == "" searches everything. Newest-first.
	Search(ctx context.Context, query, platform, person string, since time.Time, limit int) ([]event.Message, error)
	// Grep runs a regex (RE2) scan over message text — the #16 firehose grep, the
	// regex counterpart to Search's FTS. person (a NAME) narrows to that person's
	// messages via ResolvePerson→sender_ids; platform/since narrow further. A bad
	// pattern returns a store.ErrInvalidPattern-wrapped error (mapped to 400).
	Grep(ctx context.Context, pattern, platform, person string, since time.Time, limit int) (GrepResult, error)
	// QuerySQL runs an arbitrary READ-ONLY SELECT over the firehose (#36) — the "let the
	// agent SQL the message history" primitive. Hard-guarded: single SELECT/CTE only
	// (parse-guard), a dedicated mode=ro+query_only connection (can't write), a 5s
	// statement timeout, and a 1000-row cap (Truncated). A bad/unsafe query is a clean
	// error (store.ErrUnsafeSQL → 400), never a write or a 500. Every query is logged.
	QuerySQL(ctx context.Context, query string) (store.QuerySQLResult, error)
	// Media store: MediaInfo returns an attachment's metadata + local blob path
	// (re-fetching ref-only media on demand); MediaStream returns the bytes.
	// Both error cleanly when the media store is disabled.
	MediaInfo(ctx context.Context, id string) (MediaMeta, error)
	MediaStream(ctx context.Context, id string) (io.ReadCloser, MediaMeta, error)
	// ReprojectMedia retro-enqueues historical attachment-bearing events for the
	// media worker (call once after enabling the media store). chatID scopes it
	// to one chat (bounded rollout); empty = the whole backlog.
	ReprojectMedia(ctx context.Context, chatID string) (int, error)
}

// --- Wire types (requests/responses) ---

// ResolveResult is the GET /people/resolve response envelope: the echoed query,
// whether anyone matched in the people graph, and the ranked candidate cards
// (best-first). The handler builds this around the Backend's []PersonCard.
type ResolveResult struct {
	Query   string       `json:"query"`
	InGraph bool         `json:"in_graph"`
	Hits    []PersonCard `json:"hits"`
}

// PersonCard is one resolved identity — the structured "who is this" card the
// agent renders instead of grepping messages to find a user. Shape is owned
// jointly with people-layer's projector (which produces it); keep the JSON tags
// stable. Additive fields only.
type PersonCard struct {
	CanonicalID     string         `json:"canonical_id"`
	Name            string         `json:"name"`
	Identified      bool           `json:"identified"` // false = unmerged/unknown-name persona
	Platforms       []string       `json:"platforms"`
	Identities      []CardIdentity `json:"identities"`
	Emails          []string       `json:"emails,omitempty"`
	Phones          []string       `json:"phones,omitempty"`
	Handles         []string       `json:"handles,omitempty"`
	OpenIDs         []string       `json:"open_ids,omitempty"` // lark ou_… open ids
	SalienceScore   float64        `json:"salience_score,omitempty"`
	SalienceTier    string         `json:"salience_tier,omitempty"`    // core-network tier (#15): inner/active/peripheral/dormant
	LastInteraction int64          `json:"last_interaction,omitempty"` // unix seconds of the most recent interaction (#15)
	Type            string         `json:"type,omitempty"`             // node type (#49): person|entity (default person)
	Category        string         `json:"category,omitempty"`         // node category (#49): friend|family|colleague | creator|brand|org|service
	MatchedOn       string         `json:"matched_on"`                 // what the query hit (name/alias/handle)
	Source          string         `json:"source,omitempty"`           // "graph" (people graph) | "lark" (org-directory fallback, #24 P2)

	// Merge-on-read (#24): when one person is fragmented across per-platform
	// canonicals, ResolvePerson composes the fragments into ONE card. MemberIDs are
	// the underlying canonical_ids (so the user can confirm→real-merge or split).
	// MergeConfidence: "high" = fragments share a corroborating signal (handle/phone/
	// email-localpart) → safe composite; "same_name_unverified" = same name but NO
	// corroboration → may be DISTINCT people (kept as separate cards, flagged so the
	// agent never silently fuses two different "John Tan"s). Empty = single canonical.
	MemberIDs       []string `json:"member_ids,omitempty"`
	MergeConfidence string   `json:"merge_confidence,omitempty"`

	// RecentChats (#32) are the person's most-recent conversations (newest-first),
	// so an agent can go person→chat→read directly instead of guessing a chat_id.
	// people-layer resolves the name→sender_id set; core2's handler fills this from
	// the store (recent chats where sender_id ∈ the person's identities), incl. DMs.
	RecentChats []ChatRef `json:"recent_chats,omitempty"`
}

// ChatRef is a lightweight pointer to a conversation the resolved person appears in:
// the chat_id to open, when they were last active there, and a snippet for context.
// Title is the chat's display title (for a 1:1 it's the derived participant name, #31).
type ChatRef struct {
	ChatID  string `json:"chat_id"`
	Title   string `json:"title,omitempty"`
	LastTS  int64  `json:"last_ts,omitempty"` // unix seconds of the person's last message there
	Snippet string `json:"snippet,omitempty"`
}

// GrepResult is the #16 firehose-grep response: regex matches (newest-first) plus scan
// accounting so the agent can distinguish an EXHAUSTIVE result from one cut short at
// the scan cap. Truncated=true → the cap was hit before the whole history was searched
// (older matches may exist); Hint suggests how to narrow. Scanned is rows examined.
type GrepResult struct {
	Matches   []event.Message `json:"matches"`
	Scanned   int             `json:"scanned"`
	Truncated bool            `json:"truncated"`
	Hint      string          `json:"hint,omitempty"`
}

// ConnectorHealth is the read-only health snapshot for one connector (#37) — used by
// olympus to auto-ack "inbox sync live" after a re-auth restart. connected: authed +
// running (no auth/conn error). last_error: the connector's last error ("" when
// healthy — surfaces invalid_grant / auth failures). newest_ts: newest ingested
// message ts for the platform (RFC3339; "" if none) → "caught up through <time>".
// account: the connector's account id (e.g. the gmail address).
type ConnectorHealth struct {
	Connected bool   `json:"connected"`
	LastError string `json:"last_error"`
	NewestTS  string `json:"newest_ts,omitempty"`
	Account   string `json:"account,omitempty"`
}

// QuerySQLBody is the POST /query-sql request (#36): the read-only SELECT to run.
type QuerySQLBody struct {
	SQL string `json:"sql"`
}

// SweepResult is the #13 backfill-sweep preflight: what the (async) run will do —
// how many UNswept salience-ordered chats it queued, and the count + a sample of the
// explicit 'low'-tier chats it GATED (so we can eyeball bot/broadcast/dead-shell vs
// real before deciding whether to sweep those too). Queued=false → nothing to sweep
// (converged / all gated) or a sweep is already in flight.
type SweepResult struct {
	Queued     bool     `json:"queued"`
	Candidates int      `json:"candidates"`
	LowGated   int      `json:"low_gated"`
	LowSample  []string `json:"low_sample,omitempty"`
}

// Observability is the phase-2 read-only health snapshot (#39): per-platform coverage,
// ingestion lag, connector health, and last backfill-sweep convergence delta. One
// aggregate a watching agent / the morning briefing can poll.
type Observability struct {
	GeneratedAt string                  `json:"generated_at"`
	Platforms   []PlatformObservability `json:"platforms"`
}

// PlatformObservability is one platform's row in the observability snapshot.
type PlatformObservability struct {
	Platform          string  `json:"platform"`
	ChatsKnown        int     `json:"chats_known"`
	ChatsWithMessages int     `json:"chats_with_messages"`
	Messages          int     `json:"messages"`
	CoveragePct       float64 `json:"coverage_pct"`
	NewestTS          string  `json:"newest_ts,omitempty"`
	// IngestionLagSeconds is now − newest message ts; large/growing → ingestion stalled.
	IngestionLagSeconds int64 `json:"ingestion_lag_seconds"`
	// ConnectorConnected is nil when no live connector is registered for the platform.
	ConnectorConnected *bool           `json:"connector_connected,omitempty"`
	ConnectorLastError string          `json:"connector_last_error,omitempty"`
	LastSweep          *BackfillRunDTO `json:"last_sweep,omitempty"`
}

// BackfillRunDTO is the last recorded sweep run for a platform (convergence delta).
type BackfillRunDTO struct {
	TS             string `json:"ts"`
	Swept          int    `json:"swept"`
	Failed         int    `json:"failed"`
	Flooded        bool   `json:"flooded"`
	IngestedBefore int    `json:"ingested_before"`
	IngestedAfter  int    `json:"ingested_after"`
}

// CardIdentity is one platform identity attached to a canonical person.
type CardIdentity struct {
	Platform    string `json:"platform"`
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name"`
	Handle      string `json:"handle"`
}

// SendBody is the POST /send payload.
type SendBody struct {
	ChatID      string             `json:"chat_id"`
	Text        string             `json:"text"`
	ReplyTo     string             `json:"reply_to,omitempty"`
	Attachments []event.Attachment `json:"attachments,omitempty"`
	Poll        any                `json:"poll,omitempty"`
	Card        any                `json:"card,omitempty"`
	Buttons     any                `json:"buttons,omitempty"`
}

// SendResult is the POST /send response.
type SendResult struct {
	SentID string `json:"sent_id"`
}

// StartChatBody is the POST /start_chat payload. Identifier is a platform handle
// (e.g. an IG @username) or a numeric remote id.
type StartChatBody struct {
	Platform   string `json:"platform"`
	Identifier string `json:"identifier"`
}

// StartChatResult is the POST /start_chat response: the canonical chat_id a Send
// can target, plus the resolved display name (so the caller can confirm the
// right contact before sending a cold message).
type StartChatResult struct {
	ChatID       string `json:"chat_id"`
	ResolvedName string `json:"resolved_name"`
}

// SendMediaBody is the POST /send_media payload. media_id is an already-captured
// attachment id ("<event_id>#<idx>"); the daemon resolves bytes from the media
// store and asks the target connector to upload/send them.
type SendMediaBody struct {
	ChatID        string `json:"chat_id"`
	MediaID       string `json:"media_id"`
	Text          string `json:"text,omitempty"`
	ReplyTo       string `json:"reply_to,omitempty"`
	Filename      string `json:"filename,omitempty"`
	MIME          string `json:"mime,omitempty"`
	AsDocument    bool   `json:"as_document,omitempty"`
	MarkForwarded bool   `json:"mark_forwarded,omitempty"`
}

// ReactBody is the POST /react payload.
type ReactBody struct {
	ChatID    string `json:"chat_id"`
	MessageID string `json:"message_id"`
	Emoji     string `json:"emoji"`
}

// AuthSubmitBody is the POST /auth/submit payload.
type AuthSubmitBody struct {
	Platform string `json:"platform"`
	Field    string `json:"field"` // phone | code | password
	Value    string `json:"value"`
}

// TapBody is the POST /tap_button payload.
type TapBody struct {
	ChatID    string `json:"chat_id"`
	MessageID string `json:"message_id"`
	ButtonID  string `json:"button_id"`
}

// TapResult is the POST /tap_button response.
type TapResult struct {
	Result string `json:"result"`
}

// VoteBody is the POST /vote_poll payload.
type VoteBody struct {
	ChatID    string `json:"chat_id"`
	MessageID string `json:"message_id"`
	Option    string `json:"option"`
}

// BackfillBody is the POST /backfill payload. ChatID "all" or "" backfills all
// chats on the platform implied by the chat id (or every connector when "all").
type BackfillBody struct {
	ChatID string    `json:"chat_id"`
	Since  time.Time `json:"since,omitempty"`
}

// OKResult is a generic acknowledgement.
type OKResult struct {
	OK bool `json:"ok"`
}

// HealthResult is the /healthz body: liveness + the build version (git SHA), so
// a deploy can prove the running daemon is the expected commit.
type HealthResult struct {
	OK      bool   `json:"ok"`
	Version string `json:"version,omitempty"`
}

// MediaMeta is an attachment's media-store metadata (GET /media/{id}/meta and
// the get_media MCP tool). Path is the local blob path (the same-host agent can
// open it directly); Stored reports whether the bytes are currently on disk.
type MediaMeta struct {
	ID     string `json:"id"` // "<event_id>#<idx>"
	Type   string `json:"type"`
	MIME   string `json:"mime"`
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Stored bool   `json:"stored"`
	// LocalPath is the on-disk blob path (the co-located agent opens it directly
	// — no base64 through MCP). HTTPURL serves the same bytes over GET /media/{id}
	// for any non-co-located consumer. Bytes never travel base64-through-MCP.
	LocalPath string `json:"local_path,omitempty"`
	HTTPURL   string `json:"http_url,omitempty"`
}

// ProposalDTO is a fuzzy-merge proposal awaiting human review (GET
// /people/proposals). A and B are canonical person ids; the agent renders the
// names + reason and resolves by id. Defined here (not as the gbrainfeed type)
// so the api boundary stays decoupled from the projector — the json tags mirror
// gbrainfeed.Proposal exactly.
type ProposalDTO struct {
	ID     string  `json:"id"`
	A      string  `json:"a"`
	B      string  `json:"b"`
	AName  string  `json:"a_name"`
	BName  string  `json:"b_name"`
	Score  float64 `json:"score"`
	Reason string  `json:"reason"`
	Status string  `json:"status"`
}

// ResolveBody is the POST /people/proposals/{id}/resolve payload: the user's
// decision on a pending proposal ("approve" → merge, "reject" → not same).
type ResolveBody struct {
	Decision string `json:"decision"`
}

// BackfillResult reports a #54 sender-name backfill pass: how many blank-sender
// rows were scanned (distinct sender_ids), how many got filled from the people-graph,
// and how many sender_ids couldn't be resolved to a named person (left blank).
type BackfillResult struct {
	Platform   string `json:"platform"`
	Scanned    int    `json:"scanned"`
	Backfilled int    `json:"backfilled"`
	Unresolved int    `json:"unresolved"`
}

// ResolveOutcome is the merge result returned on an approve (mirrors
// gbrainfeed.MergeOutcome): the survivor the two persons merged into, so the
// agent can confirm "now shown as «survivor_name»" and rename the survivor by id.
// Omitted/empty on reject.
type ResolveOutcome struct {
	SurvivorID    string `json:"survivor_id,omitempty"`
	SurvivorName  string `json:"survivor_name,omitempty"`
	AbsorbedID    string `json:"absorbed_id,omitempty"`
	Identified    bool   `json:"identified,omitempty"`
	AlreadyMerged bool   `json:"already_merged,omitempty"`
}

// NameBody is the POST /people/{canon_id}/name payload: a human-confirmed name
// for the canonical person identified in the path.
type NameBody struct {
	Name string `json:"name"`
}

// MergeBody is the POST /people/merge payload: two canonical ids to force-merge
// (operator-directed). Accepts either {a,b} or {from_node,into_node} (aliases for
// the same two ids). Order/direction does NOT determine the survivor — ForceMerge
// keeps the strong-signal canonical (canon-email-/canon-phone-) over a singleton,
// then lexicographic — so e.g. an IG handle always folds INTO the established
// email/phone person, which is the intended human-directed outcome. (Explicit
// survivor-pinning would be a separate ForceMerge param; not needed today.)
type MergeBody struct {
	A        string `json:"a"`
	B        string `json:"b"`
	FromNode string `json:"from_node"`
	IntoNode string `json:"into_node"`
}

// Pair returns the two canonical ids from whichever field-pair the caller used
// ({a,b} preferred; {from_node,into_node} as the alias).
func (m MergeBody) Pair() (string, string) {
	if m.A != "" || m.B != "" {
		return m.A, m.B
	}
	return m.FromNode, m.IntoNode
}

// platformOf returns the "<platform>:" prefix of a canonical chat id.
func platformOf(chatID string) string {
	for i := 0; i < len(chatID); i++ {
		if chatID[i] == ':' {
			return chatID[:i]
		}
	}
	return ""
}
