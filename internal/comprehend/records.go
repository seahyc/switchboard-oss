// Package comprehend is switchboard's F1/F2 comprehension + proactive layer.
//
// It is a READ-MOSTLY consumer of the canonical event store (internal/store):
// every projection here opens switchboard.db READ-ONLY and never mutates it.
// The one thing comprehend owns and writes is its OWN sqlite database
// (comprehend.db) holding extracted commitments + the extraction content-hash
// cache — so the sacred capture spine is never touched.
//
// Two layers, in priority order:
//
//  1. Deterministic projections (projections.go) — pure SQL over the event
//     store, ZERO inference. waiting-on-me + ghosting reply-graphs. These ship
//     even if extraction is rough, and back the morning brief + the who's-waiting
//     Mini App deck.
//  2. Commitment extraction (extract.go) — importance-gated, local bulk pass via
//     ollama qwen2.5:7b, smart residue via the chatmock OpenAI-compatible proxy.
//     Content-hash cached, queued + backed off (the Codex subscription is scarce).
//
// SUGGEST-ONLY is a hard invariant: nothing here sends, merges, or mutates a
// conversation. Drafts are stored, never delivered.
package comprehend

import "time"

// Commitment is a promise/obligation extracted from a message: "who owes what to
// whom by when". Frozen v0 shape — the JSON field names are the data contract for
// the Mini App + /goal-1. Idempotent on ID (a content hash of the salient fields).
type Commitment struct {
	ID          string    `json:"id"`           // stable idempotency key (hash of source_msg+what)
	Who         string    `json:"who"`          // committer (display name; "me" if self)
	ToWhom      string    `json:"to_whom"`      // beneficiary (display name; "me" if directed at user)
	What        string    `json:"what"`         // the obligation, one phrase
	Due         string    `json:"due"`          // RFC3339 date/datetime, or "" if open-ended
	Status      string    `json:"status"`       // open|resolved|slipped
	SourceMsgID string    `json:"source_msg"`   // canonical event id provenance
	ChatID      string    `json:"chat_id"`      // chat provenance
	Platform    string    `json:"platform"`     // platform provenance
	Direction   string    `json:"direction"`    // mine|theirs (is it the user's promise or someone else's to the user)
	Confidence  float64   `json:"confidence"`   // extractor confidence [0,1]
	Extractor   string    `json:"extractor"`    // local|smart — which pass produced it
	ExtractedAt time.Time `json:"extracted_at"` // when comprehend wrote it
	SourceTS    time.Time `json:"source_ts"`    // ts of the source message
}

const (
	CommitmentOpen     = "open"
	CommitmentResolved = "resolved"
	CommitmentSlipped  = "slipped"

	DirectionMine   = "mine"   // the user committed
	DirectionTheirs = "theirs" // someone committed to the user

	ExtractorLocal = "local" // ollama qwen2.5:7b bulk pass
	ExtractorSmart = "smart" // chatmock proxy residue pass
)

// WaitingCard is one thread on the who's-waiting reply-graph. The same shape
// serves both projections (direction distinguishes them) and is the JSON the
// Mini App deck renders (field names are the agreed data contract).
type WaitingCard struct {
	ID           string    `json:"id"`            // stable card id (== chat_id)
	Person       string    `json:"person"`        // counterpart display name
	ChatID       string    `json:"chat_id"`       // routing + provenance
	Platform     string    `json:"platform"`      // whatsapp|telegram|lark|gmail|...
	ChatTitle    string    `json:"chat_title"`    // human-legible thread name
	Since        time.Time `json:"since"`         // start of the unanswered streak
	WaitingHours int       `json:"waiting_hours"` // age of the wait, hours
	LastMsgID    string    `json:"last_msg_id"`   // provenance for the latest message
	LastMsgText  string    `json:"last_msg_text"` // snippet (clipped)
	Importance   string    `json:"importance"`    // important|normal (low is filtered out)
	Direction    string    `json:"direction"`     // waiting_on_me|ghosting
	Draft        *string   `json:"draft"`         // suggest-only reply draft; nil until generated, NEVER auto-sent
}

const (
	DirWaitingOnMe = "waiting_on_me" // they sent last; the ball is in the user's court
	DirGhosting    = "ghosting"      // the user sent last; awaiting their reply
)

// CalEvent is a date-anchored thing for the brief's "today" section. v0 is fed
// from due commitments; a real calendar feed slots in later behind the same shape.
type CalEvent struct {
	Title       string    `json:"title"`
	When        time.Time `json:"when"`
	People      []string  `json:"people"`
	SourceMsgID string    `json:"source_msg"`
}

// Brief is the assembled morning-brief payload. Text v0 renders from this; the
// Mini App UI-spec v1 renders from the same struct.
type Brief struct {
	GeneratedAt     time.Time     `json:"generated_at"`
	Date            string        `json:"date"` // "Friday, 12 Jun"
	WaitingOnMe     []WaitingCard `json:"waiting_on_me"`
	Ghosting        []WaitingCard `json:"ghosting"`
	OpenCommitments []Commitment  `json:"open_commitments"` // on-your-plate: open commitments, soonest-due first
	DueToday        []Commitment  `json:"due_today"`
	Events          []CalEvent    `json:"events"`
	Nudges          []string      `json:"nudges"`
}
