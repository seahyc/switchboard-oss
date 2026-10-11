// Package idmatch is the LLM-adjudicated cross-platform identity matcher (#19/F2).
//
// It replaces the brittle deterministic fuzzy-name proposer with a precision-first
// pipeline: liberal-but-bounded CANDIDATE GENERATION (blocking keys: name variants,
// nicknames, edit-distance, shared groups, handles, semantic embeddings) →
// CONTEXT-rich LLM ADJUDICATION (local model, conservative "same name ≠ proof"
// prompt) → TIER routing (high→auto-merge once eval-gated, medium→human review,
// low→drop). The deterministic STRONG-signal (phone/email) auto-merge is unchanged
// — the LLM only handles the fuzzy name layer.
//
// Everything here is pure + dependency-injected (Embedder, LLMAdjudicator are
// interfaces) so it is fully unit-testable and the eval harness can dry-run on
// synthetic data with no model and no live writes.
package idmatch

// PersonRecord is the minimal canonical-person view the matcher operates on,
// decoupled from the gbrainfeed resolver internals so candidate-gen + adjudication
// stay pure and testable. The projector builds these from the resolver (P2 wiring).
type PersonRecord struct {
	CanonicalID string   `json:"canonical_id"`
	PrimaryName string   `json:"primary_name"`        // best resolved name
	Names       []string `json:"names"`               // all aliases / variants observed
	Platforms   []string `json:"platforms"`           // whatsapp/telegram/email/...
	Handles     []string `json:"handles,omitempty"`   // @usernames
	GroupIDs    []string `json:"group_ids,omitempty"` // chats this person participates in (shared-group block)

	// Strong identifiers (#33): normalized phone(s) / email(s) carried by this
	// canonical's members. A shared one is a DETERMINISTIC same-person signal
	// (SharedStrongIdentifier) — used both as a candidate block and to auto-merge
	// without a name match or the LLM. Pre-normalized by the projector (phone =
	// digits-only, email = lowercased). Empty when the canonical has none.
	Phones []string `json:"phones,omitempty"`
	Emails []string `json:"emails,omitempty"`

	// Context (filled by the PersonContext store feed in P2; empty during P1
	// candidate-gen). Used by the LLM to corroborate same-vs-different.
	Bio      string   `json:"bio,omitempty"`
	Snippets []string `json:"snippets,omitempty"` // a few representative message excerpts

	// Embedding is an optional semantic vector (nomic-embed-text, P2) for the
	// semantic candidate block; nil/empty falls back to deterministic blocks only.
	Embedding []float32 `json:"-"`
}

// CandidatePair is a non-strong-signal pair the candidate generator surfaced for
// adjudication, with the blocking signals that matched + a cheap pre-score used to
// rank/cap candidates before the (expensive) LLM call.
type CandidatePair struct {
	A        string   `json:"a"` // canonical ids, A < B for stable identity
	B        string   `json:"b"`
	Signals  []string `json:"signals"`   // blocks that matched: last-token/nickname/edit-distance/handle/shared-group/semantic/token-jaccard
	PreScore float64  `json:"pre_score"` // cheap rank used to cap candidates before the LLM
}

// pairKey is the order-independent identity of a candidate pair.
func pairKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "|" + b
}
