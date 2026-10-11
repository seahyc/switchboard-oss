package idmatch

import (
	"context"
	"strings"
)

// AdjudicationRequest is the two candidate records + context handed to the model.
// Strong signals (phone/email) are intentionally ABSENT — those already
// deterministically auto-merge; the LLM only judges the fuzzy, context-driven case.
type AdjudicationRequest struct {
	A PersonRecord `json:"a"`
	B PersonRecord `json:"b"`
}

// AdjudicationResult is the model's structured verdict (ollama format=json).
type AdjudicationResult struct {
	SamePerson bool     `json:"same_person"`
	Confidence float64  `json:"confidence"` // 0.0–1.0
	Reason     string   `json:"reason"`
	Evidence   []string `json:"evidence"`
}

// LLMAdjudicator judges whether two records are the same person. Implemented by the
// local-ollama client (P2) and by mocks (tests/eval dry-run). Must be conservative:
// "same name alone is NOT proof — require corroborating context; default to NOT
// same when unsure."
type LLMAdjudicator interface {
	Adjudicate(ctx context.Context, req AdjudicationRequest) (AdjudicationResult, error)
}

// Tier is the routing decision for an adjudicated pair.
type Tier string

const (
	TierAutoMerge Tier = "auto_merge" // high confidence — merge (only once eval-gated ON)
	TierPropose   Tier = "propose"    // medium — surface to the human bot review flow
	TierDrop      Tier = "drop"       // low / not-same — discard (cache to avoid re-asking)
)

// Thresholds for tier routing. Eval-calibrated; defaults match the approved design.
type Thresholds struct {
	High float64 // >= → auto-merge (default 0.95)
	Low  float64 // >= → propose; below → drop (default 0.70)
}

// DefaultThresholds — approved starting points (the eval gate tunes them).
var DefaultThresholds = Thresholds{High: 0.95, Low: 0.70}

// Route maps an adjudication result to a tier. A "not same person" verdict always
// drops regardless of confidence (confidence is in the verdict's own direction).
func Route(r AdjudicationResult, t Thresholds) Tier {
	if !r.SamePerson {
		return TierDrop
	}
	switch {
	case r.Confidence >= t.High:
		return TierAutoMerge
	case r.Confidence >= t.Low:
		return TierPropose
	default:
		return TierDrop
	}
}

// RouteGuarded is Route plus the CONFLICTING-EMAIL veto (#19): a pair the LLM would
// auto-merge is DEMOTED to propose (human review) when the two records carry conflicting
// emails — both have email(s) but share none exactly. Different exact emails under one
// display name are strong evidence of DIFFERENT senders (the Codex eval's false merges
// were all same-named org mailboxes: LinkedIn notifications@↔security@, Google noreply
// variants, Traveloka booking@↔survey@). Precision-first: such a conflict can NEVER
// auto-merge unsupervised; at most it proposes.
func RouteGuarded(r AdjudicationResult, t Thresholds, a, b PersonRecord) Tier {
	tier := Route(r, t)
	if tier == TierAutoMerge && ConflictingStrongIDs(a, b) {
		return TierPropose
	}
	return tier
}

// ConflictingStrongIDs reports whether a and b have conflicting EMAILS — both sides
// carry ≥1 email but share none exactly. One side lacking an email is NOT a conflict.
//
// PHONE conflict is deliberately NOT a veto: a person legitimately has DIFFERENT numbers
// per platform (a whatsapp number vs a telegram-linked number), and merging those
// name-corroborated cross-platform identities is precisely what the LLM matcher exists
// to do — vetoing on phone difference would gut recall (and block the canonical
// Bob-Smith-wa ↔ Robert-Smith-tg case). The Codex eval's 20 false merges were 100%
// email conflicts; zero were phone conflicts, so email-only fully closes the gate.
func ConflictingStrongIDs(a, b PersonRecord) bool {
	return identifierConflict(a.Emails, b.Emails)
}

func identifierConflict(as, bs []string) bool {
	if len(as) == 0 || len(bs) == 0 {
		return false // one side has none → not a conflict
	}
	set := map[string]bool{}
	for _, a := range as {
		if n := strings.ToLower(strings.TrimSpace(a)); n != "" {
			set[n] = true
		}
	}
	for _, b := range bs {
		if n := strings.ToLower(strings.TrimSpace(b)); n != "" && set[n] {
			return false // a shared exact value → NOT conflicting
		}
	}
	return true // both sides have values, none shared → conflict
}

// Decision pairs a candidate with its adjudicated result + tier — the matcher's output.
type Decision struct {
	Pair   CandidatePair
	Result AdjudicationResult
	Tier   Tier
}

// AdjudicateAll runs the adjudicator over candidate pairs (looking each record up by
// id) and routes each to a tier. Sequential + context-cancellable; the caller bounds
// concurrency/caching. Pairs whose records are missing are skipped.
func AdjudicateAll(ctx context.Context, adj LLMAdjudicator, byID map[string]PersonRecord, pairs []CandidatePair, t Thresholds) ([]Decision, error) {
	out := make([]Decision, 0, len(pairs))
	for _, p := range pairs {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		a, okA := byID[p.A]
		b, okB := byID[p.B]
		if !okA || !okB {
			continue
		}
		res, err := adj.Adjudicate(ctx, AdjudicationRequest{A: a, B: b})
		if err != nil {
			return out, err
		}
		out = append(out, Decision{Pair: p, Result: res, Tier: RouteGuarded(res, t, a, b)})
	}
	return out, nil
}

// IndexByID builds the id→record lookup AdjudicateAll/eval need.
func IndexByID(people []PersonRecord) map[string]PersonRecord {
	m := make(map[string]PersonRecord, len(people))
	for _, p := range people {
		m[p.CanonicalID] = p
	}
	return m
}
