package idmatch

import (
	"context"
	"strings"
)

// HeuristicAdjudicator is a deterministic, model-free stand-in used for the P1
// dry-run + as an eval baseline. It is NOT the real judge (that's the local LLM in
// P2) — it just lets the whole pipeline (candidate-gen → adjudicate → route) run +
// be tested without a model. It mimics a CONSERVATIVE judge: same name alone yields
// only medium confidence; shared groups / handles / matching bios push it higher;
// a contradiction (disjoint context, conflicting bios) pushes it to not-same.
type HeuristicAdjudicator struct{}

func (HeuristicAdjudicator) Adjudicate(_ context.Context, req AdjudicationRequest) (AdjudicationResult, error) {
	a, b := req.A, req.B
	nameSim := tokenJaccard(a.PrimaryName, b.PrimaryName)
	if nameSim == 0 && sharesNicknameGiven(a.PrimaryName, b.PrimaryName) {
		nameSim = 0.6
	}
	var ev []string
	// Name ALONE caps below the auto-merge threshold — same-name-no-corroboration
	// must route to PROPOSE (human), never auto-merge. (Empty-bio-robust: we never
	// require a bio; corroboration comes from handles/groups/snippets.)
	conf := 0.0
	if nameSim > 0 {
		ev = append(ev, "name-similarity")
		conf = 0.55 + 0.18*nameSim // ~0.6–0.73 from name alone → medium → propose
	}
	sharedHandle := shareAnyFold(a.Handles, b.Handles)
	if sharedHandle {
		ev = append(ev, "shared-handle")
		conf += 0.32 // strong → can reach auto-merge
	}
	if shareAny(a.GroupIDs, b.GroupIDs) {
		ev = append(ev, "shared-group")
		conf += 0.15
	}
	if snippetOverlap(a.Snippets, b.Snippets) {
		ev = append(ev, "snippet-overlap")
		conf += 0.15
	}
	// Bio is OPTIONAL (usually absent). Only used when BOTH present.
	if strings.TrimSpace(a.Bio) != "" && strings.TrimSpace(b.Bio) != "" {
		if strings.EqualFold(strings.TrimSpace(a.Bio), strings.TrimSpace(b.Bio)) {
			ev = append(ev, "matching-bio")
			conf += 0.2
		}
	}
	if conf > 1 {
		conf = 1
	}
	same := nameSim >= 0.34 || sharedHandle
	reason := "corroborated (handle/group/snippet)"
	if !same {
		reason = "insufficient corroboration"
	} else if !sharedHandle && !shareAny(a.GroupIDs, b.GroupIDs) && !snippetOverlap(a.Snippets, b.Snippets) {
		reason = "name match only — needs human review"
	}
	return AdjudicationResult{SamePerson: same, Confidence: conf, Reason: reason, Evidence: ev}, nil
}

// snippetOverlap reports whether two snippet sets share a distinctive (length≥5)
// token — a crude "do their messages overlap" proxy for the heuristic baseline.
func snippetOverlap(a, b []string) bool {
	sa := map[string]bool{}
	for _, s := range a {
		for _, t := range tokens(s) {
			if len(t) >= 5 {
				sa[t] = true
			}
		}
	}
	for _, s := range b {
		for _, t := range tokens(s) {
			if len(t) >= 5 && sa[t] {
				return true
			}
		}
	}
	return false
}

// MapAdjudicator returns canned results keyed by pairKey — for deterministic tests
// of exact precision/recall scenarios. Unknown pairs default to not-same.
type MapAdjudicator struct{ M map[string]AdjudicationResult }

func (m MapAdjudicator) Adjudicate(_ context.Context, req AdjudicationRequest) (AdjudicationResult, error) {
	if r, ok := m.M[pairKey(req.A.CanonicalID, req.B.CanonicalID)]; ok {
		return r, nil
	}
	return AdjudicationResult{SamePerson: false, Confidence: 0.5, Reason: "no canned result"}, nil
}

func shareAny(a, b []string) bool {
	s := map[string]bool{}
	for _, x := range a {
		s[x] = true
	}
	for _, x := range b {
		if s[x] {
			return true
		}
	}
	return false
}

func shareAnyFold(a, b []string) bool {
	s := map[string]bool{}
	for _, x := range a {
		if n := normName(x); n != "" {
			s[n] = true
		}
	}
	for _, x := range b {
		if s[normName(x)] {
			return true
		}
	}
	return false
}
