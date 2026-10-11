package idmatch

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// LabeledPair is ground truth for the eval gate. SamePerson is established WITHOUT
// the model: POSITIVES are pairs that strong-signal (phone/email) auto-merged with
// the signal stripped off; NEGATIVES are distinct canonicals with the same/similar
// name but CONFLICTING confirmed signals (provably different people).
type LabeledPair struct {
	A, B       string
	SamePerson bool
	Note       string // why it's labeled this way (for the report)
}

// EvalReport is the precision-first scorecard the lead reviews before flipping
// auto-merge on. Precision of the AUTO-MERGE tier is the gating number.
type EvalReport struct {
	Positives, Negatives int

	// Candidate-gen recall: of POSITIVE pairs, how many did blocking even surface?
	// (A pair the generator misses can never be merged — this is the recall ceiling.)
	CandidateRecall float64
	MissedPositives []LabeledPair // positives candidate-gen failed to surface

	// Pipeline outcomes (candidate-gen → adjudicate → route), measured on labeled pairs:
	AutoMergePrecision float64 // of pairs routed AUTO-MERGE, fraction truly same  ← THE GATE
	AutoMergeRecall    float64 // of positives, fraction routed AUTO-MERGE
	ProposeP, ProposeR float64
	FalseAutoMerges    []LabeledPair // NEGATIVE pairs wrongly routed auto-merge (must be ~0)

	// raw confusion on the auto-merge tier
	AMtruePos, AMfalsePos, AMfalseNeg int
}

// PassesGate reports whether the auto-merge precision clears the bar (e.g. 0.99).
func (r EvalReport) PassesGate(bar float64) bool {
	return r.AutoMergePrecision >= bar && len(r.FalseAutoMerges) == 0
}

// Evaluate runs the FULL pipeline (candidate-gen → adjudicate → route) over the
// labeled set and scores it. Pure given a deterministic adjudicator.
func Evaluate(ctx context.Context, people []PersonRecord, labeled []LabeledPair, gen *CandidateGenerator, adj LLMAdjudicator, t Thresholds) (EvalReport, error) {
	byID := IndexByID(people)

	// 1. Candidate-gen recall — which labeled positives did blocking surface?
	// Keep the full CandidatePair (Signals) so the deterministic corroborated-exact-
	// name pre-route can be scored alongside the LLM.
	cand := map[string]CandidatePair{}
	surfaced := map[string]bool{}
	for _, c := range gen.Candidates(people) {
		cand[pairKey(c.A, c.B)] = c
		surfaced[pairKey(c.A, c.B)] = true
	}
	var rep EvalReport
	var missed []LabeledPair
	for _, lp := range labeled {
		if lp.SamePerson {
			rep.Positives++
			if !surfaced[pairKey(lp.A, lp.B)] {
				missed = append(missed, lp)
			}
		} else {
			rep.Negatives++
		}
	}
	rep.MissedPositives = missed
	if rep.Positives > 0 {
		rep.CandidateRecall = float64(rep.Positives-len(missed)) / float64(rep.Positives)
	}

	// 2. Adjudicate every labeled pair (the pipeline would only see surfaced ones,
	// but for the gate we score the routing decision per label).
	label := map[string]LabeledPair{}
	for _, lp := range labeled {
		label[pairKey(lp.A, lp.B)] = lp
	}
	var amTP, amFP, amFN, prTP, prAll int
	for _, lp := range labeled {
		a, okA := byID[lp.A]
		b, okB := byID[lp.B]
		if !okA || !okB {
			continue
		}
		// Only pairs candidate-gen surfaced can be routed; an unsurfaced positive is
		// a false-negative for auto-merge by construction.
		if !surfaced[pairKey(lp.A, lp.B)] {
			if lp.SamePerson {
				amFN++
			}
			continue
		}
		// Deterministic pre-route (corroborated exact-name) — auto-merge WITHOUT the
		// LLM, exactly as the live path does. Scored so the gate's negative-precision
		// reflects this rule too.
		var tier Tier
		if CorroboratedExactName(a, b, cand[pairKey(lp.A, lp.B)]) {
			tier = TierAutoMerge
		} else {
			res, err := adj.Adjudicate(ctx, AdjudicationRequest{A: a, B: b})
			if err != nil {
				return rep, err
			}
			tier = RouteGuarded(res, t, a, b) // conflicting-strong-id veto (#19)
		}
		// The conflicting-id veto applies to the deterministic pre-route too: a shared
		// name + handle/group can't override DIFFERENT exact emails/phones.
		if tier == TierAutoMerge && ConflictingStrongIDs(a, b) {
			tier = TierPropose
		}
		switch tier {
		case TierAutoMerge:
			if lp.SamePerson {
				amTP++
			} else {
				amFP++
				rep.FalseAutoMerges = append(rep.FalseAutoMerges, lp)
			}
		case TierPropose:
			prAll++
			if lp.SamePerson {
				prTP++
			}
			if lp.SamePerson {
				amFN++ // a true pair we only proposed (not auto-merged)
			}
		case TierDrop:
			if lp.SamePerson {
				amFN++
			}
		}
	}
	rep.AMtruePos, rep.AMfalsePos, rep.AMfalseNeg = amTP, amFP, amFN
	if amTP+amFP > 0 {
		rep.AutoMergePrecision = float64(amTP) / float64(amTP+amFP)
	} else {
		rep.AutoMergePrecision = 1 // nothing auto-merged → vacuously precise
	}
	if rep.Positives > 0 {
		rep.AutoMergeRecall = float64(amTP) / float64(rep.Positives)
	}
	if prAll > 0 {
		rep.ProposeP = float64(prTP) / float64(prAll)
	}
	if rep.Positives > 0 {
		rep.ProposeR = float64(prTP) / float64(rep.Positives)
	}
	return rep, nil
}

// Markdown renders the eval scorecard for the lead's review.
func (r EvalReport) Markdown(bar float64) string {
	var b strings.Builder
	b.WriteString("# Identity-matcher eval (gate report)\n\n")
	fmt.Fprintf(&b, "Labeled set: %d positives, %d negatives.\n\n", r.Positives, r.Negatives)

	b.WriteString("## Candidate generation (recall ceiling)\n\n")
	fmt.Fprintf(&b, "- positive pairs surfaced by blocking: **%.1f%%** (%d/%d)\n",
		100*r.CandidateRecall, r.Positives-len(r.MissedPositives), r.Positives)
	if len(r.MissedPositives) > 0 {
		b.WriteString("- MISSED positives (recall gap — tune candidate-gen):\n")
		for _, m := range r.MissedPositives {
			fmt.Fprintf(&b, "    • %s ↔ %s  (%s)\n", m.A, m.B, m.Note)
		}
	}
	b.WriteString("\n## Auto-merge tier (THE GATE)\n\n")
	fmt.Fprintf(&b, "- precision: **%.2f%%** (bar %.0f%%)  → %s\n",
		100*r.AutoMergePrecision, 100*bar, passFail(r.PassesGate(bar)))
	fmt.Fprintf(&b, "- recall:    %.1f%%   (true=%d false=%d missed=%d)\n",
		100*r.AutoMergeRecall, r.AMtruePos, r.AMfalsePos, r.AMfalseNeg)
	if len(r.FalseAutoMerges) > 0 {
		b.WriteString("- ⚠️ FALSE auto-merges (different people merged — MUST be zero to gate):\n")
		for _, f := range r.FalseAutoMerges {
			fmt.Fprintf(&b, "    • %s ↔ %s  (%s)\n", f.A, f.B, f.Note)
		}
	}
	fmt.Fprintf(&b, "\n## Propose tier (human review)\n\n- precision %.1f%%, recall %.1f%%\n",
		100*r.ProposeP, 100*r.ProposeR)
	fmt.Fprintf(&b, "\n**Auto-merge gate: %s** (precision ≥ %.0f%% AND zero false auto-merges)\n",
		passFail(r.PassesGate(bar)), 100*bar)
	return b.String()
}

func passFail(ok bool) string {
	if ok {
		return "PASS ✅"
	}
	return "FAIL ❌ — stay propose-only"
}

// SortedCandidates returns candidate pairs sorted by pre-score desc (for the
// dry-run report's recall inspection).
func SortedCandidates(gen *CandidateGenerator, people []PersonRecord) []CandidatePair {
	c := gen.Candidates(people)
	sort.Slice(c, func(i, j int) bool {
		if c[i].PreScore != c[j].PreScore {
			return c[i].PreScore > c[j].PreScore
		}
		return pairKey(c[i].A, c[i].B) < pairKey(c[j].A, c[j].B)
	})
	return c
}
