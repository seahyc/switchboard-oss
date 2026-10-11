package idmatch

import "testing"

// #19: the conflicting-strong-identifier veto — the fix for the Codex eval's false
// merges (same-named org mailboxes with DIFFERENT emails). A would-be auto-merge with
// conflicting emails/phones must demote to propose; a non-conflict must pass through.
func TestRouteGuardedConflictVeto(t *testing.T) {
	hi := AdjudicationResult{SamePerson: true, Confidence: 0.98}
	th := DefaultThresholds

	// Conflicting emails (the LinkedIn/Google noreply class) → demoted to propose.
	a := PersonRecord{CanonicalID: "g1", PrimaryName: "Google", Emails: []string{"google-noreply@google.com"}}
	b := PersonRecord{CanonicalID: "g2", PrimaryName: "Google", Emails: []string{"no-reply@accounts.google.com"}}
	if got := RouteGuarded(hi, th, a, b); got != TierPropose {
		t.Fatalf("conflicting emails must demote auto-merge→propose, got %s", got)
	}

	// Conflicting PHONES must NOT veto: a person legitimately has different numbers per
	// platform (Bob-wa ↔ Robert-tg) — the matcher's core cross-platform merge case.
	pa := PersonRecord{Phones: []string{"15555550103"}}
	pb := PersonRecord{Phones: []string{"15555550104"}}
	if got := RouteGuarded(hi, th, pa, pb); got != TierAutoMerge {
		t.Fatalf("conflicting phones must NOT veto (same person, two numbers), got %s", got)
	}

	// SHARED exact email → NOT a conflict → auto-merge survives.
	sa := PersonRecord{Emails: []string{"morgan@example.com"}}
	sb := PersonRecord{Emails: []string{"morgan@example.com"}}
	if got := RouteGuarded(hi, th, sa, sb); got != TierAutoMerge {
		t.Fatalf("shared email must NOT be vetoed, got %s", got)
	}

	// One side has NO email → not a conflict (the true cross-platform positives:
	// gmail↔whatsapp, whatsapp↔whatsapp) → auto-merge survives.
	na := PersonRecord{Emails: []string{"henry@x.com"}, Platforms: []string{"gmail"}}
	nb := PersonRecord{Phones: []string{"15555550105"}, Platforms: []string{"whatsapp"}}
	if got := RouteGuarded(hi, th, na, nb); got != TierAutoMerge {
		t.Fatalf("one-sided identifier must NOT be a conflict, got %s", got)
	}

	// The veto only touches the auto-merge tier — a propose-tier verdict is untouched.
	mid := AdjudicationResult{SamePerson: true, Confidence: 0.80}
	if got := RouteGuarded(mid, th, a, b); got != TierPropose {
		t.Fatalf("propose-tier verdict unaffected, got %s", got)
	}
	// And a not-same verdict still drops.
	if got := RouteGuarded(AdjudicationResult{SamePerson: false}, th, sa, sb); got != TierDrop {
		t.Fatalf("not-same must drop, got %s", got)
	}
}
