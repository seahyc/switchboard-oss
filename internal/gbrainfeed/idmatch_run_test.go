package gbrainfeed

import (
	"context"
	"strings"
	"testing"

	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
)

type mockIDCtx struct{ rows []IdentityContext }

func (m mockIDCtx) IdentityContext(context.Context) ([]IdentityContext, error) { return m.rows, nil }

// Build two distinct canonicals "Bob Smith" (phone P1) and "Robert Smith" (phone P2)
// — DIFFERENT normalized names, so the deterministic fuzzy proposer does NOT pair
// them, but the LLM matcher's nickname-blocked candidate-gen + adjudicator does.
func twoNicknameCanonicals(t *testing.T) (*Projector, *fakeClient, string, string) {
	t.Helper()
	f := newFake()
	p := NewProjector(f, nil)
	p.Start(context.Background())
	p.OnIngest(senderMsg(event.PlatformWhatsApp, "wa1@x", "Bob Smith", "+6580001111", ""))
	p.OnIngest(senderMsg(event.PlatformTelegram, "tg1", "Robert Smith", "+6580002222", ""))
	p.Close()
	return p, f, "canon-phone-6580001111", "canon-phone-6580002222"
}

func TestRunIdentityMatchProposesNicknamePair(t *testing.T) {
	p, _, a, b := twoNicknameCanonicals(t)
	// The DETERMINISTIC proposer must NOT have paired Bob/Robert (different names).
	if _, ok := p.proposals.Get(pairKey(a, b)); ok {
		t.Fatal("precondition: deterministic proposer should not pair Bob/Robert")
	}
	// LLM says SAME at high confidence → with auto-merge OFF, it PROPOSES.
	adj := idmatch.MapAdjudicator{M: map[string]idmatch.AdjudicationResult{
		pairKeyOf(a, b): {SamePerson: true, Confidence: 0.97, Reason: "Bob is a nickname for Robert; consistent context"},
	}}
	p.SetAdjudicator(adj, nil)
	p.SetIdentityContextProvider(mockIDCtx{})
	if err := p.RunIdentityMatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	pr, ok := p.proposals.Get(pairKey(a, b))
	if !ok || pr.Status != ProposalPending {
		t.Fatalf("LLM should have PROPOSED the nickname pair, got %+v ok=%v", pr, ok)
	}
	if pr.Score != 0.97 {
		t.Fatalf("proposal should carry the LLM confidence, got %v", pr.Score)
	}
}

func TestRunIdentityMatchAutoMergeWhenGated(t *testing.T) {
	p, f, a, b := twoNicknameCanonicals(t)
	adj := idmatch.MapAdjudicator{M: map[string]idmatch.AdjudicationResult{
		pairKeyOf(a, b): {SamePerson: true, Confidence: 0.98, Reason: "same person"},
	}}
	p.SetAdjudicator(adj, nil)
	p.SetIdentityContextProvider(mockIDCtx{})
	p.SetAutoMerge(true) // gate flipped ON
	if err := p.RunIdentityMatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !p.resolver.SameLiveCanonical(a, b) {
		t.Fatal("auto-merge ON + high confidence should have merged the pair")
	}
	merged := false
	for _, l := range f.links {
		if l[2] == "merged_into" {
			merged = true
		}
	}
	if !merged {
		t.Fatalf("auto-merge should write merged_into; links=%+v", f.links)
	}
}

func TestRunIdentityMatchNoAdjudicatorNoop(t *testing.T) {
	p, _, _, _ := twoNicknameCanonicals(t)
	if err := p.RunIdentityMatch(context.Background()); err != nil {
		t.Fatalf("no adjudicator → no-op, got %v", err)
	}
}

// twoSameNameCanonicals builds two DISTINCT canonicals with the EXACT same name on
// different phones — so the deterministic same-name proposer raises a pending
// proposal (no strong signal merges them).
func twoSameNameCanonicals(t *testing.T) (*Projector, *fakeClient, string, string) {
	t.Helper()
	f := newFake()
	p := NewProjector(f, nil)
	p.Start(context.Background())
	p.OnIngest(senderMsg(event.PlatformWhatsApp, "wa1@x", "Taylor Example", "+6580001111", ""))
	p.OnIngest(senderMsg(event.PlatformTelegram, "tg1", "Taylor Example", "+6580002222", ""))
	p.Close()
	return p, f, "canon-phone-6580001111", "canon-phone-6580002222"
}

// Corroborated exact-name (same name + shared group) → deterministic auto-merge with
// NO LLM, and the pending proposal is drained.
func TestRunDeterministicMergesCorroboratedExactName(t *testing.T) {
	p, f, a, b := twoSameNameCanonicals(t)
	if pr, ok := p.proposals.Get(pairKey(a, b)); !ok || pr.Status != ProposalPending {
		t.Fatalf("precondition: same-name pair should be pending, got %+v ok=%v", pr, ok)
	}
	// Shared group "grpX" corroborates the exact name.
	p.SetIdentityContextProvider(mockIDCtx{rows: []IdentityContext{
		{Platform: "whatsapp", SenderID: "wa1@x", ChatIDs: []string{"grpX"}},
		{Platform: "telegram", SenderID: "tg1", ChatIDs: []string{"grpX"}},
	}})
	// No adjudicator set → proves this path needs NO LLM.
	merged, err := p.RunDeterministicMerges(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if merged != 1 {
		t.Fatalf("corroborated exact-name should auto-merge 1 pair, got %d", merged)
	}
	if !p.resolver.SameLiveCanonical(a, b) {
		t.Fatal("the pair should now be one person")
	}
	if pr, _ := p.proposals.Get(pairKey(a, b)); pr.Status != ProposalApproved {
		t.Fatalf("pending proposal should be DRAINED→approved, got %+v", pr)
	}
	wroteMerge := false
	for _, l := range f.links {
		if l[2] == "merged_into" {
			wroteMerge = true
		}
	}
	if !wroteMerge {
		t.Fatalf("deterministic merge must write merged_into; links=%+v", f.links)
	}
}

// Bare same-name (no shared group/handle) must NOT auto-merge — it stays pending for
// human review (the risky common-name-collision case).
func TestRunDeterministicMergesBareSameNameStaysPending(t *testing.T) {
	p, _, a, b := twoSameNameCanonicals(t)
	p.SetIdentityContextProvider(mockIDCtx{}) // no corroborating context
	merged, err := p.RunDeterministicMerges(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if merged != 0 {
		t.Fatalf("bare same-name must NOT auto-merge, got %d", merged)
	}
	if p.resolver.SameLiveCanonical(a, b) {
		t.Fatal("bare same-name must stay split")
	}
	if pr, _ := p.proposals.Get(pairKey(a, b)); pr.Status != ProposalPending {
		t.Fatalf("bare same-name must stay pending for human review, got %+v", pr)
	}
}

// pairKeyOf mirrors idmatch.pairKey (unexported) for test setup.
func pairKeyOf(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "|" + b
}

// #19 deterministic-propose: strong cross-platform signals surface as proposals (no
// LLM); name-only fuzz does not; already-merged/auto-merge pairs are skipped.
func TestRunDeterministicProposals(t *testing.T) {
	f := newFake()
	p := NewProjector(f, nil)
	p.Start(context.Background())
	// Two DIFFERENT-named people on different platforms who SHARE a distinctive handle
	// (no shared phone/email → distinct canonicals → not auto-merged). This is the
	// strong cross-platform signal the resolver's name-only propose misses.
	p.OnIngest(senderMsg(event.PlatformTelegram, "tg1", "Alice Tan", "", ""))
	p.OnIngest(senderMsg(event.PlatformWhatsApp, "wa1", "Alicia Lee", "+6590001111", ""))
	p.Close()
	// Handles come from IdentityContext (core2's feed), not the member — inject the
	// shared handle for both identities.
	p.SetIdentityContextProvider(mockIDCtx{rows: []IdentityContext{
		{Platform: event.PlatformTelegram, SenderID: "tg1", Handles: []string{"sharedhandle"}},
		{Platform: event.PlatformWhatsApp, SenderID: "wa1", Handles: []string{"sharedhandle"}},
	}})

	n, err := p.RunDeterministicProposals(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("shared-handle cross-platform pair must be proposed, added %d", n)
	}
	// the proposal must come from THIS path (reason tagged "deterministic")
	var found bool
	for _, pr := range p.proposals.Pending() {
		if strings.Contains(pr.Reason, "deterministic") &&
			((pr.AName == "Alice Tan" && pr.BName == "Alicia Lee") || (pr.AName == "Alicia Lee" && pr.BName == "Alice Tan")) {
			found = true
		}
	}
	if !found {
		t.Fatal("the shared-handle pair must be a deterministic proposal")
	}
	// idempotent: a second run adds 0 new.
	if n2, _ := p.RunDeterministicProposals(context.Background()); n2 != 0 {
		t.Fatalf("second run must add 0 (idempotent), added %d", n2)
	}
}

// #17: BackfillMergeLinks reconciles gbrain merged_into links to the resolver's true
// redirect state — even for a merge whose link was never written.
func TestBackfillMergeLinks(t *testing.T) {
	f := newFake()
	p := NewProjector(f, nil)
	p.Start(context.Background())
	p.OnIngest(senderMsg(event.PlatformWhatsApp, "wa1@x", "Cal Person", "+6580001111", ""))
	p.OnIngest(senderMsg(event.PlatformTelegram, "tg1", "Cal Person", "+6580002222", ""))
	p.Close()
	a, b := "canon-phone-6580001111", "canon-phone-6580002222"
	// ForceMerge directly on the resolver (NO projector link write) → simulates a merge
	// whose merged_into link is missing.
	if _, _, ok := p.resolver.ForceMerge(a, b); !ok {
		t.Fatal("precondition: force-merge should succeed")
	}
	// No merged_into link should exist yet from this path.
	pre := 0
	for _, l := range f.links {
		if l[2] == "merged_into" {
			pre++
		}
	}
	n, err := p.BackfillMergeLinks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("backfill should ensure ≥1 link, got %d", n)
	}
	post := 0
	for _, l := range f.links {
		if l[2] == "merged_into" {
			post++
		}
	}
	if post <= pre {
		t.Fatalf("backfill must write the missing merged_into link (pre=%d post=%d)", pre, post)
	}
	// idempotent: second run writes nothing new (linkOnce dedup).
	postLinks := len(f.links)
	if _, err := p.BackfillMergeLinks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.links) != postLinks {
		t.Fatal("backfill must be idempotent (no duplicate links)")
	}
}

// #15: WaitIdle drains the async projection queue without closing — so a settled-graph
// score/reconcile sees all canonicals.
func TestWaitIdle(t *testing.T) {
	f := newFake()
	p := NewProjector(f, nil)
	p.Start(context.Background())
	for i := 0; i < 50; i++ {
		p.OnIngest(senderMsg(event.PlatformWhatsApp, "wa"+string(rune('a'+i%26))+string(rune('0'+i/26)), "Person", "+65900000"+string(rune('0'+i%10)), ""))
	}
	if err := p.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	// after WaitIdle the queue is drained — canonicals exist WITHOUT Close().
	if len(p.resolver.AllCanonicals()) == 0 {
		t.Fatal("WaitIdle should have let the worker project the enqueued identities")
	}
	// ctx-bounded: an already-cancelled ctx returns promptly (idle path may still win,
	// but a cancelled ctx must never hang).
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = p.WaitIdle(ctx) // returns nil (idle) or ctx.Err() — must not block
	p.Close()
}
