package gbrainfeed

import (
	"context"
	"strings"
	"testing"
)

// fuzzyPair drives two same-named, different-phone identities through a projector
// and returns it plus the fake client and the two canonical ids.
func fuzzyPair(t *testing.T) (*Projector, *fakeClient, string, string) {
	t.Helper()
	f := newFake()
	p := NewProjector(f, nil)
	p.Start(context.Background())
	p.IngestContacts("whatsapp", []Contact{{UserID: "wa1", FullName: "Mary Tan", Phone: "+6580001111"}})
	p.IngestContacts("telegram", []Contact{{UserID: "tg1", FullName: "Mary Tan", Phone: "+6580002222"}})
	p.Close()
	return p, f, "canon-phone-6580001111", "canon-phone-6580002222"
}

func TestReviewFlowProposalRaised(t *testing.T) {
	p, _, a, b := fuzzyPair(t)
	pend := p.Proposals().Pending()
	if len(pend) != 1 {
		t.Fatalf("want 1 pending proposal, got %d", len(pend))
	}
	if pend[0].ID != pairKey(a, b) {
		t.Fatalf("proposal id=%q want %q", pend[0].ID, pairKey(a, b))
	}
}

func TestReviewFlowApproveMerges(t *testing.T) {
	p, f, a, b := fuzzyPair(t)
	out, err := p.ApproveMerge(context.Background(), a, b)
	if err != nil {
		t.Fatal(err)
	}
	// Survivor returned so the bot can confirm/rename.
	if out.SurvivorID == "" || out.SurvivorName != "Mary Tan" {
		t.Fatalf("ApproveMerge outcome should report survivor {id, name=Mary Tan}, got %+v", out)
	}
	if out.AbsorbedID == "" || out.AbsorbedID == out.SurvivorID {
		t.Fatalf("outcome should name a distinct absorbed id, got %+v", out)
	}
	// Proposal resolved.
	if len(p.Proposals().Pending()) != 0 {
		t.Fatal("approved proposal should leave no pending")
	}
	// A merged_into link was written from absorbed -> survivor.
	merged := false
	for _, l := range f.links {
		if l[2] == "merged_into" {
			merged = true
		}
	}
	if !merged {
		t.Fatalf("approve must write a merged_into link; links=%+v", f.links)
	}
	// Survivor now holds both identities.
	snap, ok := p.resolver.Snapshot(a)
	if !ok || len(snap.Members) != 2 {
		t.Fatalf("survivor should have 2 members, got %+v ok=%v", snap, ok)
	}
}

// TestLoadApprovedMergesSurvivesRestart is the regression for the durability bug:
// a force-merge lives only in the resolver's in-memory redirect map, so when the
// resolver rebuilds from the firehose (strong-signal merges only), an approved
// fuzzy merge is silently undone. LoadApprovedMerges must re-establish it.
func TestLoadApprovedMergesSurvivesRestart(t *testing.T) {
	store := NewMemoryProposalStore()
	a, b := "canon-phone-6580001111", "canon-phone-6580002222"

	// --- session 1: ingest the fuzzy pair, approve the merge ---
	f1 := newFake()
	p1 := NewProjector(f1, nil)
	p1.SetProposalStore(store)
	p1.Start(context.Background())
	p1.IngestContacts("whatsapp", []Contact{{UserID: "wa1", FullName: "Mary Tan", Phone: "+6580001111"}})
	p1.IngestContacts("telegram", []Contact{{UserID: "tg1", FullName: "Mary Tan", Phone: "+6580002222"}})
	p1.Close()
	if _, err := p1.ApproveMerge(context.Background(), a, b); err != nil {
		t.Fatal(err)
	}
	if !p1.resolver.SameLiveCanonical(a, b) {
		t.Fatal("session 1: approve should have merged a and b")
	}
	if len(store.Approved()) != 1 {
		t.Fatalf("session 1: store should hold 1 approved proposal, got %d", len(store.Approved()))
	}

	// --- session 2 (restart): fresh resolver, SAME durable store, firehose replay ---
	f2 := newFake()
	p2 := NewProjector(f2, nil)
	p2.SetProposalStore(store)
	p2.Start(context.Background())
	p2.IngestContacts("whatsapp", []Contact{{UserID: "wa1", FullName: "Mary Tan", Phone: "+6580001111"}})
	p2.IngestContacts("telegram", []Contact{{UserID: "tg1", FullName: "Mary Tan", Phone: "+6580002222"}})
	p2.Close()
	// The firehose only re-derives strong-signal merges → the fuzzy merge is GONE.
	if p2.resolver.SameLiveCanonical(a, b) {
		t.Fatal("restart precondition: firehose rebuild should NOT re-merge the fuzzy pair")
	}

	// --- the fix: replay approved merges ---
	applied, deferred := p2.LoadApprovedMerges(context.Background())
	if applied != 1 || deferred != 0 {
		t.Fatalf("LoadApprovedMerges want applied=1 deferred=0, got applied=%d deferred=%d", applied, deferred)
	}
	if !p2.resolver.SameLiveCanonical(a, b) {
		t.Fatal("after replay: a and b should be one live person again")
	}
	snap, ok := p2.resolver.Snapshot(a)
	if !ok || len(snap.Members) != 2 {
		t.Fatalf("after replay: survivor should hold 2 members, got %+v ok=%v", snap, ok)
	}
	// Idempotent: a second replay is a no-op (already merged → applied=0).
	if applied2, _ := p2.LoadApprovedMerges(context.Background()); applied2 != 0 {
		t.Fatalf("second replay should be a no-op, got applied=%d", applied2)
	}
}

// TestApproveMergePersistsWithoutPendingRow is the regression for the deeper hole:
// an auto-merge (or a direct approve) has NO pending proposal row, so the old
// SetStatus-only path no-op'd and the approval was never persisted — lost on the
// next restart. ApproveMerge must durably record it regardless.
func TestApproveMergePersistsWithoutPendingRow(t *testing.T) {
	store := NewMemoryProposalStore()
	f := newFake()
	p := NewProjector(f, nil)
	p.SetProposalStore(store)
	p.Start(context.Background())
	// Two DIFFERENT-named people the resolver never proposed — simulate the #19
	// auto-merge path calling ApproveMerge straight on two live canonicals.
	p.IngestContacts("whatsapp", []Contact{{UserID: "wa9", FullName: "Bob Lim", Phone: "+6581110000"}})
	p.IngestContacts("telegram", []Contact{{UserID: "tg9", FullName: "Robert Lim", Phone: "+6582220000"}})
	p.Close()
	a, b := "canon-phone-6581110000", "canon-phone-6582220000"
	if len(store.Pending()) != 0 {
		t.Fatalf("precondition: distinct names should not auto-propose, got %d pending", len(store.Pending()))
	}
	if _, err := p.ApproveMerge(context.Background(), a, b); err != nil {
		t.Fatal(err)
	}
	appr := store.Approved()
	if len(appr) != 1 {
		t.Fatalf("auto-merge approval must persist durably even with no pending row, got %d approved", len(appr))
	}
	if appr[0].ID != pairKey(a, b) {
		t.Fatalf("approved id=%q want %q", appr[0].ID, pairKey(a, b))
	}
}

// TestLoadApprovedMergesDefersWhenCanonicalAbsent verifies that an approved pair
// whose canonicals aren't rebuilt yet is reported as deferred (not lost), so a
// later post-reproject call can apply it.
func TestLoadApprovedMergesDefersWhenCanonicalAbsent(t *testing.T) {
	store := NewMemoryProposalStore()
	store.Add(Proposal{A: "canon-phone-6599990000", B: "canon-phone-6599991111",
		AName: "Ghost", BName: "Ghost", Status: ProposalApproved})
	f := newFake()
	p := NewProjector(f, nil)
	p.SetProposalStore(store)
	p.Start(context.Background())
	p.Close()
	applied, deferred := p.LoadApprovedMerges(context.Background())
	if applied != 0 || deferred != 1 {
		t.Fatalf("absent canonicals → want applied=0 deferred=1, got applied=%d deferred=%d", applied, deferred)
	}
}

func TestReviewFlowRejectRemembers(t *testing.T) {
	p, _, a, b := fuzzyPair(t)
	p.RejectProposal(a, b)
	if len(p.Proposals().Pending()) != 0 {
		t.Fatal("rejected proposal should not stay pending")
	}
	// Re-observing the same pair must not re-propose.
	pr, ok := p.Proposals().Get(pairKey(a, b))
	if !ok || pr.Status != ProposalRejected {
		t.Fatalf("proposal should be marked rejected, got %+v ok=%v", pr, ok)
	}
}

func TestResolveProposalByID(t *testing.T) {
	// The daemon-facing convenience: resolve a proposal by its id + decision.
	p, _, a, b := fuzzyPair(t)
	id := pairKey(a, b)

	// Unknown id / bad decision are errors. (back-compat error-only signature)
	if err := p.ResolveProposal(context.Background(), "nope", "approve"); err == nil {
		t.Fatal("expected error for unknown proposal id")
	}
	if _, err := p.ResolveProposalOutcome(context.Background(), id, "frobnicate"); err == nil {
		t.Fatal("expected error for invalid decision")
	}

	// Reject by id → no outcome, marked rejected, never re-proposed.
	out, err := p.ResolveProposalOutcome(context.Background(), id, "reject")
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		t.Fatalf("reject should return nil outcome, got %+v", out)
	}
	if pr, _ := p.Proposals().Get(id); pr.Status != ProposalRejected {
		t.Fatalf("want rejected, got %q", pr.Status)
	}

	// Fresh pair, approve by id → merged + survivor outcome returned.
	p2, f2, a2, b2 := fuzzyPair(t)
	id2 := pairKey(a2, b2)
	out2, err := p2.ResolveProposalOutcome(context.Background(), id2, "approve")
	if err != nil {
		t.Fatal(err)
	}
	if out2 == nil || out2.SurvivorID == "" {
		t.Fatalf("approve should return a survivor outcome, got %+v", out2)
	}
	merged := false
	for _, l := range f2.links {
		if l[2] == "merged_into" {
			merged = true
		}
	}
	if !merged {
		t.Fatalf("approve-by-id should merge; links=%+v", f2.links)
	}
}

func TestPendingProposalsDropsAlreadyMerged(t *testing.T) {
	// A fuzzy name proposal is raised for two same-named, different-phone persons.
	// Then a third identity bridges them via BOTH phones → strong-signal auto-merge
	// collapses them. The now-stale fuzzy proposal must NOT appear in
	// PendingProposals (approving it would be a no-op).
	f := newFake()
	p := NewProjector(f, nil)
	p.Start(context.Background())
	p.IngestContacts("whatsapp", []Contact{{UserID: "wa1", FullName: "Sam Lee", Phone: "+6580001111"}})
	p.IngestContacts("telegram", []Contact{{UserID: "tg1", FullName: "Sam Lee", Phone: "+6580002222"}})
	// drain so the fuzzy proposal is raised
	p.Close()

	a, b := "canon-phone-6580001111", "canon-phone-6580002222"
	if len(p.proposals.Pending()) == 0 {
		t.Fatal("expected a fuzzy proposal for the same-named pair")
	}
	if p.PendingProposals() == nil || len(p.PendingProposals()) != 1 {
		t.Fatalf("pair not yet merged → should still be actionable, got %d", len(p.PendingProposals()))
	}

	// Bridge the two phone-canonicals via a shared EMAIL carried by two distinct
	// identities: G1 (phone P1 + email E) attaches to canon-phone-P1 and registers
	// E there; G2 (phone P2 + same email E) then matches E→canon-phone-P1 AND
	// P2→canon-phone-P2 → strong-signal auto-merge collapses them.
	p2 := NewProjector(f, nil)
	p2.resolver = p.resolver // reuse resolver state
	p2.proposals = p.proposals
	p2.Start(context.Background())
	p2.OnIngest(senderMsg("gmail", "g1@x", "Sam Lee", "+6580001111", "bridge@x"))
	p2.OnIngest(senderMsg("gmail", "g2@x", "Sam Lee", "+6580002222", "bridge@x"))
	p2.Close()

	if !p2.resolver.SameCanonical(a, b) {
		t.Fatal("the two canonicals should now be merged via shared email bridge")
	}
	// The stale proposal is filtered out + self-healed.
	for _, pr := range p2.PendingProposals() {
		if (pr.A == a && pr.B == b) || (pr.A == b && pr.B == a) {
			t.Fatalf("already-merged pair must be dropped from PendingProposals, got %+v", pr)
		}
	}
}

func TestPendingProposalsDropsDeadIDs(t *testing.T) {
	// A proposal whose canonical ids were never created in this run (e.g. durable
	// residue persisted across a reproject) is a no-op on approve — it must be
	// dropped from PendingProposals and self-healed.
	f := newFake()
	p := NewProjector(f, nil)
	p.Start(context.Background())
	// One real, live, distinct pair (actionable) ...
	p.IngestContacts("whatsapp", []Contact{{UserID: "wa1", FullName: "Real Person", Phone: "+6580001111"}})
	p.IngestContacts("telegram", []Contact{{UserID: "tg1", FullName: "Real Person", Phone: "+6580002222"}})
	p.Close()
	// ... plus a dead-id proposal referencing canonicals that don't exist.
	p.proposals.Add(Proposal{A: "canon-email-ghost-a", B: "canon-email-ghost-b", AName: "Ghost", BName: "Ghost"})

	live, dead := "canon-phone-6580001111", "canon-email-ghost-a|canon-email-ghost-b"
	got := p.PendingProposals()
	for _, pr := range got {
		if pr.ID == "canon-email-ghost-a|canon-email-ghost-b" {
			t.Fatalf("dead-id proposal must be dropped, got %+v", pr)
		}
	}
	// The real actionable pair survives.
	foundLive := false
	for _, pr := range got {
		if pr.A == live || pr.B == live {
			foundLive = true
		}
	}
	if !foundLive {
		t.Fatalf("actionable proposal should remain; got %+v", got)
	}
	// Dead one self-healed in the store.
	if pr, _ := p.proposals.Get(dead); pr.Status == ProposalPending {
		t.Fatal("dead-id proposal should be self-healed out of pending")
	}
}

func TestPendingProposalsDropsLowConfidenceResidue(t *testing.T) {
	// Noise proposals persisted before the policy tightened must be dropped from the
	// live queue (self-healed), without waiting for a reproject.
	f := newFake()
	p := NewProjector(f, nil)
	p.Start(context.Background())
	// A genuine high-confidence pair (distinctive name, cross-platform).
	p.IngestContacts("whatsapp", []Contact{{UserID: "wa1", FullName: "Morgan Example", Phone: "+6580001111"}})
	p.IngestContacts("telegram", []Contact{{UserID: "tg1", FullName: "Morgan Example", Phone: "+6580002222"}})
	p.Close()
	// Persisted residue the proposer would no longer raise:
	p.proposals.Add(Proposal{A: "canon-phone-1", B: "canon-phone-2", AName: "Sean", BName: "Sean"})                             // common name
	p.proposals.Add(Proposal{A: "canon-phone-3", B: "canon-phone-4", AName: "Identified As Spam", BName: "Identified As Spam"}) // placeholder
	p.proposals.Add(Proposal{A: "canon-email-a", B: "canon-email-b", AName: "GoDaddy Support", BName: "GoDaddy Support"})       // org email pair

	got := p.PendingProposals()
	for _, pr := range got {
		if pr.AName == "Sean" || pr.AName == "Identified As Spam" || (isEmailCanonical(pr.A) && isEmailCanonical(pr.B)) {
			t.Fatalf("low-confidence residue must be dropped, got %+v", pr)
		}
	}
	// The genuine Morgan Example pair survives.
	found := false
	for _, pr := range got {
		if pr.AName == "Morgan Example" {
			found = true
		}
	}
	if !found {
		t.Fatalf("high-confidence proposal should remain; got %+v", got)
	}
}

func TestPendingProposalsDoesNotDestroyOnIncompleteResolver(t *testing.T) {
	// REGRESSION: a proposal referencing canonicals the resolver hasn't rebuilt yet
	// (e.g. right after a restart) must be FILTERED from view but NOT persisted as
	// resolved — otherwise a PendingProposals read on an empty resolver bulk-destroys
	// genuine merges.
	f := newFake()
	p := NewProjector(f, nil) // fresh resolver knows nothing
	// A genuine distinctive-name proposal whose canonicals aren't in the resolver yet.
	id := pairKey("canon-phone-111", "canon-phone-222")
	p.proposals.Add(Proposal{A: "canon-phone-111", B: "canon-phone-222", AName: "Morgan Example", BName: "Morgan Example"})

	got := p.PendingProposals()
	if len(got) != 0 {
		t.Fatalf("unrebuilt canonicals should be filtered from view, got %d", len(got))
	}
	// CRUCIAL: it must still be PENDING in the store (not destroyed).
	if stored, ok := p.proposals.Get(id); !ok || stored.Status != ProposalPending {
		t.Fatalf("must NOT persist-resolve a proposal on an incomplete resolver, got status %q ok=%v", stored.Status, ok)
	}
}

func TestApproveMergeWritesProvenanceStub(t *testing.T) {
	// #17: after an approve, the ABSORBED canonical must have a page with a
	// merged_into pointer to the survivor (so the link has a source + the merge is
	// auditable/reversible).
	p, f, a, b := fuzzyPair(t)
	out, err := p.ApproveMerge(context.Background(), a, b)
	if err != nil {
		t.Fatal(err)
	}
	stub, ok := f.pages[out.AbsorbedID]
	if !ok {
		t.Fatalf("absorbed page missing — merged_into link would have no source")
	}
	if !strings.Contains(stub, "merged_into: \""+out.SurvivorID+"\"") {
		t.Fatalf("absorbed stub should point merged_into the survivor:\n%s", stub)
	}
	// and the merged_into link exists from absorbed → survivor.
	found := false
	for _, l := range f.links {
		if l[0] == out.AbsorbedID && l[1] == out.SurvivorID && l[2] == "merged_into" {
			found = true
		}
	}
	if !found {
		t.Fatalf("merged_into link absorbed→survivor missing: %+v", f.links)
	}
}

func TestReviewFlowRenameCanonical(t *testing.T) {
	f := newFake()
	p := NewProjector(f, nil)
	p.Start(context.Background())
	p.OnIngest(senderMsg("whatsapp", "u@x", "wa-display", "+6580007777", ""))
	p.Close()
	if err := p.RenameCanonical(context.Background(), "canon-phone-6580007777", "Aunt May"); err != nil {
		t.Fatal(err)
	}
	canon := f.pages["canon-phone-6580007777"]
	if !strings.Contains(canon, "Aunt May") || !strings.Contains(canon, "name_source: manual") {
		t.Fatalf("rename not applied to canonical page:\n%s", canon)
	}
}

// #61: seeding the proposed-set (via MarkProposed, as LoadPendingProposals does on
// startup from persisted PENDING proposals) suppresses re-proposing a pair that's
// already queued — so the startup reproject doesn't re-emit every pending merge.
// Baseline (unseeded) raises the proposal; seeded does not.
func TestMarkProposedSuppressesRepropose(t *testing.T) {
	ctx := context.Background()
	a, b := "canon-phone-6580001111", "canon-phone-6580002222"
	run := func(seed bool) int {
		p := NewProjector(newFake(), nil)
		store := NewMemoryProposalStore()
		p.SetProposalStore(store)
		if seed {
			p.resolver.MarkProposed(a, b) // what LoadPendingProposals replays per pending pair
		}
		p.Start(ctx)
		p.OnIngest(senderMsg("whatsapp", "wa1@x", "Alexander Hamilton", "+6580001111", ""))
		p.OnIngest(senderMsg("whatsapp", "wa2@x", "Alexander Hamilton", "+6580002222", ""))
		p.Close()
		return len(store.Pending())
	}
	if n := run(false); n != 1 {
		t.Fatalf("baseline: a distinctive same-name pair should raise exactly 1 proposal, got %d", n)
	}
	if n := run(true); n != 0 {
		t.Fatalf("seeded (MarkProposed): the already-pending pair must NOT be re-proposed, got %d", n)
	}
}

// LoadPendingProposals replays persisted pending proposals into the resolver so a
// restart doesn't re-emit them.
func TestLoadPendingProposalsSeedsResolver(t *testing.T) {
	store := NewMemoryProposalStore()
	store.Add(Proposal{A: "canon-phone-1", B: "canon-phone-2", AName: "Sam", BName: "Sam", Status: ProposalPending})
	p := NewProjector(newFake(), nil)
	p.SetProposalStore(store)
	p.LoadPendingProposals() // must not panic + must seed the pair
	// re-adding the same pending pair via the resolver path is now a no-op (proposed-set hit)
	if !p.resolver.proposed[pairKey("canon-phone-1", "canon-phone-2")] {
		t.Fatal("LoadPendingProposals should seed the resolver's proposed-set from the pending store")
	}
}
