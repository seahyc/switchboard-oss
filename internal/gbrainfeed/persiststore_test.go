package gbrainfeed

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestSQLiteProposalStoreCRUD(t *testing.T) {
	db := openTestDB(t)
	s, err := NewSQLiteProposalStore(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Add(Proposal{A: "canon-a", B: "canon-b", AName: "Mom", BName: "Mom", Score: 1})
	id := pairKey("canon-a", "canon-b")

	if pend := s.Pending(); len(pend) != 1 || pend[0].ID != id {
		t.Fatalf("want 1 pending with id %s, got %+v", id, pend)
	}
	got, ok := s.Get(id)
	if !ok || got.AName != "Mom" || got.Status != ProposalPending {
		t.Fatalf("get mismatch: %+v ok=%v", got, ok)
	}
	// Resolve → no longer pending, and a re-Add must NOT re-open it.
	s.SetStatus(id, ProposalRejected)
	if len(s.Pending()) != 0 {
		t.Fatal("rejected proposal should not be pending")
	}
	s.Add(Proposal{A: "canon-a", B: "canon-b", AName: "Mom", BName: "Mom"})
	if len(s.Pending()) != 0 {
		t.Fatal("re-adding a resolved proposal must not re-open it")
	}
	if rej := s.Rejected(); len(rej) != 1 || rej[0].ID != id {
		t.Fatalf("want 1 rejected, got %+v", rej)
	}
}

func TestSQLiteProposalStoreSurvivesReopen(t *testing.T) {
	db := openTestDB(t)
	s1, _ := NewSQLiteProposalStore(db, nil)
	s1.Add(Proposal{A: "x", B: "y", AName: "Sam", BName: "Sam"})
	s1.SetStatus(pairKey("x", "y"), ProposalRejected)

	// A new store over the SAME db (simulating a restart) sees the rejection.
	s2, _ := NewSQLiteProposalStore(db, nil)
	if rej := s2.Rejected(); len(rej) != 1 {
		t.Fatalf("rejection should persist across store reopen, got %+v", rej)
	}
}

func TestProjectorLoadRejectionsSuppressesReproposal(t *testing.T) {
	// End-to-end durability: a rejection persisted in SQLite, replayed via
	// LoadRejections into a fresh projector, must suppress re-proposing the pair
	// even though the in-memory resolver started empty.
	db := openTestDB(t)
	ps, _ := NewSQLiteProposalStore(db, nil)
	a, b := "canon-phone-6580001111", "canon-phone-6580002222"
	ps.Add(Proposal{A: a, B: b, AName: "Mom", BName: "Mom"})
	ps.SetStatus(pairKey(a, b), ProposalRejected)

	f := newFake()
	p := NewProjector(f, nil)
	p.SetProposalStore(ps)
	p.LoadRejections() // replay persisted rejections into the resolver
	p.Start(context.Background())
	// Re-observe the same two same-named contacts that originally collided.
	p.IngestContacts("whatsapp", []Contact{{UserID: "wa1", FullName: "Mom", Phone: "+6580001111"}})
	p.IngestContacts("telegram", []Contact{{UserID: "tg1", FullName: "Mom", Phone: "+6580002222"}})
	p.Close()

	if pend := ps.Pending(); len(pend) != 0 {
		t.Fatalf("rejected pair must not be re-proposed after restart, got %+v", pend)
	}
}
