package gbrainfeed

import (
	"context"
	"testing"
)

// #62: the SQLite page-hash store round-trips + survives a reopen (so the projector's
// put-page skip cache persists across a daemon restart).
func TestSQLitePageHashStore(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	s, err := NewSQLitePageHashStore(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertPageHash(ctx, "person-a", "h1"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertPageHash(ctx, "person-b", "h2"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertPageHash(ctx, "person-a", "h1b"); err != nil { // upsert overwrites
		t.Fatal(err)
	}
	// reopen the table on the same DB → hashes persist
	s2, err := NewSQLitePageHashStore(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s2.PageHashes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["person-a"] != "h1b" || got["person-b"] != "h2" {
		t.Fatalf("PageHashes round-trip wrong: %v", got)
	}
}
