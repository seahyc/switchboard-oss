package daemon

import (
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
	"github.com/seahyc/switchboard-oss/internal/store"
)

func TestMatchCacheRoundTrip(t *testing.T) {
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c, err := NewSQLiteMatchCache(st.DB(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Get("a|b", "h1"); ok {
		t.Fatal("empty cache should miss")
	}
	c.Put("a|b", "h1", idmatch.AdjudicationResult{SamePerson: true, Confidence: 0.91, Reason: "shared handle", Evidence: []string{"handle"}})
	got, ok := c.Get("a|b", "h1")
	if !ok || !got.SamePerson || got.Confidence != 0.91 || len(got.Evidence) != 1 {
		t.Fatalf("round-trip mismatch: %+v ok=%v", got, ok)
	}
	// Different ctx hash → miss (record changed).
	if _, ok := c.Get("a|b", "h2"); ok {
		t.Fatal("different ctx hash must miss")
	}
}

func TestNewWiresMatcherWhenEnabled(t *testing.T) {
	// EnableMatcher wires the adjudicator + feeds without contacting the model at
	// construction (ollama URL resolves to a fallback if docker is absent). Stays
	// propose-only; the worker only starts in Run().
	d, err := New(Config{DBPath: ":memory:", EnableMatcher: true, OllamaURL: "http://127.0.0.1:1", MatchInterval: time.Hour})
	if err != nil {
		t.Fatalf("New with matcher enabled should not error: %v", err)
	}
	defer d.store.Close()
	if !d.matchEnabled {
		t.Fatal("matchEnabled should be true")
	}
	if d.matchInterval != time.Hour {
		t.Fatalf("matchInterval = %v, want 1h", d.matchInterval)
	}
}

func TestNewMatcherOffByDefault(t *testing.T) {
	d, err := New(Config{DBPath: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer d.store.Close()
	if d.matchEnabled {
		t.Fatal("matcher must be OFF by default (no auto-start on deploy)")
	}
}
