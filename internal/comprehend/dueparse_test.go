package comprehend

import (
	"context"
	"testing"
	"time"
)

func TestParseDueFromText(t *testing.T) {
	// Anchor: Friday 2026-06-12 (SGT).
	anchor := time.Date(2026, 6, 12, 9, 0, 0, 0, SGT)
	cases := []struct {
		text string
		want string
	}{
		// The /goal-1 gate case: explicit ISO date.
		{"I'll send Casey Example the olymp-b792f1 report by 2026-06-16.", "2026-06-16"},
		{"deck by 2026-12-01", "2026-12-01"},
		// Month-name forms.
		{"can you get it to me by June 16?", "2026-06-16"},
		{"due Jun 16th", "2026-06-16"},
		{"send it 16 June", "2026-06-16"},
		{"ship by December 1", "2026-12-01"},
		// Relative day words.
		{"I'll do it tomorrow", "2026-06-13"},
		{"sending today", "2026-06-12"},
		{"by EOD", "2026-06-12"},
		{"in 3 days", "2026-06-15"},
		// Weekdays (anchor is Friday 6/12).
		{"by Monday", "2026-06-15"},    // next Monday
		{"by Friday", "2026-06-12"},    // today is Friday → today
		{"next Tuesday", "2026-06-16"}, // Tuesday of next week
		{"end of week", "2026-06-12"},  // upcoming Friday = today
		// No date.
		{"I'll send the report soon", ""},
		{"thanks!", ""},
	}
	for _, c := range cases {
		got := parseDueFromText(c.text, anchor, SGT)
		if got != c.want {
			t.Errorf("parseDueFromText(%q) = %q, want %q", c.text, got, c.want)
		}
	}
}

func TestParseDueFromText_PastMonthRollsToNextYear(t *testing.T) {
	// Anchor late December; "January 5" with no year means next year.
	anchor := time.Date(2026, 12, 20, 9, 0, 0, 0, SGT)
	if got := parseDueFromText("let's aim for January 5", anchor, SGT); got != "2027-01-05" {
		t.Errorf("got %q, want 2027-01-05", got)
	}
}

func TestExtractor_RecoversDueFromTextWhenModelDropsIt(t *testing.T) {
	r := testReader(t)
	st, _ := OpenStore(":memory:")
	defer st.Close()
	seedCandidate(t, r, "telegram:user:100000001:1", "telegram:user:100000001", "2026-06-12T01:00:00Z",
		"I'll send Casey Example the olymp-b792f1 report by 2026-06-16", "me", 1, "important")
	// Model returns a commitment but (as the real 3b does) drops the due date.
	local := &fakeModel{replies: []string{`{"commitments":[{"who":"me","to_whom":"Casey Example","what":"send the olymp-b792f1 report","due":"","direction":"mine","confidence":1.0}]}`}}
	ex := &Extractor{Reader: r, Store: st, Local: local, Now: func() time.Time { return mustTime("2026-06-12T02:00:00Z") }}
	if _, err := ex.Run(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	cs, _ := st.Commitments(context.Background(), "")
	if len(cs) != 1 {
		t.Fatalf("want 1 commitment, got %d", len(cs))
	}
	if cs[0].Due != "2026-06-16" {
		t.Errorf("due not recovered from text: got %q want 2026-06-16", cs[0].Due)
	}
}
