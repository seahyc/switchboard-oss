package idmatch

import "testing"

// #33: deterministic shared-strong-identifier auto-merge (phone/email alone, no name
// match, no LLM). Precision is everything — these pin the merge-yes and merge-NO lines.
func TestSharedStrongIdentifier(t *testing.T) {
	cases := []struct {
		name     string
		a, b     PersonRecord
		wantKind string // "" = must NOT merge
	}{
		{
			"shared phone, DIFFERENT/blank names → merge (the @lid phone-twin case)",
			PersonRecord{CanonicalID: "lid", PrimaryName: "", Phones: []string{"15555550102"}},
			PersonRecord{CanonicalID: "wa", PrimaryName: "Morgan Example", Phones: []string{"15555550102"}},
			"phone",
		},
		{
			"shared email across platforms → merge",
			PersonRecord{CanonicalID: "lark", PrimaryName: "Morgan", Emails: []string{"morgan@example.com"}},
			PersonRecord{CanonicalID: "gmail", PrimaryName: "M. Example", Emails: []string{"morgan@example.com"}},
			"email",
		},
		{
			"DIFFERENT phones → never merge",
			PersonRecord{CanonicalID: "a", Phones: []string{"15555550103"}},
			PersonRecord{CanonicalID: "b", Phones: []string{"15555550104"}},
			"",
		},
		{
			"DIFFERENT emails (the google-noreply false-merge class) → never merge",
			PersonRecord{CanonicalID: "g1", PrimaryName: "Google", Emails: []string{"google-noreply@google.com"}},
			PersonRecord{CanonicalID: "g2", PrimaryName: "Google", Emails: []string{"no-reply@accounts.google.com"}},
			"",
		},
		{
			"exactly-shared ROLE address (noreply@) → never merge (a service, not a person)",
			PersonRecord{CanonicalID: "n1", PrimaryName: "Acme A", Emails: []string{"noreply@acme.com"}},
			PersonRecord{CanonicalID: "n2", PrimaryName: "Acme B", Emails: []string{"noreply@acme.com"}},
			"",
		},
		{
			"exactly-shared generic local-part (info@) on different domains → never merge",
			PersonRecord{CanonicalID: "i1", Emails: []string{"info@a.com"}},
			PersonRecord{CanonicalID: "i2", Emails: []string{"info@a.com"}},
			"",
		},
		{
			"shared SHORT-CODE number (<7 digits) → never merge",
			PersonRecord{CanonicalID: "s1", Phones: []string{"262966"}},
			PersonRecord{CanonicalID: "s2", Phones: []string{"262966"}},
			"",
		},
		{
			"phone normalization tolerates formatting (digits-only equal)",
			PersonRecord{CanonicalID: "f1", Phones: []string{"+1 555 555 0102"}},
			PersonRecord{CanonicalID: "f2", Phones: []string{"15555550102"}},
			"phone",
		},
		{
			"no identifiers → never merge",
			PersonRecord{CanonicalID: "x", PrimaryName: "Same Name"},
			PersonRecord{CanonicalID: "y", PrimaryName: "Same Name"},
			"",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// normalize phones the way the projector does before comparison
			tc.a.Phones = digitsAll(tc.a.Phones)
			tc.b.Phones = digitsAll(tc.b.Phones)
			kind, val, ok := SharedStrongIdentifier(tc.a, tc.b)
			if tc.wantKind == "" {
				if ok {
					t.Fatalf("expected NO merge, got %s=%q", kind, val)
				}
				return
			}
			if !ok || kind != tc.wantKind {
				t.Fatalf("expected merge kind=%s, got ok=%v kind=%q", tc.wantKind, ok, kind)
			}
			if val == "" {
				t.Fatal("merge must report the shared value for audit logging")
			}
		})
	}
}

// The candidate generator must SURFACE phone/email-sharing pairs (so the predicate can
// act on them), even when names share nothing.
func TestCandidateGenSurfacesStrongIDPairs(t *testing.T) {
	people := []PersonRecord{
		{CanonicalID: "lid", PrimaryName: "", Phones: []string{"15555550102"}},
		{CanonicalID: "wa", PrimaryName: "Totally Different Name", Phones: []string{"15555550102"}},
		{CanonicalID: "e1", PrimaryName: "A", Emails: []string{"jane.doe@corp.com"}},
		{CanonicalID: "e2", PrimaryName: "B", Emails: []string{"jane.doe@corp.com"}},
	}
	g := NewCandidateGenerator(DefaultCandidateConfig, nil)
	var phonePair, emailPair bool
	for _, p := range g.Candidates(people) {
		has := func(s string) bool {
			for _, x := range p.Signals {
				if x == s {
					return true
				}
			}
			return false
		}
		if (p.A == "lid" || p.B == "lid") && has(SignalSharedPhone) {
			phonePair = true
		}
		if (p.A == "e1" || p.B == "e1") && has(SignalSharedEmail) {
			emailPair = true
		}
	}
	if !phonePair {
		t.Fatal("shared-phone pair (different names) was not surfaced as a candidate")
	}
	if !emailPair {
		t.Fatal("shared-email pair was not surfaced as a candidate")
	}
}

func digitsAll(ss []string) []string {
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		var b []rune
		for _, r := range s {
			if r >= '0' && r <= '9' {
				b = append(b, r)
			}
		}
		out = append(out, string(b))
	}
	return out
}

// #2 coverage: handle ↔ email-localpart cross-type pair surfaces as a candidate
// (the Morgan telegram-handle ↔ gmail-localpart fragment that name/handle/email
// blocks miss alone). Propose-only; auto-merge stays gated elsewhere.
func TestCandidateGenCrossTypeHandleEmail(t *testing.T) {
	people := []PersonRecord{
		{CanonicalID: "tg", PrimaryName: "Morgan", Handles: []string{"morganexample"}},
		{CanonicalID: "gm", PrimaryName: "M. Example", Emails: []string{"morganexample@example.org"}},
		// generic local-part must NOT bridge strangers
		{CanonicalID: "s1", PrimaryName: "A", Emails: []string{"info@a.com"}},
		{CanonicalID: "s2", PrimaryName: "B", Handles: []string{"info"}},
	}
	g := NewCandidateGenerator(DefaultCandidateConfig, nil)
	var crossed, genericBridged bool
	for _, p := range g.Candidates(people) {
		ids := map[string]bool{p.A: true, p.B: true}
		has := func(s string) bool {
			for _, x := range p.Signals {
				if x == s {
					return true
				}
			}
			return false
		}
		if ids["tg"] && ids["gm"] && has(SignalCrossIdent) {
			crossed = true
		}
		if ids["s1"] && ids["s2"] {
			genericBridged = true
		}
	}
	if !crossed {
		t.Fatal("handle↔email-localpart pair must surface as a cross-ident candidate")
	}
	if genericBridged {
		t.Fatal("generic local-part 'info' must NOT bridge strangers")
	}
}
