package gbrainfeed

import (
	"testing"

	"github.com/seahyc/switchboard-oss/internal/api"
)

func hasStr(ss []string, v string) bool {
	for _, s := range ss {
		if s == v {
			return true
		}
	}
	return false
}

func TestMergeOnRead(t *testing.T) {
	// Corroborated cross-platform fragments (shared handle) → ONE composite card.
	osw := []api.PersonCard{
		{CanonicalID: "canon-phone-1", Name: "Morgan Example", Platforms: []string{"telegram"}, Handles: []string{"morganexample"},
			Identities: []api.CardIdentity{{Platform: "telegram", UserID: "t1"}}},
		{CanonicalID: "canon-email-1", Name: "Morgan Example", Platforms: []string{"gmail"}, Handles: []string{"morganexample"},
			Emails: []string{"morgan@example.com"}, Identities: []api.CardIdentity{{Platform: "gmail", UserID: "g1"}}},
	}
	got := mergeOnRead(osw)
	if len(got) != 1 {
		t.Fatalf("corroborated fragments (shared handle) should collapse to 1, got %d", len(got))
	}
	c := got[0]
	if c.MergeConfidence != "high" {
		t.Fatalf("composite should be high confidence, got %q", c.MergeConfidence)
	}
	if len(c.MemberIDs) != 2 || !hasStr(c.MemberIDs, "canon-phone-1") || !hasStr(c.MemberIDs, "canon-email-1") {
		t.Fatalf("composite must keep both member canonical_ids, got %+v", c.MemberIDs)
	}
	if !hasStr(c.Platforms, "telegram") || !hasStr(c.Platforms, "gmail") {
		t.Fatalf("composite must union platforms, got %+v", c.Platforms)
	}
	if len(c.Identities) != 2 {
		t.Fatalf("composite must union identities, got %d", len(c.Identities))
	}

	// Distinct same-names (no shared signal) → kept SEPARATE + flagged, never fused.
	johns := []api.PersonCard{
		{CanonicalID: "canon-phone-jt1", Name: "John Tan", Platforms: []string{"whatsapp"}, Phones: []string{"+6511111111"}},
		{CanonicalID: "canon-phone-jt2", Name: "John Tan", Platforms: []string{"telegram"}, Phones: []string{"+6522222222"}},
	}
	g2 := mergeOnRead(johns)
	if len(g2) != 2 {
		t.Fatalf("two different John Tans must stay separate, got %d", len(g2))
	}
	for _, jc := range g2 {
		if jc.MergeConfidence != "same_name_unverified" {
			t.Fatalf("distinct same-names must be flagged same_name_unverified, got %q", jc.MergeConfidence)
		}
		if len(jc.MemberIDs) != 0 {
			t.Fatal("unverified same-names must NOT be composed (no MemberIDs)")
		}
	}

	// Single fragment → unchanged, no merge label.
	solo := mergeOnRead([]api.PersonCard{{CanonicalID: "x", Name: "Solo Person"}})
	if len(solo) != 1 || solo[0].MergeConfidence != "" {
		t.Fatalf("single canonical must pass through unchanged, got %+v", solo)
	}

	// CROSS-TYPE: handle on one fragment == email local-part on another (Morgan's
	// real case: telegram handle "morganexample" ↔ morganexample@example.org) → high composite.
	oswCross := []api.PersonCard{
		{CanonicalID: "canon-phone-osw", Name: "Morgan Example", Platforms: []string{"telegram"}, Handles: []string{"morganexample"},
			Identities: []api.CardIdentity{{Platform: "telegram", UserID: "t9"}}},
		{CanonicalID: "canon-email-osw", Name: "Morgan Example", Platforms: []string{"gmail"}, Emails: []string{"morganexample@example.org"},
			Identities: []api.CardIdentity{{Platform: "gmail", UserID: "morganexample@example.org"}}}, // NO handle — only the email local-part
	}
	gx := mergeOnRead(oswCross)
	if len(gx) != 1 || gx[0].MergeConfidence != "high" {
		t.Fatalf("handle↔email-localpart cross-type match should composite Morgan, got %d cards conf=%q", len(gx), func() string {
			if len(gx) > 0 {
				return gx[0].MergeConfidence
			}
			return ""
		}())
	}

	// Generic local-part must NOT fuse strangers: two different "Foo Bar" sharing
	// only "info@" stays separate.
	generic := []api.PersonCard{
		{CanonicalID: "g1", Name: "Foo Bar", Emails: []string{"info@a.com"}},
		{CanonicalID: "g2", Name: "Foo Bar", Emails: []string{"info@b.com"}},
	}
	if gg := mergeOnRead(generic); len(gg) != 2 {
		t.Fatalf("generic local-part 'info' must NOT fuse same-name strangers, got %d", len(gg))
	}

	// Phone overlap corroborates (digits-only, ignores formatting).
	amy := []api.PersonCard{
		{CanonicalID: "a", Name: "Amy Lee", Platforms: []string{"whatsapp"}, Phones: []string{"+6590000000"}},
		{CanonicalID: "b", Name: "Amy Lee", Platforms: []string{"telegram"}, Phones: []string{"+65 9000 0000"}},
	}
	g4 := mergeOnRead(amy)
	if len(g4) != 1 || g4[0].MergeConfidence != "high" {
		t.Fatalf("phone overlap should corroborate → 1 high composite, got %d", len(g4))
	}
}
