package gbrainfeed

import (
	"context"
	"testing"
	"time"
)

// mockStatsProvider stands in for core's store-backed PersonStatsProvider.
type mockStatsProvider struct{ stats []PersonStat }

func (m mockStatsProvider) PersonStats(context.Context) ([]PersonStat, error) { return m.stats, nil }

func TestRollUpMergesIdentitiesIntoCanonical(t *testing.T) {
	now := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	// One person on two platforms: WhatsApp + Telegram.
	stats := []PersonStat{
		{Platform: "whatsapp", SenderID: "wa1", MsgCount: 80, YourMsgCount: 60, DMMsgCount: 80,
			MentionsYou: 2, YouInitiated: 3, HasDM: true, LastTS: now.AddDate(0, 0, -3), DMPinned: true},
		{Platform: "telegram", SenderID: "tg1", MsgCount: 40, YourMsgCount: 50, DMMsgCount: 40,
			HasDM: true, RepliedToYou: true, LastTS: now.AddDate(0, 0, -1)},
	}
	memberships := []CanonicalMembership{{
		CanonicalID: "canon-phone-1", Name: "Dana", Identified: true,
		Members: []IdentityRef{{"whatsapp", "wa1"}, {"telegram", "tg1"}},
	}}

	got := RollUpSignals(memberships, stats, now)
	if len(got) != 1 {
		t.Fatalf("want 1 person, got %d", len(got))
	}
	p := got[0]
	if p.MsgCount != 120 {
		t.Errorf("MsgCount=%d want 120 (summed)", p.MsgCount)
	}
	if len(p.Platforms) != 2 {
		t.Errorf("platforms=%v want 2 (breadth)", p.Platforms)
	}
	if p.RecencyDays != 1 {
		t.Errorf("RecencyDays=%d want 1 (newest across identities)", p.RecencyDays)
	}
	if !p.HasDM || !p.DMPinned || !p.AddressedYou || !p.YouAddressedThem {
		t.Errorf("direct-interaction/curation flags not rolled up: %+v", p)
	}
	if p.Initiated != 3 {
		t.Errorf("Initiated=%d want 3", p.Initiated)
	}
	// reciprocity: your 110 vs their 120 → ~0.92 (two-way)
	if p.ReciprocityRatio < 0.8 {
		t.Errorf("ReciprocityRatio=%.2f want high (two-way)", p.ReciprocityRatio)
	}

	// And the rolled-up person scores into a high tier.
	r := Score(p, DefaultSalienceWeights)
	if Tier(r.Salience, p.RecencyDays, DefaultSalienceThresholds, "") != "inner" {
		t.Errorf("multi-platform two-way pinned DM should be inner, got salience %d (%s)", r.Salience, r.Reason)
	}
}

func TestRollUpFromProvider(t *testing.T) {
	now := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	prov := mockStatsProvider{stats: []PersonStat{
		{Platform: "gmail", SenderID: "x@y.com", MsgCount: 5, LastTS: now.AddDate(0, 0, -200)},
	}}
	stats, err := prov.PersonStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := RollUpSignals([]CanonicalMembership{{
		CanonicalID: "canon-email-x", Name: "X", Identified: true,
		Members: []IdentityRef{{"gmail", "x@y.com"}},
	}}, stats, now)
	if got[0].RecencyDays != 200 {
		t.Fatalf("RecencyDays=%d want 200", got[0].RecencyDays)
	}
}
