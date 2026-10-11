package gbrainfeed

import (
	"strings"
	"testing"
	"time"
)

// --- synthetic personas ---

func closeFriend() PersonSignals {
	return PersonSignals{
		CanonicalID: "canon-phone-1", Name: "Close Friend", Identified: true,
		Platforms: []string{"telegram", "whatsapp"}, RecencyDays: 2,
		MsgCount: 500, ReciprocityRatio: 0.8, Initiated: 10, MentionsExch: 5,
		HasDM: true, AddressedYou: true, YouAddressedThem: true,
	}
}

func multiPlatformColleague() PersonSignals {
	return PersonSignals{
		CanonicalID: "canon-email-2", Name: "Colleague", Identified: true,
		Platforms: []string{"lark", "gmail", "telegram"}, RecencyDays: 10,
		MsgCount: 200, ReciprocityRatio: 0.5, HasDM: true,
		AddressedYou: true, YouAddressedThem: true,
	}
}

func mutedAcquaintance() PersonSignals {
	return PersonSignals{
		CanonicalID: "canon-phone-3", Name: "Muted Acq", Identified: true,
		Platforms: []string{"whatsapp"}, RecencyDays: 100,
		MsgCount: 20, ReciprocityRatio: 0.05, HasDM: true, DMMuted: true,
		YouAddressedThem: true,
	}
}

func dormantContact() PersonSignals {
	return PersonSignals{
		CanonicalID: "canon-phone-4", Name: "Old Contact", Identified: true,
		Platforms: []string{"whatsapp"}, RecencyDays: 400,
		MsgCount: 30, ReciprocityRatio: 0.3, HasDM: true, YouAddressedThem: true,
	}
}

// nameless group-only participant — excluded from the network entirely.
func groupBystander() PersonSignals {
	return PersonSignals{
		CanonicalID: "canon-whatsapp-5-lid", Identified: false,
		Platforms: []string{"whatsapp"}, RecencyDays: 5, MsgCount: 3,
		HasDM: false, AddressedYou: false, YouAddressedThem: false,
	}
}

func TestInNetworkPredicate(t *testing.T) {
	cases := []struct {
		name string
		p    PersonSignals
		want bool
	}{
		{"named", PersonSignals{Identified: true}, true},
		{"has dm", PersonSignals{HasDM: true}, true},
		{"they addressed you", PersonSignals{AddressedYou: true}, true},
		{"you addressed them", PersonSignals{YouAddressedThem: true}, true},
		{"nameless group bystander", groupBystander(), false},
		{"empty", PersonSignals{}, false},
	}
	for _, c := range cases {
		if got := InNetwork(c.p); got != c.want {
			t.Errorf("%s: InNetwork=%v want %v", c.name, got, c.want)
		}
	}
}

func TestScoreTiersForPersonas(t *testing.T) {
	w := DefaultSalienceWeights
	th := DefaultSalienceThresholds
	cases := []struct {
		name     string
		p        PersonSignals
		wantTier string
	}{
		{"close friend → inner", closeFriend(), "inner"},
		{"multi-platform colleague → inner", multiPlatformColleague(), "inner"},
		{"dormant (>1y) → dormant", dormantContact(), "dormant"},
	}
	for _, c := range cases {
		r := Score(c.p, w)
		tier := Tier(r.Salience, c.p.RecencyDays, th, c.p.Override)
		if tier != c.wantTier {
			t.Errorf("%s: salience=%d tier=%q want %q (reason: %s)", c.name, r.Salience, tier, c.wantTier, r.Reason)
		}
	}
}

func TestMutedDragsScoreDown(t *testing.T) {
	w := DefaultSalienceWeights
	muted := mutedAcquaintance()
	unmuted := muted
	unmuted.DMMuted = false
	if Score(muted, w).Salience >= Score(unmuted, w).Salience {
		t.Fatal("muting must lower salience")
	}
	// muted is the dominant negative driver here
	if Score(muted, w).TopDriver != "muted" {
		t.Fatalf("top driver=%q want muted", Score(muted, w).TopDriver)
	}
}

func TestTopDriverIsLargestMagnitude(t *testing.T) {
	r := Score(closeFriend(), DefaultSalienceWeights)
	// volume (500 msgs → cap 4) and recency(+3)/two-way(+3) compete; cap 4 wins.
	if r.TopDriver != "500 msgs" {
		t.Fatalf("top driver=%q want '500 msgs'", r.TopDriver)
	}
}

func TestTierOverrideTrumps(t *testing.T) {
	p := dormantContact()
	p.Override = "inner"
	if got := Tier(Score(p, DefaultSalienceWeights).Salience, p.RecencyDays, DefaultSalienceThresholds, p.Override); got != "inner" {
		t.Fatalf("override should force inner, got %q", got)
	}
}

func TestRefitMovesBoundaries(t *testing.T) {
	w := DefaultSalienceWeights
	p := mutedAcquaintance() // lands somewhere low-mid
	s := Score(p, w).Salience

	// With a threshold at/below the score → peripheral or better; raise it → dormant.
	loose := SalienceThresholds{Inner: 99, Active: 99, Peripheral: s, DormantDays: 365}
	tight := SalienceThresholds{Inner: 99, Active: 99, Peripheral: s + 1, DormantDays: 365}
	if Tier(s, p.RecencyDays, loose, "") != "peripheral" {
		t.Fatalf("score %d should be peripheral at floor %d", s, loose.Peripheral)
	}
	if Tier(s, p.RecencyDays, tight, "") != "dormant" {
		t.Fatalf("score %d should drop to dormant when floor raised to %d", s, tight.Peripheral)
	}
}

func TestReweightChangesScore(t *testing.T) {
	p := closeFriend()
	base := Score(p, DefaultSalienceWeights).Salience
	w2 := DefaultSalienceWeights
	w2.ReciprocityTwoWay = 10 // bump two-way weight
	if Score(p, w2).Salience <= base {
		t.Fatal("raising the two-way weight should raise a two-way person's score")
	}
}

func TestScoreAllExcludesBystandersAndSorts(t *testing.T) {
	people := []PersonSignals{
		mutedAcquaintance(), closeFriend(), groupBystander(), multiPlatformColleague(),
	}
	scored := ScoreAll(people, DefaultSalienceWeights)
	if len(scored) != 3 {
		t.Fatalf("bystander must be excluded: want 3 scored, got %d", len(scored))
	}
	for i := 1; i < len(scored); i++ {
		if scored[i-1].Result.Salience < scored[i].Result.Salience {
			t.Fatal("ScoreAll must be sorted by salience desc")
		}
	}
}

func TestVolumePointsCap(t *testing.T) {
	if volumePoints(0) != 0 || volumePoints(5) != 1 || volumePoints(50) != 2 || volumePoints(500) != 3 || volumePoints(5000) != 4 {
		t.Fatal("volume bucketing wrong")
	}
	if capInt(volumePoints(5000), 4) != 4 {
		t.Fatal("volume cap not applied")
	}
}

func TestCalibrationReportSections(t *testing.T) {
	people := []PersonSignals{closeFriend(), multiPlatformColleague(), mutedAcquaintance(), dormantContact(), groupBystander()}
	now := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	report := CalibrationReport(people, DefaultSalienceWeights, DefaultSalienceThresholds, now)
	for _, want := range []string{
		"calibration export",
		"in_network people scored: **4**", // bystander excluded
		"## Ranked (by salience)",
		"Close Friend",
		"top driver",
		"## Score distribution",
		"Quantiles:",
		"## Boundary zoom",
		"inner cut-line",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q\n---\n%s", want, report)
		}
	}
	// bystander must not appear in the report
	if strings.Contains(report, "canon-whatsapp-5-lid") {
		t.Fatalf("excluded bystander leaked into report")
	}
}

// #15: self is never part of "who matters to me" (the agent's own bot identity
// must not rank into the core network).
func TestInNetworkExcludesSelf(t *testing.T) {
	self := PersonSignals{Identified: true, HasDM: true, IsSelf: true}
	if InNetwork(self) {
		t.Fatal("IsSelf canonical must be excluded from the network")
	}
	other := PersonSignals{Identified: true, HasDM: true}
	if !InNetwork(other) {
		t.Fatal("a normal identified+DM person must be in-network")
	}
	// ScoreAll drops self entirely.
	got := ScoreAll([]PersonSignals{self, other}, DefaultSalienceWeights)
	if len(got) != 1 || got[0].Signals.IsSelf {
		t.Fatalf("ScoreAll must drop self, got %d", len(got))
	}
}
