package gbrainfeed

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/api"
	"github.com/seahyc/switchboard-oss/internal/event"
)

type mockStats struct{ rows []PersonStat }

func (m mockStats) PersonStats(context.Context) ([]PersonStat, error) { return m.rows, nil }

func TestRecomputeSalienceAndExport(t *testing.T) {
	f := newFake()
	p := NewProjector(f, nil)
	p.Start(context.Background())
	// Two named people on whatsapp.
	p.OnIngest(senderMsg(event.PlatformWhatsApp, "wa1@x", "Close Friend Lee", "+6580001111", ""))
	p.OnIngest(senderMsg(event.PlatformWhatsApp, "wa2@x", "Distant Acq Tan", "+6580002222", ""))
	p.Close()

	now := time.Now().UTC()
	p.SetPersonStatsProvider(mockStats{rows: []PersonStat{
		// close friend: lots of two-way DM, recent
		{Platform: "whatsapp", SenderID: "wa1@x", MsgCount: 400, YourMsgCount: 300, DMMsgCount: 400, HasDM: true, LastTS: now.AddDate(0, 0, -1)},
		// distant: few msgs, old
		{Platform: "whatsapp", SenderID: "wa2@x", MsgCount: 5, DMMsgCount: 5, HasDM: true, LastTS: now.AddDate(0, 0, -200)},
	}})
	if err := p.RecomputeSalience(context.Background()); err != nil {
		t.Fatal(err)
	}

	p.mu.Lock()
	sal := p.salience
	p.mu.Unlock()
	closeS := sal["canon-phone-6580001111"]
	distantS := sal["canon-phone-6580002222"]
	if closeS <= distantS {
		t.Fatalf("close friend should outscore distant acquaintance: %v vs %v", closeS, distantS)
	}

	report := p.CalibrationExport()
	for _, want := range []string{"calibration export", "Close Friend Lee", "Distant Acq Tan", "Ranked", "distribution"} {
		if !strings.Contains(report, want) {
			t.Fatalf("calibration export missing %q\n%s", want, report)
		}
	}
}

func TestRecomputeSalienceNoProviderNoop(t *testing.T) {
	p := NewProjector(newFake(), nil)
	if err := p.RecomputeSalience(context.Background()); err != nil {
		t.Fatalf("no provider → no-op, got %v", err)
	}
}

// #15: CoreNetwork ranks in-network people by salience desc, fills tier + last-
// interaction + platforms on each card, and honors the tier filter + limit.
func TestCoreNetwork(t *testing.T) {
	f := newFake()
	p := NewProjector(f, nil)
	p.Start(context.Background())
	p.OnIngest(senderMsg(event.PlatformWhatsApp, "wa1@x", "Close Friend Lee", "+6580001111", ""))
	p.OnIngest(senderMsg(event.PlatformWhatsApp, "wa2@x", "Distant Acq Tan", "+6580002222", ""))
	p.Close()

	now := time.Now().UTC()
	p.SetPersonStatsProvider(mockStats{rows: []PersonStat{
		{Platform: "whatsapp", SenderID: "wa1@x", MsgCount: 400, YourMsgCount: 300, DMMsgCount: 400, HasDM: true, LastTS: now.AddDate(0, 0, -1)},
		{Platform: "whatsapp", SenderID: "wa2@x", MsgCount: 5, DMMsgCount: 5, HasDM: true, LastTS: now.AddDate(0, 0, -200)},
	}})
	if err := p.RecomputeSalience(context.Background()); err != nil {
		t.Fatal(err)
	}

	all := p.CoreNetwork("", 0)
	if len(all) != 2 {
		t.Fatalf("expected 2 in-network people, got %d", len(all))
	}
	// ranked desc: close friend first
	if all[0].Name != "Close Friend Lee" {
		t.Fatalf("expected close friend ranked first, got %q (score %.0f)", all[0].Name, all[0].SalienceScore)
	}
	if all[0].SalienceScore <= all[1].SalienceScore {
		t.Fatalf("ranking not descending: %.0f then %.0f", all[0].SalienceScore, all[1].SalienceScore)
	}
	// card enrichment: tier + last-interaction + platforms populated
	if all[0].SalienceTier == "" {
		t.Fatal("card must carry a tier")
	}
	if all[0].LastInteraction == 0 {
		t.Fatal("card must carry last_interaction")
	}
	if len(all[0].Platforms) == 0 {
		t.Fatal("card must carry platforms")
	}
	// limit caps the result
	if got := p.CoreNetwork("", 1); len(got) != 1 {
		t.Fatalf("limit=1 should return 1, got %d", len(got))
	}
	// tier filter returns only that tier
	for _, c := range p.CoreNetwork(all[0].SalienceTier, 0) {
		if c.SalienceTier != all[0].SalienceTier {
			t.Fatalf("tier filter leaked %q into %q query", c.SalienceTier, all[0].SalienceTier)
		}
	}
}

// #15 calibration filters: nameless @lid + bot/service accounts must NOT surface in
// core_network (kept in data, excluded from "who matters").
func TestCoreNetworkFiltersNamelessAndBots(t *testing.T) {
	if namedHuman(PersonSignals{Identified: false, HasDM: true, Name: ""}) {
		t.Fatal("nameless canonical must be excluded from the view")
	}
	if !namedHuman(PersonSignals{Identified: true, Name: "Real Person"}) {
		t.Fatal("a named person must pass")
	}
	if namedHuman(PersonSignals{Identified: true, Name: "whatsapp user"}) {
		t.Fatal("placeholder name must be excluded")
	}
	if !isBotHandle([]string{"example_bot"}) || !isBotHandle([]string{"calendar_bot"}) {
		t.Fatal("telegram bot usernames (…bot) must be flagged")
	}
	if isBotHandle([]string{"morganexample"}) {
		t.Fatal("a normal handle must not be flagged as a bot")
	}
}

// #15: business/service senders excluded from core_network.
func TestIsBusinessOrService(t *testing.T) {
	yes := []api.PersonCard{
		{Name: "Morgan Example (Google Sheets)"},
		{Name: "OCBC Notifications"},
		{Name: "RESTORAN PEKIN JOHOR JAYA SDN BHD"},
		{Name: "Song-Cho (M) Sdn Bhd"},
		{Name: "Acme", Emails: []string{"noreply@acme.com"}},
		{Name: "News", Emails: []string{"newsletter@x.com"}},
	}
	for _, c := range yes {
		if !isBusinessOrService(c) {
			t.Fatalf("should be flagged business/service: %q %v", c.Name, c.Emails)
		}
	}
	no := []api.PersonCard{
		{Name: "Morgan Example", Emails: []string{"morgan@example.com"}},
		{Name: "Jordan Example"},
		{Name: "Some Unmarked Vendor"}, // no marker + not in the configured exclusions → not auto-caught (by design)
		{Name: "Drew Example", Emails: []string{"drewexample@example.org"}},
	}
	for _, c := range no {
		if isBusinessOrService(c) {
			t.Fatalf("must NOT be flagged business: %q %v", c.Name, c.Emails)
		}
	}
}

func TestUserDroppedBusinesses(t *testing.T) {
	t.Setenv("SWITCHBOARD_EXCLUDED_PEOPLE_NAMES", " Example Vendor \nANOTHER VENDOR")
	for _, n := range []string{"Example Vendor", "another vendor"} {
		if !isBusinessOrService(api.PersonCard{Name: n}) {
			t.Fatalf("configured exclusion missing: %q", n)
		}
	}
	if isBusinessOrService(api.PersonCard{Name: "Morgan Example"}) {
		t.Fatal("unconfigured person excluded")
	}
	t.Setenv("SWITCHBOARD_EXCLUDED_PEOPLE_NAMES", "")
	if isBusinessOrService(api.PersonCard{Name: "Example Vendor"}) {
		t.Fatal("exclusion retained after removal")
	}
}

// The default inner threshold is 10.
func TestInnerThresholdLockedAt10(t *testing.T) {
	if DefaultSalienceThresholds.Inner != 10 {
		t.Fatalf("inner threshold must be locked at 10, got %d", DefaultSalienceThresholds.Inner)
	}
	if Tier(9, 0, DefaultSalienceThresholds, "") != "active" {
		t.Fatal("salience 9 must be active (below inner≥10)")
	}
	if Tier(10, 0, DefaultSalienceThresholds, "") != "inner" {
		t.Fatal("salience 10 must be inner")
	}
}
