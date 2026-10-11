package gbrainfeed

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// CalibrationReport renders the markdown the lead reviews WITH the user to set the
// tier cut-lines. It is a PURE function of (signals, weights, thresholds) over a
// cached signal set, so re-fitting weights/thresholds and re-rendering is instant —
// no re-query, no daemon, no rebuild. Three sections:
//  1. ranked table (salience desc): rank, name, salience, top-driver, reason, platforms, last seen
//  2. score distribution: quantiles + a coarse histogram (so natural breaks show)
//  3. boundary zoom: the ~N people just above/below each candidate cut-line
//
// now anchors "last seen" rendering; pass the collection time so a cached report
// is reproducible.
func CalibrationReport(people []PersonSignals, w SalienceWeights, t SalienceThresholds, now time.Time) string {
	scored := ScoreAll(people, w)

	var b strings.Builder
	fmt.Fprintf(&b, "# People salience — calibration export\n\n")
	fmt.Fprintf(&b, "in_network people scored: **%d** (of %d total)\n\n", len(scored), len(people))
	fmt.Fprintf(&b, "Current draft thresholds: inner ≥ %d · active ≥ %d · peripheral ≥ %d · dormant if >%dd stale.\n",
		t.Inner, t.Active, t.Peripheral, t.DormantDays)
	b.WriteString("_(Tiers below are a PREVIEW from the draft cut-lines — not yet persisted. Adjust thresholds/weights and re-run.)_\n\n")

	writeRankedTable(&b, scored, t, now)
	writeDistribution(&b, scored)
	writeBoundaryZoom(&b, scored, t, now)
	return b.String()
}

func writeRankedTable(b *strings.Builder, scored []ScoredPerson, t SalienceThresholds, now time.Time) {
	b.WriteString("## Ranked (by salience)\n\n")
	b.WriteString("| # | name | salience | tier(preview) | top driver | reason | platforms | last seen |\n")
	b.WriteString("|--:|------|---------:|------|------|------|------|------|\n")
	for i, sp := range scored {
		s := sp.Signals
		tier := Tier(sp.Result.Salience, s.RecencyDays, t, s.Override)
		fmt.Fprintf(b, "| %d | %s | %d | %s | %s | %s | %s | %s |\n",
			i+1, nameOrID(s), sp.Result.Salience, tier, sp.Result.TopDriver,
			sp.Result.Reason, strings.Join(s.Platforms, "+"), lastSeen(s, now))
	}
	b.WriteString("\n")
}

func writeDistribution(b *strings.Builder, scored []ScoredPerson) {
	b.WriteString("## Score distribution\n\n")
	if len(scored) == 0 {
		b.WriteString("_(no scored people)_\n\n")
		return
	}
	vals := make([]int, len(scored))
	for i, sp := range scored {
		vals[i] = sp.Result.Salience
	}
	sort.Ints(vals)

	b.WriteString("Quantiles: ")
	qs := []struct {
		label string
		p     float64
	}{{"p10", .10}, {"p25", .25}, {"p50", .50}, {"p75", .75}, {"p90", .90}, {"p95", .95}, {"p99", .99}}
	parts := make([]string, 0, len(qs))
	for _, q := range qs {
		parts = append(parts, fmt.Sprintf("%s=%d", q.label, quantile(vals, q.p)))
	}
	b.WriteString(strings.Join(parts, " · "))
	fmt.Fprintf(b, " · min=%d · max=%d\n\n", vals[0], vals[len(vals)-1])

	// Coarse histogram by salience value (each distinct score → a bar).
	b.WriteString("Histogram (count per salience):\n\n```\n")
	counts := map[int]int{}
	for _, v := range vals {
		counts[v]++
	}
	keys := make([]int, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(keys)))
	for _, k := range keys {
		fmt.Fprintf(b, "%4d | %s %d\n", k, bar(counts[k]), counts[k])
	}
	b.WriteString("```\n\n")
}

func writeBoundaryZoom(b *strings.Builder, scored []ScoredPerson, t SalienceThresholds, now time.Time) {
	b.WriteString("## Boundary zoom (cut-line candidates)\n\n")
	b.WriteString("_The people straddling each draft cut-line — where your judgment matters most._\n\n")
	for _, cut := range []struct {
		name  string
		score int
	}{{"inner", t.Inner}, {"active", t.Active}, {"peripheral", t.Peripheral}} {
		fmt.Fprintf(b, "### %s cut-line (salience %d)\n\n", cut.name, cut.score)
		zoomAround(b, scored, cut.score, 5, t, now)
		b.WriteString("\n")
	}
}

// zoomAround prints the n people just above and just below the cut score.
func zoomAround(b *strings.Builder, scored []ScoredPerson, cut, n int, t SalienceThresholds, now time.Time) {
	// scored is sorted desc; find the split index where salience drops below cut.
	split := len(scored)
	for i, sp := range scored {
		if sp.Result.Salience < cut {
			split = i
			break
		}
	}
	lo := split - n
	if lo < 0 {
		lo = 0
	}
	hi := split + n
	if hi > len(scored) {
		hi = len(scored)
	}
	if lo == hi {
		b.WriteString("_(none near this cut-line)_\n")
		return
	}
	b.WriteString("| name | salience | tier(preview) | top driver | last seen |\n")
	b.WriteString("|------|---------:|------|------|------|\n")
	for i := lo; i < hi; i++ {
		sp := scored[i]
		marker := ""
		if i == split-1 {
			marker = " ⟵ above"
		} else if i == split {
			marker = " ⟵ below"
		}
		fmt.Fprintf(b, "| %s%s | %d | %s | %s | %s |\n",
			nameOrID(sp.Signals), marker, sp.Result.Salience,
			Tier(sp.Result.Salience, sp.Signals.RecencyDays, t, sp.Signals.Override),
			sp.Result.TopDriver, lastSeen(sp.Signals, now))
	}
}

// --- helpers ---

func quantile(sorted []int, p float64) int {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func bar(n int) string {
	if n > 60 {
		n = 60
	}
	return strings.Repeat("█", n)
}

func nameOrID(s PersonSignals) string {
	if s.Identified && s.Name != "" {
		return s.Name
	}
	return "(" + s.CanonicalID + ")"
}

func lastSeen(s PersonSignals, now time.Time) string {
	d := s.RecencyDays
	if d < 0 && !s.LastSeen.IsZero() && !now.IsZero() {
		d = int(now.Sub(s.LastSeen).Hours() / 24)
	}
	if d < 0 {
		return "—"
	}
	switch {
	case d <= 0:
		return "today"
	case d == 1:
		return "1d ago"
	case d < 30:
		return fmt.Sprintf("%dd ago", d)
	case d < 365:
		return fmt.Sprintf("%dmo ago", d/30)
	default:
		return fmt.Sprintf("%dy ago", d/365)
	}
}
