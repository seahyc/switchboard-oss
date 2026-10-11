package comprehend

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// BriefConfig tunes brief assembly.
type BriefConfig struct {
	Now        time.Time
	Location   *time.Location // for the date header + due-today window (default SGT)
	Projection ProjectionConfig
	// MaxPerSection caps how many cards each section renders in the text brief
	// (the full set is still in the Brief struct for the Mini App). Default 8.
	MaxPerSection int
}

// SGT is Asia/Singapore; the brief's home timezone (07:00 SGT delivery).
var SGT = mustLoad("Asia/Singapore")

func mustLoad(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		return time.FixedZone("SGT", 8*3600)
	}
	return loc
}

func (c BriefConfig) withDefaults() BriefConfig {
	if c.Now.IsZero() {
		c.Now = time.Now()
	}
	if c.Location == nil {
		c.Location = SGT
	}
	if c.MaxPerSection == 0 {
		c.MaxPerSection = 8
	}
	if c.Projection.Now.IsZero() {
		c.Projection.Now = c.Now
	}
	return c
}

// CommitmentSource supplies due/open commitments for the brief. The extraction
// store (comprehend.db) implements this; a nil source yields an empty section so
// the brief ships before extraction does.
type CommitmentSource interface {
	DueOn(ctx context.Context, day time.Time, loc *time.Location) ([]Commitment, error)
	OpenCommitments(ctx context.Context, limit int) ([]Commitment, error)
}

// AssembleBrief builds the morning-brief payload from the deterministic
// projections plus (optionally) due commitments. It performs NO inference and
// sends NOTHING — purely composes data.
func (r *Reader) AssembleBrief(ctx context.Context, cfg BriefConfig, commits CommitmentSource) (*Brief, error) {
	cfg = cfg.withDefaults()
	proj, err := r.Project(ctx, cfg.Projection)
	if err != nil {
		return nil, err
	}
	b := &Brief{
		GeneratedAt:     cfg.Now,
		Date:            cfg.Now.In(cfg.Location).Format("Monday, 2 Jan"),
		WaitingOnMe:     orEmptyCards(proj.WaitingOnMe),
		Ghosting:        orEmptyCards(proj.Ghosting),
		OpenCommitments: []Commitment{},
		DueToday:        []Commitment{},
		Events:          []CalEvent{},
		Nudges:          []string{},
	}
	if commits != nil {
		if open, err := commits.OpenCommitments(ctx, 12); err == nil && len(open) > 0 {
			b.OpenCommitments = open
		}
		due, err := commits.DueOn(ctx, cfg.Now, cfg.Location)
		if err == nil && len(due) > 0 {
			b.DueToday = due
			for _, cm := range due {
				b.Events = append(b.Events, CalEvent{
					Title:       cm.What,
					When:        cm.SourceTS,
					SourceMsgID: cm.SourceMsgID,
				})
			}
		}
	}
	if n := buildNudges(b); n != nil {
		b.Nudges = n
	}
	return b, nil
}

func orEmptyCards(c []WaitingCard) []WaitingCard {
	if c == nil {
		return []WaitingCard{}
	}
	return c
}

// buildNudges derives event-driven, suggest-only nudges from the assembled brief.
func buildNudges(b *Brief) []string {
	var n []string
	// Oldest waiting-on-me thread beyond 3 days is worth a gentle nudge.
	if len(b.WaitingOnMe) > 0 {
		oldest := b.WaitingOnMe[0]
		for _, c := range b.WaitingOnMe {
			if c.WaitingHours > oldest.WaitingHours {
				oldest = c
			}
		}
		if oldest.WaitingHours >= 72 {
			n = append(n, fmt.Sprintf("%s has been waiting %s for a reply.", oldest.Person, humanDur(oldest.WaitingHours)))
		}
	}
	if len(b.DueToday) > 0 {
		n = append(n, fmt.Sprintf("%d commitment(s) due today.", len(b.DueToday)))
	}
	return n
}

// RenderText renders the plain-text brief delivered as a Telegram message. Tight,
// scannable, no preamble — and it always states explicitly that drafts are
// suggestions, never auto-sent.
func (b *Brief) RenderText(cfg BriefConfig) string {
	cfg = cfg.withDefaults()
	max := cfg.MaxPerSection
	var sb strings.Builder
	fmt.Fprintf(&sb, "🌅 Morning brief — %s\n", b.Date)

	if len(b.WaitingOnMe) == 0 && len(b.Ghosting) == 0 && len(b.DueToday) == 0 && len(b.OpenCommitments) == 0 {
		sb.WriteString("\nAll clear — nobody's waiting on you and nothing's on your plate. 🎉\n")
		return sb.String()
	}

	fmt.Fprintf(&sb, "\n⏳ Waiting on you (%d)\n", len(b.WaitingOnMe))
	if len(b.WaitingOnMe) == 0 {
		sb.WriteString("  • nobody — inbox zero on replies\n")
	}
	// For the morning brief, lead with the MOST RECENT unanswered pings (most
	// likely to still need action); stale closers sink to the tail. The deck
	// (waiting --json) keeps the canonical overdue-first order.
	for _, c := range topN(byRecency(b.WaitingOnMe), max) {
		fmt.Fprintf(&sb, "  • %s (%s) — \"%s\" · %s\n",
			c.Person, c.Platform, c.LastMsgText, humanDur(c.WaitingHours))
	}
	if extra := len(b.WaitingOnMe) - max; extra > 0 {
		fmt.Fprintf(&sb, "  • …and %d more\n", extra)
	}

	if len(b.Ghosting) > 0 {
		fmt.Fprintf(&sb, "\n👻 No reply yet from them (%d)\n", len(b.Ghosting))
		for _, c := range topN(b.Ghosting, max) {
			fmt.Fprintf(&sb, "  • %s (%s) — you reached out %s ago\n",
				c.Person, c.Platform, humanDur(c.WaitingHours))
		}
		if extra := len(b.Ghosting) - max; extra > 0 {
			fmt.Fprintf(&sb, "  • …and %d more\n", extra)
		}
	}

	if len(b.OpenCommitments) > 0 {
		fmt.Fprintf(&sb, "\n📌 On your plate (%d)\n", len(b.OpenCommitments))
		for _, cm := range topCommitments(b.OpenCommitments, max) {
			fmt.Fprintf(&sb, "  • %s%s\n", renderCommitment(cm), dueSuffix(cm.Due))
		}
		if extra := len(b.OpenCommitments) - max; extra > 0 {
			fmt.Fprintf(&sb, "  • …and %d more\n", extra)
		}
	}

	if len(b.DueToday) > 0 {
		fmt.Fprintf(&sb, "\n🗓 Due today (%d)\n", len(b.DueToday))
		for _, cm := range b.DueToday {
			fmt.Fprintf(&sb, "  • %s\n", renderCommitment(cm))
		}
	}

	if len(b.Nudges) > 0 {
		sb.WriteString("\n💡 Nudges\n")
		for _, nd := range b.Nudges {
			fmt.Fprintf(&sb, "  • %s\n", nd)
		}
	}

	sb.WriteString("\n(Drafts are suggestions — nothing is sent until you tap.)\n")
	return sb.String()
}

// byRecency returns a copy sorted by shortest wait first (most recent ping).
func byRecency(cards []WaitingCard) []WaitingCard {
	cp := make([]WaitingCard, len(cards))
	copy(cp, cards)
	sort.SliceStable(cp, func(i, j int) bool { return cp[i].WaitingHours < cp[j].WaitingHours })
	return cp
}

// renderCommitment formats "who → to_whom: what", framing direction so the
// user sees both their own promises and what others owe them.
func renderCommitment(cm Commitment) string {
	who := strings.TrimSpace(cm.Who)
	if cm.Direction == DirectionTheirs {
		if who == "" || who == "me" {
			return cm.What // unknown ower — just state the obligation
		}
		return fmt.Sprintf("%s owes you: %s", who, cm.What)
	}
	to := strings.TrimSpace(cm.ToWhom)
	if to == "" || to == "me" {
		return cm.What
	}
	return fmt.Sprintf("%s ← %s", cm.What, to)
}

func dueSuffix(due string) string {
	if due == "" {
		return ""
	}
	return " (due " + due + ")"
}

func topCommitments(cs []Commitment, n int) []Commitment {
	if len(cs) <= n {
		return cs
	}
	return cs[:n]
}

func topN(cards []WaitingCard, n int) []WaitingCard {
	if len(cards) <= n {
		return cards
	}
	return cards[:n]
}

func humanDur(hours int) string {
	if hours < 1 {
		return "just now"
	}
	if hours < 24 {
		return fmt.Sprintf("%dh", hours)
	}
	days := hours / 24
	return fmt.Sprintf("%dd", days)
}

// sortCommitmentsByDue is a small helper used by the commitment store's DueOn.
func sortCommitmentsByDue(cs []Commitment) {
	sort.SliceStable(cs, func(i, j int) bool { return cs[i].Due < cs[j].Due })
}
