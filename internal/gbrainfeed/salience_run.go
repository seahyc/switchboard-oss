package gbrainfeed

import (
	"context"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/seahyc/switchboard-oss/internal/api"
)

// SetPersonStatsProvider wires the #15 per-identity interaction-stats feed (core2,
// same store pass as IdentityContext). Call before RecomputeSalience.
func (p *Projector) SetPersonStatsProvider(prov PersonStatsProvider) {
	p.personStatsProvider = prov
}

// RecomputeSalience pulls PersonStats, rolls them up per canonical person, scores
// salience (deterministic, LLM-free), and updates: (a) the in-memory salience map
// that orders the #19 CPU adjudication batch (core-network first), and (b) the
// cached PersonSignals the calibration export renders for the user session.
//
// Per the #15 design it scores SILENTLY — it does NOT write tier labels to pages
// until the cut-lines are calibrated + locked. Best-effort: no provider = no-op.
func (p *Projector) RecomputeSalience(ctx context.Context) error {
	if p.personStatsProvider == nil {
		return nil
	}
	stats, err := p.personStatsProvider.PersonStats(ctx)
	if err != nil {
		return err
	}

	canon := p.resolver.AllCanonicals()
	memberships := make([]CanonicalMembership, 0, len(canon))
	for _, cp := range canon {
		refs := make([]IdentityRef, 0, len(cp.Members))
		for _, m := range cp.Members {
			refs = append(refs, IdentityRef{Platform: m.Platform, UserID: m.UserID})
		}
		memberships = append(memberships, CanonicalMembership{
			CanonicalID: cp.ID, Name: cp.Name, Identified: cp.Identified, IsSelf: cp.IsSelf, Members: refs,
		})
	}

	signals := RollUpSignals(memberships, stats, time.Now().UTC())

	sal := make(map[string]float64, len(signals))
	for _, s := range signals {
		if !InNetwork(s) {
			continue // bystanders get no salience (honest people-view)
		}
		sal[s.CanonicalID] = float64(Score(s, p.salienceWeights()).Salience)
	}

	p.mu.Lock()
	p.salience = sal
	p.personSignals = signals
	p.mu.Unlock()
	p.log.Debug("salience recomputed", "scored", len(sal), "total", len(signals))
	return nil
}

// SalienceRanking returns every in-network person scored + sorted by salience desc
// (the calibration view + the data behind core_network). Pure read of the last
// RecomputeSalience pass; empty until the first tick runs.
func (p *Projector) SalienceRanking() []ScoredPerson {
	p.mu.Lock()
	signals := append([]PersonSignals(nil), p.personSignals...)
	p.mu.Unlock()
	return ScoreAll(signals, p.salienceWeights())
}

// CoreNetwork answers "who matters most to me" (#15): the salience-ranked people,
// optionally filtered to ONE tier (inner/active/peripheral/dormant), capped at limit
// (limit<=0 = all). Each card carries the salience score, tier, last-interaction ts,
// and platforms. Backs the core_network/top_people MCP tool.
func (p *Projector) CoreNetwork(tier string, limit int) []api.PersonCard {
	th := p.salienceThresholdz()
	ranked := p.SalienceRanking()
	out := make([]api.PersonCard, 0, len(ranked))
	for _, sp := range ranked {
		// NAMELESS exclusion (#15 part-A): nameless @lid group-participants rank high
		// on raw activity but aren't "people who matter" — keep them in the data, just
		// not in this view. Require a real, non-placeholder name.
		if !namedHuman(sp.Signals) {
			continue
		}
		t := Tier(sp.Result.Salience, sp.Signals.RecencyDays, th, sp.Signals.Override)
		if tier != "" && t != tier {
			continue
		}
		card := p.salienceCard(sp, t)
		// BOT/SERVICE exclusion: a Telegram bot's @username MUST end in "bot"
		// (platform rule) — so a bot-suffixed handle reliably flags the agent's own
		// bots without name hardcoding. Not "who matters to me".
		if isBotHandle(card.Handles) {
			continue
		}
		// BUSINESS/SERVICE exclusion: org mailboxes, no-reply/notification broadcasters,
		// (Google Sheets) doc pseudo-persons, and registered-company names (Sdn Bhd/Pte
		// Ltd) aren't personal relationships. (Heuristic — a named whatsapp business with
		// no marker can't be auto-detected; those stay until the user overrides them.)
		if isBusinessOrService(card) {
			continue
		}
		out = append(out, card)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// namedHuman gates the core-network view to canonicals with a real resolved name
// (excludes unidentified / placeholder-named @lid group bystanders).
func namedHuman(s PersonSignals) bool {
	n := strings.TrimSpace(s.Name)
	return s.Identified && n != "" && !isPlaceholderName(n)
}

// isBotHandle reports whether any handle is a bot username (ends in "bot").
func isBotHandle(handles []string) bool {
	for _, h := range handles {
		if strings.HasSuffix(strings.ToLower(strings.TrimSpace(h)), "bot") {
			return true
		}
	}
	return false
}

// orgMarkerRe matches registered-company suffixes in a display name.
var orgMarkerRe = regexp.MustCompile(`(?i)\b(sdn\.? ?bhd|pte\.? ?ltd|\bbhd\b|\bllc\b|gmbh|\bpvt\b)\b`)

// userDroppedBusiness checks private, installation-specific display-name exclusions.
func userDroppedBusiness(name string) bool {
	for _, excluded := range strings.Split(os.Getenv("SWITCHBOARD_EXCLUDED_PEOPLE_NAMES"), "\n") {
		if strings.EqualFold(strings.TrimSpace(excluded), name) && name != "" {
			return true
		}
	}
	return false
}

// isBusinessOrService excludes configured names, bot mailboxes and company markers.
func isBusinessOrService(card api.PersonCard) bool {
	n := strings.ToLower(strings.TrimSpace(card.Name))
	if userDroppedBusiness(n) {
		return true
	}
	if strings.Contains(n, "(google ") {
		return true
	}
	if strings.Contains(n, "notification") || strings.Contains(n, "no-reply") ||
		strings.Contains(n, "noreply") || strings.Contains(n, "do not reply") {
		return true
	}
	if orgMarkerRe.MatchString(card.Name) {
		return true
	}
	// Pure email broadcaster: has email(s) and EVERY local-part is a role/system word
	// (none distinctive) → a mailbox, not a person.
	if len(card.Emails) > 0 {
		for _, e := range card.Emails {
			if distinctiveIdent(emailLocalPart(e)) {
				return false // a personal-looking address → not a service
			}
		}
		return true
	}
	return false
}

// salienceCard builds a ranked person's card: the full identity card (from the
// resolver canonical, so platforms/handles/emails populate) enriched with the
// salience score, tier, and last-interaction. No merge-on-read — this is a ranked
// list of canonicals, so distinct same-named people stay distinct rows.
func (p *Projector) salienceCard(sp ScoredPerson, tier string) api.PersonCard {
	var card api.PersonCard
	if cp, ok := p.resolver.Snapshot(sp.Signals.CanonicalID); ok {
		card = p.buildCard(cp, "")
	} else {
		card = api.PersonCard{CanonicalID: sp.Signals.CanonicalID, Name: sp.Signals.Name, Platforms: sp.Signals.Platforms}
	}
	card.SalienceScore = float64(sp.Result.Salience)
	card.SalienceTier = tier
	if !sp.Signals.LastSeen.IsZero() {
		card.LastInteraction = sp.Signals.LastSeen.Unix()
	}
	return card
}

// CalibrationExport renders the ranked salience report (ranked table + distribution
// + boundary-zoom) for the lead's interactive cut-line calibration session with the
// user. Uses the current (default or tuned) weights + thresholds.
func (p *Projector) CalibrationExport() string {
	p.mu.Lock()
	signals := append([]PersonSignals(nil), p.personSignals...)
	p.mu.Unlock()
	return CalibrationReport(signals, p.salienceWeights(), p.salienceThresholdz(), time.Now().UTC())
}

// SetSalienceConfig overrides the weights/thresholds (the calibration re-fit). Pass
// the JSON-decoded blocks from the calibration session.
func (p *Projector) SetSalienceConfig(w *SalienceWeights, t *SalienceThresholds) {
	p.mu.Lock()
	if w != nil {
		p.salWeights = w
	}
	if t != nil {
		p.salThresholds = t
	}
	p.mu.Unlock()
}

// TierOf returns the core-network tier (inner/active/peripheral/dormant) for a
// canonical id from the last RecomputeSalience pass, or "" if the person wasn't
// scored / isn't in-network yet. Used by ResolveSocialAuthor so resolved cards carry
// salience_tier (the honcho + briefing gates depend on it). Cheap: a lookup over the
// cached signals, no re-scoring.
func (p *Projector) TierOf(canonicalID string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	score, ok := p.salience[canonicalID]
	if !ok {
		return ""
	}
	for _, s := range p.personSignals {
		if s.CanonicalID == canonicalID {
			return Tier(int(score), s.RecencyDays, p.salienceThresholdz(), s.Override)
		}
	}
	return ""
}

func (p *Projector) salienceWeights() SalienceWeights {
	if p.salWeights != nil {
		return *p.salWeights
	}
	return DefaultSalienceWeights
}

func (p *Projector) salienceThresholdz() SalienceThresholds {
	if p.salThresholds != nil {
		return *p.salThresholds
	}
	return DefaultSalienceThresholds
}
