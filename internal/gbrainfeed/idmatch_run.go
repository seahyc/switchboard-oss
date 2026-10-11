package gbrainfeed

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
)

// IdentityContext is the per-identity context the daemon supplies (core2) for the
// LLM identity matcher — handles, chats (shared-group blocking), sparse bio, and a
// few message snippets. Keyed by (Platform, SenderID == identity UserID).
type IdentityContext struct {
	Platform string
	SenderID string
	Handles  []string
	ChatIDs  []string
	Bio      string
	Snippets []string
}

// IdentityContextProvider is the daemon-wired capability (mirrors PersonStatsProvider)
// that exposes per-identity context for adjudication. Empty/sparse bio is expected.
type IdentityContextProvider interface {
	IdentityContext(ctx context.Context) ([]IdentityContext, error)
}

// SetIdentityContextProvider wires the context feed. Call before RunIdentityMatch.
func (p *Projector) SetIdentityContextProvider(prov IdentityContextProvider) {
	p.idCtxProvider = prov
}

// SetAdjudicator wires the LLM adjudicator (core2's ollama client) + cache. Call
// before RunIdentityMatch. nil adjudicator disables the matcher.
func (p *Projector) SetAdjudicator(adj idmatch.LLMAdjudicator, cache idmatch.AdjudicationCache) {
	p.adjudicator = adj
	if cache == nil {
		cache = idmatch.NewMemoryCache()
	}
	p.matchCache = cache
}

// SetAutoMerge toggles whether HIGH-confidence LLM verdicts auto-merge. Stays FALSE
// (propose-only) until the lead confirms the eval gate (≥99% precision). When false,
// even high-confidence verdicts go to the human review queue.
func (p *Projector) SetAutoMerge(on bool) { p.autoMergeEnabled = on }

// SetSalience supplies the per-canonical importance scores (#15) used to adjudicate
// the user's core-network FIRST in the CPU batch.
func (p *Projector) SetSalience(s map[string]float64) { p.salience = s }

// RunIdentityMatch is the #19 pass the daemon's off-ingest worker calls on a tick:
// build records from the resolver + IdentityContext → candidate-gen → importance-
// ordered, cached LLM adjudication → route (auto-merge if gated ON, else propose to
// the human review queue; drop low-confidence). Bounded + resumable (ctx). Best-
// effort: a missing provider/adjudicator is a no-op.
func (p *Projector) RunIdentityMatch(ctx context.Context) error {
	if p.adjudicator == nil {
		return nil
	}
	records, byID, err := p.buildMatchRecords(ctx)
	if err != nil || len(records) < 2 {
		return err
	}

	gen := idmatch.NewCandidateGenerator(idmatch.DefaultCandidateConfig, nil)

	// Deterministic pre-route (user policy): a corroborated EXACT-name pair (same
	// normalized name + shared handle/group) auto-merges WITHOUT the LLM. Everything
	// else flows to adjudication. This both saves qwen calls and applies regardless
	// of the eval gate (it's a deterministic rule, not an LLM verdict).
	var llm []idmatch.CandidatePair
	for _, pair := range gen.Candidates(records) {
		if p.tryDeterministicMerge(ctx, pair, byID) {
			continue
		}
		llm = append(llm, pair)
	}
	queue := idmatch.OrderByImportance(llm, p.salience)

	decisions, err := idmatch.AdjudicateBatch(ctx, p.adjudicator, p.matchCache, byID, queue,
		idmatch.DefaultThresholds, contextHash)
	if err != nil {
		return err
	}

	for _, d := range decisions {
		a, b := d.Pair.A, d.Pair.B
		// Skip if the resolver already merged them since candidate-gen, or the user
		// rejected this pair before.
		if p.resolver.SameLiveCanonical(a, b) {
			continue
		}
		switch d.Tier {
		case idmatch.TierAutoMerge:
			if p.autoMergeEnabled {
				if _, err := p.ApproveMerge(ctx, a, b); err != nil {
					p.log.Error("identity-match: auto-merge failed", "a", a, "b", b, "err", err)
				} else {
					p.log.Info("identity-match: AUTO-MERGED", "a", a, "b", b, "conf", d.Result.Confidence, "reason", d.Result.Reason)
				}
				continue
			}
			p.proposeMatch(d) // gate not flipped → propose
		case idmatch.TierPropose:
			p.proposeMatch(d)
		}
	}
	return nil
}

// buildMatchRecords rolls every live canonical + its members' IdentityContext into
// the matcher's PersonRecords (and the id→record index). Shared by the LLM path and
// the deterministic-merge path.
func (p *Projector) buildMatchRecords(ctx context.Context) ([]idmatch.PersonRecord, map[string]idmatch.PersonRecord, error) {
	canon := p.resolver.AllCanonicals()
	if len(canon) < 2 {
		return nil, nil, nil
	}
	ctxIndex := map[string]IdentityContext{}
	if p.idCtxProvider != nil {
		ctxs, err := p.idCtxProvider.IdentityContext(ctx)
		if err != nil {
			p.log.Warn("identity-match: context feed failed", "err", err)
		}
		for _, c := range ctxs {
			ctxIndex[identityKey(c.Platform, c.SenderID)] = c
		}
	}
	records := make([]idmatch.PersonRecord, 0, len(canon))
	for _, cp := range canon {
		records = append(records, buildPersonRecord(cp, ctxIndex))
	}
	return records, idmatch.IndexByID(records), nil
}

// deterministicEligible reports whether a candidate pair qualifies for the
// corroborated-exact-name auto-merge: same normalized name + a shared handle/group
// (idmatch.CorroboratedExactName) AND a DISTINCTIVE, non-placeholder name (the
// projector's merge-safety policy, so "Identified As Spam"-style collisions never
// auto-merge). Does NOT consider whether they're already merged — callers decide.
func (p *Projector) deterministicEligible(pair idmatch.CandidatePair, byID map[string]idmatch.PersonRecord) bool {
	return p.deterministicReason(pair, byID) != ""
}

// deterministicReason returns a human-legible provenance string for WHY a pair
// qualifies for deterministic auto-merge (for dry-run review + audit logging), or ""
// if it doesn't. Two independent paths, both LLM-free:
//   - shared strong identifier (#33): same exact phone/email → same person, NO name
//     match required (names may differ or be blank — the @lid phone-twin case);
//   - corroborated exact name (#12/#19): identical distinctive name + a shared
//     handle/group, and a non-placeholder distinctive name.
func (p *Projector) deterministicReason(pair idmatch.CandidatePair, byID map[string]idmatch.PersonRecord) string {
	a, okA := byID[pair.A]
	b, okB := byID[pair.B]
	if !okA || !okB {
		return ""
	}
	if kind, val, ok := idmatch.SharedStrongIdentifier(a, b); ok {
		return "shared-" + kind + ":" + val
	}
	if idmatch.CorroboratedExactName(a, b, pair) &&
		!isPlaceholderName(a.PrimaryName) && distinctiveName(a.PrimaryName) {
		return "corroborated-exact-name"
	}
	return ""
}

// tryDeterministicMerge applies the user's corroborated-exact-name policy to one
// candidate pair: same normalized name + a shared handle/group → auto-merge, with no
// LLM. It additionally requires a DISTINCTIVE, non-placeholder name (the projector's
// merge-safety policy) so "Identified As Spam"-style collisions never auto-merge.
// Returns true iff it merged (or the pair was already merged), so the caller drops
// it from the LLM queue. NOT gated by autoMergeEnabled — this is a deterministic
// rule the user approved, distinct from the eval-gated LLM auto-merge.
func (p *Projector) tryDeterministicMerge(ctx context.Context, pair idmatch.CandidatePair, byID map[string]idmatch.PersonRecord) bool {
	reason := p.deterministicReason(pair, byID)
	if reason == "" {
		return false
	}
	if p.resolver.SameLiveCanonical(pair.A, pair.B) {
		return true // already one person; drop from the LLM queue
	}
	a := byID[pair.A]
	if _, err := p.ApproveMerge(ctx, pair.A, pair.B); err != nil {
		p.log.Error("identity-match: deterministic auto-merge failed", "a", pair.A, "b", pair.B, "err", err)
		return false
	}
	p.log.Info("identity-match: AUTO-MERGED (deterministic)",
		"a", pair.A, "b", pair.B, "name", a.PrimaryName, "via", reason, "signals", pair.Signals)
	return true
}

// RunDeterministicMerges applies the corroborated-exact-name auto-merge policy over
// the whole graph WITHOUT the LLM — so it runs even when the qwen matcher is OFF
// (wire it on the matcher-independent tick). It also DRAINS the human-review queue:
// any pending proposal whose pair is corroborated-exact-name gets auto-approved here
// (ApproveMerge marks it approved → it leaves Pending()). Returns the number of
// pairs merged this pass. Idempotent (already-merged pairs are skipped).
//
// dryRun=true logs every pair the rule WOULD merge (name + signals) and counts them
// WITHOUT merging — use it to review a bulk backfill before committing.
func (p *Projector) RunDeterministicMerges(ctx context.Context, dryRun bool) (int, error) {
	records, byID, err := p.buildMatchRecords(ctx)
	if err != nil || len(records) < 2 {
		return 0, err
	}
	gen := idmatch.NewCandidateGenerator(idmatch.DefaultCandidateConfig, nil)
	merged := 0
	for _, pair := range gen.Candidates(records) {
		if p.resolver.SameLiveCanonical(pair.A, pair.B) {
			continue
		}
		if dryRun {
			if reason := p.deterministicReason(pair, byID); reason != "" {
				p.log.Info("deterministic merge CANDIDATE (dry-run)",
					"a", pair.A, "b", pair.B, "name", byID[pair.A].PrimaryName,
					"via", reason, "signals", pair.Signals)
				merged++
			}
			continue
		}
		if p.tryDeterministicMerge(ctx, pair, byID) {
			merged++
		}
	}
	if merged > 0 {
		verb := "applied"
		if dryRun {
			verb = "would apply (dry-run)"
		}
		p.log.Info("deterministic corroborated-exact-name merges "+verb, "count", merged,
			"pending_remaining", len(p.proposals.Pending()))
	}
	return merged, nil
}

// RunDeterministicProposals surfaces STRONG-signal cross-platform candidate pairs into
// the human review queue WITHOUT the LLM (#19 propose-only coverage — auto-merge stays
// OFF, user one-tap-approves). It complements RunDeterministicMerges: pairs NOT safe to
// auto-merge but carrying a strong same-person signal — a shared distinctive HANDLE, or
// a handle↔email-localpart CROSS-TYPE match (the Morgan/Taylor-class cross-platform
// fragments that name/handle/email blocks each miss alone) — are proposed for review.
// Name-only fuzz is deliberately NOT proposed here (that's the shelved LLM's job + risks
// common-name collisions). Idempotent: the store dedups by pair + never re-opens an
// approved/rejected pair. Returns the count of NEW proposals.
func (p *Projector) RunDeterministicProposals(ctx context.Context) (int, error) {
	records, byID, err := p.buildMatchRecords(ctx)
	if err != nil || len(records) < 2 {
		return 0, err
	}
	gen := idmatch.NewCandidateGenerator(idmatch.DefaultCandidateConfig, nil)
	added := 0
	for _, pair := range gen.Candidates(records) {
		if p.resolver.SameLiveCanonical(pair.A, pair.B) {
			continue // already one person
		}
		if p.deterministicEligible(pair, byID) {
			continue // auto-merges via RunDeterministicMerges, not a proposal
		}
		if !proposeWorthy(pair) {
			continue // only strong deterministic signals — no name-only fuzz
		}
		a, b := byID[pair.A], byID[pair.B]
		if isPlaceholderName(a.PrimaryName) && isPlaceholderName(b.PrimaryName) {
			continue // two placeholder names → nothing to review
		}
		if _, exists := p.proposals.Get(pairKey(pair.A, pair.B)); exists {
			continue // already proposed or resolved (don't recount)
		}
		p.proposals.Add(Proposal{
			A: pair.A, B: pair.B, AName: a.PrimaryName, BName: b.PrimaryName,
			Score:  pair.PreScore,
			Reason: "deterministic (" + strings.Join(pair.Signals, "+") + ")",
		})
		added++
	}
	if added > 0 {
		p.log.Info("deterministic proposals surfaced (propose-only, no LLM)",
			"added", added, "pending_total", len(p.proposals.Pending()))
	}
	return added, nil
}

// proposeWorthy reports whether a candidate pair carries a deterministic signal strong
// enough to surface for human review: a shared distinctive handle or a handle↔email-
// localpart cross-type match. (Name-token/edit-distance/nickname/shared-group alone are
// too weak — common-name collisions — and stay for the LLM tier, which is shelved.)
func proposeWorthy(pair idmatch.CandidatePair) bool {
	for _, s := range pair.Signals {
		if s == idmatch.SignalCrossIdent || s == idmatch.SignalHandle {
			return true
		}
	}
	return false
}

// proposeMatch routes an adjudicated same-person verdict into the human review queue
// (the existing #12 bot flow), tagged with the LLM's confidence + reason.
func (p *Projector) proposeMatch(d idmatch.Decision) {
	a := byIDName(p, d.Pair.A)
	b := byIDName(p, d.Pair.B)
	p.proposals.Add(Proposal{
		A: d.Pair.A, B: d.Pair.B, AName: a, BName: b,
		Score:  d.Result.Confidence,
		Reason: "LLM: " + d.Result.Reason,
	})
}

func byIDName(p *Projector, id string) string {
	if cp, ok := p.resolver.Snapshot(id); ok {
		return cp.Name
	}
	return id
}

// buildPersonRecord rolls a canonical person + its members' IdentityContext up into
// the matcher's PersonRecord (names/platforms from the resolver; handles/chats/bio/
// snippets aggregated from member contexts).
func buildPersonRecord(cp CanonicalPerson, ctxIndex map[string]IdentityContext) idmatch.PersonRecord {
	rec := idmatch.PersonRecord{
		CanonicalID: cp.ID,
		PrimaryName: cp.Name,
	}
	names := map[string]bool{}
	platforms := map[string]bool{}
	handles := map[string]bool{}
	chats := map[string]bool{}
	phones := map[string]bool{}
	emails := map[string]bool{}
	var bio strings.Builder
	for _, m := range cp.Members {
		platforms[m.Platform] = true
		// Strong identifiers (#33): carry every member's normalized phone/email so
		// SharedStrongIdentifier can deterministically collapse cross-canonical
		// fragments that share one (phone digits-only; email lowercased).
		if ph := normalizePhone(m.Phone); ph != "" {
			phones[ph] = true
		}
		if em := strings.ToLower(strings.TrimSpace(m.Email)); em != "" {
			emails[em] = true
		}
		for _, n := range m.Names.allNames() {
			if !names[strings.ToLower(n)] {
				names[strings.ToLower(n)] = true
				rec.Names = append(rec.Names, n)
			}
		}
		c, ok := ctxIndex[identityKey(m.Platform, m.UserID)]
		if !ok {
			continue
		}
		for _, h := range c.Handles {
			handles[h] = true
		}
		for _, ch := range c.ChatIDs {
			chats[ch] = true
		}
		if strings.TrimSpace(c.Bio) != "" && bio.Len() < 400 {
			bio.WriteString(strings.TrimSpace(c.Bio))
			bio.WriteByte(' ')
		}
		for _, s := range c.Snippets {
			if len(rec.Snippets) < 8 && strings.TrimSpace(s) != "" {
				rec.Snippets = append(rec.Snippets, s)
			}
		}
	}
	rec.Platforms = sortedSet(platforms)
	rec.Handles = sortedSet(handles)
	rec.GroupIDs = sortedSet(chats)
	rec.Phones = sortedSet(phones)
	rec.Emails = sortedSet(emails)
	rec.Bio = strings.TrimSpace(bio.String())
	return rec
}

// contextHash makes a stable hash of a pair's adjudication context so a cached
// verdict is reused unless the underlying records actually changed.
func contextHash(a, b idmatch.PersonRecord) string {
	h := fnv.New64a()
	for _, r := range []idmatch.PersonRecord{a, b} {
		fmt.Fprintf(h, "%s|%s|%v|%v|%s|%v\x00",
			r.CanonicalID, r.PrimaryName, r.Names, r.Handles, r.Bio, r.Snippets)
	}
	return fmt.Sprintf("%x", h.Sum64())
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
