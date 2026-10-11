package idmatch

import (
	"sort"
	"strings"
)

// Embedder produces a semantic vector for a text (nomic-embed-text in P2). Optional:
// a nil Embedder skips the semantic block and uses deterministic blocks only.
type Embedder interface {
	Embed(text string) ([]float32, error)
}

// CandidateConfig tunes candidate generation. Defaults are liberal-but-bounded:
// generate enough candidates to fix the recall gap (Bob↔Robert, typos), but cap so
// the LLM call budget stays O(n·k).
type CandidateConfig struct {
	MaxEditDistance   int     // typo tolerance on the full normalized name (default 2)
	MinTokenJaccard   float64 // name-token overlap to emit on the last-token block (default 0.34)
	TopKPerPerson     int     // cap candidates kept per person by pre-score (default 10)
	MaxBlockFanout    int     // skip a blocking key shared by more than this many people (default 60 — kills "Identified As Spam"/common-token explosions)
	SemanticThreshold float64 // cosine ≥ this → semantic candidate (default 0.80)
}

// DefaultCandidateConfig — sensible starting points (eval-tunable).
var DefaultCandidateConfig = CandidateConfig{
	MaxEditDistance:   2,
	MinTokenJaccard:   0.34,
	TopKPerPerson:     10,
	MaxBlockFanout:    60,
	SemanticThreshold: 0.80,
}

// CandidateGenerator emits candidate pairs for adjudication via blocking keys:
// shared last-token, nickname-folded given names, handles, and shared groups —
// then refines with edit-distance / token-jaccard / (optional) semantic similarity.
// Pure + deterministic given its inputs (+ a deterministic Embedder).
type CandidateGenerator struct {
	cfg CandidateConfig
	emb Embedder // optional
}

// NewCandidateGenerator builds a generator. emb may be nil (deterministic-only).
func NewCandidateGenerator(cfg CandidateConfig, emb Embedder) *CandidateGenerator {
	if cfg.TopKPerPerson <= 0 {
		cfg = DefaultCandidateConfig
	}
	return &CandidateGenerator{cfg: cfg, emb: emb}
}

// blockKeys returns the deterministic blocking keys for a person: nickname-folded
// name tokens (each), the handle set, and the group-id set. Sharing ANY key with
// another person makes them a candidate (then refined).
func (g *CandidateGenerator) blockKeys(p PersonRecord) []string {
	keys := map[string]bool{}
	for _, name := range append([]string{p.PrimaryName}, p.Names...) {
		for _, t := range tokens(name) {
			if len(t) < 2 {
				continue // skip single-letter tokens (too ambiguous to block on)
			}
			keys["t:"+canonicalGiven(t)] = true
		}
	}
	for _, h := range p.Handles {
		if hh := normName(h); hh != "" {
			keys["h:"+hh] = true
		}
	}
	for _, gid := range p.GroupIDs {
		keys["g:"+gid] = true
	}
	// Strong-identifier blocks (#33): surface phone/email-sharing pairs so the
	// deterministic SharedStrongIdentifier predicate can collapse them (e.g. an @lid
	// whatsapp identity whose recovered phone now matches its phone-twin in another
	// canonical). Eligibility-guarded so short-codes / role addresses don't block.
	for _, ph := range p.Phones {
		if n := strings.ToLower(strings.TrimSpace(ph)); n != "" && eligiblePhone(n) {
			keys["p:"+n] = true
		}
	}
	for _, em := range p.Emails {
		if n := strings.ToLower(strings.TrimSpace(em)); n != "" && eligibleEmail(n) {
			keys["e:"+n] = true
		}
	}
	// Cross-type identifier block (#2 coverage): a person's username often appears as a
	// HANDLE on one platform and the email LOCAL-PART on another (telegram "morganexample" ↔
	// morganexample@example.org) — the exact Morgan-class fragment that name/handle/email blocks
	// each miss alone. Emit a SHARED "x:" key for distinctive handles AND email local-parts
	// so those cross-platform pairs surface as candidates → the propose queue. Distinctive-
	// guarded so generic local-parts (info/noreply) don't fuse strangers.
	for _, h := range p.Handles {
		if n := strings.ToLower(strings.TrimSpace(h)); distinctiveIdent(n) {
			keys["x:"+n] = true
		}
	}
	for _, em := range p.Emails {
		if n := emailLocalPart(em); distinctiveIdent(n) {
			keys["x:"+n] = true
		}
	}
	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Candidates generates the deduped, capped candidate pairs across all people.
func (g *CandidateGenerator) Candidates(people []PersonRecord) []CandidatePair {
	idx := map[string][]int{} // blocking key -> person indices
	for i, p := range people {
		for _, k := range g.blockKeys(p) {
			idx[k] = append(idx[k], i)
		}
	}

	type acc struct {
		signals map[string]bool
		score   float64
	}
	pairs := map[string]*acc{} // pairKey -> accumulated signals

	addSignal := func(i, j int, sig string, score float64) {
		a, b := people[i].CanonicalID, people[j].CanonicalID
		k := pairKey(a, b)
		ac := pairs[k]
		if ac == nil {
			ac = &acc{signals: map[string]bool{}}
			pairs[k] = ac
		}
		ac.signals[sig] = true
		if score > ac.score {
			ac.score = score
		}
	}

	for key, members := range idx {
		if len(members) < 2 || len(members) > g.cfg.MaxBlockFanout {
			continue // singleton, or an over-broad block (common token) → skip
		}
		kind := key[:1] // t / h / g
		for a := 0; a < len(members); a++ {
			for b := a + 1; b < len(members); b++ {
				i, j := members[a], members[b]
				switch kind {
				case "p":
					// Shared exact phone — a strong identifier (#33). Top pre-score so
					// it survives the per-person cap; the deterministic predicate
					// (SharedStrongIdentifier) acts on it without the LLM.
					addSignal(i, j, SignalSharedPhone, 1.0)
				case "e":
					addSignal(i, j, SignalSharedEmail, 1.0)
				case "x":
					// handle ↔ email-localpart cross-type → strong propose signal (#2).
					addSignal(i, j, SignalCrossIdent, 0.85)
				case "h":
					addSignal(i, j, SignalHandle, 0.9)
				case "g":
					addSignal(i, j, SignalSharedGroup, 0.5)
				case "t":
					// Name-token block: refine before emitting (avoid a common-token
					// block like "Smith" emitting every pair).
					ni, nj := people[i].PrimaryName, people[j].PrimaryName
					if jac := tokenJaccard(ni, nj); jac >= g.cfg.MinTokenJaccard {
						addSignal(i, j, SignalTokenJac, jac)
					}
					if ed := levenshtein(normName(ni), normName(nj)); ed > 0 && ed <= g.cfg.MaxEditDistance {
						addSignal(i, j, SignalEditDist, 1.0-float64(ed)/4.0)
					}
					if sharesNicknameGiven(ni, nj) {
						addSignal(i, j, SignalNickname, 0.7)
					}
				}
			}
		}
	}

	// Semantic block (P2): cosine over embeddings adds/confirms candidates beyond
	// string rules (romanizations, variant spellings). Hook reserved; wired with the
	// nomic-embed Embedder in P2. (g.emb retained for that.)
	_ = g.emb

	out := make([]CandidatePair, 0, len(pairs))
	for k, ac := range pairs {
		a, b := splitPair(k)
		sigs := make([]string, 0, len(ac.signals))
		for s := range ac.signals {
			sigs = append(sigs, s)
		}
		sort.Strings(sigs)
		out = append(out, CandidatePair{A: a, B: b, Signals: sigs, PreScore: ac.score})
	}
	return capPerPerson(out, g.cfg.TopKPerPerson)
}

// capPerPerson keeps the top-K highest pre-score candidates touching each person,
// bounding the LLM budget. Deterministic (ties broken by pair key).
func capPerPerson(pairs []CandidatePair, k int) []CandidatePair {
	if k <= 0 {
		return pairs
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].PreScore != pairs[j].PreScore {
			return pairs[i].PreScore > pairs[j].PreScore
		}
		return pairKey(pairs[i].A, pairs[i].B) < pairKey(pairs[j].A, pairs[j].B)
	})
	count := map[string]int{}
	var out []CandidatePair
	for _, p := range pairs {
		if count[p.A] >= k || count[p.B] >= k {
			continue
		}
		count[p.A]++
		count[p.B]++
		out = append(out, p)
	}
	return out
}

func splitPair(k string) (string, string) {
	for i := 0; i < len(k); i++ {
		if k[i] == '|' {
			return k[:i], k[i+1:]
		}
	}
	return k, ""
}

// sharesNicknameGiven reports whether two names share a nickname-folded given-name
// token (so "Bob Smith" ↔ "Robert Smith" register beyond pure string distance).
func sharesNicknameGiven(a, b string) bool {
	sa := map[string]bool{}
	for _, t := range tokens(a) {
		sa[canonicalGiven(t)] = true
	}
	for _, t := range tokens(b) {
		if sa[canonicalGiven(t)] && nicknames[t] != "" {
			return true
		}
	}
	// also if a's nickname maps to b's canonical token
	for _, t := range tokens(a) {
		if c, ok := nicknames[t]; ok {
			for _, u := range tokens(b) {
				if canonicalGiven(u) == c {
					return true
				}
			}
		}
	}
	return false
}
