package idmatch

import (
	"context"
	"sort"
)

// AdjudicationCache memoizes verdicts so a pair is NEVER re-adjudicated — load-
// bearing on a CPU-only box where each call costs seconds. Keyed by the canonical
// pair AND a content-hash of the adjudication context, so a re-run/reproject pays
// nothing unless the underlying records actually changed. The persistent (SQLite)
// implementation lands in P2; the in-memory one suffices for the eval batch.
type AdjudicationCache interface {
	Get(pairKey, ctxHash string) (AdjudicationResult, bool)
	Put(pairKey, ctxHash string, r AdjudicationResult)
}

// MemoryCache is an in-process AdjudicationCache.
type MemoryCache struct{ m map[string]AdjudicationResult }

func NewMemoryCache() *MemoryCache { return &MemoryCache{m: map[string]AdjudicationResult{}} }

func (c *MemoryCache) key(pairKey, ctxHash string) string { return pairKey + "@" + ctxHash }
func (c *MemoryCache) Get(pairKey, ctxHash string) (AdjudicationResult, bool) {
	r, ok := c.m[c.key(pairKey, ctxHash)]
	return r, ok
}
func (c *MemoryCache) Put(pairKey, ctxHash string, r AdjudicationResult) {
	c.m[c.key(pairKey, ctxHash)] = r
}

// OrderByImportance sorts the candidate queue so the user's CORE-NETWORK / high-
// salience people are adjudicated FIRST — the high-value dedup lands early even if
// the long tail takes hours on CPU. importance maps canonical id → salience score
// (from #15; missing → 0). A pair's priority is the MAX salience of its two sides.
// Stable + deterministic (ties by pair key).
func OrderByImportance(pairs []CandidatePair, importance map[string]float64) []CandidatePair {
	out := append([]CandidatePair(nil), pairs...)
	prio := func(p CandidatePair) float64 {
		a, b := importance[p.A], importance[p.B]
		if b > a {
			a = b
		}
		// blend in the cheap pre-score so importance-ties still order sensibly.
		return a*1000 + p.PreScore
	}
	sort.SliceStable(out, func(i, j int) bool {
		pi, pj := prio(out[i]), prio(out[j])
		if pi != pj {
			return pi > pj
		}
		return pairKey(out[i].A, out[i].B) < pairKey(out[j].A, out[j].B)
	})
	return out
}

// AdjudicateBatch runs the adjudicator over an importance-ordered, cache-backed
// queue — the P2 initial-sweep / ongoing shape. Cached pairs cost nothing; new
// pairs call the (seconds-each) model and are cached. ctxHashFn yields the
// content-hash for a pair's current context (so a changed record re-adjudicates).
// Returns decisions in queue order; respects ctx cancellation (resumable).
func AdjudicateBatch(
	ctx context.Context,
	adj LLMAdjudicator,
	cache AdjudicationCache,
	byID map[string]PersonRecord,
	queue []CandidatePair,
	t Thresholds,
	ctxHashFn func(a, b PersonRecord) string,
) ([]Decision, error) {
	out := make([]Decision, 0, len(queue))
	for _, p := range queue {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		a, okA := byID[p.A]
		b, okB := byID[p.B]
		if !okA || !okB {
			continue
		}
		pk := pairKey(p.A, p.B)
		h := ""
		if ctxHashFn != nil {
			h = ctxHashFn(a, b)
		}
		res, hit := cache.Get(pk, h)
		if !hit {
			var err error
			res, err = adj.Adjudicate(ctx, AdjudicationRequest{A: a, B: b})
			if err != nil {
				return out, err
			}
			cache.Put(pk, h, res)
		}
		out = append(out, Decision{Pair: p, Result: res, Tier: RouteGuarded(res, t, a, b)})
	}
	return out, nil
}
