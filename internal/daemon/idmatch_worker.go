package daemon

import (
	"context"
	"time"
)

// matcherSettleDelay lets live ingest + any initial backfill settle before the
// first (heavy) adjudication sweep, so a cold start doesn't fight the firehose.
const matcherSettleDelay = 2 * time.Minute

// runMatcherWorker is the off-ingest #19 identity-matcher loop (started from Run
// only when the matcher is enabled). On each tick it recomputes salience (which
// orders the adjudication queue core-network-first) then runs the cached,
// importance-ordered, ctx-cancellable LLM sweep. It is PROPOSE-ONLY — the
// projector routes high-confidence verdicts to the human review queue (auto-merge
// stays off until the lead gates the eval). The per-call pace (llm.MinInterval)
// keeps the CPU from starving live ingest; the cache makes re-runs nearly free.
func (d *Daemon) runMatcherWorker(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(matcherSettleDelay):
	}
	interval := d.matchInterval
	if interval <= 0 {
		interval = 6 * time.Hour
	}
	for {
		start := time.Now()
		// Re-apply confirmed merges before each sweep so adjudication operates on the
		// durable graph (not the firehose-only rebuild that drops approved/auto merges
		// across a restart). Idempotent + deferred-safe.
		d.proj.LoadApprovedMerges(ctx)
		if err := d.proj.RecomputeSalience(ctx); err != nil && ctx.Err() == nil {
			d.log.Warn("matcher: salience recompute failed", "err", err)
		}
		if err := d.proj.RunIdentityMatch(ctx); err != nil && ctx.Err() == nil {
			d.log.Warn("matcher: identity-match sweep failed", "err", err)
		}
		if ctx.Err() != nil {
			return
		}
		d.log.Info("matcher: sweep complete", "took", time.Since(start).Round(time.Second).String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
