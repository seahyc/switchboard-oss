package daemon

import (
	"context"
	"os"
	"strconv"
	"time"
)

// salienceTickDefault is how often the people-graph salience/tiers are re-scored off
// the resolver (#59/#15). Two purposes: (1) SELF-HEAL — if the startup Reproject is
// interrupted (e.g. a deploy SIGTERM mid-reproject killed RecomputeSalience), tiers go
// empty with no other path to repopulate until a manual recompute or clean reboot; the
// tick repopulates them. (2) FRESHNESS — tiers are otherwise frozen at the last
// reproject, so a contact going quiet/active doesn't re-tier; the tick keeps membership
// current against the default inner≥10 threshold. Tunable via
// SWITCHBOARD_SALIENCE_TICK_MIN (minutes).
const salienceTickDefault = 15 * time.Minute

// runSalienceWorker periodically recomputes salience so tiers self-heal + stay fresh.
// It exists because the ONLY other periodic RecomputeSalience lives in runMatcherWorker,
// which runs only when the (shelved #19) matcher is ENABLED — so with the matcher off,
// nothing re-tiers between restarts. Launched UNCONDITIONALLY from Run. Best-effort and
// ctx-cancellable; it re-scores from the resolver and does NOT rebuild idToCanon (a
// resolver-incomplete startup is the Reproject/reprojectDone concern, not this).
func (d *Daemon) runSalienceWorker(ctx context.Context) {
	interval := salienceTickDefault
	if v := os.Getenv("SWITCHBOARD_SALIENCE_TICK_MIN"); v != "" {
		if m, err := strconv.Atoi(v); err == nil && m > 0 {
			interval = time.Duration(m) * time.Minute
		}
	}
	d.log.Info("salience tick worker started", "interval", interval.String())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.recomputeSalienceGuarded(ctx)
		}
	}
}

// recomputeSalienceGuarded runs one RecomputeSalience unless another is already in
// flight (skip rather than queue — the next tick covers it). The TryLock prevents a
// tick from racing the reproject's recompute or a concurrent on-demand /recompute.
// Best-effort: logs failures, never crashes the daemon. The recompute uses the
// existing weights/thresholds (inner≥10 stays fixed) — only tier MEMBERSHIP re-scores.
func (d *Daemon) recomputeSalienceGuarded(ctx context.Context) {
	if !d.salienceMu.TryLock() {
		return // a recompute is already running — skip this tick
	}
	defer d.salienceMu.Unlock()
	start := time.Now()
	if err := d.proj.RecomputeSalience(ctx); err != nil {
		if ctx.Err() == nil {
			d.log.Warn("salience tick: recompute failed", "err", err)
		}
		return
	}
	if ctx.Err() == nil {
		d.log.Info("salience tick: recomputed", "took", time.Since(start).Round(time.Millisecond).String())
	}
}
