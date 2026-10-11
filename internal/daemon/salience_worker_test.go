package daemon

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

// The #59 no-double-run guard: when a salience recompute is already in flight
// (salienceMu held), a tick must SKIP (return immediately) rather than block or
// queue — so overlapping ticks / a tick racing the reproject's recompute can't
// stack up.
func TestSalienceRecomputeGuardSkipsWhenInFlight(t *testing.T) {
	d := &Daemon{log: slog.Default()}
	d.salienceMu.Lock() // simulate a recompute already running
	defer d.salienceMu.Unlock()

	done := make(chan struct{})
	go func() {
		d.recomputeSalienceGuarded(context.Background())
		close(done)
	}()
	select {
	case <-done: // returned immediately = correctly skipped (TryLock failed)
	case <-time.After(2 * time.Second):
		t.Fatal("recomputeSalienceGuarded blocked on a held lock — it must TryLock-skip, not wait")
	}
}
