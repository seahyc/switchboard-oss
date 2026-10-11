package connectors

import (
	"context"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// AnchoredBackfiller is implemented by connectors whose history backfill needs a
// per-chat anchor — the oldest message the daemon already has — to page further
// back. WhatsApp is the case: whatsmeow's on-demand history sync fetches the
// messages immediately before a given (chat, msgID, timestamp) anchor, so the
// daemon (which owns the message store) supplies the anchor it knows.
//
// The daemon resolves the oldest stored message for chatID and calls this; it
// streams the returned older messages (source=backfill) to out until ctx is
// cancelled. Connectors that don't implement it fall back to the base
// Connector.Backfill (which routes the platform's push history stream).
type AnchoredBackfiller interface {
	BackfillBefore(ctx context.Context, chatID, oldestMsgID string, oldestFromMe bool, oldestTS time.Time, count int, out chan<- event.Message) error
}
