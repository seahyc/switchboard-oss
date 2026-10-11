// Package pipeline is the single normalize→log→project path shared by both
// live and backfill ingestion. Connectors emit canonical event.Message values
// onto a channel; the pipeline appends each to the event log (the source of
// truth) and updates the SQLite projections idempotently.
//
// Live and backfill differ only by the connector driver feeding the channel —
// the pipeline code is identical. Idempotency (dedupe on the global id) +
// per-chat watermark cursors mean overlap/replay never gaps or duplicates.
//
// The live socket is never blocked on projection: connectors write to a buffered
// channel and the pipeline drains it in its own goroutine.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/store"
)

// Sink consumes newly-inserted (non-duplicate) events. It drives the push event
// stream and downstream projectors (e.g. gbrain-feed). Must be fast / async;
// the pipeline calls it inline after a successful insert.
type Sink interface {
	OnIngest(m event.Message)
}

// SinkFunc adapts a function to a Sink.
type SinkFunc func(event.Message)

func (f SinkFunc) OnIngest(m event.Message) { f(m) }

// Pipeline ingests canonical events into the store.
type Pipeline struct {
	store   *store.Store
	selfIDs map[string]bool
	sinks   []Sink
	log     *slog.Logger

	clock func() time.Time // overridable for tests
}

// Option configures a Pipeline.
type Option func(*Pipeline)

// WithSelfIDs sets the user's own platform_user_ids (for mentions_self / "@me").
func WithSelfIDs(ids ...string) Option {
	return func(p *Pipeline) {
		for _, id := range ids {
			if id != "" {
				p.selfIDs[id] = true
			}
		}
	}
}

// WithSink registers a downstream consumer of newly-ingested events.
func WithSink(s Sink) Option {
	return func(p *Pipeline) { p.sinks = append(p.sinks, s) }
}

// WithLogger sets the structured logger.
func WithLogger(l *slog.Logger) Option {
	return func(p *Pipeline) { p.log = l }
}

// New builds a Pipeline over the given store.
func New(s *store.Store, opts ...Option) *Pipeline {
	p := &Pipeline{
		store:   s,
		selfIDs: map[string]bool{},
		log:     slog.Default(),
		clock:   time.Now,
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// Ingest normalizes housekeeping fields and appends one event idempotently.
// Returns true if the event was newly inserted (false on idempotent dedupe).
func (p *Pipeline) Ingest(ctx context.Context, m event.Message) (bool, error) {
	m = event.NormalizeMutation(m)
	// Normalize: ensure id + ingested_at are set. The id is the idempotency key.
	if m.ID == "" && m.Platform != "" {
		// Defensive: connectors should call event.MakeID, but never store empty ids.
		p.log.Warn("event missing id", "platform", m.Platform, "chat", m.ChatID)
	}
	if m.IngestedAt.IsZero() {
		m.IngestedAt = p.clock().UTC()
	}
	// Sanitize raw/UI JSON so one malformed field from a connector can never
	// fail json.Marshal in the store and drop the whole message from the firehose.
	sanitizeRaw(&m)
	// Resolve a missing chat_id by joining on the target message. Reactions,
	// edits and deletes (e.g. Lark) often arrive with chat_id empty and only a
	// reply_to/thread pointer to the message they act on.
	if m.ChatID == "" {
		target := m.TargetID
		if target == "" {
			target = m.ReplyTo
		}
		if target != "" {
			if chatID, _, ok, err := p.store.MessageRef(ctx, target); err == nil && ok {
				m.ChatID = chatID
			} else if err != nil {
				p.log.Warn("chat_id resolution failed", "id", m.ID, "target", target, "err", err)
			}
		}
	}
	res, err := p.store.Append(ctx, m, p.selfIDs)
	if errors.Is(err, store.ErrChatPurged) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if res.Inserted {
		for _, s := range p.sinks {
			s.OnIngest(m)
		}
	}
	return res.Inserted, nil
}

// sanitizeRaw ensures every json.RawMessage field holds valid JSON, so the
// store's json.Marshal(Message) can never fail on connector-supplied bytes.
// Invalid Raw is preserved by re-encoding it as a JSON string (we keep the
// data, just make it valid); invalid UI fields fall back to JSON null / are
// dropped. A nil/empty RawMessage marshals fine as-is, so it's left alone.
func sanitizeRaw(m *event.Message) {
	m.Raw = safeRawPreserve(m.Raw)
	m.UI.Poll = safeRawNull(m.UI.Poll)
	m.UI.Card = safeRawNull(m.UI.Card)
	if len(m.UI.Buttons) > 0 {
		kept := m.UI.Buttons[:0]
		for _, b := range m.UI.Buttons {
			if len(b) == 0 || json.Valid(b) {
				kept = append(kept, b)
			}
		}
		m.UI.Buttons = kept
	}
}

// safeRawPreserve returns r if it's empty or valid JSON; otherwise it wraps the
// raw bytes as a JSON string so the original payload is preserved, not dropped.
func safeRawPreserve(r json.RawMessage) json.RawMessage {
	if len(r) == 0 || json.Valid(r) {
		return r
	}
	return json.RawMessage(strconv.Quote(string(r)))
}

// safeRawNull returns r if empty or valid JSON; otherwise JSON null.
func safeRawNull(r json.RawMessage) json.RawMessage {
	if len(r) == 0 || json.Valid(r) {
		return r
	}
	return json.RawMessage("null")
}

// Run drains in until it is closed or ctx is cancelled, ingesting each event.
// This is the shared spine for both live and backfill drivers.
func (p *Pipeline) Run(ctx context.Context, in <-chan event.Message) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m, ok := <-in:
			if !ok {
				return nil
			}
			inserted, err := p.Ingest(ctx, m)
			if err != nil {
				// Never crash the spine on one bad event; log and continue.
				p.log.Error("ingest failed", "id", m.ID, "err", err)
				continue
			}
			if inserted {
				p.log.Debug("ingested", "id", m.ID, "chat", m.ChatID, "source", m.Source)
			}
		}
	}
}
