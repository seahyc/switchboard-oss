package daemon

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/gbrainfeed"
	"github.com/seahyc/switchboard-oss/internal/honcho"
	"github.com/seahyc/switchboard-oss/internal/store"
)

// Honcho user-model ingestion (narrow v1, flagged off by default via
// HONCHO_INGEST_ENABLED). Switchboard feeds its canonical firehose into the SAME
// "hermes" Honcho workspace Hermes uses, so the user-model sees the user's REAL
// relationships, not just chats with Hermes. NARROW SCOPE: WhatsApp DMs with INNER-tier
// people only, message bodies. Importance/judgment lives in Honcho's deriver — switchboard
// only POSTs text + ids.
//
// SEQUENCING: OnIngest is non-blocking (filter + enqueue); a single worker drains the
// queue and POSTs to Honcho OFF the live-ingest hot path, so Honcho/deriver load can never
// back up message ingestion.
const (
	honchoWorkspace     = "hermes"
	honchoSessionPrefix = "switchboard:whatsapp:dm:"
	honchoQueueSize     = 2048
)

type personRef struct{ id, name, handle string }

// honchoSink is a pipeline.Sink that feeds inner-tier WhatsApp DMs into Honcho. It holds
// its deps directly (proj/store/log) rather than *Daemon so it can be constructed before
// the pipeline (which the Daemon owns) — avoiding a construction-order cycle.
type honchoSink struct {
	proj     *gbrainfeed.Projector
	store    *store.Store
	log      *slog.Logger
	cli      *honcho.Client
	selfPeer string
	ch       chan event.Message

	// recompute forces a settled salience pass (WaitIdle → score). Wired to
	// Daemon.RecomputeSalience in Run. The worker calls it ONCE before draining so the
	// tier gate isn't reading a blank salience map right after a restart (the async
	// startup reproject may not have populated tiers yet → the sink would silently skip
	// every message until the next periodic tick). nil-safe.
	recompute func(context.Context) (int, error)

	// handle() runs from TWO goroutines — the live worker (run) and the async backfill
	// (runHonchoBackfill) — so mu serializes handle + guards the shared maps below (a
	// concurrent map write would panic). Volume is low (inner-tier DMs), so a full-handle
	// lock is fine and also serializes the per-session watermark read-modify-write.
	mu          sync.Mutex
	counterpart map[string]personRef // chatID → the DM's inner-tier counterpart (learned from inbound)
	peers       map[string]bool      // ensured Honcho peer ids
	sessions    map[string]bool      // ensured Honcho session ids
}

func newHonchoSink(proj *gbrainfeed.Projector, st *store.Store, log *slog.Logger, cli *honcho.Client) *honchoSink {
	selfPeer := strings.TrimSpace(os.Getenv("HONCHO_SELF_PEER"))
	if selfPeer == "" {
		selfPeer = "self"
	}
	return &honchoSink{
		selfPeer: selfPeer,
		proj:     proj, store: st, log: log, cli: cli, ch: make(chan event.Message, honchoQueueSize),
		counterpart: map[string]personRef{}, peers: map[string]bool{}, sessions: map[string]bool{},
	}
}

// OnIngest (pipeline.Sink) — NON-BLOCKING: a quick platform/type prefilter then enqueue.
// Drops (with a warn) if the queue is full rather than ever blocking live ingest.
func (s *honchoSink) OnIngest(m event.Message) {
	if m.Platform != event.PlatformWhatsApp || m.ChatType != event.ChatDM || m.Text == "" {
		return
	}
	select {
	case s.ch <- m:
	default:
		s.log.Warn("honcho: queue full, dropping message", "id", m.ID)
	}
}

// run drains the queue, posting to Honcho off the ingest path. Returns on ctx cancel.
func (s *honchoSink) run(ctx context.Context) {
	// Wait for the startup Reproject to finish building the resolver + settling salience
	// BEFORE we gate any queued message — else the first DMs resolve against a partial
	// resolver (UNRESOLVED) or a blank tier map. recompute blocks on reprojectDone; the
	// queue buffers incoming messages meanwhile. Best-effort: log + proceed on ctx cancel.
	if s.recompute != nil {
		if n, err := s.recompute(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Warn("honcho: waiting for resolver readiness failed; gate may skip until next ingest", "err", err)
		} else {
			s.log.Info("honcho: resolver ready, draining queue", "scored", n)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-s.ch:
			s.handle(ctx, m)
		}
	}
}

// handle does the salience gate + peer/session mapping + idempotent post for one
// message. Returns true iff a message was actually POSTed to Honcho (so backfill counts
// real writes, not scanned candidates). Logs at the external boundary per AGENTS.md.
func (s *honchoSink) handle(ctx context.Context, m event.Message) bool {
	s.mu.Lock() // serialize live-worker + backfill goroutines; guards counterpart/peers/sessions
	defer s.mu.Unlock()
	cp, ok := s.resolveCounterpart(m)
	if !ok {
		// Not an inner-tier WA-DM counterpart (or none established yet for an outbound).
		// Debug-logged so a "nothing landed" run is diagnosable (the #1 silent-skip cause).
		s.log.Debug("honcho: skip — no inner-tier counterpart", "chat", m.ChatID, "self", m.Sender.IsSelf, "sender", m.Sender.PlatformUserID)
		return false
	}
	// Honcho requires ids ^[a-zA-Z0-9_-]+$; chat ids carry ':'/'@'/'.' → sanitize.
	sessionID := honcho.SanitizeID(honchoSessionPrefix + m.ChatID)

	// Per-session ts watermark for idempotency (forward live flow + ascending backfill are
	// monotonic, so already-posted messages are skipped; re-delivery is a no-op).
	wmKey := "honcho:wm:" + sessionID
	if wm, _ := s.store.GetMeta(ctx, wmKey); wm != "" {
		if t, err := time.Parse(time.RFC3339Nano, wm); err == nil && !m.TS.After(t) {
			return false // already posted (at/behind watermark)
		}
	}

	if err := s.ensurePeer(ctx, s.selfPeer, nil); err != nil {
		s.log.Warn("honcho: ensure self peer", "err", err)
		return false
	}
	if err := s.ensurePeer(ctx, cp.id, map[string]any{"name": cp.name, "handle": cp.handle, "platform": "whatsapp"}); err != nil {
		s.log.Warn("honcho: ensure peer", "peer", cp.id, "err", err)
		return false
	}
	if err := s.ensureSession(ctx, sessionID, cp.id); err != nil {
		s.log.Warn("honcho: ensure session", "session", sessionID, "err", err)
		return false
	}

	author := s.selfPeer
	if !m.Sender.IsSelf {
		author = cp.id
	}
	if err := s.cli.AddMessages(ctx, sessionID, []honcho.Message{{
		Content: m.Text, PeerID: author, CreatedAt: m.TS.UTC().Format(time.RFC3339Nano),
	}}); err != nil {
		s.log.Warn("honcho: add message", "session", sessionID, "peer", author, "err", err)
		return false
	}
	_ = s.store.SetMeta(ctx, wmKey, m.TS.UTC().Format(time.RFC3339Nano))
	s.log.Info("honcho: posted message", "session", sessionID, "author", author, "counterpart", cp.id)
	return true
}

// resolveCounterpart returns the DM's inner-tier counterpart person. For an INBOUND
// message it resolves+caches the sender (gating inner-tier person); for an OUTBOUND
// message it returns the counterpart already learned for that chat (or ok=false if none
// — we only feed DMs with an established inner-tier counterpart).
func (s *honchoSink) resolveCounterpart(m event.Message) (personRef, bool) {
	if m.Sender.IsSelf {
		cp, ok := s.counterpart[m.ChatID]
		return cp, ok
	}
	card, ok := s.proj.ResolveSocialAuthor(event.PlatformWhatsApp, m.Sender.PlatformUserID, m.Sender.Handle)
	if !ok || card.CanonicalID == "" {
		return personRef{}, false
	}
	if card.Type != "" && card.Type != "person" {
		return personRef{}, false // entities/brands aren't people-model peers
	}
	if card.SalienceTier != "inner" {
		return personRef{}, false
	}
	h := ""
	if len(card.Handles) > 0 {
		h = card.Handles[0]
	}
	// Sanitize the canonical id to Honcho's id charset (^[a-zA-Z0-9_-]+$) once at
	// construction so the SAME value is the peer id, the session peer-map key, and the
	// message author everywhere downstream. (canonical ids are mostly hyphenated-safe,
	// but lark open_ids / raw platform ids can carry disallowed chars.)
	cp := personRef{id: honcho.SanitizeID(card.CanonicalID), name: card.Name, handle: h}
	s.counterpart[m.ChatID] = cp
	return cp, true
}

func (s *honchoSink) ensurePeer(ctx context.Context, id string, meta map[string]any) error {
	if s.peers[id] {
		return nil
	}
	if err := s.cli.EnsurePeer(ctx, id, meta); err != nil {
		return err
	}
	s.peers[id] = true
	return nil
}

func (s *honchoSink) ensureSession(ctx context.Context, id, personID string) error {
	if s.sessions[id] {
		return nil
	}
	peers := map[string]honcho.PeerConfig{
		s.selfPeer: {ObserveMe: true, ObserveOthers: true},
		personID:   {ObserveMe: true, ObserveOthers: true},
	}
	if err := s.cli.EnsureSession(ctx, id, peers, map[string]any{"source": "switchboard", "platform": "whatsapp", "kind": "dm"}); err != nil {
		return err
	}
	s.sessions[id] = true
	return nil
}

// HonchoBackfill DISPATCHES the bounded one-shot historical pass asynchronously and
// returns immediately — a full backfill waits (up to minutes) for the resolver build AND
// posts potentially thousands of messages to Honcho, far longer than an HTTP request
// should block (the synchronous version hung the handler → HTTP 000). The work runs on
// the long-lived daemon ctx (d.runCtx) so it survives the response; the outcome is in the
// structured logs ("honcho backfill ..." + per-chat lines) + Honcho counts. Idempotent
// (per-session watermark) so re-dispatching is safe. Returns (0,0,nil) = accepted.
func (d *Daemon) HonchoBackfill(ctx context.Context, maxChats, perChat int) (chats, msgs int, err error) {
	if d.honcho == nil {
		return 0, 0, nil
	}
	if maxChats <= 0 {
		maxChats = 200
	}
	if perChat <= 0 {
		perChat = 500
	}
	go d.runHonchoBackfill(maxChats, perChat)
	return 0, 0, nil
}

// runHonchoBackfill is the async body. Runs on d.runCtx (canceled only on shutdown).
func (d *Daemon) runHonchoBackfill(maxChats, perChat int) {
	ctx := d.runCtx
	if ctx == nil {
		return
	}
	// Wait for the startup Reproject to FINISH building the resolver (idToCanon) +
	// settling salience before scanning — else ResolveSocialAuthor races a partial
	// resolver and every WA sender comes back UNRESOLVED (the observed bug: scored=33
	// partial graph, all @lid senders unresolved). reprojectDone is closed post-build.
	select {
	case <-d.reprojectDone:
		d.log.Info("honcho backfill: resolver ready, starting", "scored", len(d.proj.SalienceRanking()))
	case <-ctx.Done():
		return
	}
	all, err := d.store.ListChats(ctx, event.PlatformWhatsApp, time.Time{}, maxChats)
	if err != nil {
		d.log.Warn("honcho backfill: list chats", "err", err)
		return
	}
	chats, msgs := 0, 0
	for _, c := range all {
		if c.ChatType != event.ChatDM {
			continue
		}
		hist, herr := d.store.ReadWindow(ctx, c.ChatID, store.ReadOpts{Limit: perChat}) // oldest-first
		if herr != nil {
			d.log.Warn("honcho backfill: read", "chat", c.ChatID, "err", herr)
			continue
		}
		// Per-chat diagnostic (Info so it surfaces at the daemon's log level): classify
		// the counterpart ONCE from the first inbound message — canonical id + tier +
		// whether it passes the inner-person gate. This is the "why 0 posted" visibility.
		for _, m := range hist {
			if m.Sender.IsSelf {
				continue
			}
			if card, ok := d.proj.ResolveSocialAuthor(event.PlatformWhatsApp, m.Sender.PlatformUserID, m.Sender.Handle); !ok {
				// resolver_canonicals distinguishes the two failure modes: small/zero =
				// resolver not built (timing); large = built-but-id-missing (keying/store).
				d.log.Info("honcho backfill: counterpart UNRESOLVED",
					"chat", c.ChatID, "platform", event.PlatformWhatsApp, "sender", m.Sender.PlatformUserID,
					"handle", m.Sender.Handle, "resolver_canonicals", d.proj.CanonicalCount())
			} else {
				d.log.Info("honcho backfill: counterpart",
					"chat", c.ChatID, "canonical", card.CanonicalID, "name", card.Name,
					"type", card.Type, "tier", card.SalienceTier,
					"pass", card.CanonicalID != "" && (card.Type == "" || card.Type == "person") && card.SalienceTier == "inner")
			}
			break
		}
		posted := 0
		for _, m := range hist {
			if d.honcho.handle(ctx, m) {
				posted++
			}
		}
		if posted > 0 {
			chats++ // count only chats that actually contributed a post
			msgs += posted
			d.log.Info("honcho backfill: chat posted", "chat", c.ChatID, "scanned", len(hist), "posted", posted)
		}
	}
	d.log.Info("honcho backfill complete", "chats", chats, "messages", msgs)
}
