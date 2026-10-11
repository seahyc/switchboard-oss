// Package daemon wires the whole capture+index spine together: store, the
// shared ingestion pipeline, the gbrain-feed projector, the push hub, the
// registered platform connectors, and the local HTTP API. It implements
// api.Backend so the MCP server can drive sends/reacts/backfills.
//
// Connectors are registered by the binary (cmd/switchboardd) and started by Run.
// Live events and backfill output both flow into ONE pipeline channel — the
// single normalize→log→project path — so live and backfill dedupe against each
// other on the canonical id.
package daemon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seahyc/switchboard-oss/internal/api"
	"github.com/seahyc/switchboard-oss/internal/connectors"
	"github.com/seahyc/switchboard-oss/internal/connectors/lark"
	"github.com/seahyc/switchboard-oss/internal/connectors/whatsapp"
	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/gbrainfeed"
	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
	"github.com/seahyc/switchboard-oss/internal/honcho"
	"github.com/seahyc/switchboard-oss/internal/idfeeds"
	"github.com/seahyc/switchboard-oss/internal/igweb"
	"github.com/seahyc/switchboard-oss/internal/llm"
	"github.com/seahyc/switchboard-oss/internal/media"
	"github.com/seahyc/switchboard-oss/internal/pipeline"
	"github.com/seahyc/switchboard-oss/internal/privatefs"
	"github.com/seahyc/switchboard-oss/internal/store"
	"github.com/seahyc/switchboard-oss/internal/xweb"
)

// Daemon owns the running system.
type Daemon struct {
	store *store.Store
	pipe  *pipeline.Pipeline
	hub   *api.Hub
	proj  *gbrainfeed.Projector
	media *media.Manager // nil when media store disabled
	log   *slog.Logger

	qrDir      string
	addr       string // bound API address (for building media http_url); set in Run
	events     chan event.Message
	connectors map[string]connectors.Connector
	ig         *igweb.Client // read-only Instagram private-web browser (#25); nil = disabled
	x          *xweb.Client  // read-only X.com private-web browser (#50); nil = disabled
	llm        *llm.Client   // local-only ollama client (qwen); nil = disabled. Shared by the
	// idmatch matcher (#19) + the X briefing importance classifier (#55).
	honcho *honchoSink // user-model ingestion sink (flagged via HONCHO_INGEST_ENABLED); nil = off

	// reprojectDone is closed once the startup Reproject has FINISHED building the
	// resolver (idToCanon) + settling salience. Consumers that must read a fully-built
	// graph — the Honcho sink worker (gates on resolved counterpart+tier) and the Honcho
	// backfill — wait on this so they don't race the async producer (RecomputeSalience's
	// WaitIdle only drains the CURRENT queue; it can return "idle" while Reproject is
	// still enqueuing → a partial resolver → every WA sender UNRESOLVED).
	reprojectDone chan struct{}

	// runCtx is the long-lived daemon context (set at the top of Run, canceled on
	// SIGINT/SIGTERM). Async background work kicked off by an HTTP handler (the Honcho
	// backfill) runs on this — NOT the request ctx, which dies when the handler returns
	// — so it survives the response yet is still canceled on shutdown.
	runCtx context.Context

	matchEnabled  bool          // run the off-ingest identity-matcher worker
	matchInterval time.Duration // gap between sweeps
	version       string        // build version (git SHA)
	apiPolicy     api.Policy

	larkDiscoverMu sync.Mutex // guards against overlapping Lark p2p discovery runs (#31)
	sweepMu        sync.Mutex // guards against overlapping salience backfill sweeps (#13)
	salienceMu     sync.Mutex // guards against overlapping salience recomputes — tick vs reproject vs on-demand (#59)
}

// Config configures a Daemon.
type Config struct {
	APIPolicy api.Policy
	DBPath    string            // SQLite path (":memory:" for tests)
	SelfIDs   []string          // user's own platform_user_ids (mentions / @me)
	GBrain    gbrainfeed.Client // gbrain client (NopClient if unset)
	QRDir     string            // dir to write <platform>-qr.png for QR-based auth (empty = don't)
	Logger    *slog.Logger
	Version   string // build version (git SHA), surfaced by /healthz for deploy verification

	// Media store (importance-gated attachment download). Disabled when MediaRoot
	// is empty. Zero numeric values fall back to the media package defaults
	// (40GiB cap, 512MiB per-file ceiling).
	MediaRoot       string
	MediaCapBytes   int64
	MediaPerFileMax int64
	MediaTypes      []string // if set, only these attachment types are downloaded
	MediaRatePerMin int      // throttle: max downloads/min (0 = unlimited)

	// Identity matcher (#19) — the off-ingest LLM adjudication worker. The two
	// store feeds (IdentityContext + PersonStats) are ALWAYS wired (cheap,
	// read-only, also powers #15 salience); the heavy LLM sweep runs only when
	// EnableMatcher is set, so deploying the binary never auto-starts a multi-hour
	// CPU batch without a deliberate flip. Always PROPOSE-ONLY (auto-merge stays
	// off until the lead gates the eval scorecard).
	EnableMatcher    bool
	OllamaURL        string        // explicit base URL; empty → OLLAMA_URL / optional OLLAMA_CONTAINER / localhost
	OllamaModel      string        // adjudication model (default qwen2.5:3b-instruct; 7b removed by vm-gov OOM fix)
	MatchInterval    time.Duration // gap between adjudication sweeps (default 6h)
	MatchPaceSeconds int           // per-call CPU breathing gap in seconds (default 3) so the sweep never starves live ingest
}

// New builds a Daemon from cfg.
func New(cfg Config) (*Daemon, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	hub := api.NewHub()

	var gb gbrainfeed.Client = cfg.GBrain
	if gb == nil {
		gb = gbrainfeed.NopClient{}
	}
	proj := gbrainfeed.NewProjector(gb, logger)

	// Durable review decisions: back the projector's proposal store with SQLite
	// (over the same store DB) so "Not same" rejections and the pending list
	// survive daemon restarts — the in-memory default loses them on every
	// restart, re-asking a pair the user already judged. Best-effort: on error
	// log and keep the in-memory store rather than block startup.
	if ps, err := gbrainfeed.NewSQLiteProposalStore(st.DB(), logger); err != nil {
		logger.Warn("durable proposal store unavailable; using in-memory", "err", err)
	} else {
		proj.SetProposalStore(ps)
		proj.LoadRejections()
		proj.LoadPendingProposals() // #61: seed proposed-set so the startup reproject doesn't re-emit every pending merge
	}

	// #62: durable put-page hash cache so the startup reproject SKIPS re-writing
	// unchanged gbrain pages (each write is a gbrain subprocess) — without it a restart
	// re-writes ~all ~7000 pages = the ~13min reproject + WaitIdle timeout. Seed BEFORE
	// the reproject. Best-effort: on error the reproject just re-writes everything.
	if phs, err := gbrainfeed.NewSQLitePageHashStore(st.DB(), logger); err != nil {
		logger.Warn("durable page-hash store unavailable; reproject will re-write all pages", "err", err)
	} else {
		proj.SetPageHashStore(phs)
		proj.LoadPageHashes(context.Background())
	}

	// Identity feeds (#15 salience + #19 matcher context): store-backed, read-only,
	// pulled off-ingest by the projector. Always wired — they're cheap and the
	// salience feed powers the people-graph ranking independent of the LLM matcher.
	feeds := idfeeds.NewIdentityFeeds(st.DB())
	proj.SetIdentityContextProvider(feeds)
	proj.SetPersonStatsProvider(feeds)

	// The local inference client is shared by the matcher and X classifier.
	url := cfg.OllamaURL
	// Explicit URLs are authoritative; Docker lookup requires opt-in.
	container := ""
	if url == "" {
		resolved, rerr := llm.ResolveOllamaURL(context.Background(), "")
		if rerr != nil {
			logger.Warn("ollama URL fell back to default", "err", rerr, "url", resolved)
		}
		url = resolved
		container = strings.TrimSpace(os.Getenv("OLLAMA_CONTAINER"))
	}
	pace := time.Duration(cfg.MatchPaceSeconds) * time.Second
	if cfg.MatchPaceSeconds == 0 {
		pace = 3 * time.Second
	}
	llmClient := llm.New(llm.Config{BaseURL: url, AdjModel: cfg.OllamaModel, KeepAlive: "5m", MinInterval: pace, Container: container})

	// LLM identity-matcher adjudicator (#19). Wired only when enabled. Stays
	// PROPOSE-ONLY: SetAutoMerge is never called here, so high-confidence verdicts
	// go to the human review queue until the lead flips auto-merge post-eval.
	if cfg.EnableMatcher {
		var cache idmatch.AdjudicationCache
		if mc, cerr := NewSQLiteMatchCache(st.DB(), logger); cerr != nil {
			logger.Warn("idmatch verdict cache unavailable; using in-memory", "err", cerr)
		} else {
			cache = mc
		}
		proj.SetAdjudicator(llmClient, cache)
		logger.Info("identity matcher enabled (PROPOSE-ONLY)", "ollama", url, "model", llmClient.AdjModel())
	}

	// Media store (importance-gated attachment download). Disabled when no root
	// is configured. As a pipeline sink it only registers attachments (a fast
	// insert); the gated download runs on its own worker, off the ingest path.
	var mediaMgr *media.Manager
	if cfg.MediaRoot != "" {
		var types map[string]bool
		if len(cfg.MediaTypes) > 0 {
			types = map[string]bool{}
			for _, t := range cfg.MediaTypes {
				types[t] = true
			}
		}
		mm, err := media.NewManager(st, media.Config{
			Root:          cfg.MediaRoot,
			CapBytes:      cfg.MediaCapBytes,
			PerFileMax:    cfg.MediaPerFileMax,
			IncludedTypes: types,
			RatePerMin:    cfg.MediaRatePerMin,
		}, logger)
		if err != nil {
			return nil, fmt.Errorf("media store: %w", err)
		}
		mediaMgr = mm
	}

	sinks := []pipeline.Option{
		pipeline.WithSelfIDs(cfg.SelfIDs...),
		pipeline.WithSink(hub),  // push event stream
		pipeline.WithSink(proj), // gbrain-feed projector
		pipeline.WithLogger(logger),
	}
	if mediaMgr != nil {
		sinks = append(sinks, pipeline.WithSink(mediaMgr)) // register attachments for download
	}
	// Honcho user-model ingestion (narrow v1) — flagged OFF unless HONCHO_INGEST_ENABLED is
	// set, so the binary deploys dormant (go-live is a deliberate flip after sink verify).
	// The sink is non-blocking + its worker POSTs off the ingest hot path (see honcho.go).
	var honchoHS *honchoSink
	if os.Getenv("HONCHO_INGEST_ENABLED") != "" {
		honchoHS = newHonchoSink(proj, st, logger, honcho.New(os.Getenv("HONCHO_BASE_URL"), honchoWorkspace))
		sinks = append(sinks, pipeline.WithSink(honchoHS))
		logger.Info("honcho user-model ingestion ENABLED (narrow: WA DMs + inner-tier)")
	}
	pipe := pipeline.New(st, sinks...)

	return &Daemon{
		store:         st,
		pipe:          pipe,
		hub:           hub,
		proj:          proj,
		media:         mediaMgr,
		log:           logger,
		qrDir:         cfg.QRDir,
		apiPolicy:     cfg.APIPolicy,
		events:        make(chan event.Message, 4096), // buffer so the live socket never blocks on projection
		connectors:    map[string]connectors.Connector{},
		llm:           llmClient,
		honcho:        honchoHS,
		reprojectDone: make(chan struct{}),
		matchEnabled:  cfg.EnableMatcher,
		matchInterval: cfg.MatchInterval,
		version:       cfg.Version,
	}, nil
}

// Register adds a connector under its Name(). Call before Run.
func (d *Daemon) Register(c connectors.Connector) {
	d.connectors[c.Name()] = c
	// If the connector exposes a lazy identity enricher (Lark's open_id→email
	// lookup), wire it into the gbrain-feed projector so it can enrich sparse
	// senders on demand — never on the connector hot path (locked design).
	if lc, ok := c.(*lark.Connector); ok {
		d.proj.RegisterEnricher(c.Name(), larkEnricherAdapter{e: lc.Enricher()})
	}
	// If the connector can download attachment bytes, wire it into the media
	// store so the gated worker can fetch this platform's media on demand.
	if d.media != nil {
		if md, ok := c.(connectors.MediaDownloader); ok {
			d.media.RegisterDownloader(c.Name(), md)
		}
		// Replay-safe Ref recovery: rebuild full download Refs from raw for
		// attachments whose stored Ref predates full-tuple serialization.
		if rd, ok := c.(connectors.RefDeriver); ok {
			d.media.RegisterRefDeriver(c.Name(), rd.DeriveMediaRef)
		}
	}
}

// larkEnricherAdapter maps the Lark connector's Enricher (open_id → Profile)
// onto the platform-agnostic gbrainfeed.Enricher (user id → Aliases).
type larkEnricherAdapter struct{ e *lark.Enricher }

func (a larkEnricherAdapter) Enrich(ctx context.Context, userIDs []string) (map[string]gbrainfeed.Aliases, error) {
	profs, err := a.e.Enrich(ctx, userIDs)
	if err != nil {
		return nil, err
	}
	out := make(map[string]gbrainfeed.Aliases, len(profs))
	for id, p := range profs {
		out[id] = gbrainfeed.Aliases{
			Email:       p.Email,
			Phone:       p.Phone,
			DisplayName: p.DisplayName,
		}
	}
	return out, nil
}

// --- api.Backend ---

// Store implements api.Backend.
func (d *Daemon) Store() *store.Store { return d.store }

// Send implements api.Backend, resolving the connector by chat-id platform.
func (d *Daemon) Send(ctx context.Context, req connectors.SendRequest) (string, error) {
	c, err := d.connectorFor(req.ChatID)
	if err != nil {
		return "", err
	}
	sentID, err := c.Send(ctx, req)
	if err != nil {
		return "", err
	}
	// #40: write the sent message into the canonical log so the agent keeps a record of
	// its own replies. Some connectors (e.g. the Telegram userbot on a self-chat) never
	// echo own-outbound as an inbound update, so without this the agent loses conversational
	// continuity (re-addresses / duplicate replies). Safe for connectors that DO echo:
	// store.Append is INSERT-OR-IGNORE idempotent on the canonical id, and every connector's
	// Send + inbound mapping both derive MakeID(platform, msgID) from the same underlying id,
	// so a later echo dedups to a no-op.
	d.recordOutbound(ctx, sentID, req)
	return sentID, nil
}

// recordOutbound records a just-sent message into the canonical log via the shared
// pipeline's SYNCHRONOUS Ingest (store + FTS + projector + push hub), marked is_self, so
// it's queryable the instant /send returns — an agent that sends then immediately re-reads
// the thread sees its own message. Synchronous (not a fire-and-forget channel emit) so a
// backed-up event queue can't delay the record by tens of seconds or silently drop it
// under load (#40 latency follow-up). Idempotent on the canonical id (see Send). An empty
// sentID means the connector didn't surface one — skip rather than write an un-keyed row.
func (d *Daemon) recordOutbound(ctx context.Context, sentID string, req connectors.SendRequest) {
	if sentID == "" {
		return
	}
	kind := event.KindText
	switch {
	case len(req.Attachments) > 0:
		kind = event.KindMedia
	case req.Poll != nil:
		kind = event.KindPoll
	case req.Card != nil:
		kind = event.KindCard
	}
	now := time.Now().UTC()
	m := event.Message{
		ID:          sentID,
		Platform:    platformOf(req.ChatID),
		ChatID:      req.ChatID,
		Sender:      event.Sender{IsSelf: true},
		TS:          now,
		Kind:        kind,
		Text:        req.Text,
		ReplyTo:     req.ReplyTo,
		Attachments: req.Attachments,
		Source:      event.SourceLive,
		IngestedAt:  now,
	}
	if inserted, err := d.pipe.Ingest(ctx, m); err != nil {
		d.log.Warn("outbound record: ingest failed (reply not logged)", "id", sentID, "chat", req.ChatID, "err", err)
	} else {
		d.log.Debug("outbound recorded to log", "id", sentID, "chat", req.ChatID, "inserted", inserted)
	}
}

// SendMedia implements api.Backend by resolving a captured media id to bytes
// and routing the upload through the target platform connector.
func (d *Daemon) SendMedia(ctx context.Context, req api.SendMediaBody) (string, error) {
	if d.media == nil {
		return "", fmt.Errorf("media store disabled")
	}
	c, err := d.connectorFor(req.ChatID)
	if err != nil {
		return "", err
	}
	ms, ok := c.(connectors.MediaSender)
	if !ok {
		return "", fmt.Errorf("send_media unsupported on %q", c.Name())
	}

	row, rc, err := d.media.Get(ctx, req.MediaID)
	if err != nil {
		return "", err
	}
	defer rc.Close()

	name := req.Filename
	if name == "" {
		name = row.Name
	}
	if name == "" {
		name = "attachment"
	}
	mime := req.MIME
	if mime == "" {
		mime = row.MIME
	}
	if mime == "" {
		mime = "application/octet-stream"
	}

	return ms.SendMedia(ctx, connectors.MediaSendRequest{
		ChatID:          req.ChatID,
		Text:            req.Text,
		ReplyTo:         req.ReplyTo,
		Type:            row.Type,
		MIME:            mime,
		Name:            name,
		Size:            row.Size,
		SourceMediaID:   req.MediaID,
		SourceMessageID: row.EventID,
		MarkForwarded:   req.MarkForwarded,
		AsDocument:      req.AsDocument,
		Body:            rc,
	})
}

// React implements api.Backend. For platforms whose reaction key needs the
// target message's sender (WhatsApp group reactions), it resolves the sender
// from the store and uses the connector's optional ParticipantReactor; else it
// falls back to the base React. This keeps the canonical schema + base
// Connector interface frozen.
func (d *Daemon) React(ctx context.Context, chatID, msgID, emoji string) error {
	c, err := d.connectorFor(chatID)
	if err != nil {
		return err
	}
	if pr, ok := c.(connectors.ParticipantReactor); ok {
		if _, senderID, found, err := d.store.MessageRef(ctx, msgID); err == nil && found && senderID != "" {
			return pr.ReactAs(ctx, chatID, msgID, senderID, emoji)
		}
	}
	return c.React(ctx, chatID, msgID, emoji)
}

// TapButton implements api.Backend via the optional ButtonTapper capability.
func (d *Daemon) TapButton(ctx context.Context, chatID, msgID, buttonID string) (string, error) {
	c, err := d.connectorFor(chatID)
	if err != nil {
		return "", err
	}
	bt, ok := c.(connectors.ButtonTapper)
	if !ok {
		return "", fmt.Errorf("tap_button unsupported on %q", c.Name())
	}
	return bt.TapButton(ctx, chatID, msgID, buttonID)
}

// SetIGBrowser wires the read-only Instagram private-web browser client (#25).
// Nil when not configured; the endpoints then return a clean "not configured".
func (d *Daemon) SetIGBrowser(c *igweb.Client) { d.ig = c }

// IGProfile implements api.Backend: a read-only IG profile card (egress-verified,
// proxy-gated inside the igweb client).
func (d *Daemon) IGProfile(ctx context.Context, handle string) (igweb.ProfileCard, error) {
	if d.ig == nil {
		return igweb.ProfileCard{}, fmt.Errorf("instagram browser not configured")
	}
	return d.ig.Profile(ctx, handle)
}

// IGStories implements api.Backend: the read-only current story tray.
func (d *Daemon) IGStories(ctx context.Context) (igweb.StoriesResult, error) {
	if d.ig == nil {
		return igweb.StoriesResult{}, fmt.Errorf("instagram browser not configured")
	}
	return d.ig.Stories(ctx)
}

// IGFeed implements api.Backend: the read-only home feed (posts from people the
// user follows). maxID is an optional pagination cursor ("" = first page); limit
// (>0) auto-paginates internally to return up to that many real posts.
func (d *Daemon) IGFeed(ctx context.Context, maxID string, limit int) (igweb.FeedResult, error) {
	if d.ig == nil {
		return igweb.FeedResult{}, fmt.Errorf("instagram browser not configured")
	}
	return d.ig.FeedN(ctx, limit, maxID)
}

// IGFollowingFeed implements api.Backend: recent posts from ONLY the accounts the
// user follows, merged chronologically, with no recommendations/ads (#47). Built
// by per-account fan-out; limit (<=0 → default) caps the merged result.
func (d *Daemon) IGFollowingFeed(ctx context.Context, limit int) (igweb.FeedResult, error) {
	if d.ig == nil {
		return igweb.FeedResult{}, fmt.Errorf("instagram browser not configured")
	}
	return d.ig.FollowingFeed(ctx, limit)
}

// IGFollowing implements api.Backend: the COMPLETE list of accounts the user
// follows, each enriched with friend-vs-creator signals (#49). Read-only.
func (d *Daemon) IGFollowing(ctx context.Context) (igweb.FollowingList, error) {
	if d.ig == nil {
		return igweb.FollowingList{}, fmt.Errorf("instagram browser not configured")
	}
	return d.ig.Following(ctx)
}

// SetXBrowser wires the read-only X.com private-web browser client (#50).
func (d *Daemon) SetXBrowser(c *xweb.Client) { d.x = c }

// XFollowingFeed implements api.Backend: the chronological X "Following" timeline
// — recent posts from accounts the user follows, newest-first, promoted/ads
// dropped (#50). limit (<=0 → default) caps the result.
func (d *Daemon) XFollowingFeed(ctx context.Context, limit int) (xweb.FeedResult, error) {
	if d.x == nil {
		return xweb.FeedResult{}, fmt.Errorf("x browser not configured")
	}
	return d.x.FollowingFeed(ctx, limit)
}

// XProfile implements api.Backend: a read-only X profile card + recent tweets.
func (d *Daemon) XProfile(ctx context.Context, handle string, limit int) (xweb.ProfileCard, error) {
	if d.x == nil {
		return xweb.ProfileCard{}, fmt.Errorf("x browser not configured")
	}
	return d.x.Profile(ctx, handle, limit)
}

// StartChat implements api.Backend via the optional ChatStarter capability. It
// routes by PLATFORM (not chat_id — there is no chat yet) to the connector,
// which resolves the identifier to a canonical chat_id, creating the underlying
// conversation if needed. Used for cold-DMs (e.g. messaging an IG handle the
// user has never talked to).
func (d *Daemon) StartChat(ctx context.Context, platform, identifier string) (chatID, resolvedName string, err error) {
	c, ok := d.connectors[platform]
	if !ok {
		return "", "", fmt.Errorf("no connector registered for platform %q", platform)
	}
	cs, ok := c.(connectors.ChatStarter)
	if !ok {
		return "", "", fmt.Errorf("start_chat unsupported on %q", platform)
	}
	return cs.StartChat(ctx, identifier)
}

// VotePoll implements api.Backend via the optional PollVoter capability.
func (d *Daemon) VotePoll(ctx context.Context, chatID, msgID, option string) error {
	c, err := d.connectorFor(chatID)
	if err != nil {
		return err
	}
	pv, ok := c.(connectors.PollVoter)
	if !ok {
		return fmt.Errorf("vote_poll unsupported on %q", c.Name())
	}
	return pv.VotePoll(ctx, chatID, msgID, option)
}

// GroupInvite implements api.Backend via the optional GroupInviteLinker capability
// (#52, the card's "open group" affordance). reset=true rotates the link.
func (d *Daemon) GroupInvite(ctx context.Context, chatID string, reset bool) (string, error) {
	c, err := d.connectorFor(chatID)
	if err != nil {
		return "", err
	}
	gl, ok := c.(connectors.GroupInviteLinker)
	if !ok {
		return "", fmt.Errorf("group-invite unsupported on %q", c.Name())
	}
	return gl.GroupInviteLink(ctx, chatID, reset)
}

// Backfill implements api.Backend; output flows into the shared pipeline.
// chatID "all" or "" backfills every registered connector (each connector
// treats an empty chatID as "all chats"); otherwise it routes by platform.
// Each connector's Backfill is panic-isolated so one buggy connector can't take
// down the daemon or block the others.
func (d *Daemon) Backfill(ctx context.Context, chatID string, since time.Time) error {
	if chatID == "" || chatID == "all" {
		if len(d.connectors) == 0 {
			return fmt.Errorf("backfill all: no connectors registered")
		}
		// Fan out per-connector backfills CONCURRENTLY: a slow connector (e.g.
		// Lark re-paging thousands, or Gmail) must not block the others (which is
		// exactly what starved Telegram's backfill of its turn). Each is
		// panic-isolated; we wait for all and return the first error.
		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			firstErr error
		)
		for name, c := range d.connectors {
			name, c := name, c
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := d.safeBackfill(ctx, name, c, "", since); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("backfill %s: %w", name, err)
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		return firstErr
	}
	c, err := d.connectorFor(chatID)
	if err != nil {
		return err
	}
	// Deep per-chat paging: if the connector supports anchored backfill
	// (WhatsApp), resolve the oldest stored message and page further back than
	// the push window. Bounded by a timeout since the connector waits on the
	// async history-sync reply.
	if ab, ok := c.(connectors.AnchoredBackfiller); ok {
		if id, fromMe, ts, found, lerr := d.store.OldestMessage(ctx, chatID); lerr == nil && found {
			bctx, cancel := context.WithTimeout(ctx, 45*time.Second)
			defer cancel()
			return d.safeAnchoredBackfill(bctx, c.Name(), ab, chatID, id, fromMe, ts, 200)
		}
	}
	return d.safeBackfill(ctx, c.Name(), c, chatID, since)
}

// safeAnchoredBackfill runs a connector's anchored backfill under recover().
func (d *Daemon) safeAnchoredBackfill(ctx context.Context, name string, ab connectors.AnchoredBackfiller, chatID, oldestID string, fromMe bool, oldestTS time.Time, count int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			d.log.Error("connector anchored backfill panicked (recovered)", "platform", name, "chat", chatID, "panic", r)
			err = fmt.Errorf("connector %s anchored backfill panicked: %v", name, r)
		}
	}()
	d.log.Info("anchored backfill", "platform", name, "chat", chatID, "anchor", oldestID, "count", count)
	return ab.BackfillBefore(ctx, chatID, oldestID, fromMe, oldestTS, count, d.events)
}

// safeBackfill runs a connector's Backfill, converting a panic into an error so
// a single connector's bug (e.g. a nil-deref) can never crash the daemon or
// abort the other connectors' backfill.
func (d *Daemon) safeBackfill(ctx context.Context, name string, c connectors.Connector, chatID string, since time.Time) (err error) {
	defer func() {
		if r := recover(); r != nil {
			d.log.Error("connector Backfill panicked (recovered)", "platform", name, "chat", chatID, "panic", r)
			err = fmt.Errorf("connector %s backfill panicked: %v", name, r)
		}
	}()
	return c.Backfill(ctx, chatID, since, d.events)
}

// AuthStatus reports a connector's login state (by platform name).
func (d *Daemon) AuthStatus(ctx context.Context, platform string) (connectors.LoginStatus, error) {
	c, ok := d.connectors[platform]
	if !ok {
		return connectors.LoginStatus{}, fmt.Errorf("no connector registered for platform %q", platform)
	}
	return c.Status(ctx)
}

// ConnectorHealth returns a read-only health snapshot for one connector (#37) — for
// olympus's re-auth auto-ack. connected/last_error come from the connector's Status();
// newest_ts + account come from the store (newest ingested message for the platform).
func (d *Daemon) ConnectorHealth(ctx context.Context, name string) (api.ConnectorHealth, error) {
	c, ok := d.connectors[name]
	if !ok {
		return api.ConnectorHealth{}, fmt.Errorf("no connector registered for platform %q", name)
	}
	var h api.ConnectorHealth
	if st, serr := c.Status(ctx); serr != nil {
		h.Connected = false
		h.LastError = serr.Error()
	} else {
		h.Connected = st.Connected
		if !st.Connected {
			h.LastError = st.Detail // carries invalid_grant / auth-failure / next-step when unhealthy
		}
	}
	if newest, account, err := d.store.PlatformNewestAndAccount(ctx, name); err == nil {
		if !newest.IsZero() {
			h.NewestTS = newest.Format(time.RFC3339)
		}
		h.Account = account
	}
	return h, nil
}

// platformIngested returns (chats_with_messages, chats_known) for one platform from the
// coverage scorecard — the convergence numerator/denominator. (0,0) if absent/on error.
func (d *Daemon) platformIngested(ctx context.Context, platform string) (ingested, known int) {
	cov, err := d.store.Coverage(ctx)
	if err != nil {
		return 0, 0
	}
	for _, c := range cov {
		if c.Platform == platform {
			return c.ChatsWithMessages, c.ChatsKnown
		}
	}
	return 0, 0
}

// Observability is the phase-2 read-only health snapshot (#39): per-platform coverage
// (chats_known vs chats_with_messages), ingestion lag (now − newest message ts),
// connector health (#37), and the last recorded backfill-sweep run (convergence delta).
// One aggregate the morning-briefing / a watching agent can poll to see whether ingestion
// is keeping up and backfill is converging. Read-only; never mutates.
func (d *Daemon) Observability(ctx context.Context) (api.Observability, error) {
	cov, err := d.store.Coverage(ctx)
	if err != nil {
		return api.Observability{}, err
	}
	now := time.Now().UTC()
	out := api.Observability{GeneratedAt: now.Format(time.RFC3339)}
	for _, c := range cov {
		p := api.PlatformObservability{
			Platform:          c.Platform,
			ChatsKnown:        c.ChatsKnown,
			ChatsWithMessages: c.ChatsWithMessages,
			Messages:          c.Messages,
		}
		if c.ChatsKnown > 0 {
			p.CoveragePct = float64(c.ChatsWithMessages) / float64(c.ChatsKnown) * 100
		}
		if !c.NewestTS.IsZero() {
			p.NewestTS = c.NewestTS.Format(time.RFC3339)
			p.IngestionLagSeconds = int64(now.Sub(c.NewestTS).Seconds())
		}
		// Connector health (best-effort; not every platform has a live connector registered).
		if _, ok := d.connectors[c.Platform]; ok {
			if h, herr := d.ConnectorHealth(ctx, c.Platform); herr == nil {
				p.ConnectorConnected = &h.Connected
				p.ConnectorLastError = h.LastError
			}
		}
		// Last recorded sweep run (convergence delta).
		if run, ok := d.store.LastBackfillRun(ctx, c.Platform); ok {
			p.LastSweep = &api.BackfillRunDTO{
				TS:             run.TS.Format(time.RFC3339),
				Swept:          run.Swept,
				Failed:         run.Failed,
				Flooded:        run.Flooded,
				IngestedBefore: run.IngestedBefore,
				IngestedAfter:  run.IngestedAfter,
			}
		}
		out.Platforms = append(out.Platforms, p)
	}
	return out, nil
}

// AuthSubmit feeds a user-supplied auth value to a connector's interactive login
// (Telegram phone/code/2FA). field is one of phone|code|password.
func (d *Daemon) AuthSubmit(ctx context.Context, platform, field, value string) error {
	c, ok := d.connectors[platform]
	if !ok {
		return fmt.Errorf("no connector registered for platform %q", platform)
	}
	ia, ok := c.(connectors.InteractiveAuth)
	if !ok {
		return fmt.Errorf("interactive auth unsupported on %q", platform)
	}
	switch field {
	case "phone":
		return ia.SubmitPhone(ctx, value)
	case "code":
		return ia.SubmitCode(ctx, value)
	case "password":
		return ia.SubmitPassword(ctx, value)
	default:
		return fmt.Errorf("unknown auth field %q (want phone|code|password)", field)
	}
}

// Reproject rebuilds the gbrain identity registry from the durable store: it
// reads every distinct sender and re-enqueues it into the (now non-lossy)
// projector. Use it to recover person pages dropped by the old lossy queue
// during a burst, or to rebuild after a restart. Returns the sender count.
func (d *Daemon) Reproject(ctx context.Context) (int, error) {
	senders, err := d.store.DistinctSenders(ctx)
	if err != nil {
		return 0, err
	}
	for _, si := range senders {
		d.proj.OnIngest(event.Message{
			Platform: si.Platform,
			Sender: event.Sender{
				PlatformUserID: si.UserID,
				DisplayName:    si.DisplayName,
				Handle:         si.Handle,
				Phone:          si.Phone,
				Email:          si.Email,
				IsSelf:         si.IsSelf,
			},
		})
	}
	d.log.Info("reproject enqueued identities from store", "count", len(senders))

	// SOCIAL REPLAY (#49 reproject-durability, people-layer's catch): the DistinctSenders
	// pass above re-enqueues only BARE senders — it strips the Social payload. So a follow's
	// friend-vs-creator classification (people-layer's project-time ClassifyFollow hook, which
	// fires on m.Social with Sub="follow") would NOT re-fire on reproject → node types vanish,
	// defeating Option-B's "durable-by-construction" promise. Replay the stored follow-identity
	// rows through OnIngest WITH their full Social payload (Mutual/IsBusiness/Followers/Category),
	// exactly as POST /social/ingest does — so every reproject re-derives the types, and this
	// pass retroactively (re)types any survivor that lost it. Only sub="follow" carries typing
	// signals (post/story authors are already nodes via the sender pass), so the replay is
	// bounded (~following count) and targeted. Best-effort: never fail the reproject.
	if follows, ferr := d.store.SocialActivity(ctx, "", time.Time{}, 0, "follow"); ferr != nil {
		d.log.Warn("reproject: social-follow replay query failed", "err", ferr)
	} else {
		for _, m := range follows {
			d.proj.OnIngest(m) // carries m.Social → people-layer's ClassifyFollow re-types
		}
		if len(follows) > 0 {
			d.log.Info("reproject: replayed follow-identity rows for re-typing", "follows", len(follows))
		}
	}

	// CRITICAL ORDERING (people-layer's catch): OnIngest above is ASYNC (worker pool), so
	// the post-reproject passes MUST wait for the projection queue to DRAIN first — else
	// they score/propose/reconcile a PARTIAL graph (the #15 partial-core_network bug:
	// scores capped, real people missing). WaitIdle blocks until the in-flight queue
	// empties (without closing); ctx-bounded so a stuck/slow worker can't hang forever.
	// A generous timeout — 3000+ identities take a couple minutes to project.
	idleCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
	defer cancel()
	if err := d.proj.WaitIdle(idleCtx); err != nil {
		d.log.Warn("reproject: projection drain (WaitIdle) incomplete — downstream passes may see a partial graph", "err", err)
	}
	// Now the graph is SETTLED — run the deterministic, no-LLM post-passes (all best-
	// effort, never fail the reproject):
	//  #15: salience recompute (was matcher-only → never ran with #19 shelved → empty
	//       core_network; now every reproject, post-drain → the FULL ranked network).
	if err := d.proj.RecomputeSalience(ctx); err != nil {
		d.log.Warn("reproject: salience recompute failed", "err", err)
	}
	//  #19 (propose-only): refresh the STRONG cross-platform proposal queue (shared
	//      distinctive handle / handle↔email-localpart) so Morgan/Taylor/Jordan-class
	//      handle-but-not-name fragments surface for one-tap review. No auto-merge.
	if n, err := d.proj.RunDeterministicProposals(ctx); err != nil {
		d.log.Warn("reproject: deterministic proposals failed", "err", err)
	} else if n > 0 {
		d.log.Info("reproject: deterministic proposals refreshed", "new", n)
	}
	//  #17: reconcile merged_into provenance links to the resolver's actual merge truth
	//      (under-recorded links → accurate provenance/audit).
	if n, err := d.proj.BackfillMergeLinks(ctx); err != nil {
		d.log.Warn("reproject: merge-link provenance reconcile failed", "err", err)
	} else if n > 0 {
		d.log.Info("reproject: merge-provenance links reconciled", "links", n)
	}
	return len(senders), nil
}

// RunDeterministicProposals refreshes the deterministic propose-only queue on demand
// (#19): strong cross-platform pairs (shared handle / handle↔email-localpart) that
// aren't already merged or auto-mergeable, surfaced for human review. No LLM, no
// auto-merge; idempotent. Backs POST /people/proposals/deterministic. Returns the
// count of new proposals.
func (d *Daemon) RunDeterministicProposals(ctx context.Context) (int, error) {
	return d.proj.RunDeterministicProposals(ctx)
}

// RecomputeSalience recomputes the people-graph salience ranking on demand (#15) —
// the deterministic, no-LLM, no-auto-merge scorer, decoupled from the (shelved) #19
// matcher. Backs POST /people/salience/recompute for calibration + a fresh compute
// after a manual reproject. Returns the number of scored people.
func (d *Daemon) RecomputeSalience(ctx context.Context) (int, error) {
	// WaitIdle FIRST so /recompute scores the SETTLED graph, not a mid-drain partial.
	// Post-restart the projector replays the full log through the same OnIngest queue
	// (Start → drain → processBatch); an ungated recompute races that replay and returns
	// an under-scored ring (observed scored=591 climbing to 3846 over ~90s — #39). Bounded
	// so a stuck/backed-up worker can't hang the HTTP handler indefinitely; on timeout we
	// score what's settled so far rather than fail the call.
	idleCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
	defer cancel()
	if err := d.proj.WaitIdle(idleCtx); err != nil {
		d.log.Warn("recompute: projection drain (WaitIdle) incomplete; scoring partial graph", "err", err)
	}
	if err := d.proj.RecomputeSalience(ctx); err != nil {
		return 0, err
	}
	return len(d.proj.SalienceRanking()), nil
}

// ReextractLIDPhones is the #33 one-time recovery: re-parse stored WhatsApp @lid
// message Raw to backfill the sender_phone column (blank for messages ingested
// before SenderAlt capture), so the deterministic shared-phone merge can collapse
// those fragments. Returns rows updated. Run once, then Reproject to rebuild
// canonicals with the recovered phones.
func (d *Daemon) ReextractLIDPhones(ctx context.Context) (int, error) {
	n, err := d.store.ReextractSenderPhones(ctx, event.PlatformWhatsApp, func(m event.Message) (string, bool) {
		return whatsapp.LIDPhoneFromRaw(m.Raw)
	})
	if err == nil {
		d.log.Info("reextracted @lid sender phones from raw", "rows_updated", n)
	}
	return n, err
}

// ReextractMediaRefs is the #48 one-time recovery for a platform whose media rows hold
// a pre-rich (thin/empty) fetch_ref that could never download (e.g. Telegram's old
// "telegram-photo:<id>" with no access_hash/file_reference). It re-derives a full
// downloadable ref from each attachment-bearing message's stored Raw + chat id (via the
// connector's MediaRefReextractor) and re-enqueues it; EnqueueMedia resets the matching
// failed/skipped row to pending when the ref changes, so the worker — or an on-view
// ensure() — can finally fetch the bytes. The re-downloads are NOT bursted: normal/low
// chats stay gated (fetched lazily on view), and the worker throttles the rest. Returns
// the number of rows refreshed.
func (d *Daemon) ReextractMediaRefs(ctx context.Context, platform, chatID string) (int, error) {
	if d.media == nil {
		return 0, fmt.Errorf("media store disabled")
	}
	c, ok := d.connectors[platform]
	if !ok {
		return 0, fmt.Errorf("no connector for platform %q", platform)
	}
	rx, ok := c.(connectors.MediaRefReextractor)
	if !ok {
		return 0, fmt.Errorf("connector %q has no media-ref reextractor", platform)
	}
	// chatID scopes the recovery to ONE chat (a tiny targeted run — e.g. recover a
	// specific contact's media first), ""=the whole platform backlog (converges paced
	// via on-view/gated downloads).
	msgs, err := d.store.MessagesWithAttachments(ctx, chatID)
	if err != nil {
		return 0, err
	}
	var items []store.MediaItem
	for _, m := range msgs {
		if m.Platform != platform {
			continue
		}
		for i, a := range m.Attachments {
			if strings.HasPrefix(a.Ref, "{") {
				continue // already a rich (JSON) ref — nothing to recover
			}
			ref := rx.ReextractMediaRef([]byte(m.Raw), m.ChatID)
			if ref == "" {
				continue
			}
			items = append(items, store.MediaItem{
				ID: m.ID + "#" + strconv.Itoa(i), EventID: m.ID, ChatID: m.ChatID,
				Platform: m.Platform, Idx: i, Type: a.Type, MIME: a.MIME, Name: a.Name,
				FetchRef: ref, MsgTS: m.TS,
			})
		}
	}
	if len(items) == 0 {
		return 0, nil
	}
	if _, err := d.store.EnqueueMedia(ctx, items); err != nil {
		return 0, err
	}
	d.log.Info("media ref re-extract: refreshed rows for re-download", "platform", platform, "rows", len(items))
	return len(items), nil
}

// RevalidateMedia drives the #48 ciphertext re-validation sweep: re-check every stored
// image blob for a platform against its declared type and reset the undecrypted ones
// (raw .enc ciphertext stored before the content guard, via the no-mediaKey path-only
// download) to pending with a re-derived full ref, so they re-download + decrypt. Returns
// (checked, reset). Self-scoping — no external list needed.
func (d *Daemon) RevalidateMedia(ctx context.Context, platform string) (checked, decrypted, reset int, err error) {
	if d.media == nil {
		return 0, 0, 0, fmt.Errorf("media store disabled")
	}
	return d.media.RevalidateStored(ctx, platform)
}

// larkDiscoveryStopwords are the seed queries for blind Lark DM discovery (#31).
// search/v2/message REQUIRES a non-empty query, but discovery has no natural search
// term — so we union p2p hits across a few ultra-common words (most DMs contain at
// least one). Spans the languages the user actually messages in (en + zh).
var larkDiscoveryStopwords = []string{"the", "you", "ok", "thanks", "hi", "yes", "是", "我", "你", "好"}

// DiscoverLarkP2P bootstraps the user's Lark DMs (#31). Im.Chat.List returns only
// group chats, so a DM with no recent live message stays invisible to backfill/poll.
// It finds p2p chats via the connector's message search, then backfills each newly-
// found DM's history. Best-effort + idempotent (the store dedups; re-runs are cheap).
//
// ASYNC: the search + N×Im.Message.Get + per-chat backfills can take minutes on a
// broad query (a sync endpoint timed the caller out at 6+ min), so this LAUNCHES the
// run in the background and returns queued=true immediately; the discovered-count is
// logged. A run already in flight → queued=false (the mutex guards overlap). query
// overrides the stopword seed set (single-query probe); maxSeeds>0 seeds the sender
// filter (from_ids) from known Lark identities, capped — the bounded, targeted path.
func (d *Daemon) DiscoverLarkP2P(_ context.Context, query string, maxSeeds int) (bool, error) {
	if _, ok := d.connectors[event.PlatformLark].(*lark.Connector); !ok {
		return false, nil // no Lark connector wired
	}
	if !d.larkDiscoverMu.TryLock() {
		return false, nil // a run is already in flight
	}
	queries := larkDiscoveryStopwords
	if query != "" {
		queries = []string{query} // single-query probe override
	}
	go func() {
		defer d.larkDiscoverMu.Unlock()
		// Detached context: outlive the request, bounded by the connector's per-run
		// caps (page cap + maxResolvePerRun) rather than the HTTP deadline.
		d.runLarkDiscovery(context.Background(), queries, maxSeeds)
	}()
	return true, nil
}

// rotateLarkSeedWindow returns the next maxSeeds-slice of all (deterministic order),
// starting at a persisted offset that advances each call — so over successive runs the
// whole identity set is swept rather than just the first window (#31, per team-lead).
// truncated reports whether all didn't fit in one window. Best-effort: an offset-store
// miss simply starts at 0.
func (d *Daemon) rotateLarkSeedWindow(ctx context.Context, all []string, maxSeeds int) (window []string, truncated bool) {
	if len(all) <= maxSeeds {
		return all, false
	}
	const offKey = "lark_discover_offset"
	offset := 0
	if v, err := d.store.GetMeta(ctx, offKey); err == nil && v != "" {
		if n, perr := strconv.Atoi(v); perr == nil && n > 0 {
			offset = n % len(all)
		}
	}
	window = make([]string, 0, maxSeeds)
	for i := range maxSeeds {
		window = append(window, all[(offset+i)%len(all)])
	}
	next := (offset + maxSeeds) % len(all)
	if err := d.store.SetMeta(ctx, offKey, strconv.Itoa(next)); err != nil {
		d.log.Warn("lark p2p discovery: persist seed offset", "err", err)
	}
	return window, true
}

// runLarkDiscovery is the synchronous discovery worker (see DiscoverLarkP2P). It seeds
// from_ids from known Lark identities, asks the connector to find p2p chats across the
// query set, and backfills each newly-found DM. Returns the count newly discovered.
func (d *Daemon) runLarkDiscovery(ctx context.Context, queries []string, maxSeeds int) int {
	lc, ok := d.connectors[event.PlatformLark].(*lark.Connector)
	if !ok {
		return 0
	}
	var openIDs []string
	truncated := false
	if maxSeeds > 0 {
		senders, err := d.store.DistinctSenders(ctx)
		if err != nil {
			d.log.Warn("lark p2p discovery: seed senders", "err", err)
			return 0
		}
		seen := map[string]bool{}
		var all []string
		for _, s := range senders {
			if s.Platform != event.PlatformLark || s.IsSelf || !strings.HasPrefix(s.UserID, "ou_") {
				continue
			}
			if seen[s.UserID] {
				continue
			}
			seen[s.UserID] = true
			all = append(all, s.UserID)
		}
		if len(all) == 0 {
			d.log.Info("lark p2p discovery", "new_chats", 0, "note", "no lark identities to seed from")
			return 0
		}
		// Rotate the seed WINDOW across runs (per team-lead): when there are more
		// identities than the per-run cap, successive daily runs sweep a different
		// slice (offset persisted in the store), so coverage eventually reaches
		// everyone instead of re-sweeping the first N each time.
		openIDs, truncated = d.rotateLarkSeedWindow(ctx, all, maxSeeds)
	}
	newChats, derr := lc.DiscoverP2PChats(ctx, openIDs, queries)
	if derr != nil {
		d.log.Warn("lark p2p discovery", "err", derr)
		// fall through — backfill whatever was discovered before the error
	}
	for _, raw := range newChats {
		if berr := d.Backfill(ctx, event.PlatformLark+":"+raw, time.Time{}); berr != nil {
			d.log.Warn("lark p2p backfill", "chat", raw, "err", berr)
		}
	}
	d.log.Info("lark p2p discovery", "queries", len(queries), "seeds", len(openIDs), "seeds_truncated", truncated, "new_chats", len(newChats))
	return len(newChats)
}

// backfillInterChatDelay paces the sweep between chats so a burst of getHistory /
// listMessages calls can't trip a rate-limit / flood-wait (#13). Validation showed 3s
// still tripped Telegram FLOOD_WAIT, so it's widened to 8s.
const backfillInterChatDelay = 8 * time.Second

// backfillChatTimeout bounds one chat's backfill so a single hung/flood-waiting chat
// can't stall the whole sweep (and hold the run mutex) indefinitely (#13 fix).
const backfillChatTimeout = 2 * time.Minute

// activeSweepPlatforms are the platforms the all-platform sweep + daily timer cover,
// in priority order (#13). Gmail is excluded (token-gated, #26); IG/Matrix history
// isn't chat-paged the same way. A platform with no connector simply yields no
// candidates and is skipped.
var activeSweepPlatforms = []string{event.PlatformTelegram, event.PlatformLark, event.PlatformWhatsApp}

// backfillInterPlatformGap spaces the per-platform batches within an all-platform run
// so the burst is staggered across platforms (#13 timer knob).
const backfillInterPlatformGap = 30 * time.Second

// backfillSweepCooldown is the minimum gap between sweep runs for a platform (#13).
// Telegram FLOOD_WAIT is ACCOUNT-GLOBAL, so a hammered on-demand endpoint trips a
// sustained throttle; this makes the endpoint un-hammerable. The daily timer (≫ this)
// is unaffected; only rapid manual re-fires are rejected.
const backfillSweepCooldown = 30 * time.Minute

// isFloodWait reports whether err is a Telegram FLOOD_WAIT (account-GLOBAL throttle).
// On a flood-wait the sweep must ABORT the whole run — every remaining chat would hit
// the same wall and deepen the penalty — and retry next cycle, not per-chat-retry.
var floodWaitRe = regexp.MustCompile(`FLOOD_WAIT \((\d+)\)`)

func floodWaitSeconds(err error) int {
	if err == nil {
		return 0
	}
	m := floodWaitRe.FindStringSubmatch(err.Error())
	if len(m) != 2 {
		return 0
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

// SweepBackfill triggers a bounded, salience-ordered full-history backfill of chats
// that have never been swept (#13). The systemic gap: d.Backfill is on-demand, so
// non-active chats only ever got live messages — chats_known >> chats_with_messages.
// This picks the top maxChats UNswept candidates (importance important→normal→unscored,
// then recency; explicit 'low' is gated), backfills each with an inter-chat delay for
// flood-wait safety, and stamps backfilled_at so it converges (each chat once).
//
// ASYNC: the run can take minutes, so it launches in the background and returns the
// PREFLIGHT immediately — {queued, candidates, low_gated, low_sample} — so the caller
// sees what will run + what 'low' contains without blocking. queued=false if nothing
// to sweep or a sweep is already in flight (mutex-guarded).
func (d *Daemon) SweepBackfill(_ context.Context, platform string, maxChats int) (api.SweepResult, error) {
	if maxChats <= 0 {
		maxChats = 10
	}
	bg := context.Background()
	// platform="" or "all" → every active platform, each its own capped batch (the
	// daily-timer path). A specific platform → just that one (on-demand validation).
	platforms := []string{platform}
	allMode := platform == "" || platform == "all"
	if allMode {
		platforms = activeSweepPlatforms
	}

	// Preflight (sync, cheap): aggregate candidate + gated-low counts across the target
	// platforms so the caller sees what will run without blocking on the backfill.
	res := api.SweepResult{}
	for _, p := range platforms {
		cands, err := d.store.BackfillCandidates(bg, p, maxChats)
		if err != nil {
			return api.SweepResult{}, err
		}
		res.Candidates += len(cands)
		if n, sample, lerr := d.store.LowTierBackfillStats(bg, p, 5); lerr == nil {
			res.LowGated += n
			res.LowSample = append(res.LowSample, sample...)
		}
	}
	if res.Candidates == 0 {
		return res, nil // converged (or nothing known) — nothing to do
	}

	// Cooldown guard (per target set): reject a too-soon re-fire so the endpoint can't
	// be hammered into an account-global throttle. The daily timer's cadence is far longer.
	cdKey := "backfill_sweep_last_" + platform
	if allMode {
		cdKey = "backfill_sweep_last_all"
	}
	if last, _ := d.store.GetMeta(bg, cdKey); last != "" {
		if t, perr := time.Parse(time.RFC3339Nano, last); perr == nil && time.Since(t) < backfillSweepCooldown {
			d.log.Info("backfill sweep: cooldown — skipping", "platform", platform, "since", time.Since(t).Round(time.Second))
			return res, nil
		}
	}
	if !d.sweepMu.TryLock() {
		return res, nil // a sweep is already running; queued stays false
	}
	_ = d.store.SetMeta(bg, cdKey, time.Now().UTC().Format(time.RFC3339Nano))
	res.Queued = true
	go func() {
		defer d.sweepMu.Unlock() // always release — even on panic/partial, so the next run can start
		for i, p := range platforms {
			if i > 0 {
				time.Sleep(backfillInterPlatformGap) // stagger platform batches
			}
			before, known := d.platformIngested(bg, p)
			swept, failed, flooded := d.runSweepForPlatform(bg, p, maxChats)
			after, knownAfter := d.platformIngested(bg, p)
			if knownAfter > known {
				known = knownAfter
			}
			d.log.Info("backfill sweep done", "platform", p, "swept", swept, "failed", failed, "flooded", flooded,
				"ingested_before", before, "ingested_after", after)
			// Record the run for convergence observability (phase-2). Best-effort.
			if rerr := d.store.RecordBackfillRun(bg, store.BackfillRun{
				TS: time.Now().UTC(), Platform: p, Swept: swept, Failed: failed, Flooded: flooded,
				IngestedBefore: before, IngestedAfter: after, ChatsKnown: known,
			}); rerr != nil {
				d.log.Warn("backfill sweep: record run", "platform", p, "err", rerr)
			}
			// A flood-wait is per-account/platform; it aborts THAT platform's batch but
			// the others (different accounts) still run.
		}
	}()
	return res, nil
}

// runSweepForPlatform sweeps up to maxChats unswept candidates for ONE platform,
// synchronously (the caller holds the run mutex). Each chat is backfilled under a
// per-chat timeout + paced by the inter-chat delay, then stamped backfilled_at so it
// converges. A non-flood per-chat failure is skipped (left unmarked → retried next
// run); an account-global FLOOD_WAIT aborts THIS platform's batch (flooded=true) so we
// don't march every chat into the same throttle.
func (d *Daemon) runSweepForPlatform(ctx context.Context, platform string, maxChats int) (swept, failed int, flooded bool) {
	cands, err := d.store.BackfillCandidates(ctx, platform, maxChats)
	if err != nil {
		d.log.Warn("backfill sweep: candidates", "platform", platform, "err", err)
		return 0, 0, false
	}
	for _, c := range cands {
		if ctx.Err() != nil {
			return swept, failed, flooded
		}
		if berr := d.backfillOneChat(ctx, c.ChatID); berr != nil {
			if wait := floodWaitSeconds(berr); wait > 0 {
				d.log.Warn("backfill sweep aborted: flood-wait, retry next cycle",
					"platform", platform, "chat", c.ChatID, "wait_seconds", wait, "swept", swept)
				return swept, failed, true
			}
			d.log.Warn("backfill sweep: chat failed", "chat", c.ChatID, "err", berr)
			failed++
			time.Sleep(backfillInterChatDelay)
			continue
		}
		if merr := d.store.MarkChatBackfilled(ctx, c.ChatID, time.Now().UTC()); merr != nil {
			d.log.Warn("backfill sweep: mark", "chat", c.ChatID, "err", merr)
		}
		swept++
		time.Sleep(backfillInterChatDelay)
	}
	return swept, failed, flooded
}

// backfillOneChat backfills a single chat under a per-chat timeout, so one hung or
// flood-waiting chat can't stall the sweep (and hold the run mutex) indefinitely (#13).
// It does NOT retry — a FLOOD_WAIT is account-global and handled by aborting the whole
// run (see SweepBackfill); a per-chat failure is just skipped.
func (d *Daemon) backfillOneChat(parent context.Context, chatID string) error {
	ctx, cancel := context.WithTimeout(parent, backfillChatTimeout)
	defer cancel()
	return d.Backfill(ctx, chatID, time.Time{})
}

// --- People review flow (human-in-the-loop canonical-persona mapping) ---
//
// The projector owns all the merge/rename logic; the daemon just exposes its
// review API over HTTP so the Telegram bot can render buttons and route taps.

// PendingProposals lists the fuzzy-merge proposals awaiting human approval,
// mapped onto the api DTO so the api boundary stays decoupled from gbrainfeed.
func (d *Daemon) PendingProposals(ctx context.Context, limit, offset int) ([]api.ProposalDTO, error) {
	// PendingProposals (not Proposals().Pending()) filters out pairs already
	// merged by a strong-signal auto-merge + self-heals the store — so the bot
	// never shows stale already-merged proposals (people-layer @b151b74).
	pending := d.proj.PendingProposals()
	// Page the (potentially hundreds-long) list so a caller/agent gets a
	// digestible batch and can offset for the next page.
	if limit <= 0 {
		limit = 15
	}
	if offset < 0 {
		offset = 0
	}
	if offset > len(pending) {
		offset = len(pending)
	}
	end := offset + limit
	if end > len(pending) {
		end = len(pending)
	}
	pending = pending[offset:end]
	out := make([]api.ProposalDTO, 0, len(pending))
	for _, p := range pending {
		out = append(out, api.ProposalDTO{
			ID:     p.ID,
			A:      p.A,
			B:      p.B,
			AName:  p.AName,
			BName:  p.BName,
			Score:  p.Score,
			Reason: p.Reason,
			Status: string(p.Status),
		})
	}
	return out, nil
}

// ResolveProposal applies the user's decision ("approve"→merge, "reject"→not
// same) to a pending proposal by its stable id. The projector looks up the pair
// and dispatches; unknown id / bad decision returns an error.
func (d *Daemon) ResolveProposal(ctx context.Context, id, decision string) (*api.ResolveOutcome, error) {
	out, err := d.proj.ResolveProposalOutcome(ctx, id, decision)
	if err != nil || out == nil {
		return nil, err
	}
	return &api.ResolveOutcome{
		SurvivorID:    out.SurvivorID,
		SurvivorName:  out.SurvivorName,
		AbsorbedID:    out.AbsorbedID,
		Identified:    out.Identified,
		AlreadyMerged: out.AlreadyMerged,
	}, nil
}

// RunDeterministicMerges applies the deterministic corroborated-exact-name merge
// policy over the graph + drains qualifying pending proposals (no LLM). dryRun
// only logs the would-merge candidates. Returns the count merged/would-merge.
func (d *Daemon) RunDeterministicMerges(ctx context.Context, dryRun bool) (int, error) {
	return d.proj.RunDeterministicMerges(ctx, dryRun)
}

// MergeCanonicals force-merges two operator-chosen canonical ids — the human-
// directed merge surface for pairs no auto-proposal covers (e.g. a name too
// ambiguous for the propose logic to surface). Routes through the SAME durable
// ApproveMerge path as a proposal-approve (ForceMerge + persistApproved +
// merged_into provenance #17), so it survives reproject. Idempotent (already-
// merged → AlreadyMerged=true). Returns the survivor outcome.
func (d *Daemon) MergeCanonicals(ctx context.Context, a, b string) (*api.ResolveOutcome, error) {
	out, err := d.proj.ApproveMerge(ctx, a, b)
	if err != nil {
		return nil, err
	}
	return &api.ResolveOutcome{
		SurvivorID:    out.SurvivorID,
		SurvivorName:  out.SurvivorName,
		AbsorbedID:    out.AbsorbedID,
		Identified:    out.Identified,
		AlreadyMerged: out.AlreadyMerged,
	}, nil
}

// RenameCanonical applies a human-confirmed name to a canonical person.
func (d *Daemon) RenameCanonical(ctx context.Context, canonID, name string) error {
	return d.proj.RenameCanonical(ctx, canonID, name)
}

// BackfillSenderNames fills blank message sender_name/sender_handle for a platform's
// existing rows from the people-graph (#54 people-layer half): each distinct blank
// sender_id is resolved through the resolver to its canonical name + that identity's
// handle, then written back via the blank-guarded store setter. Idempotent (only fills
// blanks; safe to re-run). Sender_ids that don't resolve to a named person are left
// blank and counted as unresolved.
func (d *Daemon) BackfillSenderNames(ctx context.Context, platform string) (*api.BackfillResult, error) {
	ids, err := d.store.BlankSenderIDs(ctx, platform)
	if err != nil {
		return nil, err
	}
	res := &api.BackfillResult{Platform: platform, Scanned: len(ids)}
	for _, id := range ids {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		name, handle, ok := d.proj.ResolveIdentityName(platform, id)
		if !ok {
			res.Unresolved++
			continue
		}
		n, e := d.store.SetSenderNameBySender(ctx, platform, id, name, handle)
		if e != nil {
			d.log.Warn("backfill sender_name: setter failed", "platform", platform, "sender_id", id, "err", e)
			res.Unresolved++
			continue
		}
		if n > 0 {
			res.Backfilled++
		}
	}
	d.log.Info("sender-name backfill complete", "platform", platform, "scanned", res.Scanned, "backfilled", res.Backfilled, "unresolved", res.Unresolved)
	return res, nil
}

// --- Media store retrieval ---

func (d *Daemon) mediaMeta(row store.MediaRow, path string) api.MediaMeta {
	m := api.MediaMeta{
		ID: row.ID, Type: row.Type, MIME: row.MIME, Name: row.Name,
		Size: row.Size, Stored: row.Stored, LocalPath: path,
	}
	if d.addr != "" {
		m.HTTPURL = "http://" + d.addr + "/media/" + url.PathEscape(row.ID)
	}
	return m
}

// ReprojectMedia retro-enqueues the historical media backlog: it scans the log
// for attachment-bearing events and runs them through the same OnIngest enqueue
// path the live pipeline uses (idempotent — already-known attachments are
// untouched). Use it once after enabling the media store so the worker downloads
// existing important-chat media, not just go-forward. Returns the message count.
// chatID scopes the reproject to a single chat (bounded rollout for the
// acceptance proof); empty = the whole backlog.
func (d *Daemon) ReprojectMedia(ctx context.Context, chatID string) (int, error) {
	if d.media == nil {
		return 0, fmt.Errorf("media store disabled")
	}
	msgs, err := d.store.MessagesWithAttachments(ctx, chatID)
	if err != nil {
		return 0, err
	}
	for _, m := range msgs {
		d.media.OnIngest(m)
	}
	d.log.Info("media reproject enqueued attachment-bearing events", "count", len(msgs), "chat", chatID)
	return len(msgs), nil
}

// MediaInfo returns an attachment's metadata + local blob path, re-fetching
// ref-only media on demand.
func (d *Daemon) MediaInfo(ctx context.Context, id string) (api.MediaMeta, error) {
	if d.media == nil {
		return api.MediaMeta{}, fmt.Errorf("media store disabled")
	}
	row, path, err := d.media.LocalPath(ctx, id)
	if err != nil {
		return api.MediaMeta{}, err
	}
	return d.mediaMeta(row, path), nil
}

// MediaStream returns an attachment's bytes (re-fetching ref-only on demand).
func (d *Daemon) MediaStream(ctx context.Context, id string) (io.ReadCloser, api.MediaMeta, error) {
	if d.media == nil {
		return nil, api.MediaMeta{}, fmt.Errorf("media store disabled")
	}
	row, rc, err := d.media.Get(ctx, id)
	if err != nil {
		return nil, api.MediaMeta{}, err
	}
	return rc, d.mediaMeta(row, ""), nil
}

// refreshChatMetadata polls every connector that implements ChatMetadataProvider
// and upserts the names + org signals into the store's chat projection. Best
// effort — a connector that errors is logged and skipped.
func (d *Daemon) refreshChatMetadata(ctx context.Context) {
	for name, c := range d.connectors {
		cmp, ok := c.(connectors.ChatMetadataProvider)
		if !ok {
			continue
		}
		metas, err := cmp.ChatMetadata(ctx)
		if err != nil {
			d.log.Warn("chat metadata refresh failed", "platform", name, "err", err)
			continue
		}
		n := 0
		for _, m := range metas {
			if err := d.store.UpsertChatMeta(ctx, store.ChatMeta{
				ChatID: m.ChatID, Platform: name, Title: m.Title,
				Archived: m.Archived, Muted: m.Muted, MutedUntil: m.MutedUntil,
				Pinned: m.Pinned, Labels: m.Labels,
			}); err != nil {
				d.log.Warn("upsert chat meta failed", "platform", name, "chat", m.ChatID, "err", err)
				continue
			}
			n++
		}
		if n > 0 {
			d.log.Info("chat metadata refreshed", "platform", name, "chats", n)
		}
	}
	// Recompute the uniform importance tier across all chats from the freshest
	// curation signals + engagement/structural stats.
	if n, err := d.store.RecomputeImportance(ctx); err != nil {
		d.log.Warn("recompute importance failed", "err", err)
	} else if n > 0 {
		d.log.Debug("importance recomputed", "chats", n)
	}
}

// reapOrphanServes kills ABANDONED `gbrain serve` processes — ones whose parent
// has died (ppid==1) and that have lingered past a grace period. gbrain is
// PGLite (single-writer file lock); each Hermes session + our MCP client spawns
// a serve, and crashed/abandoned ones hold the lock forever → "Timed out
// waiting for PGLite lock" for everyone. This self-heals that without touching
// any live serve. CONSERVATIVE: only ppid==1 (truly orphaned — never a serve
// with a live parent, so our own and Hermes's live serves are never touched).
func (d *Daemon) reapOrphanServes() {
	out, err := exec.Command("ps", "-eo", "pid=,ppid=,etimes=,args=").Output()
	if err != nil {
		return // ps unavailable (non-Linux/dev) — silently skip
	}
	const graceSecs = 600 // only reap orphans older than 10 min
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) < 4 {
			continue
		}
		ppid, _ := strconv.Atoi(f[1])
		if ppid != 1 {
			continue // has a live parent → never touch
		}
		etimes, _ := strconv.Atoi(f[2])
		if etimes < graceSecs {
			continue
		}
		rest := strings.Join(f[3:], " ")
		if !strings.Contains(rest, "gbrain") || !strings.Contains(rest, "serve") {
			continue
		}
		pid, _ := strconv.Atoi(f[0])
		if p, e := os.FindProcess(pid); e == nil {
			if e := p.Kill(); e == nil {
				d.log.Info("reaped orphaned gbrain serve", "pid", pid, "age_s", etimes)
			}
		}
	}
}

// toGbrainContacts maps connector contacts onto the projector's DTO.
func toGbrainContacts(cs []connectors.Contact) []gbrainfeed.Contact {
	out := make([]gbrainfeed.Contact, 0, len(cs))
	for _, c := range cs {
		out = append(out, gbrainfeed.Contact{
			UserID: c.UserID, Phone: c.Phone, Email: c.Email, Handle: c.Handle,
			FullName: c.FullName, FirstName: c.FirstName, LastName: c.LastName,
			PushName: c.PushName, BusinessName: c.BusinessName, IsSelf: c.IsSelf,
		})
	}
	return out
}

// refreshContacts projects every connector's full address book into the people
// graph (coverage: people the user knows get a named person even if their
// message history never backfilled). Best-effort; a connector that can't
// enumerate contacts or errors is skipped.
func (d *Daemon) refreshContacts(ctx context.Context) {
	for name, c := range d.connectors {
		cp, ok := c.(connectors.ContactsProvider)
		if !ok {
			continue
		}
		contacts, err := cp.Contacts(ctx)
		if err != nil {
			d.log.Warn("contacts refresh failed", "platform", name, "err", err)
			continue
		}
		d.proj.IngestContacts(name, toGbrainContacts(contacts))
		d.log.Info("contacts projected", "platform", name, "count", len(contacts))
	}
}

func (d *Daemon) connectorFor(chatID string) (connectors.Connector, error) {
	platform := platformOf(chatID)
	c, ok := d.connectors[platform]
	if !ok {
		// Wrap the sentinel so the API layer classifies this as a caller fault (400)
		// — unknown/unsupported platform or malformed chat_id — not an upstream 502.
		return nil, fmt.Errorf("%w %q (chat %q)", connectors.ErrNoConnector, platform, chatID)
	}
	return c, nil
}

// Run starts the projector, pipeline, every connector's live Connect, and the
// HTTP API server. It blocks until ctx is cancelled.
func (d *Daemon) Run(ctx context.Context, addr string) error {
	if addr == "" {
		addr = api.DefaultAddr
	}
	if err := api.ValidateListenAddr(addr); err != nil {
		return err
	}
	if len(d.apiPolicy.AdminToken) < 32 {
		return fmt.Errorf("API administrative credential required")
	}
	d.addr = addr
	d.runCtx = ctx // long-lived; async HTTP-triggered work (honcho backfill) runs on this
	d.proj.Start(ctx)
	defer d.proj.Close()

	// Media worker: gated downloads + eviction, on its own goroutine off the
	// ingest path (a stalled download can't back up live ingest).
	if d.media != nil {
		go d.media.Run(ctx)
		d.log.Info("media store worker started")
	}

	// Salience tick worker (#59/#15): periodically recomputes tiers so they SELF-HEAL
	// after an interrupted startup reproject (the only other periodic RecomputeSalience
	// lives in the matcher worker, which runs only when the shelved #19 matcher is
	// enabled) + stay FRESH between reprojects. Best-effort + guarded (salienceMu). It
	// only re-scores from the resolver — it does NOT rebuild idToCanon, so the Honcho
	// gate's correctness still rests on the reprojectDone guarantee above, not the tick.
	go d.runSalienceWorker(ctx)

	// Identity-matcher worker: off-ingest LLM adjudication sweep (#19), propose-only.
	// Started only when enabled so the binary can deploy dormant.
	if d.matchEnabled {
		go d.runMatcherWorker(ctx)
		d.log.Info("identity-matcher worker started (propose-only)")
	}

	// Honcho user-model ingestion worker (#57-adjacent): drains the sink's queue + POSTs
	// off the ingest hot path. Started only when the flag created the sink.
	if d.honcho != nil {
		// Gate must read a FULLY-BUILT resolver+salience, not race the async startup
		// Reproject. Wait for reprojectDone (which Reproject closes post-WaitIdle+score),
		// then report the scored count. Avoids re-reprojecting (no double startup cost).
		d.honcho.recompute = func(wctx context.Context) (int, error) {
			select {
			case <-d.reprojectDone:
				return len(d.proj.SalienceRanking()), nil
			case <-wctx.Done():
				return 0, wctx.Err()
			}
		}
		go d.honcho.run(ctx)
		d.log.Info("honcho ingestion worker started")
	}

	// Startup reproject: rebuild the FULL identity graph from the durable store so
	// the resolver isn't sparse after a restart. A sparse resolver degrades
	// everything that reads canonicals — #19 candidate-gen (misses inactive
	// contacts), #15 salience (scores a partial network), people queries, AND the
	// approved-merge replay (LoadApprovedMerges defers merges whose canonical hasn't
	// re-ingested). Idempotent: Observe folds on identityKey, ForceMerge/redirect is
	// idempotent, and putIfChanged content-hashes gbrain writes — so steady-state is
	// a no-op and the only cost is a bounded one-time cold page re-commit per boot.
	// Async so boot isn't blocked; the periodic-tick LoadApprovedMerges mops up the
	// approved merges as this queue drains (deferred→0 within a few ticks).
	go func() {
		defer close(d.reprojectDone) // signal consumers the resolver+salience are built
		if n, err := d.Reproject(ctx); err != nil && ctx.Err() == nil {
			d.log.Warn("startup reproject failed", "err", err)
		} else if err == nil {
			d.log.Info("startup reproject complete", "identities", n)
		}
	}()

	// (Periodic salience-recompute tick = #59, owned by people-layer as a separate change
	// on the #15 salience path — intentionally NOT wired here, to avoid two implementations
	// colliding. The Honcho gate's correctness rests on the reprojectDone guarantee above,
	// not on a tick.)

	// Pipeline drains the shared events channel (live + backfill).
	pipeDone := make(chan struct{})
	go func() {
		defer close(pipeDone)
		_ = d.pipe.Run(ctx, d.events)
	}()

	// Drive auth (writing QR PNGs to disk as needed) then start each connector's
	// live stream into the shared channel. connWG tracks the connector goroutines so
	// shutdown can DRAIN them before closing the store (#38).
	var connWG sync.WaitGroup
	for name, c := range d.connectors {
		c := c
		name := name
		connWG.Add(1)
		go func() {
			defer connWG.Done()
			defer func() {
				if r := recover(); r != nil {
					d.log.Error("connector goroutine panicked (recovered)", "platform", name, "panic", r)
				}
			}()
			if err := d.driveLogin(ctx, c); err != nil {
				d.log.Error("connector auth failed", "platform", name, "err", err)
				return
			}
			d.log.Info("connector starting", "platform", name)
			if err := c.Connect(ctx, d.events); err != nil && ctx.Err() == nil {
				d.log.Error("connector exited", "platform", name, "err", err)
			}
		}()
	}

	// Periodically refresh chat metadata (names + org signals) from connectors
	// that provide it. First pass shortly after startup (let connectors auth),
	// then on an interval. Cheap + best-effort.
	// metaDone closes when the chat-metadata refresher has fully returned — shutdown
	// waits on it before store.Close() so an in-flight UpsertChatMeta batch (the meta
	// connector's chat-meta) can't race the DB close ("database is closed", #38).
	metaDone := make(chan struct{})
	go func() {
		defer close(metaDone)
		t := time.NewTimer(20 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				d.reapOrphanServes() // self-heal abandoned gbrain serves (PGLite lock)
				d.refreshChatMetadata(ctx)
				d.refreshContacts(ctx)
				// Re-apply confirmed merges (human-approved + #19 auto-merge) on top of
				// the firehose-rebuilt resolver. The resolver only re-derives STRONG-
				// signal (phone/email) merges from the log, so without this replay every
				// restart silently un-does approved merges. Idempotent + deferred-safe:
				// pairs whose canonical hasn't rebuilt yet are retried next tick, so this
				// self-heals within a tick as connectors' history-sync repopulates the
				// graph (runs regardless of the matcher flag — the live unit needs it too).
				d.proj.LoadApprovedMerges(ctx)
				t.Reset(10 * time.Minute)
			}
		}
	}()

	// HTTP API.
	srv := &http.Server{Addr: addr, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, Handler: api.SecureHandler(api.NewServer(d, d.hub).WithVersion(d.version).Handler(), d.apiPolicy)}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	d.log.Info("switchboardd listening", "addr", addr, "connectors", len(d.connectors), "version", d.version)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	// Graceful shutdown ordering (#38): on ctx cancel, DRAIN the writers before closing
	// the store so in-flight UpsertChatMeta/Append can't race store.Close() ("database
	// is closed"). Order: connectors stop emitting + finish → the chat-meta refresher
	// finishes its batch → the pipeline drains queued events → store.Close() is LAST.
	// Each wait is timeout-bounded so a wedged connector can't hang shutdown forever.
	d.log.Info("shutdown: draining connectors → chat-meta → pipeline → projector before store close")
	shutdownStart := time.Now()
	drainWithTimeout(connWG.Wait, 10*time.Second, d.log, "connectors")
	select {
	case <-metaDone:
	case <-time.After(10 * time.Second):
		d.log.Warn("shutdown: chat-meta refresher drain timed out")
	}
	<-pipeDone
	// Drain the projector's in-flight OnIngest queue too before closing the store (#38).
	// The run ctx is already cancelled, so use a FRESH bounded ctx (WaitIdle is ctx-aware
	// and would return immediately on the cancelled run ctx). pipeDone above means no NEW
	// OnIngest enqueues, so this just lets the queued ones settle. Belt-and-suspenders:
	// the projector writes gbrain (not this SQLite store), but draining it here keeps
	// shutdown clean + deterministic rather than relying on the deferred proj.Close().
	projCtx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := d.proj.WaitIdle(projCtx); err != nil {
		d.log.Warn("shutdown: projector drain (WaitIdle) incomplete", "err", err)
	}
	pcancel()
	// Explicit visible confirmation the clean drain happened (the absence of "database is
	// closed" was the only prior signal). Future restarts show "drained ... in Xms".
	d.log.Info("shutdown: drained connectors/pipeline/projector — closing store",
		"elapsed", time.Since(shutdownStart).Round(time.Millisecond))
	return d.store.Close()
}

// drainWithTimeout runs wait (e.g. a WaitGroup.Wait) but gives up after d, logging a
// warning — so one wedged goroutine can't hang daemon shutdown indefinitely (#38).
func drainWithTimeout(wait func(), d time.Duration, log *slog.Logger, what string) {
	done := make(chan struct{})
	go func() { wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		log.Warn("shutdown: drain timed out", "what", what)
	}
}

// driveLogin authenticates a connector before its live stream starts. If the
// connector is already paired it returns immediately. Otherwise it kicks off
// Login(), writes any QR PNG to <qrDir>/<platform>-qr.png, and polls Status —
// rewriting the QR as the code rotates — until the connector reports Connected
// or ctx is cancelled. Interactive (non-QR) auth surfaces its next-step Detail
// in the logs for the human-in-the-loop step.
func (d *Daemon) driveLogin(ctx context.Context, c connectors.Connector) error {
	if st, err := c.Status(ctx); err == nil && st.Connected {
		return nil
	}
	st, err := c.Login(ctx)
	if err != nil {
		return err
	}
	if st.Connected {
		return nil
	}
	// No out-of-band QR pairing → the connector authenticates INSIDE Connect()
	// (Telegram's interactive phone/code/2FA flow reads from AuthSubmit; Lark
	// refreshes its token during Connect). Don't poll-wait here — that would
	// deadlock, since Connect() is what flips the connector to connected. Return
	// so the caller runs Connect(); interactive inputs arrive via AuthSubmit.
	if len(st.QRPNG) == 0 {
		if st.Detail != "" {
			d.log.Info("connector authenticates during Connect", "platform", c.Name(), "detail", st.Detail)
		}
		return nil
	}
	// QR pairing in progress (WhatsApp): persist the rotating QR and poll until
	// the background pairing flips the connector to connected, then Connect().
	d.writeQR(c.Name(), st.QRPNG)
	if st.Detail != "" {
		d.log.Info("connector needs auth", "platform", c.Name(), "detail", st.Detail)
	}

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	lastQR := append([]byte(nil), st.QRPNG...)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			st, err := c.Status(ctx)
			if err != nil {
				continue
			}
			if st.Connected {
				d.log.Info("connector authenticated", "platform", c.Name())
				return nil
			}
			if len(st.QRPNG) > 0 && !bytes.Equal(st.QRPNG, lastQR) {
				d.writeQR(c.Name(), st.QRPNG)
				lastQR = append(lastQR[:0], st.QRPNG...)
			}
		}
	}
}

// writeQR persists a QR PNG to <qrDir>/<platform>-qr.png (no-op if qrDir unset
// or png empty). The lead scp's this file and pushes it to the user's Telegram.
func (d *Daemon) writeQR(platform string, png []byte) {
	if d.qrDir == "" || len(png) == 0 {
		return
	}
	if err := privatefs.EnsureDir(d.qrDir); err != nil {
		d.log.Error("qr dir create failed", "dir", d.qrDir, "err", err)
		return
	}
	path := filepath.Join(d.qrDir, platform+"-qr.png")
	if err := privatefs.WriteFile(path, png); err != nil {
		d.log.Error("qr write failed", "path", path, "err", err)
		return
	}
	d.log.Info("QR ready", "platform", platform, "path", path)
}

func platformOf(chatID string) string {
	for i := 0; i < len(chatID); i++ {
		if chatID[i] == ':' {
			return chatID[:i]
		}
	}
	return ""
}
