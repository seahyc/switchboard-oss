package gbrainfeed

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
)

// Projector consumes the message firehose (as a pipeline.Sink) and maintains a
// NAMED, CANONICAL people graph in gbrain. It runs gbrain writes on a background
// worker so it NEVER blocks live ingestion: OnIngest enqueues and returns; if the
// queue is full the event is dropped (gbrain is a best-effort projection
// rebuildable from the log).
//
// The graph has two page tiers:
//   - per-platform identity pages (person-<platform>-<id>) — one per platform
//     account, carrying that platform's raw signals; and
//   - canonical person pages (canon-…) — ONE per real human, the queryable
//     entity, aggregating every identity, the union of external ids, and the best
//     human name. Identity pages link `identity_of` → their canonical person.
//
// Merges are deterministic on shared phone/email (auto). Name-only matches are
// emitted as proposals for human approval via the review flow — never auto-merged.
type Projector struct {
	gb  Client
	log *slog.Logger

	// Non-lossy COALESCING work queue: identities are keyed by platform:userID in
	// a pending set, so a sender enqueued thousands of times during a history-sync
	// burst collapses to ONE entry instead of overflowing a channel and dropping.
	qmu     sync.Mutex
	pending map[string]identity
	closed  bool
	busy    bool // a batch is in-flight (set atomically with drain) — for WaitIdle
	notify  chan struct{}
	wg      sync.WaitGroup

	resolver  *Resolver
	proposals ProposalStore

	// concurrency bounds how many identities the worker projects in parallel. Each
	// process() spends ~1-2s in a gbrain put_page RPC (subprocess spawn), so a
	// SEQUENTIAL drain lets one platform's startup flood (e.g. gmail live+backfill)
	// monopolize the worker for hours and starve everything queued behind it. A
	// bounded pool overlaps that latency (~10x+ throughput) and lets each drained
	// batch clear fast so the next drain promptly picks up contacts/other platforms.
	concurrency int

	mu        sync.Mutex
	putHash   map[string]string   // slug -> last content HASH (skip redundant put_page); seeded from pageHashStore on startup so a restart's reproject doesn't re-write ~all unchanged pages (#62)
	linked    map[string]bool     // dedupe links already written
	enrichers map[string]Enricher // platform -> lazy alias-signal enricher
	// pageHashStore durably persists slug->content-hash so putHash survives a restart
	// (#62). nil → no persistence (in-memory only, the pre-#62 behavior).
	pageHashStore PageHashStore

	// slugLocks serialize same-slug canonical commits across the worker pool, so a
	// shared canon-* page (multiple identities merge into it) is never written
	// concurrently — preventing a stale snapshot from clobbering a newer merge.
	slugLocks [256]sync.Mutex

	// #19 LLM identity matcher (set via SetAdjudicator/SetIdentityContextProvider;
	// nil = disabled). autoMergeEnabled stays false (propose-only) until the lead
	// confirms the eval gate. salience (#15) orders the CPU adjudication batch.
	idCtxProvider    IdentityContextProvider
	adjudicator      idmatch.LLMAdjudicator
	matchCache       idmatch.AdjudicationCache
	salience         map[string]float64
	autoMergeEnabled bool

	// #15 salience (set via SetPersonStatsProvider; scored by RecomputeSalience).
	personStatsProvider PersonStatsProvider
	personSignals       []PersonSignals
	salWeights          *SalienceWeights
	salThresholds       *SalienceThresholds
}

// defaultConcurrency is the projector's default parallel-projection width. Kept
// conservative (gbrain is single-writer PGLite; same-slug commits are serialized
// via slugLocks, and the persistent transport bounds DB write concurrency). Tune
// via SetConcurrency before Start once a higher width is proven safe.
const defaultConcurrency = 8

// Aliases are the alias signals an enricher can recover for a sparse identity.
type Aliases struct {
	Email       string
	Phone       string
	Handle      string
	DisplayName string
}

// Enricher lazily recovers alias signals (email/phone/handle) for identities a
// connector emits sparse — e.g. Lark emits only open_id + display_name to keep
// the live socket fast, and the projector enriches on demand when it's about to
// register/merge an identity. Implementations should cache + batch. Keyed by the
// platform's user id.
type Enricher interface {
	Enrich(ctx context.Context, userIDs []string) (map[string]Aliases, error)
}

// identity is the distilled per-platform person extracted from a message sender
// or an address-book contact.
type identity struct {
	Platform    string
	UserID      string
	Names       NameSet
	Handle      string
	Phone       string
	Email       string
	IsSelf      bool
	FromContact bool // sourced from the address book (coverage), not just a sender
	// follow carries #49 follow-classify signals from a Sub=="follow" social_activity
	// event, so the node type/category is RE-DERIVED at projection time (durable-by-
	// construction; node_type is not persisted). nil for every non-follow identity.
	follow *FollowSignals
}

// NewProjector builds a Projector over a gbrain Client.
func NewProjector(gb Client, logger *slog.Logger) *Projector {
	if logger == nil {
		logger = slog.Default()
	}
	return &Projector{
		gb:          gb,
		log:         logger,
		pending:     map[string]identity{},
		notify:      make(chan struct{}, 1),
		resolver:    NewResolver(),
		proposals:   NewMemoryProposalStore(),
		concurrency: defaultConcurrency,
		putHash:     map[string]string{},
		linked:      map[string]bool{},
		enrichers:   map[string]Enricher{},
	}
}

// Proposals returns the review store so the bot/review flow can list and resolve
// pending fuzzy-merge proposals.
func (p *Projector) Proposals() ProposalStore { return p.proposals }

// SetProposalStore swaps the proposal store (e.g. a persistent one). Call before Start.
func (p *Projector) SetProposalStore(s ProposalStore) {
	if s != nil {
		p.proposals = s
	}
}

// SetConcurrency sets the parallel-projection width. Call before Start. Values
// < 1 are clamped to 1 (sequential).
func (p *Projector) SetConcurrency(n int) {
	if n < 1 {
		n = 1
	}
	p.concurrency = n
}

func identityKey(platform, userID string) string { return platform + ":" + userID }

// enqueue coalesces an identity into the pending set, MERGING with any existing
// pending version for the same key so a later blank-name message can't clobber an
// address-book name that arrived first (and vice-versa). Returns false if closed.
func (p *Projector) enqueue(id identity) bool {
	k := identityKey(id.Platform, id.UserID)
	p.qmu.Lock()
	defer p.qmu.Unlock()
	if p.closed {
		return false
	}
	if existing, ok := p.pending[k]; ok {
		id = mergeIdentity(existing, id)
	}
	p.pending[k] = id
	return true
}

// signal wakes the worker without blocking.
func (p *Projector) signal() {
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

// drain atomically removes and returns all pending identities.
func (p *Projector) drain() []identity {
	p.qmu.Lock()
	defer p.qmu.Unlock()
	if len(p.pending) == 0 {
		return nil
	}
	p.busy = true // mark in-flight atomically with emptying pending (WaitIdle race-free)
	out := make([]identity, 0, len(p.pending))
	for k, id := range p.pending {
		out = append(out, id)
		delete(p.pending, k)
	}
	return out
}

// requeue re-adds an identity for retry (no-op once closed).
func (p *Projector) requeue(id identity) {
	p.qmu.Lock()
	if !p.closed {
		// Don't clobber a fresher (e.g. contact-enriched) pending version.
		if _, ok := p.pending[identityKey(id.Platform, id.UserID)]; !ok {
			p.pending[identityKey(id.Platform, id.UserID)] = id
		}
	}
	p.qmu.Unlock()
}

// RegisterEnricher attaches a lazy alias-signal enricher for a platform.
func (p *Projector) RegisterEnricher(platform string, e Enricher) {
	if e != nil {
		p.enrichers[platform] = e
	}
}

// Start launches the background worker. Call Close to drain and stop.
func (p *Projector) Start(ctx context.Context) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			batch := p.drain()
			failed := p.processBatch(ctx, batch)
			if ctx.Err() != nil {
				return
			}
			if failed {
				// Some items errored + were requeued; back off once before the next
				// drain so a persistent gbrain outage doesn't hot-loop.
				select {
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Second):
				}
			}
			p.qmu.Lock()
			empty := len(p.pending) == 0
			closed := p.closed
			if empty {
				p.busy = false // queue drained + batch done → idle (for WaitIdle)
			}
			p.qmu.Unlock()
			if empty {
				if closed {
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-p.notify:
				}
			}
		}
	}()
}

// processBatch projects a drained batch with bounded parallelism. Each process()
// is independently safe for concurrent use — the resolver, page/link dedupe maps,
// and proposal store are all mutex-guarded — so the only effect of concurrency is
// that gbrain RPC latency overlaps. Failed items are requeued (thread-safe) and
// the method reports whether any failed so the caller can back off. Returns
// promptly on ctx cancellation.
func (p *Projector) processBatch(ctx context.Context, batch []identity) (failed bool) {
	if len(batch) == 0 {
		return false
	}
	// Batch-warm enricher caches BEFORE the per-identity fan-out. enrich() resolves
	// one id at a time; for a platform that emits senders sparse (Lark: open_id
	// only), that's one contact-API round-trip per sender — hundreds of calls that
	// rate-limit and fail. Pre-warming with ONE batched Enrich per platform (the
	// Enricher chunks ≤100 + caches) collapses ~N calls to ~N/100, so the
	// per-identity enrich() then hits a warm cache.
	p.prewarmEnrichers(ctx, batch)

	// Fairness: interleave by platform so one platform's flood (e.g. a gmail
	// live+backfill burst) can't pack the front of the batch and delay everyone
	// else — every platform makes progress within each drain.
	batch = interleaveByPlatform(batch)
	width := p.concurrency
	if width < 1 {
		width = 1
	}
	sem := make(chan struct{}, width)
	var wg sync.WaitGroup
	var mu sync.Mutex
loop:
	for _, id := range batch {
		if ctx.Err() != nil {
			break
		}
		id := id
		// Acquire a slot (or bail on cancellation BEFORE spawning, so every
		// goroutine that runs has a slot its deferred release will return).
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break loop
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if ctx.Err() != nil {
				return
			}
			if err := p.process(ctx, id); err != nil {
				p.log.Warn("gbrain projection failed, will retry", "platform", id.Platform, "user", id.UserID, "err", err)
				p.requeue(id)
				mu.Lock()
				failed = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return failed
}

// prewarmEnrichers issues ONE batched Enrich per platform for the batch's
// sparse identities (no phone/email yet), warming the enricher's cache so the
// subsequent per-identity enrich() calls resolve from cache instead of making a
// round-trip each. Best-effort + sequential (runs before the parallel fan-out).
func (p *Projector) prewarmEnrichers(ctx context.Context, batch []identity) {
	if len(p.enrichers) == 0 {
		return
	}
	byPlatform := map[string][]string{}
	seen := map[string]bool{}
	for _, id := range batch {
		if id.Phone != "" || id.Email != "" {
			continue // enrich() would skip these — already have a strong signal
		}
		if _, ok := p.enrichers[id.Platform]; !ok {
			continue
		}
		k := identityKey(id.Platform, id.UserID)
		if seen[k] {
			continue
		}
		seen[k] = true
		byPlatform[id.Platform] = append(byPlatform[id.Platform], id.UserID)
	}
	for plat, ids := range byPlatform {
		if _, err := p.enrichers[plat].Enrich(ctx, ids); err != nil {
			p.log.Warn("batch enrich prewarm failed", "platform", plat, "count", len(ids), "err", err)
		}
	}
}

// interleaveByPlatform reorders a batch round-robin across platforms so no single
// platform monopolizes the front of the queue. Deterministic (platforms sorted),
// stable within a platform.
func interleaveByPlatform(batch []identity) []identity {
	if len(batch) < 2 {
		return batch
	}
	groups := map[string][]identity{}
	var platforms []string
	for _, id := range batch {
		if _, ok := groups[id.Platform]; !ok {
			platforms = append(platforms, id.Platform)
		}
		groups[id.Platform] = append(groups[id.Platform], id)
	}
	if len(platforms) < 2 {
		return batch
	}
	sort.Strings(platforms)
	out := make([]identity, 0, len(batch))
	for i := 0; len(out) < len(batch); i++ {
		for _, pl := range platforms {
			g := groups[pl]
			if i < len(g) {
				out = append(out, g[i])
			}
		}
	}
	return out
}

// Close stops accepting new work and waits for the worker to drain what's queued.
func (p *Projector) Close() {
	p.qmu.Lock()
	p.closed = true
	p.qmu.Unlock()
	p.signal()
	p.wg.Wait()
}

// WaitIdle blocks until the projection queue is fully drained — no pending identities
// AND no batch in-flight — so a caller (e.g. Reproject) can score/reconcile the SETTLED
// graph instead of a partial one. OnIngest is async, so RecomputeSalience/
// RunDeterministicProposals/BackfillMergeLinks must WaitIdle first or they see a
// partial graph (#15 partial-salience bug). Ctx-bounded: returns ctx.Err() on
// cancel/timeout so a stuck worker can't hang the caller forever. Does NOT close the
// projector (unlike Close).
func (p *Projector) WaitIdle(ctx context.Context) error {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		p.qmu.Lock()
		idle := len(p.pending) == 0 && !p.busy
		p.qmu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// OnIngest implements pipeline.Sink. Non-blocking + COALESCING.
func (p *Projector) OnIngest(m event.Message) {
	s := m.Sender
	if s.PlatformUserID == "" {
		return
	}
	id := identity{
		Platform: m.Platform,
		UserID:   s.PlatformUserID,
		Names:    NameSet{Display: s.DisplayName, Username: s.Handle},
		Handle:   s.Handle,
		Phone:    normalizePhone(s.Phone),
		Email:    strings.ToLower(strings.TrimSpace(s.Email)),
		IsSelf:   s.IsSelf,
		follow:   followSignalsFromEvent(m),
	}
	if !p.enqueue(id) {
		return
	}
	p.signal()
}

// process resolves the identity to its canonical person and upserts both the
// per-platform identity page and the canonical page, linking them. Returns an
// error only when a put_page fails (so the worker re-queues + retries).
func (p *Projector) process(ctx context.Context, id identity) error {
	id = p.enrich(ctx, id)

	asn := p.resolver.Observe(id)

	// #49 project-time classification: a follow event carries the classify signals, so
	// re-derive the node type/category EVERY projection (durable-by-construction — node
	// type isn't persisted). Tags the LIVE canonical (asn follows merge redirects), so a
	// friend merged into an existing person types the survivor person/friend.
	if id.follow != nil {
		cls := ClassifyFollow(*id.follow)
		p.resolver.SetNodeType(asn.Canonical.ID, cls.Type, cls.Category)
	}

	// Per-platform identity page. Its slug person-<platform>-<id> is UNIQUE to this
	// identity, and the coalescing queue ensures only one worker holds a given
	// identityKey at a time, so no same-slug write race here.
	islug, icontent := BuildPersonPage(id, asn.Canonical.ID)
	if err := p.putIfChanged(ctx, islug, icontent); err != nil {
		return err
	}

	// Canonical person page (the queryable entity). MANY identities can map to the
	// SAME canonical slug (deterministic merge), so two workers may commit it
	// concurrently. Serialize per-slug and RE-SNAPSHOT under the lock so the latest
	// merge state always wins — idempotent put alone wouldn't prevent a stale write
	// from a worker holding an older snapshot clobbering a newer one (lost-update).
	cslug := asn.Canonical.ID
	if err := p.commitCanonical(ctx, cslug, asn.Canonical); err != nil {
		return err
	}

	// Link the identity to its canonical person.
	p.linkOnce(ctx, "identof:"+islug+"|"+cslug, islug, cslug, "identity_of",
		"platform identity of "+displayOrID(asn.Canonical))

	// Deterministic auto-merges: record merged_into for traceability. Write the
	// absorbed canonical's page as a merged-stub FIRST so the merged_into link has a
	// real source page (#17 — otherwise the link silently fails when the absorbed
	// canonical never had its own page, losing merge provenance + reversibility).
	for _, m := range asn.AutoMerged {
		if err := p.writeMergedStub(ctx, m.Absorbed, m.Survivor, asn.Canonical.Name); err != nil {
			p.log.Warn("merged stub write failed", "absorbed", m.Absorbed, "err", err)
		}
		p.linkOnce(ctx, "merged:"+m.Absorbed+"|"+m.Survivor, m.Absorbed, m.Survivor,
			"merged_into", "auto-merged on shared "+m.Via)
		p.log.Info("gbrain auto-merged canonical persons", "survivor", m.Survivor, "absorbed", m.Absorbed, "via", m.Via)
	}

	// Fuzzy proposals: queue for human approval — never auto-applied.
	for _, pr := range asn.Proposals {
		p.proposals.Add(Proposal{
			A: pr.A, B: pr.B, AName: pr.AName, BName: pr.BName,
			Score: pr.Score, Reason: pr.Via,
		})
		p.log.Info("gbrain merge proposed (awaiting approval)", "a", pr.A, "b", pr.B, "score", pr.Score)
	}
	return nil
}

// commitCanonical serializes writes to a canonical slug across workers and writes
// the LATEST resolver snapshot under that lock, so concurrent observers of the
// same person can't lose a merge to a stale clobber. The slug is hashed onto a
// fixed bank of mutexes (same slug → same lock; unrelated slugs occasionally
// share one, which is harmless and also gently caps write concurrency for the
// single-writer PGLite backend). fallback is used only if the snapshot vanished.
func (p *Projector) commitCanonical(ctx context.Context, slug string, fallback CanonicalPerson) error {
	lk := p.slugLock(slug)
	lk.Lock()
	defer lk.Unlock()

	cp := fallback
	if fresh, ok := p.resolver.Snapshot(slug); ok {
		cp = fresh
	}
	cslug, content := BuildCanonicalPage(cp)
	return p.putIfChanged(ctx, cslug, content)
}

// slugLock returns the mutex guarding writes to slug (hashed onto a fixed bank).
func (p *Projector) slugLock(slug string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(slug))
	return &p.slugLocks[h.Sum32()%uint32(len(p.slugLocks))]
}

// putIfChanged writes a page only when its content changed; rolls back the hash
// on failure so the next pass retries.
func (p *Projector) putIfChanged(ctx context.Context, slug, content string) error {
	h := pageHash(content)
	p.mu.Lock()
	changed := p.putHash[slug] != h
	if changed {
		p.putHash[slug] = h
	}
	p.mu.Unlock()
	if !changed {
		return nil // content identical to what's already in gbrain — skip the (expensive) shell-out
	}
	if err := p.gb.PutPage(ctx, slug, content); err != nil {
		p.mu.Lock()
		delete(p.putHash, slug)
		p.mu.Unlock()
		return err
	}
	// Persist the new hash so a restart's reproject skips this page when unchanged (#62).
	if p.pageHashStore != nil {
		if err := p.pageHashStore.UpsertPageHash(ctx, slug, h); err != nil {
			p.log.Warn("page-hash persist failed (page still written)", "slug", slug, "err", err)
		}
	}
	p.log.Debug("gbrain page upserted", "slug", slug)
	return nil
}

// pageHash is the change-detection digest for a gbrain page's content (#62). FNV-1a is
// fast + ample for "did this exact content change" — not a security hash.
func pageHash(content string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(content))
	return strconv.FormatUint(h.Sum64(), 16)
}

// PageHashStore durably persists slug->content-hash so the projector's put-page skip
// cache survives a restart (#62) — without it a restart re-writes ~every gbrain page
// (each a subprocess shell-out) even though the graph is unchanged, the ~13min reproject.
type PageHashStore interface {
	// PageHashes bulk-loads all persisted slug->hash in one query (startup seed).
	PageHashes(ctx context.Context) (map[string]string, error)
	// UpsertPageHash records a page's content hash after a successful write.
	UpsertPageHash(ctx context.Context, slug, hash string) error
}

// SetPageHashStore wires durable put-hash persistence (#62). Call before LoadPageHashes.
func (p *Projector) SetPageHashStore(s PageHashStore) { p.pageHashStore = s }

// LoadPageHashes seeds the in-memory put-hash cache from the durable store so the
// startup reproject skips re-writing unchanged pages (#62). Call after
// SetPageHashStore, before the reproject. Best-effort: a load failure just means the
// first reproject re-writes everything (the pre-#62 behavior), then re-persists.
func (p *Projector) LoadPageHashes(ctx context.Context) {
	if p.pageHashStore == nil {
		return
	}
	hashes, err := p.pageHashStore.PageHashes(ctx)
	if err != nil {
		p.log.Warn("page-hash seed failed; reproject will re-write all pages", "err", err)
		return
	}
	p.mu.Lock()
	for slug, h := range hashes {
		p.putHash[slug] = h
	}
	n := len(p.putHash)
	p.mu.Unlock()
	if n > 0 {
		p.log.Info("seeded gbrain page-hash cache", "pages", n)
	}
}

// BackfillMergeLinks reconciles gbrain's merged_into provenance links against the
// resolver's TRUE merge state (#17). For every absorbed→survivor in the resolver's
// redirect map it ensures a merged-stub source page + an idempotent merged_into link —
// catching merges whose link silently failed (pre-stub-fix) or that a reproject formed
// by accretion rather than by re-emitting a MergeEvent. Idempotent (linkOnce dedups by
// pair). Returns the number of provenance links ensured. Wire it at the end of a
// reproject so gbrain's audit always matches the rebuilt graph.
func (p *Projector) BackfillMergeLinks(ctx context.Context) (int, error) {
	redirects := p.resolver.Redirects()
	n := 0
	for absorbed, survivor := range redirects {
		if ctx.Err() != nil {
			return n, ctx.Err()
		}
		name := survivor
		if cp, ok := p.resolver.Snapshot(survivor); ok {
			name = cp.Name
		}
		if err := p.writeMergedStub(ctx, absorbed, survivor, name); err != nil {
			p.log.Warn("backfill: merged stub write failed", "absorbed", absorbed, "err", err)
			continue
		}
		p.linkOnce(ctx, "merged:"+absorbed+"|"+survivor, absorbed, survivor,
			"merged_into", "merge-provenance backfill")
		n++
	}
	if n > 0 {
		p.log.Info("backfilled merged_into provenance links (#17)", "count", n)
	}
	return n, nil
}

// linkOnce writes a link once per dedupe key, rolling back on failure to retry.
func (p *Projector) linkOnce(ctx context.Context, dedupe, from, to, linkType, context_ string) {
	if from == to {
		return
	}
	p.mu.Lock()
	if p.linked[dedupe] {
		p.mu.Unlock()
		return
	}
	p.linked[dedupe] = true
	p.mu.Unlock()

	if err := p.gb.AddLink(ctx, from, to, linkType, context_); err != nil {
		p.log.Error("gbrain add_link failed", "from", from, "to", to, "type", linkType, "err", err)
		p.mu.Lock()
		delete(p.linked, dedupe)
		p.mu.Unlock()
	}
}

// enrich lazily fills missing alias signals (email/phone/handle/name) for an
// identity via its platform enricher — only when needed (no strong signal yet).
func (p *Projector) enrich(ctx context.Context, id identity) identity {
	if id.Phone != "" || id.Email != "" {
		return id // already has a strong signal; nothing to gain
	}
	e, ok := p.enrichers[id.Platform]
	if !ok {
		return id
	}
	got, err := e.Enrich(ctx, []string{id.UserID})
	if err != nil {
		p.log.Warn("identity enrichment failed", "platform", id.Platform, "user", id.UserID, "err", err)
		return id
	}
	a, ok := got[id.UserID]
	if !ok {
		return id
	}
	if id.Email == "" {
		id.Email = strings.ToLower(strings.TrimSpace(a.Email))
	}
	if id.Phone == "" {
		id.Phone = normalizePhone(a.Phone)
	}
	if id.Handle == "" {
		id.Handle = a.Handle
	}
	if id.Names.Display == "" {
		id.Names.Display = a.DisplayName
	}
	if id.Names.Username == "" {
		id.Names.Username = a.Handle
	}
	return id
}

// displayOrID returns the canonical name or, if unidentified, its ID.
func displayOrID(cp CanonicalPerson) string {
	if cp.Identified {
		return cp.Name
	}
	return cp.ID
}

// BuildPersonPage produces the slug + markdown page for a per-platform identity,
// linked to its canonical person. Pure + deterministic + idempotent on slug.
func BuildPersonPage(id identity, canonicalID string) (slug, content string) {
	slug = PersonSlug(id.Platform, id.UserID)
	name, src, identified := id.Names.Resolve()

	var fm strings.Builder
	fm.WriteString("---\n")
	fm.WriteString("type: person-identity\n")
	title := name
	if !identified {
		title = fmt.Sprintf("Unidentified %s contact", id.Platform)
	}
	fmt.Fprintf(&fm, "title: %s\n", yamlStr(title))
	fmt.Fprintf(&fm, "platform: %s\n", id.Platform)
	if canonicalID != "" {
		fmt.Fprintf(&fm, "canonical: %s\n", yamlStr(canonicalID))
	}
	fmt.Fprintf(&fm, "identified: %t\n", identified)
	if identified {
		fmt.Fprintf(&fm, "name_source: %s\n", string(src))
	}
	if id.IsSelf {
		fm.WriteString("is_self: true\n")
	}
	if id.FromContact {
		fm.WriteString("from_contact: true\n")
	}
	fm.WriteString("source: switchboard\n")

	fm.WriteString("external_ids:\n")
	fmt.Fprintf(&fm, "  - %s:%s\n", id.Platform, yamlStr(id.UserID))
	if id.Phone != "" {
		fmt.Fprintf(&fm, "  - phone:%s\n", yamlStr(id.Phone))
	}
	if id.Email != "" {
		fmt.Fprintf(&fm, "  - email:%s\n", yamlStr(id.Email))
	}

	aliases := id.Names.allNames()
	if len(aliases) > 0 {
		fm.WriteString("aliases:\n")
		for _, a := range aliases {
			fmt.Fprintf(&fm, "  - %s\n", yamlStr(a))
		}
	}
	fm.WriteString("---\n\n")

	var body strings.Builder
	if identified {
		fmt.Fprintf(&body, "# %s\n\n", name)
	} else {
		fmt.Fprintf(&body, "# %s\n\n", title)
		body.WriteString("> No human name resolved for this identity yet — kept as a flagged stub, not presented as a named person.\n\n")
	}
	fmt.Fprintf(&body, "Platform identity observed on **%s** by switchboard.\n\n", id.Platform)
	if canonicalID != "" {
		fmt.Fprintf(&body, "Canonical person: [[%s]]\n\n", canonicalID)
	}
	body.WriteString("## Identity signals\n\n")
	fmt.Fprintf(&body, "- platform user id: `%s`\n", id.UserID)
	if id.Handle != "" {
		fmt.Fprintf(&body, "- handle: `%s`\n", id.Handle)
	}
	if id.Phone != "" {
		fmt.Fprintf(&body, "- phone: `%s`\n", id.Phone)
	}
	if id.Email != "" {
		fmt.Fprintf(&body, "- email: `%s`\n", id.Email)
	}

	return slug, fm.String() + body.String()
}

// writeMergedStub writes a minimal page for an ABSORBED canonical, marking it
// merged_into the survivor — so gbrain always has a source page to anchor the
// merged_into link (#17: provenance + auto-merge reversibility/audit), even when
// the absorbed canonical never had its own page committed before the merge.
func (p *Projector) writeMergedStub(ctx context.Context, absorbed, survivor, survivorName string) error {
	if absorbed == "" || absorbed == survivor {
		return nil
	}
	var fm strings.Builder
	fm.WriteString("---\n")
	fm.WriteString("type: person\n")
	fm.WriteString("canonical: true\n")
	fmt.Fprintf(&fm, "merged_into: %s\n", yamlStr(survivor))
	title := survivorName
	if strings.TrimSpace(title) == "" {
		title = survivor
	}
	fmt.Fprintf(&fm, "title: %s\n", yamlStr(title+" (merged)"))
	fm.WriteString("source: switchboard\n")
	fm.WriteString("---\n\n")
	fmt.Fprintf(&fm, "This person record was merged into [[%s]].\n", survivor)
	return p.putIfChanged(ctx, absorbed, fm.String())
}

// BuildCanonicalPage produces the canonical person page — the queryable entity.
// Pure + deterministic + idempotent on slug.
func BuildCanonicalPage(cp CanonicalPerson) (slug, content string) {
	slug = cp.ID
	title := cp.Name
	if !cp.Identified {
		title = "Unidentified person"
	}

	// Collect unioned external ids + aliases across all member identities.
	var extPlatform, extPhone, extEmail []string
	aliasSet := map[string]bool{}
	var aliases []string
	platforms := map[string]bool{}
	for _, m := range cp.Members {
		extPlatform = append(extPlatform, fmt.Sprintf("%s:%s", m.Platform, m.UserID))
		if m.Phone != "" {
			extPhone = append(extPhone, m.Phone)
		}
		if m.Email != "" {
			extEmail = append(extEmail, m.Email)
		}
		platforms[m.Platform] = true
		for _, a := range m.Names.allNames() {
			if !aliasSet[strings.ToLower(a)] {
				aliasSet[strings.ToLower(a)] = true
				aliases = append(aliases, a)
			}
		}
	}
	extPhone = uniqSorted(extPhone)
	extEmail = uniqSorted(extEmail)
	sort.Strings(aliases)

	var fm strings.Builder
	fm.WriteString("---\n")
	fm.WriteString("type: person\n")
	fm.WriteString("canonical: true\n")
	fmt.Fprintf(&fm, "title: %s\n", yamlStr(title))
	fmt.Fprintf(&fm, "identified: %t\n", cp.Identified)
	if cp.Identified {
		fmt.Fprintf(&fm, "name_source: %s\n", string(cp.NameSrc))
	}
	if cp.IsSelf {
		fm.WriteString("is_self: true\n")
	}
	fm.WriteString("source: switchboard\n")

	fm.WriteString("external_ids:\n")
	for _, e := range extPlatform {
		fmt.Fprintf(&fm, "  - %s\n", yamlStr(e))
	}
	for _, ph := range extPhone {
		fmt.Fprintf(&fm, "  - phone:%s\n", yamlStr(ph))
	}
	for _, em := range extEmail {
		fmt.Fprintf(&fm, "  - email:%s\n", yamlStr(em))
	}

	if len(aliases) > 0 {
		fm.WriteString("aliases:\n")
		for _, a := range aliases {
			fmt.Fprintf(&fm, "  - %s\n", yamlStr(a))
		}
	}
	fm.WriteString("---\n\n")

	var body strings.Builder
	fmt.Fprintf(&body, "# %s\n\n", title)
	if !cp.Identified {
		body.WriteString("> No human name resolved across any platform identity yet — flagged for review, not presented as a named person.\n\n")
	}
	platformList := make([]string, 0, len(platforms))
	for pl := range platforms {
		platformList = append(platformList, pl)
	}
	sort.Strings(platformList)
	fmt.Fprintf(&body, "Canonical person across **%s**.\n\n", strings.Join(platformList, ", "))

	body.WriteString("## Platform identities\n\n")
	for _, m := range cp.Members {
		nm, _, ok := m.Names.Resolve()
		if !ok {
			nm = "(unnamed)"
		}
		fmt.Fprintf(&body, "- **%s**: %s — [[%s]] (`%s`)\n", m.Platform, nm, PersonSlug(m.Platform, m.UserID), m.UserID)
	}

	return slug, fm.String() + body.String()
}

var slugBad = regexp.MustCompile(`[^a-z0-9]+`)

// PersonSlug builds the stable canonical slug for a per-platform identity.
func PersonSlug(platform, userID string) string {
	clean := slugBad.ReplaceAllString(strings.ToLower(userID), "-")
	clean = strings.Trim(clean, "-")
	return fmt.Sprintf("person-%s-%s", platform, clean)
}

func strongSignals(id identity) []string {
	var out []string
	if id.Phone != "" {
		out = append(out, "phone:"+id.Phone)
	}
	if id.Email != "" {
		out = append(out, "email:"+id.Email)
	}
	return out
}

func sigLabel(sig string) string {
	if i := strings.IndexByte(sig, ':'); i >= 0 {
		return sig[:i]
	}
	return sig
}

var nonPhone = regexp.MustCompile(`[^0-9+]`)

func normalizePhone(s string) string {
	s = nonPhone.ReplaceAllString(strings.TrimSpace(s), "")
	return s
}

func uniqSorted(vals []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, v := range vals {
		v = strings.TrimSpace(v)
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func yamlStr(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"`
}
