package gbrainfeed

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// fakeClient records gbrain calls.
type fakeClient struct {
	mu    sync.Mutex
	pages map[string]string
	links [][3]string // from,to,type
}

func newFake() *fakeClient { return &fakeClient{pages: map[string]string{}} }

func (f *fakeClient) PutPage(_ context.Context, slug, content string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pages[slug] = content
	return nil
}
func (f *fakeClient) AddLink(_ context.Context, from, to, lt, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.links = append(f.links, [3]string{from, to, lt})
	return nil
}
func (f *fakeClient) pageCount() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.pages) }
func (f *fakeClient) linkCount() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.links) }

func TestBuildPersonPage(t *testing.T) {
	slug, content := BuildPersonPage(identity{
		Platform: event.PlatformWhatsApp,
		UserID:   "6591234567@s.whatsapp.net",
		Names:    NameSet{AddressBook: "Alice Tan"},
		Handle:   "alicet",
		Phone:    "+6591234567",
		Email:    "alice@example.com",
	}, "canon-phone-6591234567")
	if slug != "person-whatsapp-6591234567-s-whatsapp-net" {
		t.Fatalf("slug=%q", slug)
	}
	for _, want := range []string{
		"type: person-identity",
		"platform: whatsapp",
		"canonical: \"canon-phone-6591234567\"",
		"identified: true",
		"name_source: address_book",
		"external_ids:",
		"phone:\"+6591234567\"",
		"email:\"alice@example.com\"",
		"# Alice Tan",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("page missing %q\n---\n%s", want, content)
		}
	}
}

func TestBuildPersonPageUnidentifiedFlagged(t *testing.T) {
	// A bare @lid sender with no name must be FLAGGED, not passed off as a person.
	_, content := BuildPersonPage(identity{
		Platform: event.PlatformWhatsApp,
		UserID:   "100000000000001@lid",
	}, "canon-whatsapp-100000000000001-lid")
	for _, want := range []string{"identified: false", "Unidentified whatsapp contact"} {
		if !strings.Contains(content, want) {
			t.Fatalf("page missing %q\n---\n%s", want, content)
		}
	}
	if strings.Contains(content, "name_source:") {
		t.Fatalf("unidentified page must not claim a name_source\n%s", content)
	}
}

func runProjector(t *testing.T, f *fakeClient, msgs ...event.Message) *Projector {
	t.Helper()
	p := NewProjector(f, nil)
	ctx := context.Background()
	p.Start(ctx)
	for _, m := range msgs {
		p.OnIngest(m)
	}
	p.Close() // drains the queue
	return p
}

func senderMsg(platform, userID, name, phone, email string) event.Message {
	return event.Message{
		Platform: platform,
		Sender: event.Sender{
			PlatformUserID: userID, DisplayName: name, Phone: phone, Email: email,
		},
		TS: time.Now(),
	}
}

func TestProjectorUpsertsPersonPage(t *testing.T) {
	f := newFake()
	runProjector(t, f, senderMsg(event.PlatformWhatsApp, "u1@x", "Bob", "", ""))
	// One identity page + one canonical person page.
	if f.pageCount() != 2 {
		t.Fatalf("want 2 pages (identity + canonical), got %d", f.pageCount())
	}
	// The identity links to its canonical person.
	if f.linkCount() != 1 || f.links[0][2] != "identity_of" {
		t.Fatalf("want 1 identity_of link, got %d: %+v", f.linkCount(), f.links)
	}
}

func TestProjectorBurstNonLossyCoalescing(t *testing.T) {
	f := newFake()
	p := NewProjector(f, nil)
	ctx := context.Background()
	p.Start(ctx)
	// Simulate a history-sync burst: 5000 messages from 3 distinct senders,
	// interleaved. The OLD bounded channel dropped on overflow; the coalescing
	// queue must lose NONE and collapse to 3 person pages.
	for i := 0; i < 5000; i++ {
		who := i % 3
		p.OnIngest(senderMsg(event.PlatformWhatsApp,
			"u"+string(rune('A'+who))+"@x", "User"+string(rune('A'+who)), "", ""))
	}
	p.Close() // drains everything (non-lossy) before returning

	// 3 senders → 3 identity pages + 3 canonical pages.
	if f.pageCount() != 6 {
		t.Fatalf("want 6 coalesced pages (3 identity + 3 canonical), got %d", f.pageCount())
	}
}

func TestProjectorRetriesOnGbrainFailure(t *testing.T) {
	// gbrain fails the first N put attempts, then succeeds — the worker must
	// retry (re-queue) rather than drop the identity.
	fc := &flakyClient{failFirst: 2, pages: map[string]string{}}
	p := NewProjector(fc, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p.Start(ctx)
	p.OnIngest(senderMsg(event.PlatformWhatsApp, "retry@x", "RetryGuy", "", ""))

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if fc.successes() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	p.Close()
	if fc.successes() < 1 {
		t.Fatalf("identity never persisted after retries (attempts=%d)", fc.attempts())
	}
}

// flakyClient fails the first failFirst PutPage calls, then succeeds.
type flakyClient struct {
	mu        sync.Mutex
	failFirst int
	att       int
	succ      int
	pages     map[string]string
}

func (c *flakyClient) PutPage(_ context.Context, slug, content string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.att++
	if c.att <= c.failFirst {
		return errTransient
	}
	c.pages[slug] = content
	c.succ++
	return nil
}
func (c *flakyClient) AddLink(context.Context, string, string, string, string) error { return nil }
func (c *flakyClient) attempts() int                                                 { c.mu.Lock(); defer c.mu.Unlock(); return c.att }
func (c *flakyClient) successes() int                                                { c.mu.Lock(); defer c.mu.Unlock(); return c.succ }

var errTransient = fmt.Errorf("transient gbrain error")

func TestProjectorDedupesRepeatPuts(t *testing.T) {
	f := newFake()
	m := senderMsg(event.PlatformWhatsApp, "u1@x", "Bob", "", "")
	// Same identity 3 times — should put each page once (content unchanged).
	runProjector(t, f, m, m, m)
	if f.pageCount() != 2 {
		t.Fatalf("want 2 pages (identity + canonical), got %d", f.pageCount())
	}
}

func TestProjectorAutoMergeOnSharedPhone(t *testing.T) {
	f := newFake()
	// Same person, two platforms, shared phone → ONE canonical person, two
	// per-platform identities both linking identity_of → it.
	wa := senderMsg(event.PlatformWhatsApp, "wa1@x", "Carol", "+6580001111", "")
	tg := senderMsg(event.PlatformTelegram, "tg1", "Carol T", "+6580001111", "")
	runProjector(t, f, wa, tg)

	// 2 identity pages + 1 shared canonical page.
	if f.pageCount() != 3 {
		t.Fatalf("want 3 pages (2 identity + 1 canonical), got %d", f.pageCount())
	}
	// Both identities resolve to the SAME canonical person.
	if _, ok := f.pages["canon-phone-6580001111"]; !ok {
		t.Fatalf("missing shared canonical page; pages=%v", keysOf(f.pages))
	}
	identof := 0
	for _, l := range f.links {
		if l[2] == "identity_of" && l[1] == "canon-phone-6580001111" {
			identof++
		}
	}
	if identof != 2 {
		t.Fatalf("want 2 identity_of links into the shared canonical, got %d: %+v", identof, f.links)
	}
}

func TestProjectorNoMergeWithoutSharedSignal(t *testing.T) {
	f := newFake()
	a := senderMsg(event.PlatformWhatsApp, "a@x", "A", "+6580000001", "")
	b := senderMsg(event.PlatformWhatsApp, "b@x", "B", "+6580000002", "")
	runProjector(t, f, a, b)
	// Two distinct canonical persons → no merged_into links.
	for _, l := range f.links {
		if l[2] == "merged_into" {
			t.Fatalf("unexpected merge link: %+v", l)
		}
	}
	// Two canonical pages, distinct.
	if _, ok := f.pages["canon-phone-6580000001"]; !ok {
		t.Fatalf("missing canon for A: %v", keysOf(f.pages))
	}
	if _, ok := f.pages["canon-phone-6580000002"]; !ok {
		t.Fatalf("missing canon for B: %v", keysOf(f.pages))
	}
}

func TestProjectorConcurrentDrainCorrectness(t *testing.T) {
	// High parallelism must not corrupt the graph: distinct senders each get a
	// canonical+identity page, and shared-phone pairs still merge to ONE canonical.
	// Run with -race to catch any unsynchronized access in the parallel pool.
	f := newFake()
	p := NewProjector(f, nil)
	p.SetConcurrency(16)
	p.Start(context.Background())

	const distinct = 150
	for i := 0; i < distinct; i++ {
		p.OnIngest(senderMsg(event.PlatformWhatsApp, fmt.Sprintf("u%d@x", i), fmt.Sprintf("User%d", i), "", ""))
	}
	// 20 cross-platform pairs sharing a phone → each pair must collapse to 1 canonical.
	for i := 0; i < 20; i++ {
		ph := fmt.Sprintf("+65%08d", i)
		p.OnIngest(senderMsg(event.PlatformWhatsApp, fmt.Sprintf("wa%d@x", i), "P", ph, ""))
		p.OnIngest(senderMsg(event.PlatformTelegram, fmt.Sprintf("tg%d", i), "P", ph, ""))
	}
	p.Close()

	// Distinct senders: 150 canonical (canon-whatsapp-*) + 150 identity pages.
	// Pairs: 20 shared canon-phone-* + 40 identity pages.
	canon := 0
	for s := range f.pages {
		if strings.HasPrefix(s, "canon-") {
			canon++
		}
	}
	if canon != distinct+20 {
		t.Fatalf("want %d canonical pages, got %d", distinct+20, canon)
	}
	// Each shared-phone pair resolved to exactly one shared canonical.
	for i := 0; i < 20; i++ {
		slug := fmt.Sprintf("canon-phone-65%08d", i)
		if _, ok := f.pages[slug]; !ok {
			t.Fatalf("missing merged canonical %s", slug)
		}
	}
}

func TestProjectorConcurrentMergeNoLostUpdate(t *testing.T) {
	// The lost-update guard: two identities that merge into ONE canonical are
	// processed by different workers concurrently. The final canonical page MUST
	// list BOTH platform identities — a stale snapshot must not clobber the merge.
	// Repeated across many pairs + high concurrency to provoke the race under -race.
	f := newFake()
	p := NewProjector(f, nil)
	p.SetConcurrency(16)
	p.Start(context.Background())

	const pairs = 60
	for i := 0; i < pairs; i++ {
		ph := fmt.Sprintf("+6590%06d", i)
		p.OnIngest(senderMsg(event.PlatformWhatsApp, fmt.Sprintf("wa%d@x", i), "X", ph, ""))
		p.OnIngest(senderMsg(event.PlatformTelegram, fmt.Sprintf("tg%d", i), "X", ph, ""))
	}
	p.Close()

	for i := 0; i < pairs; i++ {
		ph := fmt.Sprintf("6590%06d", i)
		page, ok := f.pages["canon-phone-"+ph]
		if !ok {
			t.Fatalf("missing canonical canon-phone-%s", ph)
		}
		// Both the WhatsApp and Telegram identity must be present in the page body.
		if !strings.Contains(page, fmt.Sprintf("wa%d@x", i)) || !strings.Contains(page, fmt.Sprintf("tg%d", i)) {
			t.Fatalf("canon-phone-%s lost a member (stale clobber):\n%s", ph, page)
		}
	}
}

func TestInterleaveByPlatform(t *testing.T) {
	// A gmail flood at the front must not delay other platforms: the interleaved
	// order surfaces a non-gmail item within the first few slots.
	var batch []identity
	for i := 0; i < 50; i++ {
		batch = append(batch, identity{Platform: "gmail", UserID: fmt.Sprintf("g%d", i)})
	}
	batch = append(batch, identity{Platform: "whatsapp", UserID: "wa1"})
	out := interleaveByPlatform(batch)
	foundAt := -1
	for i, id := range out {
		if id.Platform == "whatsapp" {
			foundAt = i
			break
		}
	}
	if foundAt < 0 || foundAt > 1 {
		t.Fatalf("whatsapp item should surface at the front after interleave, got index %d", foundAt)
	}
	if len(out) != len(batch) {
		t.Fatalf("interleave dropped items: %d vs %d", len(out), len(batch))
	}
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// fakeEnricher fills email for a known user id, counting calls.
type fakeEnricher struct {
	mu    sync.Mutex
	calls int
	email map[string]string
}

func (f *fakeEnricher) Enrich(_ context.Context, ids []string) (map[string]Aliases, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	out := map[string]Aliases{}
	for _, id := range ids {
		if e, ok := f.email[id]; ok {
			out[id] = Aliases{Email: e}
		}
	}
	return out, nil
}

func TestProjectorLazyEnrichment(t *testing.T) {
	f := newFake()
	enr := &fakeEnricher{email: map[string]string{"oc_dave": "dave@corp.com"}}
	p := NewProjector(f, nil)
	p.RegisterEnricher(event.PlatformLark, enr)
	ctx := context.Background()
	p.Start(ctx)
	// Sparse Lark sender: only open_id + name, no email/phone on the event.
	p.OnIngest(senderMsg(event.PlatformLark, "oc_dave", "Dave", "", ""))
	p.Close()

	// The page must include the enriched email, and the enricher was consulted.
	slug := PersonSlug(event.PlatformLark, "oc_dave")
	f.mu.Lock()
	content := f.pages[slug]
	f.mu.Unlock()
	if !strings.Contains(content, "dave@corp.com") {
		t.Fatalf("enriched email missing from page:\n%s", content)
	}
	if enr.calls == 0 {
		t.Fatal("enricher was never called")
	}
}

// batchSpyEnricher records the size of each Enrich call so we can prove the
// projector pre-warms in batches rather than one id at a time.
type batchSpyEnricher struct {
	mu        sync.Mutex
	callSizes []int
	names     map[string]string
}

func (e *batchSpyEnricher) Enrich(_ context.Context, ids []string) (map[string]Aliases, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.callSizes = append(e.callSizes, len(ids))
	out := map[string]Aliases{}
	for _, id := range ids {
		if n, ok := e.names[id]; ok {
			out[id] = Aliases{DisplayName: n}
		}
	}
	return out, nil
}
func (e *batchSpyEnricher) maxCall() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := 0
	for _, s := range e.callSizes {
		if s > m {
			m = s
		}
	}
	return m
}

func TestProjectorBatchPrewarmsEnricher(t *testing.T) {
	// Many sparse Lark senders in one drain must be enriched in a BATCH (one call
	// covering all ids), not one-id-at-a-time — the fix for the Lark 6% gap.
	f := newFake()
	names := map[string]string{}
	for i := 0; i < 50; i++ {
		names[fmt.Sprintf("oc_%d", i)] = fmt.Sprintf("Lark User %d", i)
	}
	enr := &batchSpyEnricher{names: names}
	p := NewProjector(f, nil)
	p.RegisterEnricher(event.PlatformLark, enr)
	// Enqueue all 50 BEFORE Start so they form a single drained batch (the worker
	// would otherwise drain them piecemeal as they arrive).
	for i := 0; i < 50; i++ {
		p.OnIngest(senderMsg(event.PlatformLark, fmt.Sprintf("oc_%d", i), "", "", ""))
	}
	p.Start(context.Background())
	p.Close()

	// A single drained batch should be pre-warmed with one large Enrich call.
	if enr.maxCall() < 50 {
		t.Fatalf("expected a batched enrich of all 50 ids, max call size was %d", enr.maxCall())
	}
	// And every Lark sender resolved to its enriched name.
	named := 0
	for _, c := range f.pages {
		if strings.Contains(c, "Lark User ") {
			named++
		}
	}
	if named < 50 {
		t.Fatalf("expected 50 enriched Lark names in pages, got %d", named)
	}
}

func TestProjectorNoEnrichWhenSignalPresent(t *testing.T) {
	f := newFake()
	enr := &fakeEnricher{email: map[string]string{"oc_x": "should-not@used.com"}}
	p := NewProjector(f, nil)
	p.RegisterEnricher(event.PlatformLark, enr)
	ctx := context.Background()
	p.Start(ctx)
	// Sender already has a phone → enricher must NOT be called (cost-avoidance).
	p.OnIngest(senderMsg(event.PlatformLark, "oc_x", "X", "+6588889999", ""))
	p.Close()
	if enr.calls != 0 {
		t.Fatalf("enricher called %d times despite present signal", enr.calls)
	}
}

func TestProjectorMergeLinkOnlyOnce(t *testing.T) {
	f := newFake()
	wa := senderMsg(event.PlatformWhatsApp, "wa1@x", "Carol", "+6580001111", "")
	tg := senderMsg(event.PlatformTelegram, "tg1", "Carol T", "+6580001111", "")
	// Re-observe both repeatedly; each identity_of link must be created exactly
	// once (2 identities → 2 links, stable across re-observation).
	runProjector(t, f, wa, tg, wa, tg, tg)
	if f.linkCount() != 2 {
		t.Fatalf("want exactly 2 identity_of links, got %d: %+v", f.linkCount(), f.links)
	}
}

// fakePageHashStore is an in-memory PageHashStore for the #62 test.
type fakePageHashStore struct{ m map[string]string }

func (s *fakePageHashStore) PageHashes(_ context.Context) (map[string]string, error) {
	cp := map[string]string{}
	for k, v := range s.m {
		cp[k] = v
	}
	return cp, nil
}
func (s *fakePageHashStore) UpsertPageHash(_ context.Context, slug, hash string) error {
	s.m[slug] = hash
	return nil
}

// #62: persisting put-hashes lets a restart SKIP re-writing unchanged gbrain pages
// (each write is an expensive gbrain subprocess) — the cause of the ~13min reproject.
func TestPageHashSkipsUnchangedAcrossRestart(t *testing.T) {
	ctx := context.Background()
	store := &fakePageHashStore{m: map[string]string{}}

	// session 1: write a page → hash persisted to the store
	f1 := newFake()
	p1 := NewProjector(f1, nil)
	p1.SetPageHashStore(store)
	if err := p1.putIfChanged(ctx, "person-x", "v1"); err != nil {
		t.Fatal(err)
	}
	if f1.pageCount() != 1 {
		t.Fatalf("session1 should write the page, got %d", f1.pageCount())
	}
	if store.m["person-x"] == "" {
		t.Fatal("session1 should persist the page hash")
	}

	// session 2 (restart): fresh projector + fresh gbrain client, SAME store, seed from it
	f2 := newFake()
	p2 := NewProjector(f2, nil)
	p2.SetPageHashStore(store)
	p2.LoadPageHashes(ctx)
	// unchanged content → must SKIP the put (no gbrain shell-out)
	if err := p2.putIfChanged(ctx, "person-x", "v1"); err != nil {
		t.Fatal(err)
	}
	if f2.pageCount() != 0 {
		t.Fatalf("restart: an unchanged page must be SKIPPED (no PutPage), got %d", f2.pageCount())
	}
	// changed content → must write
	if err := p2.putIfChanged(ctx, "person-x", "v2"); err != nil {
		t.Fatal(err)
	}
	if f2.pageCount() != 1 {
		t.Fatalf("restart: a changed page must be written, got %d", f2.pageCount())
	}
}
