package daemon

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/api"
	"github.com/seahyc/switchboard-oss/internal/connectors"
	"github.com/seahyc/switchboard-oss/internal/event"
)

// fakeConnector emits a scripted live event, supports backfill + send/react.
type fakeConnector struct {
	name string

	mu       sync.Mutex
	sent     []connectors.SendRequest
	reacted  int
	backfill []string
}

func (f *fakeConnector) Name() string { return f.name }

func (f *fakeConnector) Connect(ctx context.Context, out chan<- event.Message) error {
	// Emit one live message, then block until ctx is done (like a real socket).
	m := event.Message{
		Platform: f.name, AccountID: f.name + ":self",
		ChatID: f.name + ":c1", ChatType: event.ChatDM,
		Sender: event.Sender{PlatformUserID: "u1", DisplayName: "Alice", Phone: "+6580001111"},
		TS:     time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC),
		Kind:   event.KindText, Text: "live hello", Source: event.SourceLive,
	}
	m.SetID("LIVE1")
	select {
	case out <- m:
	case <-ctx.Done():
		return ctx.Err()
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeConnector) Backfill(ctx context.Context, chatID string, since time.Time, out chan<- event.Message) error {
	f.mu.Lock()
	f.backfill = append(f.backfill, chatID)
	f.mu.Unlock()
	m := event.Message{
		Platform: f.name, ChatID: chatID, ChatType: event.ChatDM,
		Sender: event.Sender{PlatformUserID: "u1", DisplayName: "Alice"},
		TS:     time.Date(2026, 6, 9, 7, 0, 0, 0, time.UTC),
		Kind:   event.KindText, Text: "old backfilled", Source: event.SourceBackfill,
	}
	m.SetID("BACK1")
	select {
	case out <- m:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (f *fakeConnector) Send(_ context.Context, req connectors.SendRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, req)
	return f.name + ":sent1", nil
}
func (f *fakeConnector) React(context.Context, string, string, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reacted++
	return nil
}
func (f *fakeConnector) Login(context.Context) (connectors.LoginStatus, error) {
	return connectors.LoginStatus{Connected: true}, nil
}
func (f *fakeConnector) Status(context.Context) (connectors.LoginStatus, error) {
	return connectors.LoginStatus{Connected: true}, nil
}

// TestEndToEnd drives a full live-ingest + query + send + backfill cycle through
// the daemon's real pipeline, store, and HTTP API — the integration proof.
func TestEndToEnd(t *testing.T) {
	d, err := New(Config{DBPath: ":memory:"})
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}
	fc := &fakeConnector{name: event.PlatformWhatsApp}
	d.Register(fc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start projector + pipeline + connectors (but not the real HTTP listener;
	// we mount the API on an httptest server instead).
	d.proj.Start(ctx)
	defer d.proj.Close()
	go d.pipe.Run(ctx, d.events)
	go fc.Connect(ctx, d.events)

	srv := httptest.NewServer(api.NewServer(d, d.hub).Handler())
	defer srv.Close()
	c := api.NewClient("")
	// Point the client at the test server.
	c = clientFor(srv.URL)

	// 1. Live event should land in the store and be queryable.
	waitFor(t, func() bool {
		msgs, _ := c.Read(ctx, "whatsapp:c1", time.Time{}, 10)
		return len(msgs) == 1 && msgs[0].Text == "live hello"
	}, "live event ingested")

	// 2. Search over the firehose.
	hits, err := c.Search(ctx, "hello", "", "", time.Time{}, 10)
	if err != nil || len(hits) != 1 {
		t.Fatalf("search: %v %+v", err, hits)
	}

	// 3. Send routes to the connector.
	sentID, err := c.Send(ctx, api.SendBody{ChatID: "whatsapp:c1", Text: "reply"})
	if err != nil || sentID != "whatsapp:sent1" {
		t.Fatalf("send: %v %q", err, sentID)
	}

	// 4. Backfill routes to the connector and its output flows through the same
	//    pipeline (cross-dedupe with live).
	if err := c.Backfill(ctx, api.BackfillBody{ChatID: "whatsapp:c1"}); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	waitFor(t, func() bool {
		msgs, _ := c.Read(ctx, "whatsapp:c1", time.Time{}, 10)
		// live + the step-3 outbound "reply" (now round-tripped into the log, #40) + backfilled
		return len(msgs) == 3
	}, "backfilled event ingested")
}

// TestOutboundRecordedToLog proves #40: a POST /send round-trips the sent message INTO
// the canonical log (so the agent keeps a record of its own reply), marked is_self, and
// is idempotent — a connector that returns the same sent id (or later echoes it) does
// not create a duplicate.
func TestOutboundRecordedToLog(t *testing.T) {
	d, err := New(Config{DBPath: ":memory:"})
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}
	fc := &fakeConnector{name: event.PlatformWhatsApp}
	d.Register(fc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.proj.Start(ctx)
	defer d.proj.Close()
	go d.pipe.Run(ctx, d.events)
	// NOTE: do NOT start fc.Connect — we want ONLY the outbound in this chat, no live event.

	c := clientFor(httptest.NewServer(api.NewServer(d, d.hub).Handler()).URL)

	// Send → the reply must appear in the canonical log for that chat.
	sentID, err := c.Send(ctx, api.SendBody{ChatID: "whatsapp:cOut", Text: "my reply"})
	if err != nil || sentID != "whatsapp:sent1" {
		t.Fatalf("send: %v %q", err, sentID)
	}
	waitFor(t, func() bool {
		msgs, _ := c.Read(ctx, "whatsapp:cOut", time.Time{}, 10)
		return len(msgs) == 1 && msgs[0].Text == "my reply" && msgs[0].Sender.IsSelf
	}, "outbound recorded to log as is_self")

	// Idempotency: a second send returns the SAME canonical id (as a re-send / echo would).
	// Emit it DIRECTLY through the pipeline and wait for the pipeline to actually process
	// it (Ingest returns inserted=false on the dup), so the assertion can't pass simply
	// because the duplicate hadn't landed yet.
	dup := event.Message{
		ID: sentID, Platform: event.PlatformWhatsApp, ChatID: "whatsapp:cOut",
		Sender: event.Sender{IsSelf: true}, TS: time.Now().UTC(),
		Kind: event.KindText, Text: "my reply", Source: event.SourceLive,
	}
	inserted, err := d.pipe.Ingest(ctx, dup)
	if err != nil {
		t.Fatalf("ingest dup: %v", err)
	}
	if inserted {
		t.Fatal("duplicate outbound (same canonical id) was inserted — not deduped")
	}
	msgs, _ := c.Read(ctx, "whatsapp:cOut", time.Time{}, 10)
	if len(msgs) != 1 {
		t.Fatalf("expected exactly one outbound row after dup, got %d", len(msgs))
	}
}

// capableConnector additionally implements ButtonTapper, PollVoter, ParticipantReactor.
type capableConnector struct {
	fakeConnector
	tappedBtn   string
	voted       string
	reactAsSndr string
	submitted   string
}

func (c *capableConnector) TapButton(_ context.Context, _, _, buttonID string) (string, error) {
	c.tappedBtn = buttonID
	return "ok:" + buttonID, nil
}
func (c *capableConnector) VotePoll(_ context.Context, _, _, option string) error {
	c.voted = option
	return nil
}
func (c *capableConnector) ReactAs(_ context.Context, _, _, senderID, _ string) error {
	c.reactAsSndr = senderID
	return nil
}

// capableConnector also implements InteractiveAuth.
func (c *capableConnector) SubmitPhone(_ context.Context, v string) error {
	c.submitted = "phone:" + v
	return nil
}
func (c *capableConnector) SubmitCode(_ context.Context, v string) error {
	c.submitted = "code:" + v
	return nil
}
func (c *capableConnector) SubmitPassword(_ context.Context, v string) error {
	c.submitted = "password:" + v
	return nil
}

func TestAuthSubmitRouting(t *testing.T) {
	d, err := New(Config{DBPath: ":memory:"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	cap := &capableConnector{fakeConnector: fakeConnector{name: event.PlatformTelegram}}
	d.Register(cap)
	d.Register(&fakeConnector{name: event.PlatformWhatsApp}) // no InteractiveAuth
	ctx := context.Background()

	if err := d.AuthSubmit(ctx, event.PlatformTelegram, "code", "12345"); err != nil {
		t.Fatalf("submit code: %v", err)
	}
	if cap.submitted != "code:12345" {
		t.Fatalf("submitted=%q, want code:12345", cap.submitted)
	}
	if err := d.AuthSubmit(ctx, event.PlatformTelegram, "password", "pw"); err != nil || cap.submitted != "password:pw" {
		t.Fatalf("submit password: err=%v submitted=%q", err, cap.submitted)
	}
	// Unknown field rejected.
	if err := d.AuthSubmit(ctx, event.PlatformTelegram, "bogus", "x"); err == nil {
		t.Fatal("expected error for unknown field")
	}
	// Connector without InteractiveAuth → clean unsupported error.
	if err := d.AuthSubmit(ctx, event.PlatformWhatsApp, "code", "x"); err == nil {
		t.Fatal("expected unsupported error for whatsapp interactive auth")
	}
	// Unknown platform → error.
	if err := d.AuthSubmit(ctx, "nope", "code", "x"); err == nil {
		t.Fatal("expected error for unknown platform")
	}
}

func TestTapVoteCapabilityRouting(t *testing.T) {
	d, err := New(Config{DBPath: ":memory:"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	cap := &capableConnector{fakeConnector: fakeConnector{name: event.PlatformTelegram}}
	d.Register(cap)
	// A plain connector with NO capabilities, on a different platform.
	d.Register(&fakeConnector{name: event.PlatformWhatsApp})
	ctx := context.Background()

	// Capable platform: tap + vote succeed and reach the connector.
	res, err := d.TapButton(ctx, "telegram:c1", "telegram:m1", "btnA")
	if err != nil || res != "ok:btnA" || cap.tappedBtn != "btnA" {
		t.Fatalf("tap: res=%q err=%v tapped=%q", res, err, cap.tappedBtn)
	}
	if err := d.VotePoll(ctx, "telegram:c1", "telegram:m1", "opt2"); err != nil || cap.voted != "opt2" {
		t.Fatalf("vote: err=%v voted=%q", err, cap.voted)
	}

	// Incapable platform: clean "unsupported" error, no panic.
	if _, err := d.TapButton(ctx, "whatsapp:c1", "whatsapp:m1", "x"); err == nil {
		t.Fatal("expected unsupported error for whatsapp tap_button")
	}
	if err := d.VotePoll(ctx, "whatsapp:c1", "whatsapp:m1", "x"); err == nil {
		t.Fatal("expected unsupported error for whatsapp vote_poll")
	}
}

func TestReactParticipantResolution(t *testing.T) {
	d, err := New(Config{DBPath: ":memory:"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	cap := &capableConnector{fakeConnector: fakeConnector{name: event.PlatformWhatsApp}}
	d.Register(cap)
	ctx := context.Background()

	// Seed a target message so its sender can be resolved.
	m := event.Message{
		Platform: event.PlatformWhatsApp, ChatID: "whatsapp:grp", ChatType: event.ChatGroup,
		Sender: event.Sender{PlatformUserID: "participant-99"}, TS: time.Now(),
		Kind: event.KindText, Text: "hi", Source: event.SourceLive,
	}
	m.SetID("TGT")
	if _, err := d.store.Append(ctx, m, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// React → daemon resolves sender from store and uses ReactAs.
	if err := d.React(ctx, "whatsapp:grp", "whatsapp:TGT", "👍"); err != nil {
		t.Fatalf("react: %v", err)
	}
	if cap.reactAsSndr != "participant-99" {
		t.Fatalf("ReactAs got sender %q, want participant-99", cap.reactAsSndr)
	}
}

func clientFor(url string) *api.Client {
	// api.NewClient takes host:port; strip scheme.
	return api.NewClient(stripScheme(url))
}

func stripScheme(u string) string {
	for i := 0; i+3 <= len(u); i++ {
		if u[i:i+3] == "://" {
			return u[i+3:]
		}
	}
	return u
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}
