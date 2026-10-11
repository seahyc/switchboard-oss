package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/connectors"
	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/igweb"
	"github.com/seahyc/switchboard-oss/internal/store"
	"github.com/seahyc/switchboard-oss/internal/xweb"
)

// fakeBackend implements Backend over a real store with stubbed write actions.
type fakeBackend struct {
	st        *store.Store
	lastSend  connectors.SendRequest
	sendErr   error // when set, Send returns it (for status-classification tests)
	lastMedia SendMediaBody
	reacted   bool
	backfill  atomic.Value
	authField string
	authValue string

	proposals  []ProposalDTO
	resolvedID string
	resolvedTo string
	renamedID  string
	renamedTo  string
	mergedA    string
	mergedB    string
	resolved   []PersonCard
}

func (f *fakeBackend) Store() *store.Store { return f.st }
func (f *fakeBackend) Send(_ context.Context, req connectors.SendRequest) (string, error) {
	f.lastSend = req
	if f.sendErr != nil {
		return "", f.sendErr
	}
	return "sent:123", nil
}
func (f *fakeBackend) SendMedia(_ context.Context, req SendMediaBody) (string, error) {
	f.lastMedia = req
	return "sent:media", nil
}
func (f *fakeBackend) React(_ context.Context, _, _, _ string) error { f.reacted = true; return nil }
func (f *fakeBackend) Backfill(_ context.Context, chatID string, _ time.Time) error {
	f.backfill.Store(chatID)
	return nil
}
func (f *fakeBackend) TapButton(_ context.Context, _, _, buttonID string) (string, error) {
	return "tapped:" + buttonID, nil
}
func (f *fakeBackend) VotePoll(_ context.Context, _, _, _ string) error { return nil }
func (f *fakeBackend) GroupInvite(_ context.Context, chatID string, _ bool) (string, error) {
	return "https://chat.whatsapp.com/INVITE-" + chatID, nil
}
func (f *fakeBackend) HonchoBackfill(_ context.Context, _, _ int) (int, int, error) {
	return 0, 0, nil
}
func (f *fakeBackend) StartChat(_ context.Context, platform, identifier string) (string, string, error) {
	return platform + ":resolved:" + identifier, "Resolved " + identifier, nil
}
func (f *fakeBackend) IGProfile(_ context.Context, handle string) (igweb.ProfileCard, error) {
	return igweb.ProfileCard{Handle: handle, Followers: 10}, nil
}
func (f *fakeBackend) IGStories(_ context.Context) (igweb.StoriesResult, error) {
	return igweb.StoriesResult{Count: 0}, nil
}
func (f *fakeBackend) IGFeed(_ context.Context, maxID string, limit int) (igweb.FeedResult, error) {
	return igweb.FeedResult{Count: 1, Posts: []igweb.FeedPost{{Author: "alice"}}, NextMaxID: maxID}, nil
}
func (f *fakeBackend) IGFollowingFeed(_ context.Context, limit int) (igweb.FeedResult, error) {
	return igweb.FeedResult{Count: 1, Posts: []igweb.FeedPost{{Author: "followed_user"}}}, nil
}
func (f *fakeBackend) IGFollowing(_ context.Context) (igweb.FollowingList, error) {
	return igweb.FollowingList{Count: 1, Follows: []igweb.FollowEntry{{Handle: "creator1", Followers: 50000, IsBusiness: true}}}, nil
}
func (f *fakeBackend) XFollowingFeed(_ context.Context, limit int) (xweb.FeedResult, error) {
	return xweb.FeedResult{Count: 1, Posts: []xweb.Post{{Platform: "x", Author: "levelsio"}}}, nil
}
func (f *fakeBackend) XProfile(_ context.Context, handle string, limit int) (xweb.ProfileCard, error) {
	return xweb.ProfileCard{Handle: handle, Followers: 5}, nil
}
func (f *fakeBackend) AuthStatus(_ context.Context, _ string) (connectors.LoginStatus, error) {
	return connectors.LoginStatus{NeedsAuth: true, Detail: "awaiting code"}, nil
}
func (f *fakeBackend) AuthSubmit(_ context.Context, _, field, value string) error {
	f.authField, f.authValue = field, value
	return nil
}
func (f *fakeBackend) ConnectorHealth(_ context.Context, name string) (ConnectorHealth, error) {
	return ConnectorHealth{Connected: true, Account: name + ":self"}, nil
}
func (f *fakeBackend) Observability(_ context.Context) (Observability, error) {
	return Observability{GeneratedAt: "2026-06-13T00:00:00Z"}, nil
}
func (f *fakeBackend) Reproject(_ context.Context) (int, error)          { return 0, nil }
func (f *fakeBackend) ReextractLIDPhones(_ context.Context) (int, error) { return 0, nil }
func (f *fakeBackend) ReextractMediaRefs(_ context.Context, _, _ string) (int, error) {
	return 0, nil
}
func (f *fakeBackend) RevalidateMedia(_ context.Context, _ string) (int, int, int, error) {
	return 0, 0, 0, nil
}
func (f *fakeBackend) IngestSocialActivity(_ context.Context, _ int) (int, int, int, error) {
	return 0, 0, 0, nil
}
func (f *fakeBackend) IngestXActivity(_ context.Context, _ int) (int, error) {
	return 0, nil
}
func (f *fakeBackend) DiscoverLarkP2P(_ context.Context, _ string, _ int) (bool, error) {
	return true, nil
}
func (f *fakeBackend) SweepBackfill(ctx context.Context, platform string, maxChats int) (SweepResult, error) {
	cands, err := f.st.BackfillCandidates(ctx, platform, maxChats)
	if err != nil {
		return SweepResult{}, err
	}
	n, sample, _ := f.st.LowTierBackfillStats(ctx, platform, 10)
	return SweepResult{Queued: len(cands) > 0, Candidates: len(cands), LowGated: n, LowSample: sample}, nil
}
func (f *fakeBackend) RunDeterministicMerges(_ context.Context, _ bool) (int, error) {
	return 0, nil
}
func (f *fakeBackend) RunDeterministicProposals(_ context.Context) (int, error) { return 0, nil }
func (f *fakeBackend) PendingProposals(_ context.Context, limit, offset int) ([]ProposalDTO, error) {
	if offset >= len(f.proposals) {
		return nil, nil
	}
	end := offset + limit
	if limit <= 0 || end > len(f.proposals) {
		end = len(f.proposals)
	}
	return f.proposals[offset:end], nil
}
func (f *fakeBackend) ResolveProposal(_ context.Context, id, decision string) (*ResolveOutcome, error) {
	f.resolvedID, f.resolvedTo = id, decision
	if decision == "approve" {
		return &ResolveOutcome{SurvivorID: "survivor-" + id, SurvivorName: "Survivor"}, nil
	}
	return nil, nil
}
func (f *fakeBackend) MergeCanonicals(_ context.Context, a, b string) (*ResolveOutcome, error) {
	f.mergedA, f.mergedB = a, b
	return &ResolveOutcome{SurvivorID: a, SurvivorName: "Survivor", AbsorbedID: b}, nil
}
func (f *fakeBackend) BackfillSenderNames(_ context.Context, platform string) (*BackfillResult, error) {
	return &BackfillResult{Platform: platform}, nil
}
func (f *fakeBackend) RenameCanonical(_ context.Context, canonID, name string) error {
	f.renamedID, f.renamedTo = canonID, name
	return nil
}
func (f *fakeBackend) ResolvePerson(_ context.Context, q string) ([]PersonCard, error) {
	return f.resolved, nil
}
func (f *fakeBackend) CoreNetwork(_ context.Context, _ string, _ int) ([]PersonCard, error) {
	return f.resolved, nil
}
func (f *fakeBackend) RecomputeSalience(_ context.Context) (int, error) { return len(f.resolved), nil }
func (f *fakeBackend) Search(ctx context.Context, query, platform, person string, since time.Time, limit int) ([]event.Message, error) {
	return f.st.Search(ctx, query, platform, person, since, limit)
}
func (f *fakeBackend) Grep(ctx context.Context, pattern, platform, person string, since time.Time, limit int) (GrepResult, error) {
	r, err := f.st.Grep(ctx, pattern, platform, nil, since, limit)
	return GrepResult{Matches: r.Matches, Scanned: r.Scanned, Truncated: r.Truncated}, err
}
func (f *fakeBackend) QuerySQL(ctx context.Context, query string) (store.QuerySQLResult, error) {
	return f.st.QuerySQL(ctx, query, 1000)
}
func (f *fakeBackend) MediaInfo(_ context.Context, id string) (MediaMeta, error) {
	return MediaMeta{ID: id, Type: "image", MIME: "image/jpeg", Size: 3, Stored: true, LocalPath: "/blobs/" + id}, nil
}
func (f *fakeBackend) MediaStream(_ context.Context, id string) (io.ReadCloser, MediaMeta, error) {
	return io.NopCloser(strings.NewReader("img")), MediaMeta{ID: id, MIME: "image/jpeg", Name: "x.jpg"}, nil
}
func (f *fakeBackend) ReprojectMedia(_ context.Context, _ string) (int, error) { return 0, nil }

func setup(t *testing.T) (*Client, *fakeBackend, func()) {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	be := &fakeBackend{st: st}
	srv := httptest.NewServer(NewServer(be, NewHub()).Handler())
	c := &Client{base: srv.URL, http: srv.Client()}
	return c, be, func() { srv.Close(); st.Close() }
}

func seed(t *testing.T, st *store.Store) {
	t.Helper()
	ctx := context.Background()
	t0 := time.Date(2026, 6, 9, 8, 0, 0, 0, time.UTC)
	m := event.Message{
		Platform: event.PlatformWhatsApp, ChatID: "whatsapp:c1", ChatType: event.ChatDM,
		Sender: event.Sender{PlatformUserID: "u1", DisplayName: "Alice"}, TS: t0,
		Kind: event.KindText, Text: "lunch tomorrow", Source: event.SourceLive,
	}
	m.SetID("A")
	if _, err := st.Append(ctx, m, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

// TestSendStatusClassification proves the #39 /send 502→400 split: caller faults
// (empty chat_id, empty body, unroutable chat_id → ErrNoConnector) return 400, while a
// genuine upstream/platform failure stays 502, and a successful send is 200.
func TestSendStatusClassification(t *testing.T) {
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()
	be := &fakeBackend{st: st}
	srv := httptest.NewServer(NewServer(be, NewHub()).Handler())
	defer srv.Close()

	post := func(body string) int {
		resp, perr := srv.Client().Post(srv.URL+"/send", "application/json", strings.NewReader(body))
		if perr != nil {
			t.Fatalf("post: %v", perr)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	cases := []struct {
		name string
		body string
		err  error
		want int
	}{
		{"empty chat_id", `{"text":"hi"}`, nil, http.StatusBadRequest},
		{"empty body", `{"chat_id":"whatsapp:c1"}`, nil, http.StatusBadRequest},
		{"unroutable → ErrNoConnector", `{"chat_id":"bogus:x","text":"hi"}`, connectors.ErrNoConnector, http.StatusBadRequest},
		{"wrapped ErrNoConnector", `{"chat_id":"bogus:x","text":"hi"}`, fmt.Errorf("send: %w", connectors.ErrNoConnector), http.StatusBadRequest},
		{"upstream failure → 502", `{"chat_id":"whatsapp:c1","text":"hi"}`, fmt.Errorf("lark api 503"), http.StatusBadGateway},
		{"success → 200", `{"chat_id":"whatsapp:c1","text":"hi"}`, nil, http.StatusOK},
	}
	for _, tc := range cases {
		be.sendErr = tc.err
		if got := post(tc.body); got != tc.want {
			t.Errorf("%s: status = %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestAPIReadAndSearch(t *testing.T) {
	c, be, done := setup(t)
	defer done()
	seed(t, be.st)
	ctx := context.Background()

	msgs, err := c.Read(ctx, "whatsapp:c1", time.Time{}, 10)
	if err != nil || len(msgs) != 1 || msgs[0].Text != "lunch tomorrow" {
		t.Fatalf("read: %v %+v", err, msgs)
	}

	hits, err := c.Search(ctx, "lunch", "", "", time.Time{}, 10)
	if err != nil || len(hits) != 1 || hits[0].ID != "whatsapp:A" {
		t.Fatalf("search: %v %+v", err, hits)
	}

	chats, err := c.ListChats(ctx, "", time.Time{}, 10)
	if err != nil || len(chats) != 1 || chats[0].ChatID != "whatsapp:c1" {
		t.Fatalf("listchats: %v %+v", err, chats)
	}
}

func TestAPIGrep(t *testing.T) {
	c, be, done := setup(t)
	defer done()
	seed(t, be.st) // one message: "lunch tomorrow"
	ctx := context.Background()

	// RE2 with inline case-insensitive flag → 1 match, not truncated.
	res, err := c.Grep(ctx, `(?i)LUNCH`, "", "", time.Time{}, 10)
	if err != nil || len(res.Matches) != 1 || res.Matches[0].ID != "whatsapp:A" {
		t.Fatalf("grep match: %v %+v", err, res)
	}
	if res.Truncated {
		t.Fatalf("tiny corpus must not be truncated")
	}

	// A pattern that matches nothing → 0 matches, clean.
	if res, err := c.Grep(ctx, `nope-\d+`, "", "", time.Time{}, 10); err != nil || len(res.Matches) != 0 {
		t.Fatalf("grep no-match: %v %+v", err, res)
	}

	// Invalid regex → surfaced as an error (handler maps it to 400, not 500).
	if _, err := c.Grep(ctx, `(unclosed`, "", "", time.Time{}, 10); err == nil {
		t.Fatal("grep invalid pattern must return an error")
	}
}

func TestAPIConnectorHealth(t *testing.T) {
	c, _, done := setup(t)
	defer done()
	// Direct GET (no Client method — olympus polls the endpoint); use the raw http client.
	resp, err := c.http.Get(c.base + "/connectors/gmail/health")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var h ConnectorHealth
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !h.Connected || h.Account != "gmail:self" {
		t.Fatalf("health: %+v", h)
	}
}

func TestAPIQuerySQL(t *testing.T) {
	c, be, done := setup(t)
	defer done()
	seed(t, be.st) // one message
	ctx := context.Background()

	// Valid read-only SELECT round-trips through /query-sql.
	res, err := c.QuerySQL(ctx, "SELECT COUNT(*) AS n FROM messages")
	if err != nil || len(res.Rows) != 1 || len(res.Columns) != 1 {
		t.Fatalf("query_sql select: %v cols=%v rows=%v", err, res.Columns, res.Rows)
	}

	// A write is rejected (handler maps store.ErrUnsafeSQL → 400 → client error).
	if _, err := c.QuerySQL(ctx, "DELETE FROM messages"); err == nil {
		t.Fatal("query_sql write must return an error")
	}
	// Multi-statement rejected.
	if _, err := c.QuerySQL(ctx, "SELECT 1; DROP TABLE messages"); err == nil {
		t.Fatal("query_sql multi-statement must return an error")
	}
}

func TestAPISendReactBackfill(t *testing.T) {
	c, be, done := setup(t)
	defer done()
	ctx := context.Background()

	sentID, err := c.Send(ctx, SendBody{ChatID: "whatsapp:c1", Text: "hi", ReplyTo: "whatsapp:A"})
	if err != nil || sentID != "sent:123" {
		t.Fatalf("send: %v %q", err, sentID)
	}
	if be.lastSend.ChatID != "whatsapp:c1" || be.lastSend.Text != "hi" || be.lastSend.ReplyTo != "whatsapp:A" {
		t.Fatalf("send not forwarded: %+v", be.lastSend)
	}

	sentID, err = c.SendMedia(ctx, SendMediaBody{
		ChatID:        "whatsapp:c1",
		MediaID:       "whatsapp:A#0",
		Text:          "see attached",
		Filename:      "quote.pdf",
		MIME:          "application/pdf",
		MarkForwarded: true,
	})
	if err != nil || sentID != "sent:media" {
		t.Fatalf("send media: %v %q", err, sentID)
	}
	if be.lastMedia.ChatID != "whatsapp:c1" || be.lastMedia.MediaID != "whatsapp:A#0" || be.lastMedia.Filename != "quote.pdf" || !be.lastMedia.MarkForwarded {
		t.Fatalf("send_media not forwarded: %+v", be.lastMedia)
	}

	if err := c.React(ctx, ReactBody{ChatID: "whatsapp:c1", MessageID: "whatsapp:A", Emoji: "👍"}); err != nil {
		t.Fatalf("react: %v", err)
	}
	if !be.reacted {
		t.Fatal("react not forwarded")
	}

	if err := c.Backfill(ctx, BackfillBody{ChatID: "whatsapp:c1"}); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	// Backfill is async; give the goroutine a moment.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && be.backfill.Load() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	if be.backfill.Load() != "whatsapp:c1" {
		t.Fatalf("backfill not forwarded: %q", be.backfill.Load())
	}
}

func TestAPIPeopleReviewFlow(t *testing.T) {
	c, be, done := setup(t)
	defer done()
	be.proposals = []ProposalDTO{{
		ID: "canonA|canonB", A: "canonA", B: "canonB",
		AName: "Robin Example", BName: "Robin E.", Score: 0.86, Reason: "fuzzy name match",
	}}
	ctx := context.Background()

	got, err := c.PendingProposals(ctx, 15, 0)
	if err != nil || len(got) != 1 || got[0].AName != "Robin Example" || got[0].A != "canonA" {
		t.Fatalf("proposals: %v %+v", err, got)
	}

	outc, err := c.ResolveProposal(ctx, "canonA|canonB", "approve")
	if err != nil {
		t.Fatalf("resolve approve: %v", err)
	}
	if outc == nil || outc.SurvivorID == "" {
		t.Fatalf("approve should return a survivor outcome, got %+v", outc)
	}
	if be.resolvedID != "canonA|canonB" || be.resolvedTo != "approve" {
		t.Fatalf("resolve not forwarded: %q %q", be.resolvedID, be.resolvedTo)
	}

	if rej, err := c.ResolveProposal(ctx, "canonC|canonD", "reject"); err != nil || rej != nil {
		t.Fatalf("resolve reject: %v outcome=%+v", err, rej)
	}
	if be.resolvedID != "canonC|canonD" || be.resolvedTo != "reject" {
		t.Fatalf("reject not forwarded: %q %q", be.resolvedID, be.resolvedTo)
	}

	if err := c.RenameCanonical(ctx, "canonA", "Robin Example Long"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if be.renamedID != "canonA" || be.renamedTo != "Robin Example Long" {
		t.Fatalf("rename not forwarded: %q %q", be.renamedID, be.renamedTo)
	}

	// Validation: a bad decision and an empty name must 400 (not silent no-op).
	if _, err := c.ResolveProposal(ctx, "canonA|canonB", "maybe"); err == nil {
		t.Fatal("expected error for invalid decision")
	}
	if err := c.RenameCanonical(ctx, "canonA", ""); err == nil {
		t.Fatal("expected error for empty name")
	}

	// Operator-directed force-merge → forwards both ids, returns the survivor.
	mo, err := c.MergeCanonicals(ctx, "canonX", "canonY")
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if mo == nil || mo.SurvivorID == "" || mo.AbsorbedID != "canonY" {
		t.Fatalf("merge should return survivor outcome, got %+v", mo)
	}
	if be.mergedA != "canonX" || be.mergedB != "canonY" {
		t.Fatalf("merge not forwarded: %q %q", be.mergedA, be.mergedB)
	}
	// {a,b} preferred; {from_node,into_node} is the accepted alias.
	if a, b := (MergeBody{A: "x", B: "y"}).Pair(); a != "x" || b != "y" {
		t.Fatalf("Pair() {a,b}: %q %q", a, b)
	}
	if a, b := (MergeBody{FromNode: "f", IntoNode: "i"}).Pair(); a != "f" || b != "i" {
		t.Fatalf("Pair() {from_node,into_node} alias: %q %q", a, b)
	}
	// Validation: missing id and a==b must 400.
	if _, err := c.MergeCanonicals(ctx, "canonX", ""); err == nil {
		t.Fatal("expected error for missing merge id")
	}
	if _, err := c.MergeCanonicals(ctx, "canonX", "canonX"); err == nil {
		t.Fatal("expected error for self-merge")
	}
}

func TestAPIResolvePerson(t *testing.T) {
	c, be, done := setup(t)
	defer done()
	ctx := context.Background()

	// No match → in_graph=false, hits=[] (never null, so the agent can len()).
	res, err := c.ResolvePerson(ctx, "nobody")
	if err != nil || res == nil || res.Query != "nobody" || res.InGraph || len(res.Hits) != 0 {
		t.Fatalf("empty resolve: %v %+v", err, res)
	}

	// A graph hit round-trips the full card through the wire envelope.
	be.resolved = []PersonCard{{
		CanonicalID: "p1", Name: "Alex Example", Identified: true,
		Platforms:  []string{"lark"},
		Identities: []CardIdentity{{Platform: "lark", UserID: "ou_1", DisplayName: "Bình An Đỗ"}},
		Emails:     []string{"mai@example.com"}, MatchedOn: "name", Source: "graph",
	}}
	res, err = c.ResolvePerson(ctx, "Alex Example")
	if err != nil || res == nil || !res.InGraph || len(res.Hits) != 1 {
		t.Fatalf("resolve hit: %v %+v", err, res)
	}

	if got := res.Hits[0]; got.CanonicalID != "p1" || got.Name != "Alex Example" ||
		len(got.Identities) != 1 || got.Identities[0].UserID != "ou_1" || got.Emails[0] != "mai@example.com" {
		t.Fatalf("card not round-tripped: %+v", res.Hits[0])
	}

	// A directory-only result (all source:"lark") has hits but is NOT in the
	// message graph → in_graph must be false (derived from source, not count).
	be.resolved = []PersonCard{{CanonicalID: "d1", Name: "Alex Example", Source: "lark", OpenIDs: []string{"ou_x"}}}
	res, err = c.ResolvePerson(ctx, "Alex Example")
	if err != nil || res == nil || res.InGraph || len(res.Hits) != 1 {
		t.Fatalf("directory-only should be in_graph=false with 1 hit: %v %+v", err, res)
	}

	// Empty query is a 400 (not a silent empty result).
	if _, err := c.ResolvePerson(ctx, ""); err == nil {
		t.Fatal("expected error for empty query")
	}
}

func TestAPIPushStream(t *testing.T) {
	st, _ := store.Open(":memory:")
	defer st.Close()
	hub := NewHub()
	be := &fakeBackend{st: st}
	srv := httptest.NewServer(NewServer(be, hub).Handler())
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/events", nil)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer resp.Body.Close()

	// Broadcast an event; it should appear on the SSE stream.
	go func() {
		time.Sleep(20 * time.Millisecond)
		hub.OnIngest(event.Message{ID: "whatsapp:Z", Text: "ping"})
	}()

	buf := make([]byte, 256)
	resp.Body.Read(buf) // flush headers / first write
	got := string(buf)
	if !strings.Contains(got, "data:") && !strings.Contains(got, "whatsapp:Z") {
		// Read once more in case the first read only caught the flush.
		n, _ := resp.Body.Read(buf)
		got += string(buf[:n])
	}
	if !strings.Contains(got, "whatsapp:Z") {
		t.Fatalf("push stream missing event, got %q", got)
	}
}
