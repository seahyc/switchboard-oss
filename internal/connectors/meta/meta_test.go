package meta

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/connectors"
	"github.com/seahyc/switchboard-oss/internal/event"
)

// fakeBackend is an in-memory matrixBackend for unit tests — no homeserver.
type fakeBackend struct {
	self    string
	live    []MatrixEvent            // events the sync loop will emit
	history map[string][]MatrixEvent // roomID → history newest→oldest
	rooms   map[string]RoomInfo

	// emit, when non-nil, gates live emission until the test closes it — lets a
	// test register ALL subscribers before any event is dispatched, so fan-out is
	// deterministic regardless of goroutine scheduling (no drop-race under load).
	emit chan struct{}

	sent     []sentMsg
	reacted  []string
	loggedIn map[string]bool

	// StartChat canned response.
	startRoom, startThread, startName string
	startErr                          error
	startCalls                        []string
}

type sentMsg struct {
	room, text, replyTo string
	media               bool
}

func newFake(self string) *fakeBackend {
	return &fakeBackend{
		self:     self,
		history:  map[string][]MatrixEvent{},
		rooms:    map[string]RoomInfo{},
		loggedIn: map[string]bool{},
	}
}

func (f *fakeBackend) SelfMXID() string { return f.self }

func (f *fakeBackend) Sync(ctx context.Context, onEvent func(MatrixEvent)) error {
	if f.emit != nil {
		select {
		case <-f.emit: // wait until the test says all subscribers are registered
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, ev := range f.live {
		onEvent(ev)
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeBackend) Paginate(ctx context.Context, roomID, from string, limit int) ([]MatrixEvent, string, error) {
	return f.history[roomID], "", nil // single page
}

func (f *fakeBackend) JoinedRooms(ctx context.Context) ([]string, error) {
	out := make([]string, 0, len(f.rooms))
	for r := range f.rooms {
		out = append(out, r)
	}
	return out, nil
}

func (f *fakeBackend) RoomInfo(ctx context.Context, roomID string) (RoomInfo, error) {
	return f.rooms[roomID], nil
}

func (f *fakeBackend) SendText(ctx context.Context, roomID, text, replyTo string) (string, error) {
	f.sent = append(f.sent, sentMsg{room: roomID, text: text, replyTo: replyTo})
	return "sent1", nil
}

func (f *fakeBackend) SendMedia(ctx context.Context, roomID string, att event.Attachment, data []byte) (string, error) {
	f.sent = append(f.sent, sentMsg{room: roomID, media: true})
	return "sentmedia1", nil
}

func (f *fakeBackend) React(ctx context.Context, roomID, target, emoji string) (string, error) {
	f.reacted = append(f.reacted, roomID+"|"+target+"|"+emoji)
	return "rx1", nil
}

func (f *fakeBackend) Download(ctx context.Context, mxc string) (io.ReadCloser, int64, error) {
	return io.NopCloser(strings.NewReader("bytes-of-" + mxc)), int64(len("bytes-of-" + mxc)), nil
}

func (f *fakeBackend) LoggedIn(ctx context.Context, network string) (bool, string, error) {
	return f.loggedIn[network], "fake", nil
}

func (f *fakeBackend) StartChat(ctx context.Context, network, identifier string) (string, string, string, error) {
	f.startCalls = append(f.startCalls, network+":"+identifier)
	return f.startRoom, f.startThread, f.startName, f.startErr
}

func newTestClient(t *testing.T, f *fakeBackend) *Client {
	t.Helper()
	return NewClient(Config{
		StatePath:  filepath.Join(t.TempDir(), "state.json"),
		SelfHandle: "me_ig",
	}, f)
}

func TestFacadeNames(t *testing.T) {
	c := newTestClient(t, newFake("@sb:meta.local"))
	if c.Instagram().Name() != "instagram" || c.Messenger().Name() != "messenger" {
		t.Fatal("facade names wrong")
	}
}

func TestConnect_FanoutByNetwork(t *testing.T) {
	f := newFake("@sb:meta.local")
	f.live = []MatrixEvent{
		{RoomID: "!ig:meta.local", EventID: "$1", Sender: "@meta_1:meta.local", Type: evTypeMessage,
			Network: PlatformInstagram, TSMillis: 100, MsgType: msgText, Body: "ig msg",
			RemoteMsgID: "mid.IG", RemoteThreadID: "T_IG"},
		{RoomID: "!fb:meta.local", EventID: "$2", Sender: "@meta_2:meta.local", Type: evTypeMessage,
			Network: PlatformMessenger, TSMillis: 101, MsgType: msgText, Body: "fb msg",
			RemoteMsgID: "mid.FB", RemoteThreadID: "T_FB"},
	}
	f.emit = make(chan struct{}) // hold emission until both facades have subscribed
	c := newTestClient(t, f)

	igOut := make(chan event.Message, 4)
	fbOut := make(chan event.Message, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Instagram().Connect(ctx, igOut)
	go c.Messenger().Connect(ctx, fbOut)

	// Deterministic: wait until BOTH networks have a registered subscriber, THEN
	// release emission. This removes the startup race where the shared sync loop
	// (started by the first Connect) could dispatch the messenger event before the
	// messenger facade subscribed — which would drop it and hang the test.
	waitForSubs(t, c, PlatformInstagram, PlatformMessenger)
	close(f.emit)

	ig := recv(t, igOut)
	if ig.Platform != "instagram" || ig.Text != "ig msg" {
		t.Errorf("instagram subscriber got %+v", ig)
	}
	fb := recv(t, fbOut)
	if fb.Platform != "messenger" || fb.Text != "fb msg" {
		t.Errorf("messenger subscriber got %+v", fb)
	}
	// IG subscriber must NOT receive the messenger message.
	select {
	case extra := <-igOut:
		t.Errorf("instagram subscriber leaked a cross-network message: %+v", extra)
	case <-time.After(50 * time.Millisecond):
	}

	// Sync loop started exactly once though both facades subscribed.
	c.mu.Lock()
	started := c.syncStarted
	c.mu.Unlock()
	if !started {
		t.Error("sync not started")
	}
}

func TestBackfill_SinceBoundAndSource(t *testing.T) {
	f := newFake("@sb:meta.local")
	room := "!ig:meta.local"
	f.history[room] = []MatrixEvent{
		{RoomID: room, EventID: "$new", Sender: "@meta_1:meta.local", Type: evTypeMessage, Network: PlatformInstagram, TSMillis: 5000, MsgType: msgText, Body: "recent", RemoteMsgID: "r1", RemoteThreadID: "T"},
		{RoomID: room, EventID: "$old", Sender: "@meta_1:meta.local", Type: evTypeMessage, Network: PlatformInstagram, TSMillis: 1000, MsgType: msgText, Body: "too old", RemoteMsgID: "r2", RemoteThreadID: "T"},
	}
	c := newTestClient(t, f)
	c.st.upsertRoom(room, "instagram:T", PlatformInstagram, false, "")

	out := make(chan event.Message, 8)
	if err := c.Instagram().Backfill(context.Background(), "instagram:T", time.UnixMilli(3000), out); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	close(out)
	var got []event.Message
	for m := range out {
		got = append(got, m)
	}
	if len(got) != 1 {
		t.Fatalf("emitted %d, want 1 (since bound drops the old one)", len(got))
	}
	if got[0].Text != "recent" || got[0].Source != event.SourceBackfill {
		t.Errorf("backfilled msg = %+v", got[0])
	}
}

func TestSend_TextReplyRouting(t *testing.T) {
	f := newFake("@sb:meta.local")
	c := newTestClient(t, f)
	c.st.upsertRoom("!ig:meta.local", "instagram:T", PlatformInstagram, false, "")

	id, err := c.Instagram().Send(context.Background(), connectors.SendRequest{
		ChatID:  "instagram:T",
		Text:    "hello",
		ReplyTo: "instagram:evt:$parent",
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if id != "instagram:evt:sent1" {
		t.Errorf("sent id = %q", id)
	}
	if len(f.sent) != 1 || f.sent[0].room != "!ig:meta.local" || f.sent[0].text != "hello" || f.sent[0].replyTo != "$parent" {
		t.Errorf("backend.SendText got %+v", f.sent)
	}
}

func TestSend_UnknownChat(t *testing.T) {
	c := newTestClient(t, newFake("@sb:meta.local"))
	if _, err := c.Instagram().Send(context.Background(), connectors.SendRequest{ChatID: "instagram:nope", Text: "x"}); err == nil {
		t.Error("expected error for unknown chat")
	}
}

func TestSend_RichUIUnsupported(t *testing.T) {
	f := newFake("@sb:meta.local")
	c := newTestClient(t, f)
	c.st.upsertRoom("!ig:meta.local", "instagram:T", PlatformInstagram, false, "")
	_, err := c.Instagram().Send(context.Background(), connectors.SendRequest{ChatID: "instagram:T", Card: map[string]string{"x": "y"}})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Errorf("expected unsupported error, got %v", err)
	}
}

func TestReact(t *testing.T) {
	f := newFake("@sb:meta.local")
	c := newTestClient(t, f)
	c.st.upsertRoom("!ig:meta.local", "instagram:T", PlatformInstagram, false, "")
	if err := c.Instagram().React(context.Background(), "instagram:T", "instagram:evt:$target", "🔥"); err != nil {
		t.Fatalf("react: %v", err)
	}
	if len(f.reacted) != 1 || f.reacted[0] != "!ig:meta.local|$target|🔥" {
		t.Errorf("reacted = %+v", f.reacted)
	}
}

func TestDownloadMedia(t *testing.T) {
	f := newFake("@sb:meta.local")
	c := newTestClient(t, f)
	rc, n, err := c.Instagram().DownloadMedia(context.Background(), "mxc://meta.local/xyz")
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	if string(b) != "bytes-of-mxc://meta.local/xyz" || n != int64(len(b)) {
		t.Errorf("download = %q (%d)", b, n)
	}
}

func TestStatus_NeedsAuthThenConnected(t *testing.T) {
	f := newFake("@sb:meta.local")
	c := newTestClient(t, f)
	st, _ := c.Instagram().Status(context.Background())
	if st.Connected || !st.NeedsAuth {
		t.Errorf("pre-login status = %+v, want NeedsAuth", st)
	}
	f.loggedIn[PlatformInstagram] = true
	st, _ = c.Instagram().Status(context.Background())
	if !st.Connected {
		t.Errorf("post-login status = %+v, want Connected", st)
	}
}

func TestChatMetadataAndContacts(t *testing.T) {
	f := newFake("@sb:meta.local")
	f.rooms["!ig:meta.local"] = RoomInfo{Network: PlatformInstagram, Title: "Jane Doe", RemoteThreadID: "T1"}
	f.rooms["!g:meta.local"] = RoomInfo{Network: PlatformInstagram, Title: "Group Chat", RemoteThreadID: "G1", IsGroup: true}
	c := newTestClient(t, f)

	metas, err := c.Instagram().ChatMetadata(context.Background())
	if err != nil {
		t.Fatalf("chatmeta: %v", err)
	}
	if len(metas) != 2 {
		t.Errorf("chat metas = %d, want 2", len(metas))
	}
	contacts, err := c.Instagram().Contacts(context.Background())
	if err != nil {
		t.Fatalf("contacts: %v", err)
	}
	// Only the DM counterpart becomes a contact (group skipped).
	if len(contacts) != 1 || contacts[0].FullName != "Jane Doe" {
		t.Errorf("contacts = %+v", contacts)
	}
}

func TestStartChat_ResolvesAndIndexes(t *testing.T) {
	f := newFake("@sb:meta.local")
	f.startRoom = "!example:example.org"
	f.startThread = "100000003"
	f.startName = "Example Profile"
	c := newTestClient(t, f)

	chatID, name, err := c.Instagram().StartChat(context.Background(), "exampleprofile")
	if err != nil {
		t.Fatalf("StartChat: %v", err)
	}
	if chatID != "instagram:100000003" {
		t.Errorf("chatID = %q, want instagram:100000003 (clean thread-id form)", chatID)
	}
	if name != "Example Profile" {
		t.Errorf("resolvedName = %q", name)
	}
	// The chat must be registered so a subsequent Send resolves it immediately.
	if rid, ok := c.st.roomForChat("instagram:100000003"); !ok || rid != "!example:example.org" {
		t.Errorf("chat not indexed after StartChat: %q,%v", rid, ok)
	}
	if len(f.startCalls) != 1 || f.startCalls[0] != "instagram:exampleprofile" {
		t.Errorf("backend.StartChat calls = %+v", f.startCalls)
	}
}

func TestStartChat_RoomFallbackWhenNoThread(t *testing.T) {
	f := newFake("@sb:meta.local")
	f.startRoom = "!only:meta.local"
	f.startThread = "" // no thread id resolved
	c := newTestClient(t, f)
	chatID, _, err := c.Messenger().StartChat(context.Background(), "12345")
	if err != nil {
		t.Fatalf("StartChat: %v", err)
	}
	if chatID != "messenger:room:!only:meta.local" {
		t.Errorf("chatID = %q, want room fallback", chatID)
	}
}

func TestStartChat_BackendError(t *testing.T) {
	f := newFake("@sb:meta.local")
	f.startErr = errInvalidProbe
	c := newTestClient(t, f)
	if _, _, err := c.Instagram().StartChat(context.Background(), "nobody"); err == nil {
		t.Error("expected error from backend StartChat")
	}
}

var errInvalidProbe = errFake("no such user")

type errFake string

func (e errFake) Error() string { return string(e) }

func recv(t *testing.T, ch <-chan event.Message) event.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(5 * time.Second): // generous for loaded CI runners
		t.Fatal("timed out waiting for message")
		return event.Message{}
	}
}

// waitForSubs blocks until every named network has at least one registered
// subscriber on the shared client (or fails after a generous deadline).
func waitForSubs(t *testing.T, c *Client, networks ...string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		ready := true
		for _, n := range networks {
			if len(c.subs[n]) == 0 {
				ready = false
				break
			}
		}
		c.mu.Unlock()
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for subscribers to register")
		}
		time.Sleep(time.Millisecond)
	}
}
