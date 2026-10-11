package telegram

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/seahyc/switchboard-oss/internal/event"
)

func TestParseChatID(t *testing.T) {
	cases := []struct {
		in       string
		wantKind string
		wantID   int64
		wantErr  bool
	}{
		{"telegram:user:123", "user", 123, false},
		{"telegram:chat:55", "chat", 55, false},
		{"telegram:channel:77", "channel", 77, false},
		{"whatsapp:user:1", "", 0, true},
		{"telegram:bogus:1", "", 0, true},
		{"telegram:user:notnum", "", 0, true},
		{"garbage", "", 0, true},
	}
	for _, tc := range cases {
		kind, id, err := parseChatID(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseChatID(%q) expected error", tc.in)
			}
			continue
		}
		if err != nil || kind != tc.wantKind || id != tc.wantID {
			t.Errorf("parseChatID(%q) = (%q,%d,%v), want (%q,%d,nil)", tc.in, kind, id, err, tc.wantKind, tc.wantID)
		}
	}
}

func TestBotChatID(t *testing.T) {
	cases := map[string]int64{
		"telegram:user:123":   123,
		"telegram:chat:55":    -55,
		"telegram:channel:77": -1_000_000_000_077,
	}
	for in, want := range cases {
		got, err := botChatID(in)
		if err != nil || got != want {
			t.Errorf("botChatID(%q) = (%d,%v), want %d", in, got, err, want)
		}
	}
}

func TestLocalMsgID(t *testing.T) {
	cases := map[string]int{
		"telegram:user:123:456": 456,
		"telegram:chat:55:1":    1,
		"789":                   789,
	}
	for in, want := range cases {
		got, err := localMsgID(in)
		if err != nil || got != want {
			t.Errorf("localMsgID(%q) = (%d,%v), want %d", in, got, err, want)
		}
	}
	if _, err := localMsgID("telegram:user:abc"); err == nil {
		t.Error("localMsgID should error on non-numeric tail")
	}
}

func TestReplyToMsgID(t *testing.T) {
	if got := replyToMsgID(""); got != 0 {
		t.Errorf("empty reply = %d, want 0", got)
	}
	if got := replyToMsgID("telegram:user:1:42"); got != 42 {
		t.Errorf("reply = %d, want 42", got)
	}
}

func TestButtonsToMarkup(t *testing.T) {
	// Already-built markup passes through.
	mk := &inlineKeyboardMarkup{InlineKeyboard: [][]inlineButton{{{Text: "Hi", CallbackData: "x"}}}}
	got, err := buttonsToMarkup(mk)
	if err != nil || got != mk {
		t.Fatalf("passthrough failed: %v", err)
	}
	// nil → nil.
	if got, err := buttonsToMarkup(nil); err != nil || got != nil {
		t.Fatalf("nil case: %v %v", got, err)
	}
	// Generic [][]map shape (what a JSON caller would send).
	generic := []any{
		[]any{
			map[string]any{"text": "Yes", "callback_data": "yes"},
			map[string]any{"text": "Docs", "url": "https://x"},
		},
	}
	g, err := buttonsToMarkup(generic)
	if err != nil {
		t.Fatalf("generic: %v", err)
	}
	if len(g.InlineKeyboard) != 1 || len(g.InlineKeyboard[0]) != 2 {
		t.Fatalf("generic shape wrong: %+v", g)
	}
	if g.InlineKeyboard[0][0].Text != "Yes" || g.InlineKeyboard[0][0].CallbackData != "yes" {
		t.Errorf("button0 = %+v", g.InlineKeyboard[0][0])
	}
}

func TestSessionStringStorage_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "tg.session")
	s := newStringSessionStorage(path)

	if s.Exists() {
		t.Fatal("new storage should not exist")
	}
	if _, err := s.LoadSession(context.Background()); err == nil {
		t.Fatal("LoadSession on empty should error (ErrNotFound)")
	}

	want := []byte(`{"DC":2,"Addr":"x","AuthKey":"k"}`)
	if err := s.StoreSession(context.Background(), want); err != nil {
		t.Fatalf("store: %v", err)
	}
	if !s.Exists() {
		t.Fatal("storage should exist after store")
	}
	got, err := s.LoadSession(context.Background())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("round-trip = %q, want %q", got, want)
	}
	// The on-disk form is a portable base64 string.
	if s.String() == "" || s.String() == string(want) {
		t.Errorf("expected base64-encoded string on disk, got %q", s.String())
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	t.Setenv(EnvAppID, "")
	t.Setenv(EnvAppHash, "")
	t.Setenv(EnvSession, "")
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if cfg.SessionPath != "telegram.session" {
		t.Errorf("default session path = %q", cfg.SessionPath)
	}
	if cfg.HasCredentials() {
		t.Error("no creds expected")
	}

	t.Setenv(EnvAppID, "12345")
	t.Setenv(EnvAppHash, "abc")
	cfg, err = ConfigFromEnv()
	if err != nil || cfg.AppID != 12345 || !cfg.HasCredentials() {
		t.Errorf("with creds: %+v err=%v", cfg, err)
	}

	t.Setenv(EnvAppID, "notanint")
	if _, err := ConfigFromEnv(); err == nil {
		t.Error("invalid app id should error")
	}
}

func TestAuthPhaseProgression(t *testing.T) {
	c := New(Config{SessionPath: filepath.Join(t.TempDir(), "s.session")})

	// Satisfies the PhaseReporter capability the daemon type-asserts.
	var _ PhaseReporter = c
	if c.AuthPhase() != AuthIdle {
		t.Errorf("initial phase = %v, want idle", c.AuthPhase())
	}

	ctx := context.Background()
	auth := userAuthenticator{c}

	// Phone: feed via SubmitPhone while Phone() blocks; phase flips to awaiting_phone.
	go func() { _ = c.SubmitPhone(ctx, "+6591234567") }()
	if _, err := auth.Phone(ctx); err != nil {
		t.Fatalf("Phone: %v", err)
	}
	if c.AuthPhase() != AuthAwaitingPhone {
		t.Errorf("phase after Phone = %v, want awaiting_phone", c.AuthPhase())
	}

	go func() { _ = c.SubmitCode(ctx, "12345") }()
	if _, err := auth.Code(ctx, nil); err != nil {
		t.Fatalf("Code: %v", err)
	}
	if c.AuthPhase() != AuthAwaitingCode {
		t.Errorf("phase after Code = %v, want awaiting_code", c.AuthPhase())
	}

	go func() { _ = c.SubmitPassword(ctx, "hunter2") }()
	if _, err := auth.Password(ctx); err != nil {
		t.Fatalf("Password: %v", err)
	}
	if c.AuthPhase() != AuthAwaiting2FA {
		t.Errorf("phase after Password = %v, want awaiting_2fa", c.AuthPhase())
	}
	if c.AuthPhase().String() != "awaiting_2fa" {
		t.Errorf("String() = %q", c.AuthPhase().String())
	}

	// Config-provided phone short-circuits the awaiting_phone prompt.
	c2 := New(Config{Phone: "+10000000000", SessionPath: filepath.Join(t.TempDir(), "s2.session")})
	got, err := (userAuthenticator{c2}).Phone(ctx)
	if err != nil || got != "+10000000000" {
		t.Errorf("config phone = %q,%v", got, err)
	}
	if c2.AuthPhase() != AuthIdle {
		t.Errorf("config-phone path should not flip phase, got %v", c2.AuthPhase())
	}
}

func TestExtractDialogs(t *testing.T) {
	full := &tg.MessagesDialogs{
		Dialogs:  []tg.DialogClass{&tg.Dialog{Peer: &tg.PeerUser{UserID: 1}, TopMessage: 9}},
		Messages: []tg.MessageClass{&tg.Message{ID: 9, PeerID: &tg.PeerUser{UserID: 1}, Date: 1234}},
		Users:    []tg.UserClass{&tg.User{ID: 1}},
	}
	d, m, u, _, isSlice := extractDialogs(full)
	if len(d) != 1 || len(m) != 1 || len(u) != 1 || isSlice {
		t.Errorf("full dialogs: d=%d m=%d u=%d slice=%v", len(d), len(m), len(u), isSlice)
	}

	slice := &tg.MessagesDialogsSlice{
		Count:   500,
		Dialogs: []tg.DialogClass{&tg.Dialog{Peer: &tg.PeerChannel{ChannelID: 77}, TopMessage: 3}},
	}
	_, _, _, _, isSlice = extractDialogs(slice)
	if !isSlice {
		t.Error("slice result should report isSlice=true (more pages)")
	}
}

func userWithHash(id, hash int64) *tg.User {
	u := &tg.User{ID: id}
	u.SetAccessHash(hash) // sets the "has access hash" flag, mirroring decoded wire data
	return u
}

func channelWithHash(id, hash int64) *tg.Channel {
	ch := &tg.Channel{ID: id}
	ch.SetAccessHash(hash)
	return ch
}

func TestDialogInputPeer(t *testing.T) {
	users := map[int64]*tg.User{1: userWithHash(1, 111)}
	channels := map[int64]*tg.Channel{77: channelWithHash(77, 777)}
	chats := map[int64]*tg.Chat{}

	if ip, ok := dialogInputPeer(&tg.PeerUser{UserID: 1}, users, chats, channels).(*tg.InputPeerUser); !ok || ip.AccessHash != 111 {
		t.Errorf("user input peer wrong: %v", dialogInputPeer(&tg.PeerUser{UserID: 1}, users, chats, channels))
	}
	if ip, ok := dialogInputPeer(&tg.PeerChannel{ChannelID: 77}, users, chats, channels).(*tg.InputPeerChannel); !ok || ip.AccessHash != 777 {
		t.Error("channel input peer wrong")
	}
	if ip, ok := dialogInputPeer(&tg.PeerChat{ChatID: 5}, users, chats, channels).(*tg.InputPeerChat); !ok || ip.ChatID != 5 {
		t.Error("chat input peer wrong")
	}
	// Unknown user (no access hash cached) → nil so enumeration skips it.
	if got := dialogInputPeer(&tg.PeerUser{UserID: 999}, users, chats, channels); got != nil {
		t.Errorf("unknown peer should be nil, got %v", got)
	}
}

func TestMessageDateIndex(t *testing.T) {
	msgs := []tg.MessageClass{
		&tg.Message{ID: 9, PeerID: &tg.PeerUser{UserID: 1}, Date: 1000},
		&tg.MessageService{ID: 10, PeerID: &tg.PeerChat{ChatID: 5}, Date: 2000},
	}
	idx := messageDateIndex(msgs)
	if idx["user:1:9"] != 1000 {
		t.Errorf("date idx user = %d, want 1000", idx["user:1:9"])
	}
	if idx["chat:5:10"] != 2000 {
		t.Errorf("date idx chat = %d, want 2000", idx["chat:5:10"])
	}
}

func TestPeerCacheLearnFromClasses(t *testing.T) {
	pc := newPeerCache()
	pc.learnFromClasses(
		[]tg.UserClass{userWithHash(1, 111)},
		[]tg.ChatClass{
			&tg.Chat{ID: 5},
			channelWithHash(77, 777),
		},
	)
	if _, ok := pc.get("telegram:user:1"); !ok {
		t.Error("user not cached")
	}
	if _, ok := pc.get("telegram:chat:5"); !ok {
		t.Error("chat not cached")
	}
	if _, ok := pc.get("telegram:channel:77"); !ok {
		t.Error("channel not cached")
	}
	// resolve uses the cached access-hashed peer.
	ip, err := pc.resolve("telegram:channel:77")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if c, ok := ip.(*tg.InputPeerChannel); !ok || c.AccessHash != 777 {
		t.Errorf("resolved channel peer wrong: %v", ip)
	}
}

func TestTitleForPeer(t *testing.T) {
	users := map[int64]*tg.User{1: {ID: 1, FirstName: "Jordan", LastName: "Example"}, 2: {ID: 2, Username: "bob"}}
	chats := map[int64]*tg.Chat{5: {ID: 5, Title: "Project Chat"}}
	channels := map[int64]*tg.Channel{77: {ID: 77, Title: "Announcements"}}

	if got := titleForPeer(&tg.PeerUser{UserID: 1}, users, chats, channels); got != "Jordan Example" {
		t.Errorf("DM title = %q, want Jordan Example", got)
	}
	if got := titleForPeer(&tg.PeerUser{UserID: 2}, users, chats, channels); got != "bob" {
		t.Errorf("username-only DM title = %q, want bob", got)
	}
	if got := titleForPeer(&tg.PeerChat{ChatID: 5}, users, chats, channels); got != "Project Chat" {
		t.Errorf("group title = %q", got)
	}
	if got := titleForPeer(&tg.PeerChannel{ChannelID: 77}, users, chats, channels); got != "Announcements" {
		t.Errorf("channel title = %q", got)
	}
	// Unknown peer → "" so the daemon derives a name from sender display_name.
	if got := titleForPeer(&tg.PeerUser{UserID: 999}, users, chats, channels); got != "" {
		t.Errorf("unknown peer title = %q, want empty", got)
	}
}

func TestIsMuted(t *testing.T) {
	now := 1000
	var muted tg.PeerNotifySettings
	muted.SetMuteUntil(2000) // future → muted
	if !isMuted(muted, now) {
		t.Error("future mute_until should be muted")
	}
	var past tg.PeerNotifySettings
	past.SetMuteUntil(500) // past → not muted
	if isMuted(past, now) {
		t.Error("past mute_until should not be muted")
	}
	var unset tg.PeerNotifySettings // no mute_until → not muted
	if isMuted(unset, now) {
		t.Error("unset mute_until should not be muted")
	}
}

func TestInputPeerToChatID(t *testing.T) {
	cases := map[tg.InputPeerClass]string{
		&tg.InputPeerUser{UserID: 1}:       "telegram:user:1",
		&tg.InputPeerChat{ChatID: 5}:       "telegram:chat:5",
		&tg.InputPeerChannel{ChannelID: 7}: "telegram:channel:7",
		&tg.InputPeerEmpty{}:               "",
	}
	for ip, want := range cases {
		if got := inputPeerToChatID(ip); got != want {
			t.Errorf("inputPeerToChatID(%T) = %q, want %q", ip, got, want)
		}
	}
}

func TestSignUpRejected(t *testing.T) {
	c := New(Config{SessionPath: filepath.Join(t.TempDir(), "s.session")})
	if _, err := (userAuthenticator{c}).SignUp(context.Background()); err == nil {
		t.Error("SignUp should be rejected (account must already exist)")
	}
}

func TestNewBuildsWithoutCredentials(t *testing.T) {
	// New must never dial — building a credential-less connector is valid.
	c := New(Config{SessionPath: filepath.Join(t.TempDir(), "s.session")})
	if c.Name() != "telegram" {
		t.Errorf("Name = %q", c.Name())
	}
	// Connect without credentials should fail fast, not panic.
	err := c.Connect(context.Background(), make(chan event.Message, 1))
	if err != ErrNoCredentials {
		t.Errorf("Connect without creds = %v, want ErrNoCredentials", err)
	}
	st, _ := c.Status(context.Background())
	if st.Connected {
		t.Error("should not be connected")
	}
}
