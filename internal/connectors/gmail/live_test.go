package gmail

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// TestLiveStandaloneCredentials reads at most one message using an explicitly
// selected account and private JSON credentials. It never sends or marks mail.
func TestLiveStandaloneCredentials(t *testing.T) {
	if os.Getenv("GMAIL_LIVE") != "1" || os.Getenv("GMAIL_CREDENTIALS_FILE") == "" {
		t.Skip("set GMAIL_LIVE=1, GMAIL_CREDENTIALS_FILE and GMAIL_LIVE_ACCOUNT")
	}
	addr := os.Getenv("GMAIL_LIVE_ACCOUNT")
	if addr == "" {
		t.Fatal("GMAIL_LIVE_ACCOUNT must select one mailbox")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := New(ctx, Config{
		CredentialsFile: os.Getenv("GMAIL_CREDENTIALS_FILE"),
		Accounts:        []string{addr}, StatePath: t.TempDir() + "/state.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.accounts) != 1 {
		t.Fatal("credential allowlist did not select exactly one mailbox")
	}
	status, err := c.Login(ctx)
	if err != nil || !status.Connected {
		t.Fatalf("standalone credential authentication failed: %v", err)
	}
	acc := c.firstAccount()
	list, err := acc.svc.Users.Messages.List("me").MaxResults(1).Context(ctx).Do()
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Messages) == 0 {
		t.Fatal("selected mailbox needs one fixture message")
	}
	m, ok := c.fetchAndMap(ctx, acc, list.Messages[0].Id, event.SourceBackfill)
	if !ok || m.ID == "" || m.Platform != event.PlatformGmail || m.ChatType != event.ChatEmail || m.TS.IsZero() {
		t.Fatal("live message did not map to the canonical schema")
	}
	t.Log("standalone credentials: authentication, explicit account allowlist, and one-message read/mapping passed")
}

// TestLiveSmoke exercises the real path against actual gw-cli credentials:
// load keyring -> mint Gmail token via oauth2 refresh -> fetch a message ->
// map it to a canonical event. Gated on GMAIL_LIVE=1 so it never runs in CI.
//
//	GMAIL_LIVE=1 GMAIL_KEYRING=/path/to/keyring_pass.cfg go test -run TestLiveSmoke -v ./internal/connectors/gmail/
func TestLiveSmoke(t *testing.T) {
	if os.Getenv("GMAIL_LIVE") != "1" {
		t.Skip("set GMAIL_LIVE=1 to run the live Gmail smoke test")
	}
	keyring := os.Getenv("GMAIL_KEYRING")
	if keyring == "" {
		keyring = DefaultKeyringPath()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stateDir := t.TempDir()
	c, err := New(ctx, Config{
		KeyringPath: keyring,
		StatePath:   stateDir + "/state.json",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Logf("usable gmail accounts: %v", c.order)

	st, err := c.Login(ctx)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	t.Logf("login: connected=%v detail=%q", st.Connected, st.Detail)
	if !st.Connected {
		t.Fatal("expected at least one authenticated account")
	}

	// Backfill the last 24h into a channel and assert we map at least one event
	// cleanly into the frozen schema.
	out := make(chan event.Message, 64)
	go func() {
		_ = c.Backfill(ctx, "", time.Now().Add(-24*time.Hour), out)
		close(out)
	}()

	n := 0
	for m := range out {
		n++
		if m.Platform != event.PlatformGmail || m.ChatType != event.ChatEmail {
			t.Errorf("bad event: platform=%q chat_type=%q", m.Platform, m.ChatType)
		}
		if m.ID == "" || m.AccountID == "" || m.ChatID == "" {
			t.Errorf("missing ids: %+v", m)
		}
		if n <= 3 {
			t.Logf("event: id=%s from=%s subj-text=%.40q mentions=%v", m.ID, m.Sender.Email, m.Text, m.Mentions)
		}
		if n >= 5 {
			break
		}
	}
	t.Logf("mapped %d live events", n)
}

// TestLiveChatMetadata validates the chat-metadata projection against the real
// mailbox: subjects + labels + pinned/archived/importance signals. Gated on
// GMAIL_LIVE=1.
func TestLiveChatMetadata(t *testing.T) {
	if os.Getenv("GMAIL_LIVE") != "1" {
		t.Skip("set GMAIL_LIVE=1 to run the live Gmail metadata test")
	}
	keyring := os.Getenv("GMAIL_KEYRING")
	if keyring == "" {
		keyring = DefaultKeyringPath()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	c, err := New(ctx, Config{KeyringPath: keyring, StatePath: t.TempDir() + "/state.json"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Only enumerate a handful of threads for the smoke test.
	metas, err := c.ChatMetadata(ctx)
	if err != nil {
		t.Logf("ChatMetadata returned err (may be partial): %v", err)
	}
	if len(metas) == 0 {
		t.Fatal("expected at least one chat metadata entry")
	}
	t.Logf("got %d chat metadata entries", len(metas))
	for i, m := range metas {
		if !strings.HasPrefix(m.ChatID, "gmail:") {
			t.Errorf("bad chat_id %q", m.ChatID)
		}
		if i < 5 {
			t.Logf("meta: id=%s title=%.40q pinned=%v archived=%v importance=%q labels=%v",
				m.ChatID, m.Title, m.Pinned, m.Archived, m.Importance, m.Labels)
		}
	}
}
