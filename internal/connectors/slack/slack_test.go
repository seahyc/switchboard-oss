package slack

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
)

func testConnector(t *testing.T, handler http.HandlerFunc) (*Connector, *httptest.Server) {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	path := filepath.Join(t.TempDir(), "slack-token")
	if err := os.WriteFile(path, []byte("xoxp-private-test-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := New(Config{TokenFile: path, Channels: []string{"C1"}, BaseURL: s.URL, HTTPClient: s.Client()})
	if err != nil {
		t.Fatal(err)
	}
	return c, s
}

func requireAuth(t *testing.T, r *http.Request) {
	t.Helper()
	if got := r.Header.Get("Authorization"); got != "Bearer xoxp-private-test-token" {
		t.Errorf("Authorization=%q", got)
	}
}

func TestNewAuthAndRequiredAllowlist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(path, []byte("secret"), 0o600)
	if _, err := New(Config{TokenFile: path}); err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("New without channels err=%v", err)
	}
	var authCalls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("Authorization=%q", got)
		}
		if r.URL.Path != "/auth.test" {
			t.Errorf("path=%s", r.URL.Path)
		}
		authCalls.Add(1)
		_, _ = w.Write([]byte(`{"ok":true,"team_id":"T9","user_id":"U7"}`))
	}))
	defer s.Close()
	c, err := New(Config{TokenFile: path, Channels: []string{"C1"}, BaseURL: s.URL, HTTPClient: s.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if c.teamID != "T9" || c.userID != "U7" || authCalls.Load() != 1 {
		t.Fatalf("identity=%s/%s calls=%d", c.teamID, c.userID, authCalls.Load())
	}
}

func TestTokenFileAndBaseURLSafety(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{TokenFile: link, Channels: []string{"C1"}, BaseURL: "https://slack.com/api"}); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("symlink err=%v", err)
	}
	if _, err := New(Config{TokenFile: target, Channels: []string{"C1"}, BaseURL: "http://example.com/api"}); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("remote HTTP err=%v", err)
	}
	if err := validateBaseURL("https://slack.com/api"); err != nil {
		t.Fatalf("HTTPS endpoint: %v", err)
	}
}

func TestThreadAndPaginationEvents(t *testing.T) {
	var historyCalls, replyCalls atomic.Int32
	c, _ := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"team_id":"T","user_id":"U"}`))
		case "/conversations.history":
			if r.URL.Query().Get("oldest") != "100.000000" {
				t.Errorf("missing backfill lower bound")
			}
			if historyCalls.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"ok":true,"messages":[{"user":"U1","text":"root","ts":"1700000000.000001","reply_count":1}],"response_metadata":{"next_cursor":"p2"}}`))
			} else {
				_, _ = w.Write([]byte(`{"ok":true,"messages":[{"user":"U2","text":"next page","ts":"1700000001.000002"}]}`))
			}
		case "/conversations.replies":
			replyCalls.Add(1)
			_, _ = w.Write([]byte(`{"ok":true,"messages":[{"user":"U1","text":"root","ts":"1700000000.000001","reply_count":1},{"user":"U3","text":"reply","ts":"1700000000.000009","thread_ts":"1700000000.000001"}]}`))
		}
	})
	// Importing the event package explicitly keeps this test focused on the public connector contract.
	ch := make(chan event.Message, 8)
	if err := c.Backfill(context.Background(), "slack:T:C1", time.Unix(100, 0), ch); err != nil {
		t.Fatal(err)
	}
	if historyCalls.Load() != 2 || replyCalls.Load() != 1 {
		t.Fatalf("history=%d replies=%d", historyCalls.Load(), replyCalls.Load())
	}
	if len(ch) != 3 {
		t.Fatalf("got %d events", len(ch))
	}
	got := []event.Message{<-ch, <-ch, <-ch}
	if got[0].ID != "slack:T:C1:1700000000.000001" || got[0].ChatID != "slack:T:C1" {
		t.Fatalf("root=%+v", got[0])
	}
	if got[1].ID != "slack:T:C1:1700000000.000009" || got[1].ReplyTo != "slack:T:C1:1700000000.000001" {
		t.Fatalf("reply=%+v", got[1])
	}
	if got[2].Text != "next page" {
		t.Fatalf("page2=%+v", got[2])
	}
}

func TestRateLimitCancellation(t *testing.T) {
	c, _ := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.URL.Path == "/auth.test" {
			_, _ = w.Write([]byte(`{"ok":true,"team_id":"T","user_id":"U"}`))
			return
		}
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := c.api(ctx, "conversations.history", url.Values{"channel": {"C1"}}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatalf("cancellation took too long")
	}
}

func TestChangedMessageEmitsEdit(t *testing.T) {
	var text atomic.Value
	text.Store("before")
	c, _ := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.URL.Path == "/auth.test" {
			_, _ = w.Write([]byte(`{"ok":true,"team_id":"T","user_id":"U"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "messages": []any{map[string]any{"user": "U1", "text": text.Load().(string), "ts": "1700000000.000001"}}})
	})
	ch := make(chan event.Message, 2)
	if err := c.fetch(context.Background(), "C1", time.Time{}, event.SourceLive, ch); err != nil {
		t.Fatal(err)
	}
	text.Store("after")
	if err := c.fetch(context.Background(), "C1", time.Time{}, event.SourceLive, ch); err != nil {
		t.Fatal(err)
	}
	first, second := <-ch, <-ch
	if first.Kind != event.KindText || second.Kind != event.KindEdit {
		t.Fatalf("kinds=%q,%q", first.Kind, second.Kind)
	}
	if second.ID == first.ID || second.ReplyTo != first.ID || second.Text != "after" {
		t.Fatalf("edit=%+v original=%+v", second, first)
	}
	if second.EditedTS == nil || second.EditedTS.Equal(first.TS) {
		t.Fatalf("edit timestamp should be observation time: %+v", second.EditedTS)
	}
}

func TestRateLimitAboveBoundFailsWithoutEarlyRetry(t *testing.T) {
	var historyCalls atomic.Int32
	c, _ := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth.test" {
			_, _ = w.Write([]byte(`{"ok":true,"team_id":"T","user_id":"U"}`))
			return
		}
		historyCalls.Add(1)
		w.Header().Set("Retry-After", "31")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	err := c.api(context.Background(), "conversations.history", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "exceeds the 30s retry bound") {
		t.Fatalf("err=%v", err)
	}
	if historyCalls.Load() != 1 {
		t.Fatalf("retried before Retry-After: calls=%d", historyCalls.Load())
	}
}

func TestHasMoreWithoutCursorFailsClosed(t *testing.T) {
	for _, method := range []string{"conversations.history", "conversations.replies"} {
		t.Run(method, func(t *testing.T) {
			c, _ := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/auth.test" {
					_, _ = w.Write([]byte(`{"ok":true,"team_id":"T","user_id":"U"}`))
					return
				}
				_, _ = w.Write([]byte(`{"ok":true,"has_more":true,"messages":[]}`))
			})
			var err error
			if method == "conversations.history" {
				err = c.fetch(context.Background(), "C1", time.Time{}, event.SourceBackfill, make(chan event.Message, 1))
			} else {
				err = c.fetchThread(context.Background(), "C1", "1700000000.000001", event.SourceBackfill, make(chan event.Message, 1))
			}
			if err == nil || !strings.Contains(err.Error(), "has_more without a next_cursor") {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestCancelledEmitDoesNotPoisonSeenAndBackfillRepeats(t *testing.T) {
	c, _ := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth.test" {
			_, _ = w.Write([]byte(`{"ok":true,"team_id":"T","user_id":"U"}`))
		}
	})
	sm := slackMessage{User: "U1", Text: "hello", TS: "1700000000.000001"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.emitMessage(ctx, make(chan event.Message), "C1", sm, event.SourceLive, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("emit err=%v", err)
	}
	if _, ok := c.seen["slack:T:C1:1700000000.000001"]; ok {
		t.Fatal("cancelled event recorded in seen map")
	}
	first := make(chan event.Message, 1)
	if err := c.emitMessage(context.Background(), first, "C1", sm, event.SourceBackfill, ""); err != nil {
		t.Fatal(err)
	}
	second := make(chan event.Message, 1)
	if err := c.emitMessage(context.Background(), second, "C1", sm, event.SourceBackfill, ""); err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("backfill emissions=%d,%d", len(first), len(second))
	}
}

func TestPollRetriesWithoutAdvancingFailedChannel(t *testing.T) {
	var calls atomic.Int32
	var firstOldest, secondOldest string
	c, _ := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth.test" {
			_, _ = w.Write([]byte(`{"ok":true,"team_id":"T","user_id":"U"}`))
			return
		}
		n := calls.Add(1)
		if n == 1 {
			firstOldest = r.URL.Query().Get("oldest")
			_, _ = w.Write([]byte(`{"ok":false,"error":"temporary"}`))
			return
		}
		if n == 2 {
			secondOldest = r.URL.Query().Get("oldest")
		}
		_, _ = w.Write([]byte(`{"ok":true,"messages":[]}`))
	})
	c.cfg.Since = time.Unix(100, 0)
	c.cfg.PollInterval = 5 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_ = c.Connect(ctx, make(chan event.Message, 8))
	if calls.Load() < 2 || firstOldest != "100.000000" || secondOldest != firstOldest {
		t.Fatalf("calls=%d bounds=%q,%q", calls.Load(), firstOldest, secondOldest)
	}
}

func TestAPIErrorAndAllowlist(t *testing.T) {
	c, _ := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.URL.Path == "/auth.test" {
			_, _ = w.Write([]byte(`{"ok":true,"team_id":"T","user_id":"U"}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":false,"error":"missing_scope"}`))
	})
	if err := c.api(context.Background(), "conversations.history", nil, nil); err == nil || !strings.Contains(err.Error(), "missing_scope") {
		t.Fatalf("api err=%v", err)
	}
	if err := c.Backfill(context.Background(), "slack:T:OTHER", time.Time{}, make(chan event.Message)); err == nil {
		t.Fatal("out-of-allowlist chat accepted")
	}
}

func TestParseSlackTimestampKeepsPrecision(t *testing.T) {
	got, err := parseSlackTS("1700000000.123456789")
	if err != nil {
		t.Fatal(err)
	}
	if got.Unix() != 1700000000 || got.Nanosecond() != 123456789 {
		t.Fatalf("timestamp=%s", got.Format(time.RFC3339Nano))
	}
}

func TestBackfillUsesSlackMicrosecondBound(t *testing.T) {
	c, _ := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth.test" {
			_, _ = w.Write([]byte(`{"ok":true,"team_id":"T","user_id":"U"}`))
			return
		}
		// Slack accepts a six-digit microsecond fraction. A nine-digit fraction
		// can return an empty history even when the requested interval has messages.
		if r.URL.Query().Get("oldest") != "100.123456" {
			_, _ = w.Write([]byte(`{"ok":true,"messages":[]}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"messages":[{"type":"message","user":"U","ts":"101.000001","text":"after bound"}]}`))
	})
	out := make(chan event.Message, 1)
	if err := c.Backfill(context.Background(), "C1", time.Unix(100, 123456789), out); err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatal("message after a fractional history bound was missed")
	}
}
