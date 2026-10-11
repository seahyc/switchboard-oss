package honcho

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestClientWireShape locks the v3 ingest contract: paths + bodies for get-or-create
// peer, get-or-create session (with peer membership), and add-messages.
func TestClientWireShape(t *testing.T) {
	type call struct {
		path string
		body map[string]any
	}
	var calls []call
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("want POST, got %s", r.Method)
		}
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		calls = append(calls, call{r.URL.Path, m})
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "hermes")
	ctx := context.Background()

	if err := c.EnsurePeer(ctx, "Self", nil); err != nil {
		t.Fatal(err)
	}
	if err := c.EnsurePeer(ctx, "canon-1", map[string]any{"name": "Casey", "platform": "whatsapp"}); err != nil {
		t.Fatal(err)
	}
	if err := c.EnsureSession(ctx, "switchboard:whatsapp:dm:wa1",
		map[string]PeerConfig{"Self": {ObserveMe: true, ObserveOthers: true}, "canon-1": {ObserveMe: true, ObserveOthers: true}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.AddMessages(ctx, "switchboard:whatsapp:dm:wa1", []Message{
		{Content: "hi", PeerID: "canon-1", CreatedAt: "2026-06-14T09:00:00Z"},
	}); err != nil {
		t.Fatal(err)
	}

	if len(calls) != 4 {
		t.Fatalf("want 4 calls, got %d", len(calls))
	}
	// paths
	wantPaths := []string{
		"/v3/workspaces/hermes/peers",
		"/v3/workspaces/hermes/peers",
		"/v3/workspaces/hermes/sessions",
		"/v3/workspaces/hermes/sessions/switchboard:whatsapp:dm:wa1/messages",
	}
	for i, wp := range wantPaths {
		if calls[i].path != wp {
			t.Errorf("call %d path = %q, want %q", i, calls[i].path, wp)
		}
	}
	// peer get-or-create carries id
	if calls[0].body["id"] != "Self" {
		t.Errorf("peer[0] id = %v", calls[0].body["id"])
	}
	if calls[1].body["metadata"] == nil {
		t.Errorf("peer[1] should carry metadata")
	}
	// session carries id + peers membership
	if calls[2].body["id"] != "switchboard:whatsapp:dm:wa1" || calls[2].body["peers"] == nil {
		t.Errorf("session body wrong: %+v", calls[2].body)
	}
	// messages batch carries content+peer_id+created_at
	msgs, _ := calls[3].body["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("want 1 message, got %d", len(msgs))
	}
	m0, _ := msgs[0].(map[string]any)
	if m0["content"] != "hi" || m0["peer_id"] != "canon-1" || m0["created_at"] != "2026-06-14T09:00:00Z" {
		t.Errorf("message body wrong: %+v", m0)
	}
}

// TestClientNon2xxError surfaces a non-2xx as an error (so the sink logs + skips).
func TestClientNon2xxError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	if err := New(srv.URL, "hermes").EnsurePeer(context.Background(), "x", nil); err == nil {
		t.Fatal("want error on 500")
	}
}

func TestSanitizeID(t *testing.T) {
	cases := map[string]string{
		"switchboard:whatsapp:dm:whatsapp:100000000000002@lid": "switchboard_whatsapp_dm_whatsapp_100000000000002_lid",
		"canon-email-abc123": "canon-email-abc123", // hyphens preserved
		"ou_a1b2.c3@d":       "ou_a1b2_c3_d",
		"Self":               "Self",
		"plain123_OK-id":     "plain123_OK-id",
	}
	for in, want := range cases {
		if got := SanitizeID(in); got != want {
			t.Errorf("SanitizeID(%q) = %q, want %q", in, got, want)
		}
	}
	// deterministic + idempotent (sanitizing an already-clean id is a no-op)
	once := SanitizeID("a:b@c")
	if SanitizeID(once) != once {
		t.Errorf("SanitizeID not idempotent: %q → %q", once, SanitizeID(once))
	}
}
