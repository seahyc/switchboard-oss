// Package honcho is a thin client for posting switchboard's canonical messages into a
// Honcho workspace (the user-model deriver). Switchboard writes into the SAME "hermes"
// workspace Hermes uses, so the user-model unifies real-relationship messages with the
// Hermes-conversation history. Our Honcho deployment runs with USE_AUTH=false, so there
// is no bearer header. All paths are /v3/workspaces/<workspace>/...
//
// The client only POSTs text + ids; Honcho's deriver does the (local, no-pay-per-token)
// embedding/inference. get-or-create peer/session are idempotent on the id you pass.
package honcho

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// DefaultBaseURL is the on-VM Honcho address (reachable from switchboardd on the host).
const DefaultBaseURL = "http://127.0.0.1:8000"

// Client posts into one Honcho workspace.
type Client struct {
	base      string
	workspace string
	http      *http.Client
}

// New builds a client. base="" → DefaultBaseURL; workspace is e.g. "hermes".
func New(base, workspace string) *Client {
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	if base == "" {
		base = DefaultBaseURL
	}
	return &Client{base: base, workspace: workspace, http: &http.Client{Timeout: 30 * time.Second}}
}

// SanitizeID maps an arbitrary id to Honcho's required id charset ^[a-zA-Z0-9_-]+$
// (alphanumeric + underscore + hyphen): every other byte → '_'. Deterministic +
// collision-stable (same input → same id), so peer/session ids derived from chat ids
// — which contain ':' '@' '.' — are accepted. Honcho 422s ("string_pattern_mismatch")
// any id with a disallowed char, so EVERY id POSTed (peer + session) must pass through
// this. Callers must apply it consistently (the same id is the peer key, the session's
// peer-map key, the message author, and the local watermark key — all must match).
func SanitizeID(s string) string {
	b := []byte(s)
	for i := 0; i < len(b); i++ {
		c := b[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
			// allowed
		default:
			b[i] = '_'
		}
	}
	return string(b)
}

// PeerConfig is a peer's observation config within a session.
type PeerConfig struct {
	ObserveMe     bool `json:"observe_me"`
	ObserveOthers bool `json:"observe_others"`
}

// Message is one message added to a session.
type Message struct {
	Content   string         `json:"content"`
	PeerID    string         `json:"peer_id"`
	CreatedAt string         `json:"created_at,omitempty"` // ISO8601 (the canonical msg ts)
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// EnsurePeer get-or-creates a peer (idempotent on id). metadata may be nil.
func (c *Client) EnsurePeer(ctx context.Context, id string, metadata map[string]any) error {
	body := map[string]any{"id": id}
	if len(metadata) > 0 {
		body["metadata"] = metadata
	}
	return c.post(ctx, "/peers", body)
}

// EnsureSession get-or-creates a session (idempotent on id) with the given peer
// membership. peers may be nil (set later); metadata may be nil.
func (c *Client) EnsureSession(ctx context.Context, id string, peers map[string]PeerConfig, metadata map[string]any) error {
	body := map[string]any{"id": id}
	if len(peers) > 0 {
		body["peers"] = peers
	}
	if len(metadata) > 0 {
		body["metadata"] = metadata
	}
	return c.post(ctx, "/sessions", body)
}

// AddMessages appends messages to a session (batched).
func (c *Client) AddMessages(ctx context.Context, sessionID string, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	return c.post(ctx, "/sessions/"+sessionID+"/messages", map[string]any{"messages": msgs})
}

func (c *Client) post(ctx context.Context, path string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	url := c.base + "/v3/workspaces/" + c.workspace + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("honcho: POST %s → HTTP %d: %s", path, resp.StatusCode, strings.TrimSpace(string(snippet)))
}
