package gbrainfeed

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"
)

// MCPClient talks to a PERSISTENT `gbrain serve` (stdio MCP) process instead of
// spawning `gbrain call …` per write. The CLI path pays ~1-2s of bun/brain
// startup on EVERY put_page — fatal when projecting thousands of identities
// (the people graph would take hours to drain). Reusing one warm server makes
// each call a single JSON-RPC round-trip (~ms), ~30x faster.
//
// Calls are serialized by a mutex (write request → read until the matching id),
// which is fine: the projector worker is sequential, and the win is removing the
// per-call process spawn, not transport parallelism. On any IO error the server
// is torn down and respawned on the next call.
type MCPClient struct {
	bin string

	mu      sync.Mutex
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	out     *bufio.Reader
	nextID  int64
	running bool

	// idleTimeout: after this long with no calls, the persistent `gbrain serve`
	// is torn down so it RELEASES the PGLite single-writer lock — otherwise it
	// would monopolize the file-backed brain and time out other gbrain users
	// (the user's Hermes gbrain MCP, ad-hoc CLI). During an active drain (calls
	// arrive faster than the timeout) the server stays warm = full speed; in
	// steady-state gaps it yields the lock. Next call transparently respawns.
	idleTimeout time.Duration
	idleTimer   *time.Timer
}

// NewMCPClient builds an MCP client; bin "" → "gbrain".
func NewMCPClient(bin string) *MCPClient {
	if bin == "" {
		bin = "gbrain"
	}
	return &MCPClient{bin: bin, idleTimeout: 20 * time.Second}
}

// armIdleLocked (re)schedules the idle teardown. Caller holds m.mu.
func (m *MCPClient) armIdleLocked() {
	if m.idleTimeout <= 0 {
		return
	}
	if m.idleTimer != nil {
		m.idleTimer.Stop()
	}
	m.idleTimer = time.AfterFunc(m.idleTimeout, func() {
		m.mu.Lock()
		m.teardownLocked() // releases the PGLite lock for Hermes/CLI
		m.mu.Unlock()
	})
}

// PutPage → gbrain put_page tool over the persistent server.
func (m *MCPClient) PutPage(ctx context.Context, slug, content string) error {
	return m.callTool(ctx, "put_page", map[string]string{"slug": slug, "content": content})
}

// AddLink → gbrain add_link tool over the persistent server.
func (m *MCPClient) AddLink(ctx context.Context, from, to, linkType, linkContext string) error {
	return m.callTool(ctx, "add_link", map[string]string{
		"from": from, "to": to, "link_type": linkType, "context": linkContext,
	})
}

// Close stops the persistent server.
func (m *MCPClient) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.teardownLocked()
	return nil
}

func (m *MCPClient) teardownLocked() {
	if m.idleTimer != nil {
		m.idleTimer.Stop()
		m.idleTimer = nil
	}
	if m.cmd != nil && m.cmd.Process != nil {
		_ = m.stdin.Close()
		_ = m.cmd.Process.Kill()
		_ = m.cmd.Wait()
	}
	m.cmd, m.stdin, m.out, m.running = nil, nil, nil, false
}

// ensureLocked (re)spawns `gbrain serve` and runs the initialize handshake.
func (m *MCPClient) ensureLocked(ctx context.Context) error {
	if m.running {
		return nil
	}
	cmd := exec.Command(m.bin, "serve")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("gbrain serve stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("gbrain serve stdout: %w", err)
	}
	cmd.Stderr = nil // discard server logs
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("gbrain serve start: %w", err)
	}
	m.cmd, m.stdin, m.out = cmd, stdin, bufio.NewReaderSize(stdout, 1<<20)

	// initialize → expect a response; then send the initialized notification.
	if _, err := m.roundtrip(map[string]any{
		"jsonrpc": "2.0", "id": m.id(), "method": "initialize",
		"params": map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]any{"name": "switchboardd", "version": "1"},
		},
	}, true); err != nil {
		m.teardownLocked()
		return fmt.Errorf("gbrain initialize: %w", err)
	}
	if err := m.writeMsg(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"}); err != nil {
		m.teardownLocked()
		return err
	}
	m.running = true
	return nil
}

func (m *MCPClient) id() int64 { m.nextID++; return m.nextID }

func (m *MCPClient) writeMsg(v map[string]any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if _, err := m.stdin.Write(b); err != nil {
		return err
	}
	return nil
}

// roundtrip writes a request and (if expectResp) reads line-delimited JSON until
// it finds the response whose id matches the request's, returning an error on a
// JSON-RPC error or a tool isError result.
func (m *MCPClient) roundtrip(req map[string]any, expectResp bool) (json.RawMessage, error) {
	if err := m.writeMsg(req); err != nil {
		return nil, err
	}
	if !expectResp {
		return nil, nil
	}
	wantID, _ := req["id"].(int64)
	for {
		line, err := m.out.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		var resp struct {
			ID     *int64          `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(line, &resp) != nil {
			continue // skip non-JSON / log noise
		}
		if resp.ID == nil || *resp.ID != wantID {
			continue // a notification or another id
		}
		if resp.Error != nil {
			return nil, fmt.Errorf("gbrain rpc error %d: %s", resp.Error.Code, resp.Error.Message)
		}
		// Detect a tool-level error (isError:true in the CallToolResult).
		var tr struct {
			IsError bool `json:"isError"`
		}
		_ = json.Unmarshal(resp.Result, &tr)
		if tr.IsError {
			return resp.Result, fmt.Errorf("gbrain tool error: %s", string(resp.Result))
		}
		return resp.Result, nil
	}
}

func (m *MCPClient) callTool(ctx context.Context, tool string, args map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ensureLocked(ctx); err != nil {
		return err
	}
	_, err := m.roundtrip(map[string]any{
		"jsonrpc": "2.0", "id": m.id(), "method": "tools/call",
		"params": map[string]any{"name": tool, "arguments": args},
	}, true)
	if err != nil {
		// IO/transport failure → drop the server so the next call respawns it.
		m.teardownLocked()
		return fmt.Errorf("gbrain %s: %w", tool, err)
	}
	m.armIdleLocked() // yield the PGLite lock if no more calls arrive soon
	return nil
}
