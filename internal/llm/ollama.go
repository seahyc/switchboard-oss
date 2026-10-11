// Package llm is the local-only inference client for switchboard's intelligence
// layer. It talks to the on-VM ollama server (the honcho-ollama-1 container) over
// plain HTTP — NO pay-per-token API, ever (the ecosystem-wide hard rule).
//
// It provides the real idmatch.LLMAdjudicator (qwen2.5:7b-instruct) and a
// nomic-embed-text Embedder, both DEPENDENCY-INJECTED into the pure idmatch
// package so that package stays model-free + unit-testable.
//
// CPU-only box reality: 7B inference is seconds/pair, so this is BATCH-ONLY (never
// on the live ingest path) and the model is loaded ON-DEMAND with a short
// keep_alive so steady-state RAM is unchanged. The off-ingest worker bounds
// concurrency + throttles; this client just makes one well-formed call.
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
)

// Local inference defaults; explicit URLs and container lookup are optional.
const (
	DefaultBaseURL = "http://127.0.0.1:11434"
	// DefaultAdjModel is the 3B (not 7B): vm-gov removed qwen2.5:7b-instruct in the OOM
	// fix, and re-pulling it reintroduces the storm. The 3B is resident + OOM-safe; the
	// matcher is eval-gated + PROPOSE-ONLY, so a smaller model just means more conservative
	// proposals (safe). Override via -ollama-model / cfg.OllamaModel.
	DefaultAdjModel   = "qwen2.5:3b-instruct"
	DefaultEmbedModel = "nomic-embed-text"
	// DefaultKeepAlive is intentionally SHORT: the model unloads soon after a batch
	// finishes so steady-state RAM on the shared box is unchanged.
	DefaultKeepAlive = "5m"
)

// Config configures the ollama client. Zero values fall back to the Default*
// constants; BaseURL also reads OLLAMA_URL from the environment.
type Config struct {
	BaseURL    string
	AdjModel   string
	EmbedModel string
	KeepAlive  string
	// Timeout per request. CPU 7B inference can take many seconds per pair; default
	// 180s gives headroom. Embedding calls are sub-second.
	Timeout time.Duration
	// Retries on transient transport errors (not on a parsed bad verdict). Default 2.
	Retries int
	// MinInterval paces Adjudicate calls: at least this long must elapse between the
	// START of consecutive adjudications, inserting a CPU breathing-gap between the
	// (multi-second) 7B inferences so the background matcher never starves live
	// ingest on the shared box. Zero = no pacing (the offline eval runs flat-out).
	MinInterval time.Duration
	// Container is the ollama container name (e.g. honcho-ollama-1) used to
	// RE-RESOLVE its docker-network IP on a dial failure (#23). The honcho stack
	// gets recreated by co-tenant compose churn, changing ollama's IP; without
	// this a long-lived client keeps dialing the stale address. Empty → no
	// re-resolution (the BaseURL is used as-is).
	Container string
}

func (c Config) withDefaults() Config {
	if c.BaseURL == "" {
		if env := strings.TrimSpace(os.Getenv("OLLAMA_URL")); env != "" {
			c.BaseURL = env
		} else {
			c.BaseURL = DefaultBaseURL
		}
	}
	c.BaseURL = strings.TrimRight(c.BaseURL, "/")
	if c.AdjModel == "" {
		c.AdjModel = DefaultAdjModel
	}
	if c.EmbedModel == "" {
		c.EmbedModel = DefaultEmbedModel
	}
	if c.KeepAlive == "" {
		c.KeepAlive = DefaultKeepAlive
	}
	if c.Timeout <= 0 {
		c.Timeout = 180 * time.Second
	}
	if c.Retries < 0 {
		c.Retries = 0
	}
	if c.Retries == 0 {
		c.Retries = 2
	}
	return c
}

// Client is the ollama HTTP client. Safe for concurrent use; the off-ingest
// worker bounds how many in-flight calls actually hit the (slow) CPU model.
type Client struct {
	cfg  Config
	http *http.Client

	urlMu   sync.RWMutex
	baseURL string // current resolved base URL; updated on dial-error re-resolve (#23)

	// reresolve re-resolves the base URL when a dial fails (#23). nil = disabled.
	// Injected in New from cfg.Container; overridable in tests.
	reresolve func(context.Context) (string, error)

	mu       sync.Mutex
	lastCall time.Time // for MinInterval pacing
}

// New builds an ollama client. A nil/zero Config is filled from Default* + env.
func New(cfg Config) *Client {
	cfg = cfg.withDefaults()
	c := &Client{
		cfg:     cfg,
		http:    &http.Client{Timeout: cfg.Timeout},
		baseURL: cfg.BaseURL,
	}
	if cfg.Container != "" {
		container := cfg.Container
		c.reresolve = func(ctx context.Context) (string, error) { return ResolveOllamaURL(ctx, container) }
	}
	return c
}

// base returns the current resolved base URL (re-resolution may have updated it).
func (c *Client) base() string {
	c.urlMu.RLock()
	defer c.urlMu.RUnlock()
	return c.baseURL
}

func (c *Client) setBase(u string) {
	c.urlMu.Lock()
	defer c.urlMu.Unlock()
	c.baseURL = u
}

// reresolveOnDial follows a moved ollama container: on a DIAL failure (the
// container bounced — IP changed or briefly down, e.g. co-tenant compose churn),
// re-resolve its current docker IP so the next retry targets the new address
// instead of the stale one. No-op when re-resolution is disabled or the error is
// not a dial failure (a timeout/5xx is a live server, not a moved one). (#23)
func (c *Client) reresolveOnDial(ctx context.Context, err error) {
	if c.reresolve == nil {
		return
	}
	var ne *net.OpError
	if !errors.As(err, &ne) || ne.Op != "dial" {
		return
	}
	newURL, rerr := c.reresolve(ctx)
	if rerr != nil {
		return // advisory only — keep the current URL and let the retry try it
	}
	if newURL = strings.TrimRight(newURL, "/"); newURL != "" && newURL != c.base() {
		c.setBase(newURL)
	}
}

// pace blocks until MinInterval has elapsed since the previous call (no-op when
// MinInterval is 0). Serializes the gap across concurrent callers.
func (c *Client) pace(ctx context.Context) error {
	if c.cfg.MinInterval <= 0 {
		return nil
	}
	c.mu.Lock()
	now := time.Now()
	earliest := c.lastCall.Add(c.cfg.MinInterval)
	var wait time.Duration
	if now.Before(earliest) {
		wait = earliest.Sub(now)
		c.lastCall = earliest
	} else {
		c.lastCall = now
	}
	c.mu.Unlock()
	if wait <= 0 {
		return nil
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// BaseURL reports the current resolved server URL (for logging / health checks).
func (c *Client) BaseURL() string { return c.base() }

// AdjModel reports the adjudication model name.
func (c *Client) AdjModel() string { return c.cfg.AdjModel }

// --- LLMAdjudicator ---

// chatReq is the ollama /api/chat request body.
type chatReq struct {
	Model     string        `json:"model"`
	Messages  []chatMessage `json:"messages"`
	Stream    bool          `json:"stream"`
	Format    string        `json:"format"`     // "json" → constrained JSON output
	KeepAlive string        `json:"keep_alive"` // on-demand load + short retention
	Options   chatOptions   `json:"options"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatOptions struct {
	Temperature float64 `json:"temperature"`
	NumCtx      int     `json:"num_ctx"`
}

type chatResp struct {
	Message chatMessage `json:"message"`
	Done    bool        `json:"done"`
}

// Adjudicate implements idmatch.LLMAdjudicator against the local qwen2.5:7b model.
//
// Precision-first failure policy: a TRANSPORT error (network/timeout) is returned
// as an error so the worker can back off and resume (the cache means no re-pay for
// completed pairs). A successful HTTP response whose body is UNPARSEABLE as a
// verdict is NOT an error — it returns a conservative same_person=false result, so
// one malformed generation can never auto-merge two people, and a multi-hour batch
// never aborts on a single bad pair.
func (c *Client) Adjudicate(ctx context.Context, req idmatch.AdjudicationRequest) (idmatch.AdjudicationResult, error) {
	if err := c.pace(ctx); err != nil {
		return idmatch.AdjudicationResult{}, err
	}
	body := chatReq{
		Model: c.cfg.AdjModel,
		Messages: []chatMessage{
			{Role: "system", Content: idmatch.SystemPrompt},
			{Role: "user", Content: idmatch.RenderUser(req.A, req.B)},
		},
		Stream:    false,
		Format:    "json",
		KeepAlive: c.cfg.KeepAlive,
		Options:   chatOptions{Temperature: 0, NumCtx: 4096},
	}
	raw, err := c.postJSON(ctx, "/api/chat", body)
	if err != nil {
		return idmatch.AdjudicationResult{}, err
	}
	var cr chatResp
	if err := json.Unmarshal(raw, &cr); err != nil {
		return conservativeNotSame("unparseable chat envelope: " + err.Error()), nil
	}
	res, ok := parseVerdict(cr.Message.Content)
	if !ok {
		return conservativeNotSame("unparseable verdict JSON"), nil
	}
	return res, nil
}

// parseVerdict extracts an AdjudicationResult from the model's content string.
// Tolerant of stray prose / markdown fences around the JSON object; clamps
// confidence into [0,1].
func parseVerdict(content string) (idmatch.AdjudicationResult, bool) {
	j := extractJSONObject(content)
	if j == "" {
		return idmatch.AdjudicationResult{}, false
	}
	var r idmatch.AdjudicationResult
	if err := json.Unmarshal([]byte(j), &r); err != nil {
		return idmatch.AdjudicationResult{}, false
	}
	if r.Confidence < 0 {
		r.Confidence = 0
	}
	if r.Confidence > 1 {
		r.Confidence = 1
	}
	return r, true
}

// extractJSONObject returns the first balanced {...} object in s (handling braces
// inside strings), or "" if none. Cheap robustness for models that wrap JSON in
// ```json fences or add a sentence.
func extractJSONObject(s string) string {
	start := strings.IndexByte(s, '{')
	if start < 0 {
		return ""
	}
	depth := 0
	inStr := false
	esc := false
	for i := start; i < len(s); i++ {
		ch := s[i]
		if inStr {
			switch {
			case esc:
				esc = false
			case ch == '\\':
				esc = true
			case ch == '"':
				inStr = false
			}
			continue
		}
		switch ch {
		case '"':
			inStr = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return s[start : i+1]
			}
		}
	}
	return ""
}

func conservativeNotSame(reason string) idmatch.AdjudicationResult {
	return idmatch.AdjudicationResult{
		SamePerson: false,
		Confidence: 0,
		Reason:     "adjudicator fallback: " + reason,
	}
}

// Chat is a generic single-turn chat call against the local ollama model — used
// by callers beyond idmatch (e.g. the comprehend commitment extractor). It is
// BATCH-ONLY by contract (CPU 7B inference is seconds/call; never on live ingest).
// When jsonFormat is true the model is constrained to emit a JSON object. The
// model arg overrides the configured AdjModel when non-empty.
func (c *Client) Chat(ctx context.Context, model, system, user string, jsonFormat bool) (string, error) {
	if err := c.pace(ctx); err != nil {
		return "", err
	}
	if model == "" {
		model = c.cfg.AdjModel
	}
	format := ""
	if jsonFormat {
		format = "json"
	}
	body := chatReq{
		Model: model,
		Messages: []chatMessage{
			{Role: "system", Content: system},
			{Role: "user", Content: user},
		},
		Stream:    false,
		Format:    format,
		KeepAlive: c.cfg.KeepAlive,
		Options:   chatOptions{Temperature: 0, NumCtx: 4096},
	}
	raw, err := c.postJSON(ctx, "/api/chat", body)
	if err != nil {
		return "", err
	}
	var cr chatResp
	if err := json.Unmarshal(raw, &cr); err != nil {
		return "", fmt.Errorf("chat: decode envelope: %w", err)
	}
	return cr.Message.Content, nil
}

// --- Embedder (nomic-embed-text) ---

type embedReq struct {
	Model     string `json:"model"`
	Prompt    string `json:"prompt"`
	KeepAlive string `json:"keep_alive"`
}

type embedResp struct {
	Embedding []float32 `json:"embedding"`
}

// Embed implements idmatch.Embedder against the local nomic-embed-text model.
func (c *Client) Embed(text string) ([]float32, error) {
	body := embedReq{Model: c.cfg.EmbedModel, Prompt: text, KeepAlive: c.cfg.KeepAlive}
	raw, err := c.postJSON(context.Background(), "/api/embeddings", body)
	if err != nil {
		return nil, err
	}
	var er embedResp
	if err := json.Unmarshal(raw, &er); err != nil {
		return nil, fmt.Errorf("embed: decode: %w", err)
	}
	if len(er.Embedding) == 0 {
		return nil, fmt.Errorf("embed: empty embedding for %q", clip(text, 40))
	}
	return er.Embedding, nil
}

// --- transport ---

// Health pings the server (GET /api/version) to confirm reachability at startup.
func (c *Client) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base()+"/api/version", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("ollama health: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ollama health: status %d", resp.StatusCode)
	}
	return nil
}

// postJSON POSTs v as JSON to path and returns the raw response body, retrying on
// transient transport errors with a small backoff.
func (c *Client) postJSON(ctx context.Context, path string, v any) ([]byte, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	var lastErr error
	for attempt := 0; attempt <= c.cfg.Retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base()+path, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			// #23: if the container moved (dial failure), re-resolve its IP so the
			// next attempt targets the new address instead of the stale one.
			c.reresolveOnDial(ctx, err)
			continue
		}
		raw, rerr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if rerr != nil {
			lastErr = rerr
			continue
		}
		if resp.StatusCode != http.StatusOK {
			// 5xx is transient (model loading, OOM-retry); 4xx is not.
			lastErr = fmt.Errorf("ollama %s: status %d: %s", path, resp.StatusCode, clip(string(raw), 200))
			if resp.StatusCode >= 500 {
				continue
			}
			return nil, lastErr
		}
		return raw, nil
	}
	return nil, fmt.Errorf("ollama %s: %w", path, lastErr)
}

func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// compile-time interface assertions.
var (
	_ idmatch.LLMAdjudicator = (*Client)(nil)
	_ idmatch.Embedder       = (*Client)(nil)
)
