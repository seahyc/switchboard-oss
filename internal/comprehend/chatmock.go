package comprehend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ChatModel is the minimal chat interface the extractor needs. Both the local
// ollama client (llm.Client.Chat) and the chatmock proxy client satisfy it, so
// the extractor stays model-agnostic + unit-testable with a fake.
type ChatModel interface {
	Chat(ctx context.Context, model, system, user string, jsonFormat bool) (string, error)
}

// ChatMock is a client for the on-VM chatmock OpenAI-compatible proxy
// (127.0.0.1:8401), which fronts the Codex Pro subscription. It is $0 per-token
// (flat subscription) but the SESSION is scarce, so calls are PACED (MinInterval)
// and RETRIED with backoff. NEVER carries a raw API key — auth is the proxy's job.
type ChatMock struct {
	baseURL string
	http    *http.Client

	minInterval time.Duration
	retries     int

	mu       sync.Mutex
	lastCall time.Time
}

// ChatMockConfig configures the chatmock client.
type ChatMockConfig struct {
	BaseURL     string        // default http://127.0.0.1:8401
	Timeout     time.Duration // default 120s (reasoning models are slow)
	MinInterval time.Duration // pacing between calls; default 2s
	Retries     int           // transient-error retries; default 2
}

// NewChatMock builds a chatmock client.
func NewChatMock(cfg ChatMockConfig) *ChatMock {
	if cfg.BaseURL == "" {
		cfg.BaseURL = "http://127.0.0.1:8401"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 120 * time.Second
	}
	if cfg.MinInterval <= 0 {
		cfg.MinInterval = 2 * time.Second
	}
	if cfg.Retries <= 0 {
		cfg.Retries = 2
	}
	return &ChatMock{
		baseURL:     strings.TrimRight(cfg.BaseURL, "/"),
		http:        &http.Client{Timeout: cfg.Timeout},
		minInterval: cfg.MinInterval,
		retries:     cfg.Retries,
	}
}

type oaiChatReq struct {
	Model          string       `json:"model"`
	Messages       []oaiMessage `json:"messages"`
	Temperature    float64      `json:"temperature"`
	ResponseFormat *oaiFormat   `json:"response_format,omitempty"`
}

type oaiMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type oaiFormat struct {
	Type string `json:"type"` // "json_object"
}

type oaiChatResp struct {
	Choices []struct {
		Message oaiMessage `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// Chat implements ChatModel against the chatmock /v1/chat/completions endpoint.
func (c *ChatMock) Chat(ctx context.Context, model, system, user string, jsonFormat bool) (string, error) {
	c.pace(ctx)
	if model == "" {
		model = "gpt-5.4-mini"
	}
	req := oaiChatReq{
		Model:       model,
		Temperature: 0,
		Messages: []oaiMessage{
			{Role: "system", Content: system},
			{Role: "user", Content: user},
		},
	}
	if jsonFormat {
		req.ResponseFormat = &oaiFormat{Type: "json_object"}
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	var lastErr error
	for attempt := 0; attempt <= c.retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(time.Duration(attempt*attempt) * time.Second): // quadratic backoff
			}
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/chat/completions", bytes.NewReader(payload))
		if err != nil {
			return "", err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(httpReq)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			continue
		}
		raw, rerr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if rerr != nil {
			lastErr = rerr
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("chatmock status %d: %s", resp.StatusCode, clip(string(raw), 200))
			continue // transient — back off and retry
		}
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("chatmock status %d: %s", resp.StatusCode, clip(string(raw), 200))
		}
		var cr oaiChatResp
		if err := json.Unmarshal(raw, &cr); err != nil {
			return "", fmt.Errorf("chatmock decode: %w", err)
		}
		if cr.Error != nil {
			return "", fmt.Errorf("chatmock error: %s", cr.Error.Message)
		}
		if len(cr.Choices) == 0 {
			return "", fmt.Errorf("chatmock: empty choices")
		}
		return cr.Choices[0].Message.Content, nil
	}
	return "", fmt.Errorf("chatmock: %w", lastErr)
}

// Health pings the proxy's model list to confirm reachability.
func (c *ChatMock) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/models", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("chatmock health: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("chatmock health: status %d", resp.StatusCode)
	}
	return nil
}

// pace blocks until MinInterval has elapsed since the previous call (the scarce
// subscription gets a breathing gap between calls).
func (c *ChatMock) pace(ctx context.Context) {
	c.mu.Lock()
	now := time.Now()
	earliest := c.lastCall.Add(c.minInterval)
	var wait time.Duration
	if now.Before(earliest) {
		wait = earliest.Sub(now)
		c.lastCall = earliest
	} else {
		c.lastCall = now
	}
	c.mu.Unlock()
	if wait <= 0 {
		return
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

var _ ChatModel = (*ChatMock)(nil)
