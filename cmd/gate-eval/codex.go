package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
)

// openAIAdjudicator implements idmatch.LLMAdjudicator against an OpenAI-compatible
// /v1/chat/completions endpoint — the VM's chatmock / Codex ChatGPT-OAuth proxy
// (subscription, NO pay-per-token; the same path hermes/kratos use). This is the #19
// adjudicator after the local 3B failed the gate (precision 0%, 1 false noreply merge)
// and 7B OOM-dies on the free-tier box.
//
// Same conservative contract as the ollama client: a TRANSPORT/upstream error is
// returned (so the worker backs off + the resumable cache replays completed pairs),
// but a reachable-yet-unparseable response yields a conservative same_person=false so
// one bad generation can never auto-merge two people.
type openAIAdjudicator struct {
	baseURL string
	model   string
	cli     *http.Client
	retries int
}

func newOpenAIAdjudicator(baseURL, model string, timeout time.Duration, retries int) *openAIAdjudicator {
	return &openAIAdjudicator{
		baseURL: strings.TrimRight(baseURL, "/"),
		model:   model,
		cli:     &http.Client{Timeout: timeout},
		retries: retries,
	}
}

type oaiReq struct {
	Model    string       `json:"model"`
	Messages []oaiMessage `json:"messages"`
}

type oaiMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type oaiResp struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (o *openAIAdjudicator) Adjudicate(ctx context.Context, req idmatch.AdjudicationRequest) (idmatch.AdjudicationResult, error) {
	body, _ := json.Marshal(oaiReq{
		Model: o.model,
		Messages: []oaiMessage{
			{Role: "system", Content: idmatch.SystemPrompt +
				"\n\nRespond with ONLY a JSON object, no prose: " +
				`{"same_person":<bool>,"confidence":<0.0-1.0>,"reason":<string>}.`},
			{Role: "user", Content: idmatch.RenderUser(req.A, req.B)},
		},
	})
	var lastErr error
	for attempt := 0; attempt <= o.retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return idmatch.AdjudicationResult{}, ctx.Err()
			case <-time.After(time.Duration(attempt) * 2 * time.Second):
			}
		}
		raw, err := o.post(ctx, body)
		if err != nil {
			lastErr = err
			continue // transport error → back off + retry (cache protects completed pairs)
		}
		var r oaiResp
		if err := json.Unmarshal(raw, &r); err != nil {
			return conservativeNotSame("unparseable chat envelope: " + err.Error()), nil
		}
		if r.Error != nil {
			lastErr = fmt.Errorf("codex upstream: %s", r.Error.Message)
			continue // upstream/rate error → retry
		}
		if len(r.Choices) == 0 {
			return conservativeNotSame("no choices in response"), nil
		}
		res, ok := parseVerdict(r.Choices[0].Message.Content)
		if !ok {
			return conservativeNotSame("unparseable verdict JSON"), nil
		}
		return res, nil
	}
	return idmatch.AdjudicationResult{}, lastErr
}

func (o *openAIAdjudicator) post(ctx context.Context, body []byte) ([]byte, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, o.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := o.cli.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		snippet := strings.TrimSpace(string(raw))
		if len(snippet) > 200 {
			snippet = snippet[:200]
		}
		return nil, fmt.Errorf("codex http %d: %s", resp.StatusCode, snippet)
	}
	return raw, nil
}

// conservativeNotSame is the precision-first fallback verdict when a response is
// reachable but unusable — never auto-merge on a bad generation.
func conservativeNotSame(reason string) idmatch.AdjudicationResult {
	return idmatch.AdjudicationResult{SamePerson: false, Confidence: 0, Reason: reason}
}

// parseVerdict extracts the {same_person,confidence,reason,evidence} object from the
// model's content, tolerant of markdown fences / stray prose, and clamps confidence.
func parseVerdict(content string) (idmatch.AdjudicationResult, bool) {
	js := extractJSONObject(content)
	if js == "" {
		return idmatch.AdjudicationResult{}, false
	}
	var r idmatch.AdjudicationResult
	if err := json.Unmarshal([]byte(js), &r); err != nil {
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

// extractJSONObject returns the first balanced {...} object in s, ignoring braces that
// appear inside JSON strings (so a brace in the reason text can't truncate it).
func extractJSONObject(s string) string {
	start := strings.IndexByte(s, '{')
	if start < 0 {
		return ""
	}
	depth, inStr, esc := 0, false, false
	for i := start; i < len(s); i++ {
		c := s[i]
		switch {
		case esc:
			esc = false
		case c == '\\' && inStr:
			esc = true
		case c == '"':
			inStr = !inStr
		case inStr:
			// skip
		case c == '{':
			depth++
		case c == '}':
			depth--
			if depth == 0 {
				return s[start : i+1]
			}
		}
	}
	return ""
}
