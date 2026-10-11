// Command gate-eval runs the #19 identity-matcher EVAL GATE: candidate-gen →
// adjudicate (real local qwen2.5:7b via internal/llm, or the model-free heuristic
// baseline) → tier routing → precision/recall scorecard over the labeled set.
//
// The gate number is AUTO-MERGE PRECISION on the negatives (different-people pairs):
// auto-merge only flips ON (team-lead's call) when it clears the bar (≥99%) with
// ZERO false auto-merges.
//
// On the ARM CPU box the 7B is ~27s/pair, so a few-hundred-pair labeled set is a
// multi-hour run — drive it detached (it's idempotent; re-run hits the cache). No
// pay-per-token: the model is local.
//
// Usage:
//
//	gate-eval -people people.json -labeled labeled.json [-ollama-url http://...:11434] [-model qwen2.5:7b-instruct] [-bar 0.99]
//
// Omit -ollama-url to score the deterministic heuristic baseline (no model, fast).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
	"github.com/seahyc/switchboard-oss/internal/llm"
)

func main() {
	peoplePath := flag.String("people", "", "JSON []idmatch.PersonRecord (required)")
	labeledPath := flag.String("labeled", "", "JSON []idmatch.LabeledPair (required)")
	ollamaURL := flag.String("ollama-url", "", "ollama base URL; empty → heuristic baseline (no model)")
	codexURL := flag.String("codex-url", "", "OpenAI-compatible base URL (chatmock/Codex ChatGPT-OAuth, subscription, NO pay-per-token); takes precedence over -ollama-url")
	model := flag.String("model", "qwen2.5:7b-instruct", "adjudication model")
	bar := flag.Float64("bar", 0.99, "auto-merge precision gate")
	out := flag.String("out", "", "write the scorecard markdown here (default stdout)")
	cachePath := flag.String("cache", "", "JSONL verdict cache file → makes the multi-hour run RESUMABLE across crashes")
	keepAlive := flag.String("keepalive", "2h", "ollama keep_alive — pin the model so memory pressure can't idle-evict it mid-run")
	timeout := flag.Duration("timeout", 600*time.Second, "per-call timeout — must tolerate a COLD ~5GB model reload after an eviction, else the run crashes")
	progressPath := flag.String("progress", "", "write live progress (adjudicated count) here so a silent death is pollable")
	flag.Parse()
	if *peoplePath == "" || *labeledPath == "" {
		fmt.Fprintln(os.Stderr, "error: -people and -labeled are required")
		flag.Usage()
		os.Exit(2)
	}

	var people []idmatch.PersonRecord
	must(readJSON(*peoplePath, &people), "read people")
	var labeled []idmatch.LabeledPair
	must(readJSON(*labeledPath, &labeled), "read labeled")

	var adj idmatch.LLMAdjudicator
	if *codexURL != "" {
		// Codex (chatmock / ChatGPT-OAuth) adjudicator — subscription, no pay-per-token.
		// Used after the local 3B failed the gate (precision 0%) and 7B OOM-dies here.
		adj = newOpenAIAdjudicator(*codexURL, *model, *timeout, 3)
		fmt.Fprintf(os.Stderr, "gate-eval: Codex model %s @ %s over %d pairs (subscription path; timeout=%s)\n",
			*model, *codexURL, len(labeled), *timeout)
	} else if *ollamaURL != "" {
		adj = llm.New(llm.Config{BaseURL: *ollamaURL, AdjModel: *model, KeepAlive: *keepAlive, Timeout: *timeout, Retries: 3})
		fmt.Fprintf(os.Stderr, "gate-eval: real model %s @ %s over %d pairs (~27s/pair → long run; keep_alive=%s timeout=%s)\n",
			*model, *ollamaURL, len(labeled), *keepAlive, *timeout)
		// PRE-WARM: force a cold load NOW (pinned by keep_alive) so the first scored
		// pair isn't a cold ~5GB load that blows the timeout. Tolerant: a slow warm is
		// fine, a failed warm just means the first real call pays the load.
		if err := prewarm(*ollamaURL, *model, *keepAlive, *timeout); err != nil {
			fmt.Fprintf(os.Stderr, "gate-eval: prewarm warning (continuing): %v\n", err)
		} else {
			fmt.Fprintln(os.Stderr, "gate-eval: model pre-warmed + pinned")
		}
	} else {
		adj = idmatch.HeuristicAdjudicator{}
		fmt.Fprintln(os.Stderr, "gate-eval: heuristic baseline (no model)")
	}

	// Wrap with a persistent cache so a multi-hour run resumes across crashes.
	if *cachePath != "" {
		cache, err := idmatch.NewFileCache(*cachePath)
		must(err, "open cache")
		defer cache.Close()
		fmt.Fprintf(os.Stderr, "gate-eval: resumable cache %s (%d verdicts already cached)\n", *cachePath, cache.Len())
		adj = idmatch.CachingAdjudicator{Inner: adj, Cache: cache}
	}

	// Progress wrapper (OUTSIDE the cache so it counts every pair, including instant
	// cache hits) — writes a live count to -progress so a silent death is pollable.
	if *progressPath != "" {
		adj = &progressAdjudicator{inner: adj, path: *progressPath, total: len(labeled)}
		fmt.Fprintf(os.Stderr, "gate-eval: progress → %s\n", *progressPath)
	}

	gen := idmatch.NewCandidateGenerator(idmatch.DefaultCandidateConfig, nil)
	rep, err := idmatch.Evaluate(context.Background(), people, labeled, gen, adj, idmatch.DefaultThresholds)
	must(err, "evaluate")

	md := rep.Markdown(*bar)
	if *out == "" {
		fmt.Print(md)
	} else {
		must(os.WriteFile(*out, []byte(md), 0o644), "write out")
		fmt.Fprintf(os.Stderr, "wrote %s — gate %s\n", *out, gateWord(rep.PassesGate(*bar)))
	}
}

// prewarm forces ollama to load the model now and pin it (keep_alive), so the first
// scored pair isn't a cold ~5GB load that exceeds the per-call timeout. A tiny
// generate with num_predict=1 is enough to trigger the load.
func prewarm(baseURL, model, keepAlive string, timeout time.Duration) error {
	body, _ := json.Marshal(map[string]any{
		"model": model, "prompt": "ok", "stream": false,
		"keep_alive": keepAlive, "options": map[string]any{"num_predict": 1},
	})
	cli := &http.Client{Timeout: timeout}
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/generate", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("prewarm status %d", resp.StatusCode)
	}
	return nil
}

// progressAdjudicator counts every adjudicated pair and writes a live count to a
// file, so a long detached run is pollable and a silent death is visible.
type progressAdjudicator struct {
	inner idmatch.LLMAdjudicator
	path  string
	total int
	n     int
}

func (p *progressAdjudicator) Adjudicate(ctx context.Context, req idmatch.AdjudicationRequest) (idmatch.AdjudicationResult, error) {
	r, err := p.inner.Adjudicate(ctx, req)
	p.n++
	_ = os.WriteFile(p.path, []byte(fmt.Sprintf("adjudicated=%d/%d\n", p.n, p.total)), 0o644)
	return r, err
}

func gateWord(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func must(err error, ctx string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s: %v\n", ctx, err)
		os.Exit(1)
	}
}
