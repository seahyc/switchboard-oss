package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
)

// TestPostJSONReresolvesOnDial verifies #23: a dial failure (the ollama container
// moved) triggers a re-resolve of the base URL, and the retry hits the new
// address. The first request dials a closed port (connection refused = a dial
// error); the injected resolver points at a live server; the retry succeeds.
func TestPostJSONReresolvesOnDial(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"ok":true}`))
	}))
	defer good.Close()
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close() // now dialing deadURL → connection refused (net.OpError Op:"dial")

	c := New(Config{BaseURL: deadURL, Retries: 2, Timeout: 2 * time.Second})
	c.reresolve = func(context.Context) (string, error) { return good.URL, nil }

	raw, err := c.postJSON(context.Background(), "/api/x", map[string]string{"a": "b"})
	if err != nil {
		t.Fatalf("postJSON should recover after re-resolving to the live server: %v", err)
	}
	if string(raw) != `{"ok":true}` {
		t.Fatalf("unexpected body: %s", raw)
	}
	if want := strings.TrimRight(good.URL, "/"); c.base() != want {
		t.Fatalf("base not re-resolved: got %q want %q", c.base(), want)
	}
}

// TestReresolveDisabledWithoutContainer: no Container → no re-resolver wired.
func TestReresolveDisabledWithoutContainer(t *testing.T) {
	c := New(Config{BaseURL: "http://127.0.0.1:1"})
	if c.reresolve != nil {
		t.Fatal("reresolve must be nil when Container is unset")
	}
}

func TestAdjudicateParsesVerdict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		var req chatReq
		json.NewDecoder(r.Body).Decode(&req)
		if req.Format != "json" || req.Stream {
			t.Errorf("want format=json, stream=false; got %+v", req)
		}
		if req.Model != DefaultAdjModel {
			t.Errorf("model = %q", req.Model)
		}
		// Model returns the verdict as a JSON string in message.content.
		json.NewEncoder(w).Encode(chatResp{
			Message: chatMessage{Role: "assistant", Content: `{"same_person": true, "confidence": 0.93, "reason": "shared handle", "evidence": ["handle"]}`},
			Done:    true,
		})
	}))
	defer srv.Close()

	c := New(Config{BaseURL: srv.URL})
	res, err := c.Adjudicate(context.Background(), idmatch.AdjudicationRequest{
		A: idmatch.PersonRecord{CanonicalID: "a", PrimaryName: "Amy", Handles: []string{"amychen"}},
		B: idmatch.PersonRecord{CanonicalID: "b", PrimaryName: "Amy C", Handles: []string{"amychen"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.SamePerson || res.Confidence != 0.93 {
		t.Fatalf("bad verdict: %+v", res)
	}
}

func TestAdjudicateTolerantOfFencedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(chatResp{
			Message: chatMessage{Content: "Sure!\n```json\n{\"same_person\": false, \"confidence\": 0.2}\n```\n"},
			Done:    true,
		})
	}))
	defer srv.Close()
	c := New(Config{BaseURL: srv.URL})
	res, err := c.Adjudicate(context.Background(), idmatch.AdjudicationRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if res.SamePerson || res.Confidence != 0.2 {
		t.Fatalf("should parse fenced JSON: %+v", res)
	}
}

func TestAdjudicateUnparseableIsConservativeNotError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(chatResp{Message: chatMessage{Content: "I cannot determine this."}, Done: true})
	}))
	defer srv.Close()
	c := New(Config{BaseURL: srv.URL})
	res, err := c.Adjudicate(context.Background(), idmatch.AdjudicationRequest{})
	if err != nil {
		t.Fatalf("unparseable verdict must NOT be a transport error: %v", err)
	}
	if res.SamePerson {
		t.Fatalf("unparseable verdict must be conservative not-same: %+v", res)
	}
	// And it must route to drop (never auto-merge).
	if idmatch.Route(res, idmatch.DefaultThresholds) == idmatch.TierAutoMerge {
		t.Fatal("conservative fallback must never auto-merge")
	}
}

func TestAdjudicateClampsConfidence(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(chatResp{Message: chatMessage{Content: `{"same_person": true, "confidence": 1.7}`}, Done: true})
	}))
	defer srv.Close()
	c := New(Config{BaseURL: srv.URL})
	res, _ := c.Adjudicate(context.Background(), idmatch.AdjudicationRequest{})
	if res.Confidence != 1.0 {
		t.Fatalf("confidence should clamp to 1.0, got %v", res.Confidence)
	}
}

func TestEmbed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/embeddings" {
			t.Errorf("path = %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(embedResp{Embedding: []float32{0.1, 0.2, 0.3}})
	}))
	defer srv.Close()
	c := New(Config{BaseURL: srv.URL})
	v, err := c.Embed("hello")
	if err != nil || len(v) != 3 {
		t.Fatalf("embed failed: v=%v err=%v", v, err)
	}
}

func TestRetryOn5xxThenSucceed(t *testing.T) {
	var n int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("loading model"))
			return
		}
		json.NewEncoder(w).Encode(chatResp{Message: chatMessage{Content: `{"same_person": true, "confidence": 0.9}`}, Done: true})
	}))
	defer srv.Close()
	c := New(Config{BaseURL: srv.URL})
	res, err := c.Adjudicate(context.Background(), idmatch.AdjudicationRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !res.SamePerson || n < 2 {
		t.Fatalf("should retry past 503: n=%d res=%+v", n, res)
	}
}

func TestEnvURLFallback(t *testing.T) {
	t.Setenv("OLLAMA_URL", "http://example.invalid:1234/")
	c := New(Config{})
	if c.BaseURL() != "http://example.invalid:1234" {
		t.Fatalf("env URL not honored (trailing slash trimmed): %q", c.BaseURL())
	}
}
