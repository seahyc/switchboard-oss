package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestReadAcceptsTrailingFlagsAtHTTPBoundary(t *testing.T) {
	t.Setenv("SWITCHBOARD_TOKEN", "synthetic-test-token-with-more-than-32-characters")
	got := make(chan url.Values, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()
	if err := cmdRead(context.Background(), []string{"chat-123", "--limit", "1", "--addr", srv.Listener.Addr().String()}); err != nil {
		t.Fatal(err)
	}
	q := <-got
	if q.Get("chat_id") != "chat-123" || q.Get("limit") != "1" {
		t.Fatalf("GET /read query = %v, want chat_id=chat-123 and limit=1", q)
	}
}

func TestSearchAcceptsTrailingFlagsAtHTTPBoundary(t *testing.T) {
	t.Setenv("SWITCHBOARD_TOKEN", "synthetic-test-token-with-more-than-32-characters")
	got := make(chan url.Values, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()
	if err := cmdSearch(context.Background(), []string{"needle", "--platform", "slack", "--limit", "1", "--addr", srv.Listener.Addr().String()}); err != nil {
		t.Fatal(err)
	}
	q := <-got
	if q.Get("q") != "needle" || q.Get("platform") != "slack" || q.Get("limit") != "1" {
		t.Fatalf("GET /search query = %v, want q=needle, platform=slack, limit=1", q)
	}
}

func TestAuthStatusRequiresPlatformLocally(t *testing.T) {
	err := cmdAuthStatus(context.Background(), nil)
	if err == nil || err.Error() != "platform required" {
		t.Fatalf("cmdAuthStatus() error = %v, want platform required", err)
	}
}
