package whatsapp

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/store"
)

type versionTransport func(*http.Request) (*http.Response, error)

func (f versionTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestUpdateWAVersionUsesPublishedRevision(t *testing.T) {
	old := store.GetWAVersion()
	defer store.SetWAVersion(old)
	client := &http.Client{Transport: versionTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host != "web.whatsapp.com" {
			t.Errorf("unexpected host: %s", r.URL.Host)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"client_revision":1049999999,}`)), Header: make(http.Header)}, nil
	})}
	if err := updateWAVersion(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	if got := store.GetWAVersion(); got != (store.WAVersionContainer{2, 3000, 1049999999}) {
		t.Fatalf("version = %v", got)
	}
}

func TestUpdateWAVersionPreservesVersionOnFailure(t *testing.T) {
	old := store.GetWAVersion()
	client := &http.Client{Transport: versionTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("unavailable")), Header: make(http.Header)}, nil
	})}
	if err := updateWAVersion(context.Background(), client); err == nil {
		t.Fatal("expected discovery failure")
	}
	if got := store.GetWAVersion(); got != old {
		t.Fatalf("version changed on failed discovery: %v", got)
	}
}
