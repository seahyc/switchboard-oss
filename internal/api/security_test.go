package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSecureHandlerRejectsMissingCredential(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true; w.WriteHeader(204) })
	handler := SecureHandler(next, Policy{AdminToken: "admin-credential"})
	r := httptest.NewRequest("GET", "http://127.0.0.1:8717/read", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 401 || called {
		t.Fatalf("status=%d called=%v", w.Code, called)
	}
}

func TestSecureHandlerCapabilities(t *testing.T) {
	cases := []struct {
		name, method, path, token, origin, host string
		allow                                   bool
		want                                    int
	}{
		{name: "reader", method: "GET", path: "/read", token: "reader", want: 204},
		{name: "read sql", method: "POST", path: "/query-sql", token: "reader", want: 204},
		{name: "read cannot maintain", method: "POST", path: "/backfill", token: "reader", want: 403},
		{name: "read cannot see auth", method: "GET", path: "/auth/status", token: "reader", want: 403},
		{name: "outbound disabled", method: "POST", path: "/send", token: "admin", want: 403},
		{name: "outbound enabled", method: "POST", path: "/send", token: "admin", allow: true, want: 204},
		{name: "reader cannot send", method: "POST", path: "/send", token: "reader", allow: true, want: 403},
		{name: "rotate invite", method: "GET", path: "/group-invite?reset=true", token: "reader", allow: true, want: 403},
		{name: "browser blocked", method: "GET", path: "/read", token: "admin", origin: "https://evil.example", want: 403},
		{name: "rebind blocked", method: "GET", path: "/healthz", host: "evil.example", want: 403},
		{name: "health", method: "GET", path: "/healthz", want: 204},
		{name: "empty config fails closed", method: "GET", path: "/read", want: 401},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			h := SecureHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true; w.WriteHeader(204) }), Policy{AdminToken: "admin", ReadToken: "reader", AllowOutbound: tc.allow})
			r := httptest.NewRequest(tc.method, "http://127.0.0.1:8717"+tc.path, nil)
			if tc.token != "" {
				r.Header.Set("Authorization", "Bearer "+tc.token)
			}
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if tc.host != "" {
				r.Host = tc.host
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want || called != (tc.want == 204) {
				t.Fatalf("status=%d called=%v", w.Code, called)
			}
		})
	}
}

func TestValidateListenAddr(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:8717", "[::1]:8717", "localhost:8717"} {
		if err := ValidateListenAddr(addr); err != nil {
			t.Fatal(err)
		}
	}
	for _, addr := range []string{":8717", "0.0.0.0:8717", "[::]:8717", "host.example:8717", "127.0.0.1"} {
		if ValidateListenAddr(addr) == nil {
			t.Fatalf("accepted %s", addr)
		}
	}
}

func TestAuthenticatedClientUsesCredentialAndRejectsRemote(t *testing.T) {
	t.Setenv("SWITCHBOARD_TOKEN", "synthetic-test-token-with-more-than-32-characters")
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"columns":[],"rows":[],"truncated":false}`))
	}))
	defer srv.Close()
	c := NewClient(strings.TrimPrefix(srv.URL, "http://"))
	if _, err := c.QuerySQL(context.Background(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if got != "Bearer synthetic-test-token-with-more-than-32-characters" {
		t.Fatalf("credential not attached")
	}
	if _, err := NewClient("example.com:8717").QuerySQL(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("client accepted non-loopback destination")
	}
}

func TestCredentialGenerationAndPrivateRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "api.token")
	first, err := LoadOrCreateToken(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadOrCreateToken(path)
	if err != nil || second != first || len(first) != 64 {
		t.Fatalf("credential changed or malformed: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode=%o", info.Mode().Perm())
	}
	if err = os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = LoadToken(path); err == nil {
		t.Fatal("read broad credential file")
	}
	if _, err = LoadOrCreateToken(path); err == nil {
		t.Fatal("silently accepted broad credential file")
	}
}

func TestAuthenticatedClientDoesNotFollowRedirect(t *testing.T) {
	t.Setenv("SWITCHBOARD_TOKEN", "synthetic-test-token-with-more-than-32-characters")
	followed := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed = true }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	_, err := NewClient(strings.TrimPrefix(source.URL, "http://")).QuerySQL(context.Background(), "SELECT 1")
	if err == nil || followed {
		t.Fatalf("redirect err=%v followed=%v", err, followed)
	}
}
