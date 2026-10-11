package lark

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
)

// writeTokens writes a minimal tokens.json into dir and returns a loaded store.
func writeTokens(t *testing.T, dir string, tf tokenFile) *tokenStore {
	t.Helper()
	b, _ := json.Marshal(tf)
	if err := os.WriteFile(filepath.Join(dir, "tokens.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	ts, err := newTokenStore(Config{Dir: dir, AppID: "cli_app", AppSecret: "secret123"})
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

func TestResolveDomain(t *testing.T) {
	t.Setenv("LARK_DOMAIN", "") // ensure env doesn't override the table
	cases := map[string]string{
		"":                           lark.LarkBaseUrl, // default = international
		"larksuite":                  lark.LarkBaseUrl,
		"lark":                       lark.LarkBaseUrl,
		"intl":                       lark.LarkBaseUrl,
		"feishu":                     lark.FeishuBaseUrl,
		"cn":                         lark.FeishuBaseUrl,
		"https://custom.example.com": "https://custom.example.com",
	}
	for in, want := range cases {
		if got := resolveDomain(in); got != want {
			t.Errorf("resolveDomain(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestResolveDomainEnvOverride(t *testing.T) {
	t.Setenv("LARK_DOMAIN", "feishu")
	if got := resolveDomain("larksuite"); got != lark.FeishuBaseUrl {
		t.Errorf("env override failed: got %q", got)
	}
}

func TestRefreshUsesV2AndPersistsRotation(t *testing.T) {
	dir := t.TempDir()
	ts := writeTokens(t, dir, tokenFile{
		AccessToken:  "u-old",
		RefreshToken: "rt-old",
		ExpiresAt:    "2026-06-07T00:00:00Z", // expired
	})

	var gotPath, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
		  "code": 0, "access_token": "u-new", "token_type": "Bearer",
		  "expires_in": 7200, "refresh_token": "rt-new",
		  "refresh_token_expires_in": 2592000, "scope": "im:message im:chat"
		}`))
	}))
	defer srv.Close()
	ts.domain = srv.URL

	client := lark.NewClient("cli_app", "secret123", lark.WithOpenBaseUrl(srv.URL))
	now := time.Date(2026, 6, 9, 9, 0, 0, 0, time.UTC)

	if err := ts.Refresh(context.Background(), client, now); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	// Hit the v2 OAuth endpoint with the right grant + credentials.
	if gotPath != oauthTokenPath {
		t.Errorf("path = %q, want %q", gotPath, oauthTokenPath)
	}
	for _, want := range []string{`"grant_type":"refresh_token"`, `"refresh_token":"rt-old"`, `"client_id":"cli_app"`, `"client_secret":"secret123"`} {
		if !strings.Contains(gotBody, want) {
			t.Errorf("request body missing %s; got %s", want, gotBody)
		}
	}

	// In-memory state rotated.
	if ts.tf.AccessToken != "u-new" || ts.tf.RefreshToken != "rt-new" {
		t.Errorf("tokens not rotated: %+v", ts.tf)
	}
	wantExp := now.Add(7200 * time.Second).Format(time.RFC3339)
	if ts.tf.ExpiresAt != wantExp {
		t.Errorf("expires_at = %q, want %q", ts.tf.ExpiresAt, wantExp)
	}
	if !ts.fresh(now) {
		t.Error("token should be fresh after refresh")
	}

	// Rotation persisted to disk (so the chain survives restarts).
	var onDisk tokenFile
	b, _ := os.ReadFile(filepath.Join(dir, "tokens.json"))
	_ = json.Unmarshal(b, &onDisk)
	if onDisk.RefreshToken != "rt-new" || onDisk.AccessToken != "u-new" {
		t.Errorf("rotation not persisted: %+v", onDisk)
	}
}

func TestRefreshSurfacesV2Error(t *testing.T) {
	dir := t.TempDir()
	ts := writeTokens(t, dir, tokenFile{RefreshToken: "rt-dead", ExpiresAt: "2026-06-07T00:00:00Z"})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// OAuth v2 may encode an OAuth error in a successful HTTP response.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":20026,"error":"invalid_grant","error_description":"refresh token not found"}`))
	}))
	defer srv.Close()
	ts.domain = srv.URL

	client := lark.NewClient("cli_app", "secret123", lark.WithOpenBaseUrl(srv.URL))
	err := ts.Refresh(context.Background(), client, time.Now().UTC())
	if err == nil {
		t.Fatal("expected error for dead refresh token")
	}
	for _, want := range []string{"status=200", "code=20026", `error="invalid_grant"`, `error_description="refresh token not found"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should surface %q, got: %v", want, err)
		}
	}
	if ts.tf.AccessToken != "" || ts.tf.RefreshToken != "rt-dead" {
		t.Fatalf("tokens changed after failed refresh: access=%q refresh=%q", ts.tf.AccessToken, ts.tf.RefreshToken)
	}
	var onDisk tokenFile
	b, readErr := os.ReadFile(filepath.Join(dir, "tokens.json"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if err := json.Unmarshal(b, &onDisk); err != nil {
		t.Fatal(err)
	}
	if onDisk.AccessToken != "" || onDisk.RefreshToken != "rt-dead" {
		t.Fatalf("tokens file changed after failed refresh: access=%q refresh=%q", onDisk.AccessToken, onDisk.RefreshToken)
	}
}

func TestAccessTokenSkipsRefreshWhenFresh(t *testing.T) {
	dir := t.TempDir()
	// expires comfortably in the future → no refresh, no network needed.
	future := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	ts := writeTokens(t, dir, tokenFile{AccessToken: "u-valid", RefreshToken: "rt", ExpiresAt: future})

	// A client whose base URL would fail if actually called — proves no refresh.
	client := lark.NewClient("cli_app", "secret123", lark.WithOpenBaseUrl("http://127.0.0.1:0"))
	tok, err := ts.AccessToken(context.Background(), client, time.Now().UTC())
	if err != nil {
		t.Fatalf("AccessToken should serve cached token without refresh: %v", err)
	}
	if tok != "u-valid" {
		t.Errorf("token = %q, want u-valid", tok)
	}
}

// TestRefreshReloadsFreshTokenFromDisk proves the #28 single-owner reload-recheck: if
// our in-memory token is stale but ANOTHER owner already rotated the on-disk token to a
// fresh one, refreshLocked reloads from disk under the lock and REUSES it — it does NOT
// re-rotate the (single-use) refresh_token, which is what was revoking the token.
func TestRefreshReloadsFreshTokenFromDisk(t *testing.T) {
	dir := t.TempDir()
	future := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	// On-disk: a FRESH token (as if another owner just rotated it).
	ts := writeTokens(t, dir, tokenFile{AccessToken: "u-fresh-other-owner", RefreshToken: "rt-new", ExpiresAt: future})
	// Our in-memory copy is STALE (we loaded before the other owner's rotation).
	ts.tf.AccessToken = "u-stale"
	ts.tf.ExpiresAt = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)

	// Stale in-memory → refreshLocked runs, but under the lock it reloads the disk-fresh
	// token + reuses it. A dead client proves NO network refresh happened.
	client := lark.NewClient("cli_app", "secret123", lark.WithOpenBaseUrl("http://127.0.0.1:0"))
	tok, err := ts.AccessToken(context.Background(), client, time.Now().UTC())
	if err != nil {
		t.Fatalf("should reuse the disk-fresh token without re-rotating: %v", err)
	}
	if tok != "u-fresh-other-owner" {
		t.Errorf("token = %q, want the disk-rotated fresh token (no re-rotate)", tok)
	}
}
