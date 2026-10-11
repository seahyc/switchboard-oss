package lark

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/seahyc/switchboard-oss/internal/privatefs"
)

// Config is the Lark app identity, sourced from ~/.lark-config/config.yaml
// (app_id) plus the app secret (LARK_APP_SECRET env or config.yaml app_secret).
type Config struct {
	AppID     string
	AppSecret string
	Dir       string // the LARK_CONFIG_DIR (default ~/.lark-config)
	Domain    string // open-API base URL (Lark international vs Feishu CN)
}

// resolveDomain maps a config/env hint to the correct open-API base URL. This
// matters because the user's tenant is on Lark international
// (open.larksuite.com), but the SDK defaults to Feishu CN (open.feishu.cn) — the
// websocket endpoint is domain-specific and rejects the wrong one
// ("1000040351: Incorrect domain name"). REST tolerates either (the token
// validates cross-domain) but should match for correctness. Default: Lark intl.
func resolveDomain(raw string) string {
	if env := os.Getenv("LARK_DOMAIN"); env != "" {
		raw = env
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "lark", "larksuite", "intl", "international":
		return lark.LarkBaseUrl // https://open.larksuite.com
	case "feishu", "cn", "china":
		return lark.FeishuBaseUrl // https://open.feishu.cn
	default:
		return raw // assume an explicit base URL was given
	}
}

// DefaultConfigDir resolves the Lark config directory the same way the user's
// lark-cli wrapper does: $LARK_CONFIG_DIR, else ~/.lark-config.
func DefaultConfigDir() string {
	if d := os.Getenv("LARK_CONFIG_DIR"); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".lark-config")
}

// LoadConfig reads app_id/app_secret from the config dir. The secret prefers the
// LARK_APP_SECRET env var (as the config.yaml comment documents) and falls back
// to an app_secret line in config.yaml.
func LoadConfig(dir string) (Config, error) {
	cfg := Config{Dir: dir}
	b, err := privatefs.ReadSecret(filepath.Join(dir, "config.yaml"))
	if err != nil {
		return cfg, fmt.Errorf("lark: open config.yaml: %w", err)
	}

	var domainRaw string
	sc := bufio.NewScanner(strings.NewReader(string(b)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := splitYAMLScalar(line)
		if !ok {
			continue
		}
		switch k {
		case "app_id":
			cfg.AppID = v
		case "app_secret":
			cfg.AppSecret = v
		case "domain":
			domainRaw = v
		}
	}
	if env := os.Getenv("LARK_APP_SECRET"); env != "" {
		cfg.AppSecret = env
	}
	cfg.Domain = resolveDomain(domainRaw)
	if cfg.AppID == "" {
		return cfg, fmt.Errorf("lark: app_id missing in %s/config.yaml", dir)
	}
	if cfg.AppSecret == "" {
		return cfg, fmt.Errorf("lark: app_secret missing (set LARK_APP_SECRET or config.yaml)")
	}
	return cfg, nil
}

// splitYAMLScalar parses a top-level `key: "value"` (or `key: value`) line.
// Only handles the flat scalars we need; nested mappings are ignored.
func splitYAMLScalar(line string) (key, val string, ok bool) {
	i := strings.IndexByte(line, ':')
	if i < 0 {
		return "", "", false
	}
	key = strings.TrimSpace(line[:i])
	val = strings.TrimSpace(line[i+1:])
	val = strings.Trim(val, `"'`)
	if key == "" || val == "" {
		return "", "", false
	}
	return key, val, true
}

// tokenFile mirrors ~/.lark-config/tokens.json as written by lark-cli.
type tokenFile struct {
	AccessToken           string `json:"access_token"`
	RefreshToken          string `json:"refresh_token"`
	ExpiresAt             string `json:"expires_at"`
	RefreshTokenExpiresAt string `json:"refresh_token_expires_at"`
	Scope                 string `json:"scope"`
	// Enriched on refresh so the connector knows the user's own identity.
	OpenID string `json:"open_id,omitempty"`
	UserID string `json:"user_id,omitempty"`
}

// tokenStore manages the user access token lifecycle: load from disk, refresh
// via the refresh_token, and persist the rotated token back. Reusing the
// already-approved credentials at ~/.lark-config.
type tokenStore struct {
	path       string
	appID      string
	appSecret  string
	domain     string
	httpClient *http.Client
	mu         sync.Mutex
	tf         tokenFile
}

func newTokenStore(cfg Config) (*tokenStore, error) {
	ts := &tokenStore{
		path:      filepath.Join(cfg.Dir, "tokens.json"),
		appID:     cfg.AppID,
		appSecret: cfg.AppSecret,
		domain:    resolveDomain(cfg.Domain),
		httpClient: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}
	if err := ts.load(); err != nil {
		return nil, err
	}
	return ts, nil
}

func (s *tokenStore) load() error {
	b, err := privatefs.ReadSecret(s.path)
	if err != nil {
		return fmt.Errorf("lark: read tokens.json: %w", err)
	}
	if err := json.Unmarshal(b, &s.tf); err != nil {
		return fmt.Errorf("lark: parse tokens.json: %w", err)
	}
	return nil
}

// crossProcessLock takes an exclusive advisory flock on a sidecar .lock file so only
// ONE switchboardd process rotates the single-use Lark refresh_token at a time (#28).
// Without it, a deploy-overlap second daemon (old + new alive briefly) can both refresh
// → double-rotate the single-use token → the recurring revoke. Blocking LOCK_EX is
// deliberate: a contending owner WAITS, then (in refreshLocked) reloads + reuses the
// rotated token instead of refreshing with a now-stale one. Best-effort: if locking is
// unavailable, returns a no-op unlock so refresh degrades rather than hard-failing.
//
// NOTE: this guards switchboardd-vs-switchboardd only — a FOREIGN `lark` CLI run against
// this config won't respect our lock, so the standing invariant remains: NEVER run the
// lark CLI against the connector config dir; switchboardd is the SOLE token owner.
func (s *tokenStore) crossProcessLock() (func(), error) {
	if err := privatefs.PrepareFile(s.path + ".lock"); err != nil {
		return func() {}, err
	}
	f, err := os.OpenFile(s.path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return func() {}, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return func() {}, err
	}
	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}

func (s *tokenStore) save() error {
	b, err := json.MarshalIndent(s.tf, "", "  ")
	if err != nil {
		return err
	}
	return privatefs.WriteFile(s.path, b)
}

// selfOpenID returns the user's own open_id (known after a refresh).
func (s *tokenStore) selfOpenID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tf.OpenID
}

// expiresAt parses the stored expiry; zero time if unset/unparseable.
func (s *tokenStore) expiresAt() time.Time {
	t, err := time.Parse(time.RFC3339, s.tf.ExpiresAt)
	if err != nil {
		return time.Time{}
	}
	return t
}

// fresh reports whether the access token is valid with a safety margin.
func (s *tokenStore) fresh(now time.Time) bool {
	exp := s.expiresAt()
	return !exp.IsZero() && now.Add(60*time.Second).Before(exp)
}

// AccessToken returns a valid user access token, refreshing if necessary.
func (s *tokenStore) AccessToken(ctx context.Context, client *lark.Client, now time.Time) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fresh(now) && s.tf.AccessToken != "" {
		return s.tf.AccessToken, nil
	}
	if err := s.refreshLocked(ctx, client, now); err != nil {
		return "", err
	}
	return s.tf.AccessToken, nil
}

// Refresh forces a token refresh (used at Connect/Login time).
func (s *tokenStore) Refresh(ctx context.Context, client *lark.Client, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refreshLocked(ctx, client, now)
}

// oauthTokenPath is the Lark v2 OAuth token endpoint. The user tokens minted by
// the lark-cli `auth login` are v2-OAuth tokens, so refresh MUST use v2 — the
// legacy authen/v1 refresh_access_token rejects a v2 refresh_token with
// code=20026 "refresh token not found".
const oauthTokenPath = "/open-apis/authen/v2/oauth/token"

// v2TokenResponse covers both the success shape and the OAuth-style error shape
// the v2 token endpoint can return.
type v2TokenResponse struct {
	Code                  int    `json:"code"`
	Msg                   string `json:"msg"`
	Error                 string `json:"error"`
	ErrorDescription      string `json:"error_description"`
	AccessToken           string `json:"access_token"`
	RefreshToken          string `json:"refresh_token"`
	TokenType             string `json:"token_type"`
	ExpiresIn             int    `json:"expires_in"`
	RefreshTokenExpiresIn int    `json:"refresh_token_expires_in"`
	Scope                 string `json:"scope"`
}

// refreshLocked exchanges the refresh_token for a new user_access_token via the
// v2 OAuth token endpoint, rotates the (single-use) refresh_token, and persists
// atomically so the rotation chain survives restarts. Caller holds s.mu.
func (s *tokenStore) refreshLocked(ctx context.Context, _ *lark.Client, now time.Time) error {
	if s.tf.RefreshToken == "" {
		return fmt.Errorf("lark: no refresh_token in tokens.json (needs interactive auth)")
	}
	// Single-owner cross-process guard (#28): serialize refresh across processes so the
	// single-use refresh_token is never double-rotated. Under the lock, RELOAD from disk
	// (another owner may have rotated since we loaded) and re-check — if the on-disk
	// token is now fresh, reuse it instead of refreshing again with a stale token.
	if unlock, lerr := s.crossProcessLock(); lerr == nil {
		defer unlock()
		if rerr := s.load(); rerr == nil && s.fresh(now) && s.tf.AccessToken != "" {
			return nil // another owner already refreshed under the lock — reuse, don't re-rotate
		}
		if s.tf.RefreshToken == "" { // reload could have surfaced a cleared token
			return fmt.Errorf("lark: no refresh_token in tokens.json (needs interactive auth)")
		}
	}
	body := map[string]string{
		"grant_type":    "refresh_token",
		"client_id":     s.appID,
		"client_secret": s.appSecret,
		"refresh_token": s.tf.RefreshToken,
	}
	// AccessTokenTypeNone: the v2 token endpoint authenticates via
	// client_id/client_secret in the body, not a bearer token.
	// OAuth uses a string error; the SDK expects the resource API error object.
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("lark: v2 refresh encode: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.domain, "/")+oauthTokenPath, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("lark: v2 refresh request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("lark: v2 refresh request: %w", err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("lark: v2 refresh response: %w", err)
	}
	var out v2TokenResponse
	if err := json.Unmarshal(responseBody, &out); err != nil {
		return fmt.Errorf("lark: v2 refresh decode (status=%d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || out.Code != 0 || out.AccessToken == "" {
		return fmt.Errorf("lark: refresh token failed: status=%d code=%d error=%q error_description=%q msg=%q", resp.StatusCode, out.Code, out.Error, out.ErrorDescription, out.Msg)
	}
	s.tf.AccessToken = out.AccessToken
	if out.RefreshToken != "" {
		s.tf.RefreshToken = out.RefreshToken
	}
	if out.ExpiresIn > 0 {
		s.tf.ExpiresAt = now.Add(time.Duration(out.ExpiresIn) * time.Second).Format(time.RFC3339)
	}
	if out.RefreshTokenExpiresIn > 0 {
		s.tf.RefreshTokenExpiresAt = now.Add(time.Duration(out.RefreshTokenExpiresIn) * time.Second).Format(time.RFC3339)
	}
	if out.Scope != "" {
		s.tf.Scope = out.Scope
	}
	return s.save()
}

// ensureIdentity fills the user's own open_id (and user_id) via authen user_info
// using the CURRENT access token — WITHOUT forcing a refresh. This is how the
// connector learns self identity when the access token is already valid (the
// v2 refresh path no longer returns open_id). Best-effort: a failure here does
// not block capture, it only leaves is_self unmarked.
func (s *tokenStore) ensureIdentity(ctx context.Context, client *lark.Client, now time.Time) error {
	s.mu.Lock()
	have := s.tf.OpenID != ""
	s.mu.Unlock()
	if have {
		return nil
	}
	tok, err := s.AccessToken(ctx, client, now) // lazy: refreshes only if stale
	if err != nil {
		return err
	}
	resp, err := client.Authen.UserInfo.Get(ctx, larkcore.WithUserAccessToken(tok))
	if err != nil {
		return fmt.Errorf("lark: user_info request: %w", err)
	}
	if !resp.Success() {
		return fmt.Errorf("lark: user_info failed: code=%d msg=%s", resp.Code, resp.Msg)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if resp.Data != nil {
		if resp.Data.OpenId != nil {
			s.tf.OpenID = *resp.Data.OpenId
		}
		if resp.Data.UserId != nil {
			s.tf.UserID = *resp.Data.UserId
		}
	}
	return s.save()
}
