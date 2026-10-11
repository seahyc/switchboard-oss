// Package xweb is a READ-ONLY client for X.com (Twitter)'s private GraphQL web
// API, used to "browse" the user's X network as the user (chronological Following
// timeline, a user's tweets, profile) and return structured cards mapped to the
// social_activity shape — not pixels. It reuses the user's EXISTING logged-in web
// session (the auth_token + ct0 cookies, exported once from the browser to a local
// file), the same pattern as igweb reuses the IG bridge cookies.
//
// HARD RULES (enforced in code, mirroring igweb):
//   - Every request is SOCKS5-proxied through the residential (home-IP) proxy AND
//     the egress IP is asserted to equal the expected home IP BEFORE the read.
//     If egress can't be confirmed as the home IP, we FAIL CLOSED (error, no read)
//     — never fall back to the datacenter IP. X actively flags automation; a
//     datacenter IP is the fastest way to get the account limited.
//   - Read-only. Only GETs to read GraphQL endpoints. No posts/likes/follows/DMs.
//   - A stale/rotated session surfaces as ErrLoggedOut (a clean connector-health
//     signal) rather than a silent empty read.
//   - queryIds / bearer / feature-switches are HARVESTED from X's JS bundle at
//     runtime (they rotate per web build — the X analog of IG's doc_id) so nothing
//     fragile is hardcoded; baked values are only a fallback.
package xweb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/seahyc/switchboard-oss/internal/privatefs"
	"golang.org/x/net/proxy"
)

// requiredCookies: a session missing either is unusable. auth_token is the
// session credential; ct0 is the CSRF token echoed back as the x-csrf-token header.
var requiredCookies = []string{"auth_token", "ct0"}

// publicBearer is X's long-standing public web Bearer (the same constant the web
// app ships). Used as a fallback if bundle-harvesting the bearer fails; the web
// client has shipped this value for years.
const publicBearer = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"

const webUA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
	"(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

// ErrLoggedOut means the session is missing/expired/rotated — surfaced as a
// connector-health error so "logged out" is distinguishable from "nothing new".
var ErrLoggedOut = errors.New("xweb: x.com session missing or expired (re-auth needed)")

// ErrProxyEgress means the egress IP through the proxy was NOT the expected home
// IP (or couldn't be confirmed). We fail closed — never read off the DC IP.
var ErrProxyEgress = errors.New("xweb: proxy egress is not the expected home IP — refusing to read")

// Config configures the client.
type Config struct {
	SessionPath    string        // file with the exported X cookies (JSON: auth_token + ct0)
	ProxyAddr      string        // SOCKS5 host:port, e.g. 127.0.0.1:1080
	ExpectedEgress string        // the home IP the proxy must egress (e.g. 203.0.113.10)
	EgressTTL      time.Duration // cache a good egress check this long (0 → every call)
}

// Client is the read-only X private-web client.
type Client struct {
	cfg  Config
	http *http.Client

	mu           sync.Mutex
	lastEgressOK time.Time
	ipEchoURL    string // overridable in tests
	pageDelay    time.Duration

	bundleMu sync.Mutex
	bundle   bundleMeta // harvested queryIds / bearer / features (cached)
	bundleAt time.Time

	loadSessionFn func() (map[string]string, error)
}

// New builds a client whose HTTP transport dials EVERYTHING through the SOCKS5
// proxy (so no X traffic can ever take the datacenter path).
func New(cfg Config) (*Client, error) {
	if cfg.ProxyAddr == "" || cfg.ExpectedEgress == "" {
		return nil, errors.New("xweb: proxy addr + expected egress required")
	}
	dialer, err := proxy.SOCKS5("tcp", cfg.ProxyAddr, nil, proxy.Direct)
	if err != nil {
		return nil, fmt.Errorf("xweb: socks5 dialer: %w", err)
	}
	tr := &http.Transport{
		DialContext: func(_ context.Context, network, addr string) (net.Conn, error) {
			return dialer.Dial(network, addr)
		},
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        4,
		IdleConnTimeout:     30 * time.Second,
		TLSHandshakeTimeout: 15 * time.Second,
	}
	c := &Client{
		cfg:       cfg,
		http:      &http.Client{Transport: tr, Timeout: 30 * time.Second},
		ipEchoURL: "https://api.ipify.org",
		pageDelay: 1500 * time.Millisecond,
	}
	c.loadSessionFn = c.loadSessionFromFile
	return c, nil
}

// loadSessionFromFile reads the exported cookie JSON. Accepts either the raw
// {"auth_token":..,"ct0":..} map or a {"cookies":{...}} envelope.
func (c *Client) loadSessionFromFile() (map[string]string, error) {
	b, err := privatefs.ReadSecret(c.cfg.SessionPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrLoggedOut
		}
		return nil, err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("xweb: bad session file: %w", err)
	}
	ck := map[string]string{}
	if inner, ok := raw["cookies"]; ok {
		_ = json.Unmarshal(inner, &ck)
	} else {
		for k, v := range raw {
			var s string
			if json.Unmarshal(v, &s) == nil {
				ck[k] = s
			}
		}
	}
	for _, k := range requiredCookies {
		if strings.TrimSpace(ck[k]) == "" {
			return nil, ErrLoggedOut
		}
	}
	return ck, nil
}

// verifyEgress asserts the proxy egresses the expected home IP. FAIL CLOSED.
func (c *Client) verifyEgress(ctx context.Context) error {
	if c.cfg.EgressTTL > 0 {
		c.mu.Lock()
		fresh := time.Since(c.lastEgressOK) < c.cfg.EgressTTL && !c.lastEgressOK.IsZero()
		c.mu.Unlock()
		if fresh {
			return nil
		}
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.ipEchoURL, nil)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: egress check failed: %v", ErrProxyEgress, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64))
	got := strings.TrimSpace(string(body))
	if got != c.cfg.ExpectedEgress {
		return fmt.Errorf("%w: got %q want %q", ErrProxyEgress, got, c.cfg.ExpectedEgress)
	}
	c.mu.Lock()
	c.lastEgressOK = time.Now()
	c.mu.Unlock()
	return nil
}

// graphqlGet performs an egress-verified, session-authed GET against an X private
// GraphQL endpoint (operation `op`, with the given variables) and decodes the JSON
// into out. Read-only by construction. queryId/bearer/features come from the
// harvested bundle. A 401/403 or an HTML login page → ErrLoggedOut.
func (c *Client) graphqlGet(ctx context.Context, op string, variables map[string]any, out any) error {
	if err := c.verifyEgress(ctx); err != nil {
		return err
	}
	ck, err := c.loadSessionFn()
	if err != nil {
		return err
	}
	bm, err := c.bundleInfo(ctx, ck)
	if err != nil {
		return err
	}
	qid := bm.queryIDs[op]
	if qid == "" {
		return fmt.Errorf("xweb: no queryId for operation %q", op)
	}
	feats := map[string]bool{}
	for _, k := range bm.features {
		feats[k] = true
	}
	q := url.Values{}
	q.Set("variables", mustJSON(variables))
	q.Set("features", mustJSON(feats))
	raw := "https://x.com/i/api/graphql/" + qid + "/" + op + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return err
	}
	bearer := bm.bearer
	if bearer == "" {
		bearer = publicBearer
	}
	req.Header.Set("User-Agent", webUA)
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("X-Csrf-Token", ck["ct0"])
	req.Header.Set("X-Twitter-Active-User", "yes")
	req.Header.Set("X-Twitter-Auth-Type", "OAuth2Session")
	req.Header.Set("X-Twitter-Client-Language", "en")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cookie", cookieHeader(ck))
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return ErrLoggedOut
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("xweb: %s → HTTP %d", op, resp.StatusCode)
	}
	if len(body) > 0 && body[0] == '<' {
		return ErrLoggedOut // a login/redirect HTML page
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("xweb: decode %s: %w", op, err)
	}
	return nil
}

func cookieHeader(ck map[string]string) string {
	parts := make([]string, 0, len(ck))
	for k, v := range ck {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, "; ")
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// pace sleeps the inter-call delay (skipped when pageDelay==0, e.g. tests),
// honoring context cancellation. Conservative spacing for the fan-out reads.
func (c *Client) pace(ctx context.Context) error {
	if c.pageDelay <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(c.pageDelay):
		return nil
	}
}
