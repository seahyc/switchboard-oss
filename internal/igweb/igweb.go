// Package igweb is a READ-ONLY client for Instagram's private web API, used to
// "browse" the user's IG network as the user (profiles, stories, feed) and
// return structured cards — not pixels. It reuses the EXISTING session from the
// mautrix-meta bridge (no separate login, no re-auth): the cookies are exported
// to a local file (see the ig-session export timer) and loaded here.
//
// HARD RULES (enforced in code):
//   - Every request is SOCKS5-proxied through the residential (home-IP) proxy AND
//     the egress IP is asserted to equal the expected home IP BEFORE the read.
//     If egress can't be confirmed as the home IP, we FAIL CLOSED (error, no read)
//     — never fall back to the datacenter IP.
//   - Read-only. No likes/follows/DMs/writes. Only GETs to read endpoints.
//   - A stale/rotated session surfaces as ErrLoggedOut (a clean connector-health
//     signal) rather than a silent empty read.
package igweb

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

// Required IG cookie keys; a session missing any of these is unusable.
var requiredCookies = []string{"sessionid", "csrftoken", "ds_user_id"}

// igAppID is the public web app id IG's web client sends; required for the
// private web endpoints to return JSON rather than an HTML login page.
const igAppID = "936619743392459"

const webUA = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15"

// ErrLoggedOut means the session is missing/expired/rotated — surfaced as a
// connector-health error so "logged out" is distinguishable from "nothing new".
var ErrLoggedOut = errors.New("igweb: instagram session missing or expired (re-auth needed)")

// ErrProxyEgress means the egress IP through the proxy was NOT the expected home
// IP (or couldn't be confirmed). We fail closed — never read off the DC IP.
var ErrProxyEgress = errors.New("igweb: proxy egress is not the expected home IP — refusing to read")

// Config configures the client.
type Config struct {
	SessionPath    string // file with the exported IG cookies (JSON)
	ProxyAddr      string // SOCKS5 host:port, e.g. 127.0.0.1:1080
	ExpectedEgress string // the home IP the proxy must egress (e.g. 203.0.113.10)
	// EgressTTL caches a successful egress check for this long to avoid hammering
	// the IP-echo service on bursts; 0 → re-verify every call.
	EgressTTL time.Duration
}

// Client is the read-only IG private-web client.
type Client struct {
	cfg  Config
	http *http.Client

	mu            sync.Mutex
	lastEgressOK  time.Time
	ipEchoURL     string        // overridable in tests
	pageDelay     time.Duration // inter-page pacing for auto-paginated reads (0 in tests)
	loadSessionFn func() (map[string]string, error)

	followMu     sync.Mutex    // guards the following-feed + following-list caches
	followCache  []FeedPost    // last merged following-feed (sorted newest-first)
	followCachAt time.Time     // when followCache was built
	followList   []FollowEntry // last enriched following-list (#49 classify source)
	followListAt time.Time     // when followList was built
}

// pace sleeps for the inter-call delay (skipped when pageDelay==0, e.g. tests),
// honoring context cancellation. Used to space out fan-out reads conservatively.
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

// New builds a client whose HTTP transport dials EVERYTHING through the SOCKS5
// proxy (so no IG traffic can ever take the datacenter path).
func New(cfg Config) (*Client, error) {
	if cfg.ProxyAddr == "" || cfg.ExpectedEgress == "" {
		return nil, errors.New("igweb: proxy addr + expected egress required")
	}
	dialer, err := proxy.SOCKS5("tcp", cfg.ProxyAddr, nil, proxy.Direct)
	if err != nil {
		return nil, fmt.Errorf("igweb: socks5 dialer: %w", err)
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
		pageDelay: feedPageDelay,
	}
	c.loadSessionFn = c.loadSessionFromFile
	return c, nil
}

// loadSessionFromFile reads the exported cookie JSON. Accepts either the raw
// {"sessionid":..} map or the bridge's {"cookies":{...}} envelope.
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
		return nil, fmt.Errorf("igweb: bad session file: %w", err)
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

// get performs an egress-verified, session-authed GET against an IG private web
// endpoint and decodes the JSON into out. Read-only by construction.
func (c *Client) get(ctx context.Context, rawURL string, out any) error {
	if err := c.verifyEgress(ctx); err != nil {
		return err
	}
	ck, err := c.loadSessionFn()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", webUA)
	req.Header.Set("X-IG-App-ID", igAppID)
	req.Header.Set("Cookie", cookieHeader(ck))
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return ErrLoggedOut
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("igweb: %s → HTTP %d", endpointName(rawURL), resp.StatusCode)
	}
	if err := json.Unmarshal(body, out); err != nil {
		// A login redirect returns HTML, not JSON — treat as logged out.
		if len(body) > 0 && body[0] == '<' {
			return ErrLoggedOut
		}
		return fmt.Errorf("igweb: decode %s: %w", endpointName(rawURL), err)
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

func endpointName(rawURL string) string {
	if u, err := url.Parse(rawURL); err == nil {
		return u.Path
	}
	return rawURL
}
