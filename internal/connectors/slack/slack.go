// Package slack implements a read-only Slack Web API connector.
package slack

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seahyc/switchboard-oss/internal/connectors"
	"github.com/seahyc/switchboard-oss/internal/event"
)

const platform = "slack"

var ErrUnsupported = errors.New("slack: action is unsupported (read-only connector)")

// Config selects a Slack user token and the exact conversations the connector
// may read. Channels must be explicit IDs; an empty list is rejected.
type Config struct {
	TokenFile    string
	Channels     []string
	PollInterval time.Duration
	Since        time.Time
	BaseURL      string
	HTTPClient   *http.Client
}

type Connector struct {
	cfg       Config
	client    *http.Client
	token     string
	teamID    string
	userID    string
	channels  []string
	mu        sync.Mutex
	connected bool
	seen      map[string]string
	seenOrder []string
}

var _ connectors.Connector = (*Connector)(nil)
var _ connectors.ChatMetadataProvider = (*Connector)(nil)

func New(cfg Config) (*Connector, error) {
	if strings.TrimSpace(cfg.TokenFile) == "" {
		return nil, errors.New("slack: TokenFile is required")
	}
	if len(cfg.Channels) == 0 {
		return nil, errors.New("slack: explicit Channels allowlist is required")
	}
	info, err := os.Lstat(cfg.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("slack: inspect token file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("slack: token path must be a regular file, not a symlink")
	}
	f, err := os.Open(cfg.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("slack: open token file: %w", err)
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) || !opened.Mode().IsRegular() {
		return nil, errors.New("slack: token file changed while opening")
	}
	if opened.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("slack: token file must not be accessible by group or others (chmod 600)")
	}
	b, err := io.ReadAll(io.LimitReader(f, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("slack: read token file: %w", err)
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return nil, errors.New("slack: token file is empty")
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://slack.com/api"
	}
	if err := validateBaseURL(cfg.BaseURL); err != nil {
		return nil, err
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 30 * time.Second
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 20 * time.Second}
	}
	// Do not let a redirect forward the bearer credential to another host.
	client := *cfg.HTTPClient
	priorRedirect := client.CheckRedirect
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) > 0 && !strings.EqualFold(req.URL.Host, via[0].URL.Host) {
			return http.ErrUseLastResponse
		}
		if priorRedirect != nil {
			return priorRedirect(req, via)
		}
		return http.ErrUseLastResponse
	}
	seenChannels := map[string]bool{}
	channels := make([]string, 0, len(cfg.Channels))
	for _, ch := range cfg.Channels {
		ch = strings.TrimSpace(strings.TrimPrefix(ch, "slack:"))
		if ch == "" || strings.Contains(ch, ":") {
			return nil, fmt.Errorf("slack: invalid channel ID %q", ch)
		}
		if !seenChannels[ch] {
			channels = append(channels, ch)
			seenChannels[ch] = true
		}
	}
	c := &Connector{cfg: cfg, client: &client, token: token, channels: channels, seen: map[string]string{}}
	var auth struct {
		TeamID string `json:"team_id"`
		UserID string `json:"user_id"`
	}
	if err := c.api(context.Background(), "auth.test", nil, &auth); err != nil {
		return nil, fmt.Errorf("slack: auth.test: %w", err)
	}
	if auth.TeamID == "" || auth.UserID == "" {
		return nil, errors.New("slack: auth.test returned incomplete identity")
	}
	c.teamID, c.userID = auth.TeamID, auth.UserID
	return c, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validateBaseURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil {
		return errors.New("slack: BaseURL must be an absolute URL")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && isLoopbackHost(u.Hostname())) {
		return errors.New("slack: BaseURL must use HTTPS except for localhost test servers")
	}
	return nil
}

func (c *Connector) Name() string              { return platform }
func canonChat(team, channel string) string    { return platform + ":" + team + ":" + channel }
func canonMsg(team, channel, ts string) string { return canonChat(team, channel) + ":" + ts }

func (c *Connector) api(ctx context.Context, method string, q url.Values, dst any) error {
	base := strings.TrimRight(c.cfg.BaseURL, "/")
	u, err := url.Parse(base + "/" + method)
	if err != nil {
		return errors.New("slack: invalid API URL")
	}
	if q != nil {
		u.RawQuery = q.Encode()
	}
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	retryUntil := time.Now().Add(31 * time.Second)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return fmt.Errorf("slack: create request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Accept", "application/json")
		resp, err := c.client.Do(req)
		if err != nil {
			return fmt.Errorf("slack: request failed: %w", err)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait, err := retryDelay(resp.Header.Get("Retry-After"))
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if err != nil {
				return fmt.Errorf("slack: rate limited: %w", err)
			}
			if time.Now().Add(wait).After(retryUntil) {
				return errors.New("slack: rate limited beyond retry bound")
			}
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			resp.Body.Close()
			return fmt.Errorf("slack: HTTP status %d", resp.StatusCode)
		}
		var body json.RawMessage
		if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&body); err != nil {
			resp.Body.Close()
			return fmt.Errorf("slack: decode response: %w", err)
		}
		resp.Body.Close()
		var envelope struct {
			OK    bool   `json:"ok"`
			Error string `json:"error"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			return fmt.Errorf("slack: decode response: %w", err)
		}
		if !envelope.OK {
			if envelope.Error == "" {
				envelope.Error = "unknown_error"
			}
			return fmt.Errorf("slack: API error %s", envelope.Error)
		}
		if dst != nil {
			if err := json.Unmarshal(body, dst); err != nil {
				return fmt.Errorf("slack: decode payload: %w", err)
			}
		}
		return nil
	}
}

func retryDelay(v string) (time.Duration, error) {
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 0 {
		return 0, errors.New("invalid Retry-After header")
	}
	if n > 30 {
		return 0, fmt.Errorf("Retry-After %ds exceeds the 30s retry bound", n)
	}
	return time.Duration(n) * time.Second, nil
}

type slackMessage struct {
	Type       string `json:"type"`
	User       string `json:"user"`
	BotID      string `json:"bot_id"`
	Text       string `json:"text"`
	TS         string `json:"ts"`
	ThreadTS   string `json:"thread_ts"`
	ReplyCount int    `json:"reply_count"`
	Subtype    string `json:"subtype"`
	Edited     *struct {
		TS string `json:"ts"`
	} `json:"edited"`
}
type historyResponse struct {
	Messages         []slackMessage `json:"messages"`
	HasMore          bool           `json:"has_more"`
	ResponseMetadata struct {
		NextCursor string `json:"next_cursor"`
	} `json:"response_metadata"`
}

func parseSlackTS(s string) (time.Time, error) {
	parts := strings.SplitN(s, ".", 2)
	sec, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid Slack ts")
	}
	ns := int64(0)
	if len(parts) == 2 {
		frac := parts[1]
		if frac == "" {
			return time.Time{}, errors.New("invalid Slack ts")
		}
		if len(frac) > 9 {
			frac = frac[:9]
		}
		frac += strings.Repeat("0", 9-len(frac))
		ns, err = strconv.ParseInt(frac, 10, 64)
		if err != nil {
			return time.Time{}, errors.New("invalid Slack ts")
		}
	}
	return time.Unix(sec, ns).UTC(), nil
}

func (c *Connector) emitMessage(ctx context.Context, out chan<- event.Message, channel string, sm slackMessage, source string, root string) error {
	if sm.TS == "" {
		return nil
	}
	ts, err := parseSlackTS(sm.TS)
	if err != nil {
		return nil
	}
	chat := canonChat(c.teamID, channel)
	id := canonMsg(c.teamID, channel, sm.TS)
	sender := sm.User
	if sender == "" {
		sender = sm.BotID
	}
	kind, editTS := event.KindText, ""
	var observedEditTS *time.Time
	if sm.Edited != nil && sm.Edited.TS != "" {
		kind, editTS = event.KindEdit, sm.Edited.TS
	}
	key := chat + ":" + sm.TS
	fingerprint := sha256.Sum256([]byte(sm.Text + "\x00" + sender + "\x00" + sm.ThreadTS))
	fp := hex.EncodeToString(fingerprint[:])
	c.mu.Lock()
	old, exists := c.seen[key]
	c.mu.Unlock()
	if source == event.SourceLive && exists && old == fp {
		return nil
	}
	if source == event.SourceLive && exists && old != fp {
		kind = event.KindEdit
		if editTS == "" {
			observed := time.Now().UTC()
			observedEditTS = &observed
		}
	}
	if kind == event.KindEdit {
		version := editTS
		if version == "" {
			// Some history responses omit edited.ts. Make each observed content
			// version deterministic and distinct even when two edits arrive in
			// the same second.
			version = "observed-" + hex.EncodeToString(fingerprint[:8])
		}
		id += ":edit:" + version
	}
	m := event.Message{ID: id, Platform: platform, AccountID: platform + ":" + c.teamID + ":" + c.userID, ChatID: chat, ChatType: event.ChatGroup,
		Sender: event.Sender{PlatformUserID: platform + ":" + c.teamID + ":" + sender, IsSelf: sender == c.userID}, TS: ts, Kind: kind, Text: sm.Text, Source: source, IngestedAt: time.Now().UTC()}
	if sm.ThreadTS != "" {
		m.ThreadID = canonMsg(c.teamID, channel, sm.ThreadTS)
		if sm.ThreadTS != sm.TS {
			m.ReplyTo = m.ThreadID
		}
	}
	if kind == event.KindEdit {
		m.ReplyTo = canonMsg(c.teamID, channel, sm.TS)
		if observedEditTS != nil {
			m.EditedTS = observedEditTS
		} else if et, e := parseSlackTS(editTS); e == nil {
			m.EditedTS = &et
		}
	}
	if root != "" && sm.TS != root && m.ReplyTo == "" {
		m.ReplyTo = canonMsg(c.teamID, channel, root)
		m.ThreadID = m.ReplyTo
	}
	select {
	case out <- m:
		c.remember(key, fp)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Connector) remember(key, fingerprint string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.seen[key]; !exists {
		c.seenOrder = append(c.seenOrder, key)
	}
	c.seen[key] = fingerprint
	const maxSeen = 20000
	if len(c.seenOrder) > maxSeen {
		oldest := c.seenOrder[0]
		c.seenOrder = c.seenOrder[1:]
		delete(c.seen, oldest)
	}
}

func (c *Connector) channelForChat(chatID string) (string, error) {
	if chatID == "" {
		return "", nil
	}
	parts := strings.Split(chatID, ":")
	channel := chatID
	if len(parts) == 3 && parts[0] == platform && parts[1] == c.teamID {
		channel = parts[2]
	}
	for _, allowed := range c.channels {
		if channel == allowed {
			return channel, nil
		}
	}
	return "", fmt.Errorf("slack: chat %q is outside the configured channel allowlist", chatID)
}

func (c *Connector) fetch(ctx context.Context, channel string, since time.Time, source string, out chan<- event.Message) error {
	cursor := ""
	for {
		q := url.Values{"channel": {channel}, "limit": {"200"}}
		if !since.IsZero() {
			q.Set("oldest", strconv.FormatInt(since.Unix(), 10)+"."+fmt.Sprintf("%06d", since.Nanosecond()/1000))
		}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var page historyResponse
		if err := c.api(ctx, "conversations.history", q, &page); err != nil {
			return err
		}
		for _, msg := range page.Messages {
			if err := c.emitMessage(ctx, out, channel, msg, source, ""); err != nil {
				return err
			}
			if msg.ReplyCount > 0 {
				if err := c.fetchThread(ctx, channel, msg.TS, source, out); err != nil {
					return err
				}
			}
		}
		cursor = page.ResponseMetadata.NextCursor
		if cursor == "" && page.HasMore {
			return errors.New("slack: history response has_more without a next_cursor; refusing to truncate")
		}
		if cursor == "" {
			break
		}
	}
	return nil
}

func (c *Connector) fetchThread(ctx context.Context, channel, root, source string, out chan<- event.Message) error {
	cursor := ""
	for {
		q := url.Values{"channel": {channel}, "ts": {root}, "limit": {"200"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var page historyResponse
		if err := c.api(ctx, "conversations.replies", q, &page); err != nil {
			return err
		}
		for _, msg := range page.Messages {
			if msg.TS == root {
				continue // root was already emitted by conversations.history
			}
			if err := c.emitMessage(ctx, out, channel, msg, source, root); err != nil {
				return err
			}
		}
		cursor = page.ResponseMetadata.NextCursor
		if cursor == "" && page.HasMore {
			return errors.New("slack: replies response has_more without a next_cursor; refusing to truncate")
		}
		if cursor == "" {
			return nil
		}
	}
}

func (c *Connector) Connect(ctx context.Context, out chan<- event.Message) error {
	c.mu.Lock()
	c.connected = true
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.connected = false; c.mu.Unlock() }()
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()
	pollSince := make(map[string]time.Time, len(c.channels))
	for _, channel := range c.channels {
		pollSince[channel] = c.cfg.Since
		if pollSince[channel].IsZero() {
			pollSince[channel] = time.Now().Add(-5 * time.Minute)
		}
	}
	for {
		for _, channel := range c.channels {
			if err := c.fetch(ctx, channel, pollSince[channel], event.SourceLive, out); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				slog.Warn("slack poll failed", "channel_id", channel, "err", err)
				// Keep this channel's lower bound unchanged. The next poll retries
				// the entire incomplete window, including any partial page results.
				continue
			}
			pollSince[channel] = time.Now().Add(-5 * time.Minute)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Connector) Backfill(ctx context.Context, chatID string, since time.Time, out chan<- event.Message) error {
	if chatID != "" {
		channel, err := c.channelForChat(chatID)
		if err != nil {
			return err
		}
		return c.fetch(ctx, channel, since, event.SourceBackfill, out)
	}
	for _, channel := range c.channels {
		if err := c.fetch(ctx, channel, since, event.SourceBackfill, out); err != nil {
			return err
		}
	}
	return nil
}

func (c *Connector) Send(context.Context, connectors.SendRequest) (string, error) {
	return "", ErrUnsupported
}
func (c *Connector) React(context.Context, string, string, string) error       { return ErrUnsupported }
func (c *Connector) Login(ctx context.Context) (connectors.LoginStatus, error) { return c.Status(ctx) }
func (c *Connector) Status(ctx context.Context) (connectors.LoginStatus, error) {
	if ctx.Err() != nil {
		return connectors.LoginStatus{}, ctx.Err()
	}
	c.mu.Lock()
	connected := c.connected
	c.mu.Unlock()
	return connectors.LoginStatus{Connected: connected, Detail: fmt.Sprintf("Slack user %s in team %s; read-only, %d allowlisted channels", c.userID, c.teamID, len(c.channels))}, nil
}

func (c *Connector) ChatMetadata(ctx context.Context) ([]connectors.ChatMeta, error) {
	var out []connectors.ChatMeta
	for _, channel := range c.channels {
		q := url.Values{"channel": {channel}}
		var result struct {
			Channel struct {
				ID     string `json:"id"`
				Name   string `json:"name"`
				IsIM   bool   `json:"is_im"`
				IsMpim bool   `json:"is_mpim"`
			} `json:"channel"`
		}
		if err := c.api(ctx, "conversations.info", q, &result); err != nil {
			return nil, err
		}
		out = append(out, connectors.ChatMeta{ChatID: canonChat(c.teamID, channel), Title: result.Channel.Name})
	}
	return out, nil
}
