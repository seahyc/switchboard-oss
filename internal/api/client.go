package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/igweb"
	"github.com/seahyc/switchboard-oss/internal/store"
	"github.com/seahyc/switchboard-oss/internal/xweb"
)

// Client talks to the daemon's local API. Used by the MCP server.
type Client struct {
	base     string
	http     *http.Client
	token    string
	tokenErr error
}

// NewClient dials the daemon at addr (host:port). If addr is empty, DefaultAddr.
func NewClient(addr string) *Client {
	if addr == "" {
		addr = DefaultAddr
	}
	token, tokenErr := clientCredential()
	if err := ValidateListenAddr(addr); err != nil {
		tokenErr = err
	}
	return &Client{
		token: token, tokenErr: tokenErr,
		base: "http://" + addr,
		http: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
}

func (c *Client) get(ctx context.Context, path string, q url.Values, out any) error {
	u := c.base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	return c.do(req, out)
}

func (c *Client) post(ctx context.Context, path string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req, out)
}

func (c *Client) do(req *http.Request, out any) error {
	if c.tokenErr != nil {
		return fmt.Errorf("API credential: %w", c.tokenErr)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("api %s: %s", resp.Status, bytes.TrimSpace(b))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func tsParam(q url.Values, key string, t time.Time) {
	if !t.IsZero() {
		q.Set(key, t.UTC().Format(time.RFC3339))
	}
}

// ListChats → GET /chats
func (c *Client) ListChats(ctx context.Context, platform string, since time.Time, limit int) ([]store.ChatSummary, error) {
	q := url.Values{}
	if platform != "" {
		q.Set("platform", platform)
	}
	tsParam(q, "since", since)
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out []store.ChatSummary
	return out, c.get(ctx, "/chats", q, &out)
}

// Message → GET /message?id= (#53): single-message point lookup by canonical id (PK).
func (c *Client) Message(ctx context.Context, id string) (event.Message, error) {
	var out event.Message
	return out, c.get(ctx, "/message", url.Values{"id": {id}}, &out)
}

// Read → GET /read
func (c *Client) Read(ctx context.Context, chatID string, since time.Time, limit int) ([]event.Message, error) {
	return c.ReadWindow(ctx, chatID, since, time.Time{}, limit, false)
}

// ReadWindow is the #45 newest-first read: desc=true returns the newest `limit` messages
// by ts (recent-context), before is an exclusive upper-ts cursor for backward pagination.
// desc=false + before=zero is identical to Read (oldest-first, back-compat).
func (c *Client) ReadWindow(ctx context.Context, chatID string, since, before time.Time, limit int, desc bool) ([]event.Message, error) {
	q := url.Values{"chat_id": {chatID}}
	tsParam(q, "since", since)
	tsParam(q, "before", before)
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if desc {
		q.Set("desc", "true")
	}
	var out []event.Message
	return out, c.get(ctx, "/read", q, &out)
}

// Search → GET /search
func (c *Client) Search(ctx context.Context, query, platform, person string, since time.Time, limit int) ([]event.Message, error) {
	q := url.Values{"q": {query}}
	if platform != "" {
		q.Set("platform", platform)
	}
	if person != "" {
		q.Set("person", person)
	}
	tsParam(q, "since", since)
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out []event.Message
	return out, c.get(ctx, "/search", q, &out)
}

// Grep → GET /grep (#16): regex (RE2) scan over message text.
func (c *Client) Grep(ctx context.Context, pattern, platform, person string, since time.Time, limit int) (GrepResult, error) {
	q := url.Values{"pattern": {pattern}}
	if platform != "" {
		q.Set("platform", platform)
	}
	if person != "" {
		q.Set("person", person)
	}
	tsParam(q, "since", since)
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out GrepResult
	return out, c.get(ctx, "/grep", q, &out)
}

// QuerySQL → POST /query-sql (#36): arbitrary read-only SELECT over the firehose.
func (c *Client) QuerySQL(ctx context.Context, sql string) (store.QuerySQLResult, error) {
	var out store.QuerySQLResult
	return out, c.post(ctx, "/query-sql", QuerySQLBody{SQL: sql}, &out)
}

// Mentions → GET /mentions
func (c *Client) Mentions(ctx context.Context, since time.Time, limit int) ([]event.Message, error) {
	q := url.Values{}
	tsParam(q, "since", since)
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out []event.Message
	return out, c.get(ctx, "/mentions", q, &out)
}

// SocialActivity → GET /social/activity?platform=&since=&limit=&sub= (the unified social
// log: IG + X posts/reposts/stories/trends, raw). subs is comma-joined into ?sub=.
func (c *Client) SocialActivity(ctx context.Context, platform string, since time.Time, limit int, subs []string) ([]event.Message, error) {
	q := url.Values{}
	if platform != "" {
		q.Set("platform", platform)
	}
	tsParam(q, "since", since)
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if len(subs) > 0 {
		q.Set("sub", strings.Join(subs, ","))
	}
	var out []event.Message
	return out, c.get(ctx, "/social/activity", q, &out)
}

// Send → POST /send
func (c *Client) Send(ctx context.Context, body SendBody) (string, error) {
	var out SendResult
	if err := c.post(ctx, "/send", body, &out); err != nil {
		return "", err
	}
	return out.SentID, nil
}

// SendMedia → POST /send_media
func (c *Client) SendMedia(ctx context.Context, body SendMediaBody) (string, error) {
	var out SendResult
	if err := c.post(ctx, "/send_media", body, &out); err != nil {
		return "", err
	}
	return out.SentID, nil
}

// React → POST /react
func (c *Client) React(ctx context.Context, body ReactBody) error {
	return c.post(ctx, "/react", body, nil)
}

// StartChat → POST /start_chat. Resolves a platform identifier to a canonical
// chat_id (creating the conversation if needed), returning the chat_id + the
// resolved display name for confirmation.
func (c *Client) StartChat(ctx context.Context, body StartChatBody) (StartChatResult, error) {
	var out StartChatResult
	if err := c.post(ctx, "/start_chat", body, &out); err != nil {
		return StartChatResult{}, err
	}
	return out, nil
}

// IGProfile → GET /instagram/profile/{handle}.
func (c *Client) IGProfile(ctx context.Context, handle string) (igweb.ProfileCard, error) {
	var out igweb.ProfileCard
	if err := c.get(ctx, "/instagram/profile/"+url.PathEscape(handle), nil, &out); err != nil {
		return igweb.ProfileCard{}, err
	}
	return out, nil
}

// IGStories → GET /instagram/stories.
func (c *Client) IGStories(ctx context.Context) (igweb.StoriesResult, error) {
	var out igweb.StoriesResult
	if err := c.get(ctx, "/instagram/stories", nil, &out); err != nil {
		return igweb.StoriesResult{}, err
	}
	return out, nil
}

// IGFeed → GET /instagram/feed?max_id=&limit=. maxID is an optional pagination
// cursor (from a prior result's NextMaxID), "" for the first page; limit (>0)
// auto-paginates internally to return up to that many real posts.
func (c *Client) IGFeed(ctx context.Context, maxID string, limit int) (igweb.FeedResult, error) {
	q := url.Values{}
	if maxID != "" {
		q.Set("max_id", maxID)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out igweb.FeedResult
	if err := c.get(ctx, "/instagram/feed", q, &out); err != nil {
		return igweb.FeedResult{}, err
	}
	return out, nil
}

// IGFollowingFeed → GET /instagram/following_feed?limit=. Recent posts from ONLY
// the accounts the user follows, merged chronologically, no recommendations.
func (c *Client) IGFollowingFeed(ctx context.Context, limit int) (igweb.FeedResult, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out igweb.FeedResult
	if err := c.get(ctx, "/instagram/following_feed", q, &out); err != nil {
		return igweb.FeedResult{}, err
	}
	return out, nil
}

// IGFollowing → GET /instagram/following. The complete enriched follow-list.
func (c *Client) IGFollowing(ctx context.Context) (igweb.FollowingList, error) {
	var out igweb.FollowingList
	if err := c.get(ctx, "/instagram/following", nil, &out); err != nil {
		return igweb.FollowingList{}, err
	}
	return out, nil
}

// XFollowingFeed → GET /x/following_feed?limit=. Chronological X Following timeline.
func (c *Client) XFollowingFeed(ctx context.Context, limit int) (xweb.FeedResult, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out xweb.FeedResult
	if err := c.get(ctx, "/x/following_feed", q, &out); err != nil {
		return xweb.FeedResult{}, err
	}
	return out, nil
}

// IngestXActivity → POST /x/ingest?limit=. Pulls the X following-feed into the log as
// social_activity events (#55). Returns the count ingested.
func (c *Client) IngestXActivity(ctx context.Context, limit int) (int, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out struct {
		Posts int `json:"posts"`
	}
	if err := c.post(ctx, "/x/ingest?"+q.Encode(), nil, &out); err != nil {
		return 0, err
	}
	return out.Posts, nil
}

// XProfile → GET /x/profile/{handle}?limit=. Profile card + recent tweets.
func (c *Client) XProfile(ctx context.Context, handle string, limit int) (xweb.ProfileCard, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out xweb.ProfileCard
	if err := c.get(ctx, "/x/profile/"+url.PathEscape(handle), q, &out); err != nil {
		return xweb.ProfileCard{}, err
	}
	return out, nil
}

// HonchoBackfill → POST /honcho/backfill?max_chats=&per_chat= — dispatches the bounded
// inner-tier WA-DM history replay into Honcho ASYNC (no-op when ingestion is flagged off).
// Returns immediately with a status string; the actual outcome is in the daemon logs +
// Honcho counts (the pass can run for minutes / thousands of posts).
func (c *Client) HonchoBackfill(ctx context.Context, maxChats, perChat int) (string, error) {
	q := url.Values{}
	if maxChats > 0 {
		q.Set("max_chats", strconv.Itoa(maxChats))
	}
	if perChat > 0 {
		q.Set("per_chat", strconv.Itoa(perChat))
	}
	var out struct {
		Status string `json:"status"`
		Note   string `json:"note"`
	}
	if err := c.post(ctx, "/honcho/backfill?"+q.Encode(), nil, &out); err != nil {
		return "", err
	}
	if out.Note != "" {
		return out.Status + ": " + out.Note, nil
	}
	return out.Status, nil
}

// GroupInvite → GET /group-invite?chat_id=&reset= (#52). Returns the invite link.
func (c *Client) GroupInvite(ctx context.Context, chatID string, reset bool) (string, error) {
	q := url.Values{}
	q.Set("chat_id", chatID)
	if reset {
		q.Set("reset", "true")
	}
	var out struct {
		InviteLink string `json:"invite_link"`
	}
	if err := c.get(ctx, "/group-invite", q, &out); err != nil {
		return "", err
	}
	return out.InviteLink, nil
}

// TapButton → POST /tap_button
func (c *Client) TapButton(ctx context.Context, body TapBody) (string, error) {
	var out TapResult
	if err := c.post(ctx, "/tap_button", body, &out); err != nil {
		return "", err
	}
	return out.Result, nil
}

// VotePoll → POST /vote_poll
func (c *Client) VotePoll(ctx context.Context, body VoteBody) error {
	return c.post(ctx, "/vote_poll", body, nil)
}

// Backfill → POST /backfill
func (c *Client) Backfill(ctx context.Context, body BackfillBody) error {
	return c.post(ctx, "/backfill", body, nil)
}

// Reproject → POST /reproject
func (c *Client) Reproject(ctx context.Context) (int, error) {
	var out struct {
		Reprojected int `json:"reprojected"`
	}
	if err := c.post(ctx, "/reproject", struct{}{}, &out); err != nil {
		return 0, err
	}
	return out.Reprojected, nil
}

// PendingProposals → GET /people/proposals (paged)
func (c *Client) PendingProposals(ctx context.Context, limit, offset int) ([]ProposalDTO, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if offset > 0 {
		q.Set("offset", strconv.Itoa(offset))
	}
	var out []ProposalDTO
	return out, c.get(ctx, "/people/proposals", q, &out)
}

// ResolveProposal → POST /people/proposals/{id}/resolve. Returns the merge
// outcome (survivor id+name) on approve; nil on reject.
func (c *Client) ResolveProposal(ctx context.Context, id, decision string) (*ResolveOutcome, error) {
	var out ResolveOutcome
	if err := c.post(ctx, "/people/proposals/"+url.PathEscape(id)+"/resolve", ResolveBody{Decision: decision}, &out); err != nil {
		return nil, err
	}
	if out.SurvivorID == "" {
		return nil, nil // reject (OKResult) — no survivor
	}
	return &out, nil
}

// RenameCanonical → POST /people/{canon_id}/name
func (c *Client) RenameCanonical(ctx context.Context, canonID, name string) error {
	return c.post(ctx, "/people/"+url.PathEscape(canonID)+"/name", NameBody{Name: name}, nil)
}

// MergeCanonicals → POST /people/merge. Force-merges two operator-chosen
// canonical ids (durable via ApproveMerge); returns the survivor outcome.
func (c *Client) MergeCanonicals(ctx context.Context, a, b string) (*ResolveOutcome, error) {
	var out ResolveOutcome
	if err := c.post(ctx, "/people/merge", MergeBody{A: a, B: b}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ResolvePerson → GET /people/resolve?q=<name>. Returns the full envelope
// (echoed query, in_graph flag, ranked hits); Hits is [] when no one matches.
func (c *Client) ResolvePerson(ctx context.Context, name string) (*ResolveResult, error) {
	q := url.Values{}
	q.Set("q", name)
	var out ResolveResult
	if err := c.get(ctx, "/people/resolve", q, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CoreNetwork → GET /people/core_network?tier=&limit= (#15): salience-ranked people.
func (c *Client) CoreNetwork(ctx context.Context, tier string, limit int) ([]PersonCard, error) {
	q := url.Values{}
	if tier != "" {
		q.Set("tier", tier)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out []PersonCard
	return out, c.get(ctx, "/people/core_network", q, &out)
}

// ReextractMediaRefs → POST /media/reextract-refs?platform= (#48 recovery): re-derive
// downloadable media refs from stored Raw + reset failed/skipped rows to pending.
func (c *Client) ReextractMediaRefs(ctx context.Context, platform, chatID string) (int, error) {
	q := url.Values{}
	if platform != "" {
		q.Set("platform", platform)
	}
	if chatID != "" {
		q.Set("chat", chatID)
	}
	var out struct {
		Refreshed int `json:"refreshed"`
	}
	return out.Refreshed, c.post(ctx, "/media/reextract-refs?"+q.Encode(), nil, &out)
}

// RevalidateMedia → POST /media/revalidate?platform= (#48): re-check stored media blobs
// vs declared type + reset the undecrypted ciphertext ones for re-download.
func (c *Client) RevalidateMedia(ctx context.Context, platform string) (checked, decrypted, reset int, err error) {
	q := url.Values{}
	if platform != "" {
		q.Set("platform", platform)
	}
	var out struct {
		Checked   int `json:"checked"`
		Decrypted int `json:"decrypted"`
		Reset     int `json:"reset"`
	}
	err = c.post(ctx, "/media/revalidate?"+q.Encode(), nil, &out)
	return out.Checked, out.Decrypted, out.Reset, err
}

// Observability → GET /observability (phase-2 health snapshot: per-platform coverage,
// ingestion lag, connector health, last backfill-sweep convergence delta).
func (c *Client) Observability(ctx context.Context) (Observability, error) {
	var out Observability
	return out, c.get(ctx, "/observability", nil, &out)
}

// MediaInfo → GET /media/{id}/meta (re-fetches ref-only media on demand).
func (c *Client) MediaInfo(ctx context.Context, id string) (MediaMeta, error) {
	var out MediaMeta
	return out, c.get(ctx, "/media/"+url.PathEscape(id)+"/meta", nil, &out)
}

// AuthStatus → GET /auth/status
func (c *Client) AuthStatus(ctx context.Context, platform string) (map[string]any, error) {
	q := url.Values{"platform": {platform}}
	var out map[string]any
	return out, c.get(ctx, "/auth/status", q, &out)
}

// AuthSubmit → POST /auth/submit
func (c *Client) AuthSubmit(ctx context.Context, platform, field, value string) error {
	return c.post(ctx, "/auth/submit", AuthSubmitBody{Platform: platform, Field: field, Value: value}, nil)
}
