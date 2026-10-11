package igweb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// FeedPost is one post in the home feed (from people the user follows).
type FeedPost struct {
	Shortcode string    `json:"shortcode,omitempty"`
	URL       string    `json:"url,omitempty"`
	Author    string    `json:"author"`
	AuthorID  string    `json:"author_id,omitempty"`
	Caption   string    `json:"caption,omitempty"`
	MediaType string    `json:"media_type,omitempty"` // image|video|carousel
	MediaURL  string    `json:"media_url,omitempty"`
	Likes     int       `json:"likes"`
	Comments  int       `json:"comments,omitempty"`
	TS        time.Time `json:"ts"`
}

// FeedResult is a page of the home feed.
type FeedResult struct {
	Count     int        `json:"count"`
	Posts     []FeedPost `json:"posts"`
	NextMaxID string     `json:"next_max_id,omitempty"` // pagination cursor (opaque)
}

type igFeedResp struct {
	NumResults    int    `json:"num_results"`
	MoreAvailable bool   `json:"more_available"`
	NextMaxID     string `json:"next_max_id"`
	FeedItems     []struct {
		MediaOrAd *igMedia `json:"media_or_ad"`
	} `json:"feed_items"`
}

type igMedia struct {
	Code      string `json:"code"`
	MediaType int    `json:"media_type"` // 1 image, 2 video, 8 carousel
	TakenAt   int64  `json:"taken_at"`
	LikeCount int    `json:"like_count"`
	CommentCt int    `json:"comment_count"`
	User      struct {
		PK       json.Number `json:"pk"`
		Username string      `json:"username"`
	} `json:"user"`
	Caption *struct {
		Text string `json:"text"`
	} `json:"caption"`
	ImageVersions2 struct {
		Candidates []struct {
			URL string `json:"url"`
		} `json:"candidates"`
	} `json:"image_versions2"`
}

const (
	// feedMaxInternalPages bounds how many timeline pages FeedN will fetch in one
	// call to satisfy a limit — a hard stop so an unfilled limit can't loop.
	feedMaxInternalPages = 6
	// feedPageDelay paces internal page fetches (IG's cold_start returns short
	// first pages; we follow next_max_id but never hammer).
	feedPageDelay = 1200 * time.Millisecond
)

// FeedN reads up to `limit` real posts, auto-paginating INTERNALLY: it follows
// next_max_id, accumulates + dedupes by shortcode, and stops at `limit`, when IG
// reports no more pages (empty next_max_id), or after feedMaxInternalPages
// fetches — whichever comes first. Inter-page calls are paced (c.pageDelay) so a
// caller asking for 15 gets ~15 real posts without hammering. startMaxID resumes
// from a prior cursor (""=cold start). limit<=0 falls back to a single page.
// The returned NextMaxID is the cursor after the last fetched page, so callers
// can keep paginating past what one FeedN call returned.
func (c *Client) FeedN(ctx context.Context, limit int, startMaxID string) (FeedResult, error) {
	if limit <= 0 {
		return c.Feed(ctx, startMaxID)
	}
	seen := make(map[string]bool)
	acc := make([]FeedPost, 0, limit)
	maxID, next := startMaxID, startMaxID
	for page := 0; page < feedMaxInternalPages && len(acc) < limit; page++ {
		if page > 0 && c.pageDelay > 0 {
			select {
			case <-ctx.Done():
				return FeedResult{Count: len(acc), Posts: acc, NextMaxID: next}, ctx.Err()
			case <-time.After(c.pageDelay):
			}
		}
		pg, err := c.Feed(ctx, maxID)
		if err != nil {
			// Best-effort: if we already have posts, return them with the last good
			// cursor rather than discarding the page(s) we did get. Propagate only a
			// cold failure (logged-out / bad egress on the very first fetch).
			if len(acc) > 0 {
				break
			}
			return FeedResult{}, err
		}
		for _, p := range pg.Posts {
			key := p.Shortcode
			if key == "" {
				key = p.Author + "\x00" + p.URL
			}
			if seen[key] {
				continue
			}
			seen[key] = true
			acc = append(acc, p)
			if len(acc) >= limit {
				break
			}
		}
		next = pg.NextMaxID
		if pg.NextMaxID == "" {
			break // IG has no more pages
		}
		maxID = pg.NextMaxID
	}
	if len(acc) > limit {
		acc = acc[:limit]
	}
	return FeedResult{Count: len(acc), Posts: acc, NextMaxID: next}, nil
}

// Feed reads the home feed — recent posts from people the user follows
// (egress-verified, read-only). maxID is an optional pagination cursor (from a
// prior result's NextMaxID); "" for the first page.
func (c *Client) Feed(ctx context.Context, maxID string) (FeedResult, error) {
	form := url.Values{}
	form.Set("reason", "cold_start_fetch")
	form.Set("is_pull_to_refresh", "0")
	if maxID != "" {
		form.Set("max_id", maxID)
		form.Set("reason", "pagination")
	}
	var r igFeedResp
	if err := c.postForm(ctx, "https://www.instagram.com/api/v1/feed/timeline/", form, &r); err != nil {
		return FeedResult{}, err
	}
	return mapFeed(&r), nil
}

func mapFeed(r *igFeedResp) FeedResult {
	out := FeedResult{NextMaxID: r.NextMaxID}
	for _, it := range r.FeedItems {
		if p, ok := mapMedia(it.MediaOrAd); ok {
			out.Posts = append(out.Posts, p)
		}
	}
	out.Count = len(out.Posts)
	return out
}

// mapMedia converts one IG media node to a FeedPost. ok=false for items with no
// author (ads / suggested-users / non-post entries), which callers skip.
func mapMedia(m *igMedia) (FeedPost, bool) {
	if m == nil || m.User.Username == "" {
		return FeedPost{}, false
	}
	p := FeedPost{
		Shortcode: m.Code,
		Author:    m.User.Username,
		AuthorID:  m.User.PK.String(),
		Likes:     m.LikeCount,
		Comments:  m.CommentCt,
		MediaType: mediaTypeName(m.MediaType),
	}
	if m.Code != "" {
		p.URL = "https://www.instagram.com/p/" + m.Code + "/"
	}
	if m.Caption != nil {
		p.Caption = m.Caption.Text
	}
	if m.TakenAt > 0 {
		p.TS = time.Unix(m.TakenAt, 0).UTC()
	}
	if len(m.ImageVersions2.Candidates) > 0 {
		p.MediaURL = m.ImageVersions2.Candidates[0].URL
	}
	return p, true
}

func mediaTypeName(t int) string {
	switch t {
	case 1:
		return "image"
	case 2:
		return "video"
	case 8:
		return "carousel"
	}
	return ""
}

// postForm is an egress-verified, session-authed POST (form-encoded) against an
// IG private web endpoint. Adds the X-CSRFToken + X-IG-WWW-Claim headers the
// authed POST endpoints require. Read-only in intent (only used for feed reads).
func (c *Client) postForm(ctx context.Context, rawURL string, form url.Values, out any) error {
	if err := c.verifyEgress(ctx); err != nil {
		return err
	}
	ck, err := c.loadSessionFn()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", webUA)
	req.Header.Set("X-IG-App-ID", igAppID)
	req.Header.Set("X-IG-WWW-Claim", "0")
	req.Header.Set("X-CSRFToken", ck["csrftoken"])
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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
		if len(body) > 0 && body[0] == '<' {
			return ErrLoggedOut
		}
		return err
	}
	return nil
}
