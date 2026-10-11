package igweb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"
)

// The "following feed" (#47): recent posts ONLY from accounts the user follows,
// merged chronologically, with ZERO recommendations/ads. IG's web home feed
// (feed/timeline) is always ranked + interleaves suggestions, and there is no
// reachable chronological "Following" endpoint on the web private API — so we
// build it deterministically by fan-out: enumerate the following list, pull each
// account's recent posts (feed/user/{pk}, already chronological), and merge by ts.
// Cheap + exact for a small following count; bounded + paced regardless.

const (
	// followingMaxPages bounds the following-list enumeration (50/page → up to 400
	// accounts). A hard stop so a huge following set can't loop unbounded.
	followingMaxPages = 8
	// userFeedCount is how many recent posts to pull per followed account.
	userFeedCount = 6
	// followingCacheTTL serves repeated following_feed calls from a brief cache so
	// they don't re-fan-out across every account each time.
	followingCacheTTL = 5 * time.Minute
	// followingDefaultLimit is the page size when the caller doesn't specify one.
	followingDefaultLimit = 12
)

type followUser struct {
	PK         json.Number `json:"pk"`
	Username   string      `json:"username"`
	FullName   string      `json:"full_name"`
	IsPrivate  bool        `json:"is_private"`
	IsVerified bool        `json:"is_verified"`
	Friendship struct {
		FollowedBy bool `json:"followed_by"` // they follow the user back → mutual
	} `json:"friendship_status"`
}

type igFollowingResp struct {
	Users     []followUser `json:"users"`
	BigList   bool         `json:"big_list"`
	NextMaxID string       `json:"next_max_id"`
	Status    string       `json:"status"`
}

type igUserFeedResp struct {
	Items         []igMedia `json:"items"`
	MoreAvailable bool      `json:"more_available"`
	NextMaxID     string    `json:"next_max_id"`
	Status        string    `json:"status"`
}

// FollowingFeed returns recent posts from the accounts the user follows, merged
// newest-first, with no recommendations/ads (egress-verified, read-only). It
// enumerates the following list, fans out per-account (paced), dedupes, and
// sorts by ts. Results are cached briefly (followingCacheTTL) so repeated calls
// don't re-fan-out. limit<=0 uses followingDefaultLimit.
func (c *Client) FollowingFeed(ctx context.Context, limit int) (FeedResult, error) {
	if limit <= 0 {
		limit = followingDefaultLimit
	}

	// Serve from the brief cache when fresh (avoids re-fanning out per call).
	c.followMu.Lock()
	if !c.followCachAt.IsZero() && time.Since(c.followCachAt) < followingCacheTTL && len(c.followCache) > 0 {
		cached := c.followCache
		c.followMu.Unlock()
		return sliceFeed(cached, limit), nil
	}
	c.followMu.Unlock()

	users, err := c.following(ctx)
	if err != nil {
		return FeedResult{}, err
	}

	seen := make(map[string]bool)
	var all []FeedPost
	for i, u := range users {
		if i > 0 {
			if err := c.pace(ctx); err != nil {
				return FeedResult{}, err
			}
		}
		posts, err := c.userRecentPosts(ctx, u.PK.String(), userFeedCount)
		if err != nil {
			// Skip an account that errors (deactivated / blocked / transient) rather
			// than failing the whole feed — best-effort across the following set.
			continue
		}
		for _, p := range posts {
			if p.Shortcode != "" {
				if seen[p.Shortcode] {
					continue
				}
				seen[p.Shortcode] = true
			}
			all = append(all, p)
		}
	}

	sort.SliceStable(all, func(i, j int) bool { return all[i].TS.After(all[j].TS) })

	c.followMu.Lock()
	c.followCache = all
	c.followCachAt = time.Now()
	c.followMu.Unlock()

	return sliceFeed(all, limit), nil
}

func sliceFeed(posts []FeedPost, limit int) FeedResult {
	if limit > 0 && len(posts) > limit {
		posts = posts[:limit]
	}
	return FeedResult{Count: len(posts), Posts: posts}
}

// following enumerates the accounts the session user follows (paginated, bounded).
func (c *Client) following(ctx context.Context) ([]followUser, error) {
	ck, err := c.loadSessionFn()
	if err != nil {
		return nil, err
	}
	ds := ck["ds_user_id"]
	if ds == "" {
		return nil, ErrLoggedOut
	}
	var users []followUser
	maxID := ""
	for page := 0; page < followingMaxPages; page++ {
		if page > 0 {
			if err := c.pace(ctx); err != nil {
				return users, err
			}
		}
		u := "https://www.instagram.com/api/v1/friendships/" + ds + "/following/?count=50"
		if maxID != "" {
			u += "&max_id=" + url.QueryEscape(maxID)
		}
		var r igFollowingResp
		if err := c.getV1(ctx, u, &r); err != nil {
			if len(users) > 0 {
				break // best-effort: keep what we enumerated
			}
			return nil, err
		}
		users = append(users, r.Users...)
		if r.NextMaxID == "" {
			break
		}
		maxID = r.NextMaxID
	}
	return users, nil
}

// userRecentPosts pulls one account's most recent posts (already chronological).
func (c *Client) userRecentPosts(ctx context.Context, pk string, count int) ([]FeedPost, error) {
	if pk == "" {
		return nil, fmt.Errorf("igweb: empty user pk")
	}
	var r igUserFeedResp
	u := "https://www.instagram.com/api/v1/feed/user/" + pk + "/?count=" + strconv.Itoa(count)
	if err := c.getV1(ctx, u, &r); err != nil {
		return nil, err
	}
	out := make([]FeedPost, 0, len(r.Items))
	for i := range r.Items {
		if p, ok := mapMedia(&r.Items[i]); ok {
			out = append(out, p)
		}
	}
	return out, nil
}

// getV1 is an egress-verified, session-authed GET against an IG private /api/v1/
// endpoint, adding the X-CSRFToken + X-IG-WWW-Claim headers these endpoints want
// (the friendships/feed-user reads need them where web_profile_info does not).
// Read-only by construction; fails closed on bad egress and ErrLoggedOut on
// 401/403/HTML-login-redirect.
func (c *Client) getV1(ctx context.Context, rawURL string, out any) error {
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
	req.Header.Set("X-IG-WWW-Claim", "0")
	req.Header.Set("X-CSRFToken", ck["csrftoken"])
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
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
		return fmt.Errorf("igweb: decode %s: %w", endpointName(rawURL), err)
	}
	return nil
}
