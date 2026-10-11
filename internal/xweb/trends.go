package xweb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// Trend is one entry from X's "What's happening" / Trends-for-you panel (#55).
type Trend struct {
	Name      string `json:"name"`                 // the trend term / hashtag
	PostCount int    `json:"post_count,omitempty"` // best-effort parse of "12.3K posts" → 12300 (0 if absent)
	Context   string `json:"context,omitempty"`    // domainContext, e.g. "Trending in Technology"
}

// Trends reads the trending sidebar via the guide.json REST endpoint (#55). It routes
// through the SAME proxied + authed transport as FollowingFeed (c.http = the residential
// home-IP egress; same bearer + ct0 + cookies) — NEVER a fresh client — so all X traffic
// stays on the ban-safe path. Parsed DEFENSIVELY against the documented guide.json shape:
// X mutates this payload, so every level is nil-tolerant and unknown/renamed fields
// skip-and-continue rather than error. Returns the trends in panel order.
func (c *Client) Trends(ctx context.Context) ([]Trend, error) {
	raw, err := c.authedGet(ctx, "https://x.com/i/api/2/guide.json?count=20&candidate_source=trends")
	if err != nil {
		return nil, err
	}
	return parseTrends(raw), nil
}

// guide.json shape (documented; parsed tolerantly):
//
//	timeline.instructions[].addEntries.entries[].content.timelineModule.items[]
//	  .item.content.trend.{name, trendMetadata.{domainContext, metaDescription}}
type guideResp struct {
	Timeline struct {
		Instructions []struct {
			AddEntries struct {
				Entries []guideEntry `json:"entries"`
			} `json:"addEntries"`
		} `json:"instructions"`
	} `json:"timeline"`
}

type guideEntry struct {
	Content struct {
		TimelineModule struct {
			Items []struct {
				Item struct {
					Content struct {
						Trend *struct {
							Name          string `json:"name"`
							TrendMetadata struct {
								DomainContext   string `json:"domainContext"`
								MetaDescription string `json:"metaDescription"`
							} `json:"trendMetadata"`
						} `json:"trend"`
					} `json:"content"`
				} `json:"item"`
			} `json:"items"`
		} `json:"timelineModule"`
	} `json:"content"`
}

func parseTrends(raw []byte) []Trend {
	var g guideResp
	if err := json.Unmarshal(raw, &g); err != nil {
		return nil // tolerant: a shape we don't recognize yields no trends, never a panic
	}
	var out []Trend
	seen := map[string]bool{}
	for _, ins := range g.Timeline.Instructions {
		for _, e := range ins.AddEntries.Entries {
			for _, it := range e.Content.TimelineModule.Items {
				tr := it.Item.Content.Trend
				if tr == nil || strings.TrimSpace(tr.Name) == "" || seen[tr.Name] {
					continue
				}
				seen[tr.Name] = true
				out = append(out, Trend{
					Name:      tr.Name,
					PostCount: parsePostCount(tr.TrendMetadata.MetaDescription),
					Context:   tr.TrendMetadata.DomainContext,
				})
			}
		}
	}
	return out
}

// parsePostCount best-effort parses "12.3K posts" / "1,234 posts" / "2M posts" → an int.
// Returns 0 on anything unrecognized (absent volume is common + fine — it's rank-only).
func parsePostCount(s string) int {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0
	}
	f := strings.Fields(s)
	if len(f) == 0 {
		return 0
	}
	tok := strings.ReplaceAll(f[0], ",", "")
	mult := 1.0
	switch {
	case strings.HasSuffix(tok, "k"):
		mult, tok = 1e3, strings.TrimSuffix(tok, "k")
	case strings.HasSuffix(tok, "m"):
		mult, tok = 1e6, strings.TrimSuffix(tok, "m")
	case strings.HasSuffix(tok, "b"):
		mult, tok = 1e9, strings.TrimSuffix(tok, "b")
	}
	v, err := strconv.ParseFloat(tok, 64)
	if err != nil {
		return 0
	}
	return int(v * mult)
}

// authedGet performs a GET against an arbitrary X API URL using the SAME proxied +
// authed transport as the graphql reads (c.http = home-IP egress; bearer + ct0 +
// cookies). It mirrors graphqlGet's auth/egress exactly — the only difference is the URL
// is caller-supplied (for REST endpoints like guide.json that aren't graphql ops). NEVER
// constructs a fresh http.Client, so X traffic can't leak onto the datacenter IP.
func (c *Client) authedGet(ctx context.Context, rawURL string) ([]byte, error) {
	if err := c.verifyEgress(ctx); err != nil {
		return nil, err
	}
	ck, err := c.loadSessionFn()
	if err != nil {
		return nil, err
	}
	bm, err := c.bundleInfo(ctx, ck)
	if err != nil {
		return nil, err
	}
	bearer := bm.bearer
	if bearer == "" {
		bearer = publicBearer
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, ErrLoggedOut
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("xweb: GET %s → HTTP %d", rawURL, resp.StatusCode)
	}
	if len(body) > 0 && body[0] == '<' {
		return nil, ErrLoggedOut // a login/redirect HTML page
	}
	return body, nil
}
