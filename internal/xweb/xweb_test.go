package xweb

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func resp(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}
}

// testClient builds a Client with an injected transport + session, bundle
// pre-seeded (so no harvest), bypassing the real SOCKS5 dialer.
func testClient(egressIP string, rt roundTripFunc) *Client {
	c := &Client{
		cfg:       Config{ExpectedEgress: egressIP},
		http:      &http.Client{Transport: rt},
		ipEchoURL: "http://ipecho.test/",
		bundle:    bakedBundle,
		bundleAt:  time.Now(),
	}
	c.loadSessionFn = func() (map[string]string, error) {
		return map[string]string{"auth_token": "a", "ct0": "c"}, nil
	}
	return c
}

// HomeLatestTimeline with an organic tweet, a promoted tweet, a who-to-follow
// module, and a bottom cursor — only the organic tweet should survive.
const homeLatestJSON = `{"data":{"home":{"home_timeline_urt":{"instructions":[
{"type":"TimelineAddEntries","entries":[
 {"entryId":"tweet-111","content":{"entryType":"TimelineTimelineItem","itemContent":{"itemType":"TimelineTweet","tweet_results":{"result":{"__typename":"Tweet","rest_id":"111","core":{"user_results":{"result":{"rest_id":"u1","core":{"screen_name":"levelsio","name":"levels.io"}}}},"legacy":{"full_text":"New airplane food box","created_at":"Sat Jun 13 09:58:25 +0000 2026","favorite_count":71,"retweet_count":1,"reply_count":6,"quote_count":2,"entities":{"media":[{"media_url_https":"https://pbs/x.jpg"}]}},"views":{"count":"10288"}}}}}},
 {"entryId":"promoted-tweet-999","content":{"entryType":"TimelineTimelineItem","itemContent":{"itemType":"TimelineTweet","promotedMetadata":{"adId":"x"},"tweet_results":{"result":{"__typename":"Tweet","rest_id":"999","core":{"user_results":{"result":{"rest_id":"u9","core":{"screen_name":"figma","name":"Figma"}}}},"legacy":{"full_text":"ad","created_at":"Sat Jun 13 08:00:00 +0000 2026"}}}}}},
 {"entryId":"who-to-follow-1","content":{"entryType":"TimelineTimelineModule","itemContent":{}}},
 {"entryId":"cursor-bottom-1","content":{"entryType":"TimelineTimelineCursor","cursorType":"Bottom","value":"CURSOR123"}}
]}]}}}}`

func TestMapTimeline_FiltersPromotedAndModules(t *testing.T) {
	var r homeLatestResp
	if err := json.Unmarshal([]byte(homeLatestJSON), &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	posts, cursor := mapTimeline(&r.Data.Home.HomeTimelineUrt)
	if len(posts) != 1 {
		t.Fatalf("want 1 post (promoted+module dropped), got %d: %+v", len(posts), posts)
	}
	p := posts[0]
	if p.Platform != "x" || p.Author != "levelsio" || p.AuthorID != "u1" || p.ID != "111" {
		t.Errorf("identity: %+v", p)
	}
	if p.Likes != 71 || p.Retweets != 1 || p.Replies != 6 || p.Quotes != 2 || p.Views != 10288 {
		t.Errorf("engagement: %+v", p)
	}
	if p.URL != "https://x.com/levelsio/status/111" {
		t.Errorf("url: %q", p.URL)
	}
	if len(p.MediaURLs) != 1 || p.MediaURLs[0] != "https://pbs/x.jpg" {
		t.Errorf("media: %+v", p.MediaURLs)
	}
	if p.TS.Unix() != time.Date(2026, 6, 13, 9, 58, 25, 0, time.UTC).Unix() {
		t.Errorf("ts: %v", p.TS)
	}
	if cursor != "CURSOR123" {
		t.Errorf("cursor: %q", cursor)
	}
}

func TestFollowingFeed_Live(t *testing.T) {
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Host, "ipecho") {
			return resp(200, "203.0.113.10"), nil
		}
		if strings.Contains(r.URL.Path, "/HomeLatestTimeline") {
			if r.Header.Get("Authorization") == "" || r.Header.Get("X-Csrf-Token") != "c" {
				t.Error("missing auth headers on graphql read")
			}
			return resp(200, homeLatestJSON), nil
		}
		return resp(404, "{}"), nil
	})
	c := testClient("203.0.113.10", rt)
	out, err := c.FollowingFeed(context.Background(), 10)
	if err != nil {
		t.Fatalf("FollowingFeed: %v", err)
	}
	if out.Count != 1 || out.Posts[0].Author != "levelsio" {
		t.Fatalf("result: %+v", out)
	}
	if out.NextCursor != "CURSOR123" {
		t.Errorf("cursor: %q", out.NextCursor)
	}
}

func TestFollowingFeed_FailsClosedOnWrongEgress(t *testing.T) {
	read := false
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Host, "ipecho") {
			return resp(200, "198.51.100.20"), nil // datacenter IP
		}
		read = true
		return resp(200, homeLatestJSON), nil
	})
	c := testClient("203.0.113.10", rt)
	_, err := c.FollowingFeed(context.Background(), 10)
	if !errors.Is(err, ErrProxyEgress) {
		t.Fatalf("want ErrProxyEgress, got %v", err)
	}
	if read {
		t.Fatal("SAFETY VIOLATION: read happened despite wrong egress IP")
	}
}

func TestFollowingFeed_LoggedOutOn401(t *testing.T) {
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Host, "ipecho") {
			return resp(200, "203.0.113.10"), nil
		}
		return resp(401, `{"errors":[{"code":32}]}`), nil
	})
	c := testClient("203.0.113.10", rt)
	if _, err := c.FollowingFeed(context.Background(), 10); !errors.Is(err, ErrLoggedOut) {
		t.Fatalf("want ErrLoggedOut on 401, got %v", err)
	}
}

func TestLoadSession(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x-session.json")
	os.WriteFile(p, []byte(`{"cookies":{"auth_token":"AT","ct0":"CT","extra":"e"}}`), 0o600)
	c := &Client{cfg: Config{SessionPath: p}}
	ck, err := c.loadSessionFromFile()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if ck["auth_token"] != "AT" || ck["ct0"] != "CT" {
		t.Errorf("cookies: %+v", ck)
	}
	// missing ct0 → logged out
	os.WriteFile(p, []byte(`{"auth_token":"AT"}`), 0o600)
	if _, err := c.loadSessionFromFile(); !errors.Is(err, ErrLoggedOut) {
		t.Errorf("want ErrLoggedOut on missing ct0, got %v", err)
	}
}
