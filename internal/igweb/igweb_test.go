package igweb

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func resp(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}
}

const profileJSON = `{"data":{"user":{"id":"100000003","username":"exampleprofile","full_name":"Example Profile",
"biography":"Dress & Gowns Rental","is_verified":false,"is_private":false,"is_business_account":true,
"category_name":"Bridal Shop","followed_by_viewer":true,"follows_viewer":true,"highlight_reel_count":3,
"edge_followed_by":{"count":2971},"edge_follow":{"count":1},
"edge_owner_to_timeline_media":{"count":235,"edges":[{"node":{"shortcode":"ABC123","display_url":"https://img/x.jpg",
"is_video":false,"taken_at_timestamp":1700000000,"edge_liked_by":{"count":42},"edge_media_to_comment":{"count":3},
"edge_media_to_caption":{"edges":[{"node":{"text":"new arrivals"}}]}}}]}}}}`

const trayJSON = `{"tray":[
{"id":"t1","user":{"pk":12345,"username":"alice","full_name":"Alice"},"latest_reel_media":1700000100,"media_count":2,"seen":1700000050},
{"id":"t2","user":{"pk":67890,"username":"bob","full_name":"Bob"},"latest_reel_media":0,"media_count":0,"seen":0}]}`

// testClient builds a Client with an injected transport + session, bypassing the
// real SOCKS5 dialer.
func testClient(egressIP string, rt roundTripFunc) *Client {
	c := &Client{
		cfg:       Config{ExpectedEgress: egressIP},
		http:      &http.Client{Transport: rt},
		ipEchoURL: "http://ipecho.test/",
	}
	c.loadSessionFn = func() (map[string]string, error) {
		return map[string]string{"sessionid": "s", "csrftoken": "c", "ds_user_id": "1"}, nil
	}
	return c
}

func TestProfile_EgressVerifiedThenRead(t *testing.T) {
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(r.URL.Host, "ipecho"):
			return resp(200, "203.0.113.10"), nil
		case strings.Contains(r.URL.Path, "web_profile_info"):
			if r.Header.Get("X-IG-App-ID") == "" || r.Header.Get("Cookie") == "" {
				t.Error("missing IG headers on profile read")
			}
			return resp(200, profileJSON), nil
		}
		return resp(404, ""), nil
	})
	c := testClient("203.0.113.10", rt)
	card, err := c.Profile(context.Background(), "exampleprofile")
	if err != nil {
		t.Fatalf("Profile: %v", err)
	}
	if card.Handle != "exampleprofile" || card.Followers != 2971 || card.Posts != 235 || card.Following != 1 {
		t.Errorf("card counts wrong: %+v", card)
	}
	if !card.IsBusiness || card.Category != "Bridal Shop" || card.Bio != "Dress & Gowns Rental" {
		t.Errorf("card business fields: %+v", card)
	}
	if !card.YouFollow || !card.FollowsYou || !card.Mutual {
		t.Errorf("mutual-follow not derived: %+v", card)
	}
	if len(card.RecentPosts) != 1 || card.RecentPosts[0].Likes != 42 || card.RecentPosts[0].Caption != "new arrivals" {
		t.Errorf("recent posts: %+v", card.RecentPosts)
	}
	if card.RecentPosts[0].URL != "https://www.instagram.com/p/ABC123/" {
		t.Errorf("post url: %q", card.RecentPosts[0].URL)
	}
}

func TestProfile_FailsClosedOnWrongEgress(t *testing.T) {
	read := false
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Host, "ipecho") {
			return resp(200, "198.51.100.20"), nil // the DATACENTER ip
		}
		read = true
		return resp(200, profileJSON), nil
	})
	c := testClient("203.0.113.10", rt)
	_, err := c.Profile(context.Background(), "exampleprofile")
	if !errors.Is(err, ErrProxyEgress) {
		t.Fatalf("expected ErrProxyEgress, got %v", err)
	}
	if read {
		t.Fatal("SAFETY VIOLATION: read happened despite wrong egress IP")
	}
}

func TestProfile_LoggedOutWhenSessionStale(t *testing.T) {
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Host, "ipecho") {
			return resp(200, "203.0.113.10"), nil
		}
		return resp(401, `{"message":"login_required"}`), nil
	})
	c := testClient("203.0.113.10", rt)
	if _, err := c.Profile(context.Background(), "exampleprofile"); !errors.Is(err, ErrLoggedOut) {
		t.Fatalf("expected ErrLoggedOut on 401, got %v", err)
	}
}

func TestProfile_LoggedOutOnHTMLRedirect(t *testing.T) {
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Host, "ipecho") {
			return resp(200, "203.0.113.10"), nil
		}
		return resp(200, "<!DOCTYPE html><html>login</html>"), nil
	})
	c := testClient("203.0.113.10", rt)
	if _, err := c.Profile(context.Background(), "exampleprofile"); !errors.Is(err, ErrLoggedOut) {
		t.Fatalf("expected ErrLoggedOut on HTML, got %v", err)
	}
}

func TestStories_Mapping(t *testing.T) {
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Host, "ipecho") {
			return resp(200, "203.0.113.10"), nil
		}
		return resp(200, trayJSON), nil
	})
	c := testClient("203.0.113.10", rt)
	s, err := c.Stories(context.Background())
	if err != nil {
		t.Fatalf("Stories: %v", err)
	}
	// bob (no media) filtered out; only alice has an active story.
	if s.Count != 1 || len(s.Users) != 1 {
		t.Fatalf("count = %d, want 1: %+v", s.Count, s.Users)
	}
	u := s.Users[0]
	if u.Handle != "alice" || u.UserID != "12345" || u.MediaCount != 2 {
		t.Errorf("story user: %+v", u)
	}
	if u.Seen { // seen(50) < latest(100) → unseen
		t.Errorf("alice story should be unseen")
	}
}

func TestFeed_Mapping(t *testing.T) {
	r := &igFeedResp{NextMaxID: "CURSOR"}
	// item 1: a real post by alice (image)
	var a igMedia
	a.Code = "ABC123"
	a.MediaType = 1
	a.TakenAt = 1700000000
	a.LikeCount = 42
	a.CommentCt = 3
	a.User.PK = json.Number("12345")
	a.User.Username = "alice"
	a.Caption = &struct {
		Text string `json:"text"`
	}{Text: "hello world"}
	a.ImageVersions2.Candidates = []struct {
		URL string `json:"url"`
	}{{URL: "https://cdn/img.jpg"}}
	// item 2: an ad / suggested-user with no username → must be filtered
	var b igMedia
	b.MediaType = 2
	r.FeedItems = []struct {
		MediaOrAd *igMedia `json:"media_or_ad"`
	}{{MediaOrAd: &a}, {MediaOrAd: &b}, {MediaOrAd: nil}}

	out := mapFeed(r)
	if out.NextMaxID != "CURSOR" {
		t.Errorf("next_max_id = %q", out.NextMaxID)
	}
	if out.Count != 1 || len(out.Posts) != 1 {
		t.Fatalf("count = %d, want 1 (ad + nil filtered): %+v", out.Count, out.Posts)
	}
	p := out.Posts[0]
	if p.Author != "alice" || p.AuthorID != "12345" || p.Shortcode != "ABC123" {
		t.Errorf("post identity: %+v", p)
	}
	if p.Likes != 42 || p.Comments != 3 {
		t.Errorf("post counts: %+v", p)
	}
	if p.MediaType != "image" || p.MediaURL != "https://cdn/img.jpg" {
		t.Errorf("post media: %+v", p)
	}
	if p.URL != "https://www.instagram.com/p/ABC123/" {
		t.Errorf("post url: %q", p.URL)
	}
	if p.Caption != "hello world" {
		t.Errorf("post caption: %q", p.Caption)
	}
	if p.TS.Unix() != 1700000000 {
		t.Errorf("post ts: %v", p.TS)
	}
}

// feedPageJSON builds a timeline page with the given shortcodes (each by author
// "u"+code) and a next_max_id cursor.
func feedPageJSON(nextMaxID string, codes ...string) string {
	items := make([]string, 0, len(codes))
	for _, code := range codes {
		items = append(items, `{"media_or_ad":{"code":"`+code+`","media_type":1,"taken_at":1700000000,`+
			`"like_count":1,"comment_count":0,"user":{"pk":"1","username":"u`+code+`"}}}`)
	}
	return `{"num_results":` + itoa(len(codes)) + `,"more_available":` +
		map[bool]string{true: "true", false: "false"}[nextMaxID != ""] +
		`,"next_max_id":"` + nextMaxID + `","feed_items":[` + strings.Join(items, ",") + `]}`
}

func itoa(n int) string { return string(rune('0' + n)) }

func TestFeedN_AutoPaginatesDedupesStops(t *testing.T) {
	// page1: A,B (cursor→p2); page2: B(dup),C (cursor→p3); page3 would be D but
	// FeedN should stop at limit=3 after page2 (A,B,C unique) — never fetch page3.
	pages := []string{
		feedPageJSON("p2", "A", "B"),
		feedPageJSON("p3", "B", "C"),
		feedPageJSON("", "D"),
	}
	var feedCalls int
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Host, "ipecho") {
			return resp(200, "203.0.113.10"), nil
		}
		i := feedCalls
		feedCalls++
		if i >= len(pages) {
			t.Fatalf("FeedN fetched too many pages: %d", feedCalls)
		}
		return resp(200, pages[i]), nil
	})
	c := testClient("203.0.113.10", rt) // pageDelay==0 → no sleeps

	out, err := c.FeedN(context.Background(), 3, "")
	if err != nil {
		t.Fatalf("FeedN: %v", err)
	}
	if feedCalls != 2 {
		t.Errorf("expected 2 page fetches (stopped at limit), got %d", feedCalls)
	}
	if out.Count != 3 || len(out.Posts) != 3 {
		t.Fatalf("count = %d, want 3 (deduped): %+v", out.Count, out.Posts)
	}
	got := []string{out.Posts[0].Shortcode, out.Posts[1].Shortcode, out.Posts[2].Shortcode}
	want := []string{"A", "B", "C"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("posts[%d] = %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}
	if out.NextMaxID != "p3" {
		t.Errorf("next_max_id = %q, want p3 (cursor after last page)", out.NextMaxID)
	}
}

func TestFeedN_StopsWhenNoMorePages(t *testing.T) {
	// Only 2 real posts available, IG reports no more (empty next_max_id), but
	// caller asked for 10 → FeedN returns the 2 it has, doesn't loop the page cap.
	var feedCalls int
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Host, "ipecho") {
			return resp(200, "203.0.113.10"), nil
		}
		feedCalls++
		return resp(200, feedPageJSON("", "A", "B")), nil
	})
	c := testClient("203.0.113.10", rt)
	out, err := c.FeedN(context.Background(), 10, "")
	if err != nil {
		t.Fatalf("FeedN: %v", err)
	}
	if feedCalls != 1 {
		t.Errorf("expected 1 fetch (no more_available), got %d", feedCalls)
	}
	if out.Count != 2 {
		t.Errorf("count = %d, want 2", out.Count)
	}
}

// userFeedJSON builds a feed/user response: posts (code, taken_at, author).
func userFeedJSON(author string, posts ...[2]interface{}) string {
	items := make([]string, 0, len(posts))
	for _, p := range posts {
		code := p[0].(string)
		taken := p[1].(int)
		items = append(items, `{"code":"`+code+`","media_type":1,"taken_at":`+strconv.Itoa(taken)+
			`,"like_count":1,"comment_count":0,"user":{"pk":"9","username":"`+author+`"}}`)
	}
	return `{"status":"ok","more_available":false,"items":[` + strings.Join(items, ",") + `]}`
}

func TestFollowingFeed_FanOutMergeDedupeSort(t *testing.T) {
	following := `{"status":"ok","big_list":false,"users":[
{"pk":"100","username":"alice","is_private":false},
{"pk":"200","username":"bob","is_private":true},
{"pk":"300","username":"carol","is_private":false}]}`
	// alice: 2 posts; bob (private but followed → readable): 1 post; carol: errors (skipped).
	var followCalls, userCalls int
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(r.URL.Host, "ipecho"):
			return resp(200, "203.0.113.10"), nil
		case strings.Contains(r.URL.Path, "/following/"):
			followCalls++
			return resp(200, following), nil
		case strings.Contains(r.URL.Path, "/feed/user/100/"):
			userCalls++
			return resp(200, userFeedJSON("alice", [2]interface{}{"A1", 300}, [2]interface{}{"A2", 100})), nil
		case strings.Contains(r.URL.Path, "/feed/user/200/"):
			userCalls++
			return resp(200, userFeedJSON("bob", [2]interface{}{"B1", 200})), nil
		case strings.Contains(r.URL.Path, "/feed/user/300/"):
			userCalls++
			return resp(500, "boom"), nil // carol errors → skipped, not fatal
		}
		return resp(404, "{}"), nil
	})
	c := testClient("203.0.113.10", rt) // pageDelay==0

	out, err := c.FollowingFeed(context.Background(), 10)
	if err != nil {
		t.Fatalf("FollowingFeed: %v", err)
	}
	if followCalls != 1 || userCalls != 3 {
		t.Errorf("calls: following=%d (want 1), user=%d (want 3)", followCalls, userCalls)
	}
	// alice(2) + bob(1) = 3; carol skipped. Sorted newest-first by taken_at: A1(300),B1(200),A2(100).
	if out.Count != 3 || len(out.Posts) != 3 {
		t.Fatalf("count=%d want 3: %+v", out.Count, out.Posts)
	}
	gotOrder := []string{out.Posts[0].Shortcode, out.Posts[1].Shortcode, out.Posts[2].Shortcode}
	want := []string{"A1", "B1", "A2"}
	for i := range want {
		if gotOrder[i] != want[i] {
			t.Errorf("order[%d]=%q want %q (full %v)", i, gotOrder[i], want[i], gotOrder)
		}
	}

	// Second call within TTL must hit the cache (no new HTTP fan-out).
	out2, err := c.FollowingFeed(context.Background(), 2)
	if err != nil {
		t.Fatalf("FollowingFeed cached: %v", err)
	}
	if followCalls != 1 || userCalls != 3 {
		t.Errorf("cache miss: refetched (following=%d user=%d)", followCalls, userCalls)
	}
	if out2.Count != 2 { // limit applied to cached set
		t.Errorf("cached limit: count=%d want 2", out2.Count)
	}
}

func TestFollowing_EnrichesEntries(t *testing.T) {
	following := `{"status":"ok","big_list":false,"users":[
{"pk":"100","username":"creator1","full_name":"Creator One","is_private":false,"is_verified":true,"friendship_status":{"followed_by":false}}]}`
	profile := `{"data":{"user":{"id":"100","username":"creator1","full_name":"Creator One",
"biography":"AI creator","is_business_account":true,"category_name":"Blogger","followed_by_viewer":true,"follows_viewer":true,
"edge_followed_by":{"count":50000},"edge_follow":{"count":120},"edge_owner_to_timeline_media":{"count":300,"edges":[]}}}}`
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(r.URL.Host, "ipecho"):
			return resp(200, "203.0.113.10"), nil
		case strings.Contains(r.URL.Path, "/following/"):
			return resp(200, following), nil
		case strings.Contains(r.URL.Path, "web_profile_info"):
			return resp(200, profile), nil
		}
		return resp(404, "{}"), nil
	})
	c := testClient("203.0.113.10", rt) // pageDelay==0

	out, err := c.Following(context.Background())
	if err != nil {
		t.Fatalf("Following: %v", err)
	}
	if out.Count != 1 || len(out.Follows) != 1 {
		t.Fatalf("count=%d want 1: %+v", out.Count, out.Follows)
	}
	e := out.Follows[0]
	if e.Handle != "creator1" || e.UserID != "100" || !e.IsVerified {
		t.Errorf("enumeration fields: %+v", e)
	}
	if e.Followers != 50000 || e.Following != 120 || !e.IsBusiness || e.Bio != "AI creator" || e.Category != "Blogger" {
		t.Errorf("enrichment fields: %+v", e)
	}
	if !e.Mutual { // follows_viewer=true → mutual
		t.Errorf("mutual should be true (follows_viewer): %+v", e)
	}
	if !e.Enriched {
		t.Errorf("Enriched should be true")
	}
}

func TestLoadSession(t *testing.T) {
	dir := t.TempDir()
	// envelope form {"cookies":{...}}
	env := filepath.Join(dir, "env.json")
	os.WriteFile(env, []byte(`{"platform":"instagram","cookies":{"sessionid":"s","csrftoken":"c","ds_user_id":"9","mid":"m"}}`), 0o600)
	c := &Client{cfg: Config{SessionPath: env}}
	ck, err := c.loadSessionFromFile()
	if err != nil || ck["sessionid"] != "s" || ck["mid"] != "m" {
		t.Fatalf("envelope load: %v %+v", err, ck)
	}
	// raw map form
	raw := filepath.Join(dir, "raw.json")
	os.WriteFile(raw, []byte(`{"sessionid":"s","csrftoken":"c","ds_user_id":"9"}`), 0o600)
	c.cfg.SessionPath = raw
	if ck, err := c.loadSessionFromFile(); err != nil || ck["csrftoken"] != "c" {
		t.Fatalf("raw load: %v %+v", err, ck)
	}
	// missing required → ErrLoggedOut
	bad := filepath.Join(dir, "bad.json")
	os.WriteFile(bad, []byte(`{"csrftoken":"c"}`), 0o600)
	c.cfg.SessionPath = bad
	if _, err := c.loadSessionFromFile(); !errors.Is(err, ErrLoggedOut) {
		t.Errorf("missing sessionid should be ErrLoggedOut, got %v", err)
	}
	// absent file → ErrLoggedOut
	c.cfg.SessionPath = filepath.Join(dir, "nope.json")
	if _, err := c.loadSessionFromFile(); !errors.Is(err, ErrLoggedOut) {
		t.Errorf("absent file should be ErrLoggedOut, got %v", err)
	}
}
