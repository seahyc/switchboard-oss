package daemon

import (
	"strconv"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/xweb"
)

// TestXPostEventMapping locks the #55 X-post → canonical social_activity mapping:
// platform/feed/chat-id, stable idempotency id, author attribution, and the raw
// engagement envelope (likes/replies→comments/retweets/quotes/views).
func TestXPostEventMapping(t *testing.T) {
	p := xweb.Post{
		ID: "1929", URL: "https://x.com/n/status/1929", Author: "naval", AuthorID: "745",
		AuthorName: "Naval", Content: "the thread", MediaURLs: []string{"https://x/img.jpg"},
		Likes: 1000, Retweets: 200, Replies: 50, Quotes: 30, Views: 90000,
		TS: time.Date(2026, 6, 13, 9, 0, 0, 0, time.UTC),
	}
	m := xPostEvent(p)
	if m.Platform != event.PlatformX || m.Kind != event.KindSocialActivity || m.ChatType != event.ChatFeed {
		t.Fatalf("platform/kind/type: %q/%q/%q", m.Platform, m.Kind, m.ChatType)
	}
	if m.ID != "x:post:1929" {
		t.Fatalf("id = %q, want x:post:1929", m.ID)
	}
	if m.ChatID != "x:feed:745" || m.Sender.PlatformUserID != "745" || m.Sender.Handle != "naval" || m.Sender.DisplayName != "Naval" {
		t.Fatalf("attribution: chat=%q sender=%+v", m.ChatID, m.Sender)
	}
	if m.Social == nil || m.Social.Sub != "post" || m.Social.Likes != 1000 || m.Social.Comments != 50 ||
		m.Social.Retweets != 200 || m.Social.Quotes != 30 || m.Social.Views != 90000 {
		t.Fatalf("social envelope: %+v", m.Social)
	}
	if len(m.Attachments) != 1 || m.Attachments[0].Ref != "https://x/img.jpg" {
		t.Fatalf("media: %+v", m.Attachments)
	}
	if rp := xPostEvent(xweb.Post{ID: "2", AuthorID: "1", IsRepost: true}); rp.Social.Sub != "repost" {
		t.Fatalf("repost sub = %q", rp.Social.Sub)
	}
}

// TestXTrendEvent locks the #55 trend → social_activity mapping: synthetic sender, the
// shared x:trends feed, volume in Social.Views, Sub="trend", idempotent per-day id.
func TestXTrendEvent(t *testing.T) {
	now := time.Date(2026, 6, 14, 9, 0, 0, 0, time.UTC)
	m := xTrendEvent(xweb.Trend{Name: "WWDC", PostCount: 42100, Context: "Trending in Technology"}, now)
	if m.Platform != event.PlatformX || m.Social == nil || m.Social.Sub != "trend" {
		t.Fatalf("platform/sub: %q %+v", m.Platform, m.Social)
	}
	if m.ChatID != "x:trends" || m.Sender.PlatformUserID != "trends" {
		t.Fatalf("trend chat/sender: chat=%q sender=%+v", m.ChatID, m.Sender)
	}
	if m.Social.Views != 42100 {
		t.Fatalf("trend volume (Views) = %d, want 42100", m.Social.Views)
	}
	if m.Text != "WWDC — Trending in Technology" {
		t.Fatalf("trend text = %q", m.Text)
	}
	wantID := "x:trend:wwdc:" + strconv.FormatInt(now.Unix()/86400, 10)
	if m.ID != wantID {
		t.Fatalf("trend id = %q, want %q", m.ID, wantID)
	}
	if h := xTrendEvent(xweb.Trend{Name: "#AI"}, now); h.ID != "x:trend:ai:"+strconv.FormatInt(now.Unix()/86400, 10) {
		t.Fatalf("hashtag slug id = %q", h.ID)
	}
}
