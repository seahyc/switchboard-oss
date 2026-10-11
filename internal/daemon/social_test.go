package daemon

import (
	"strconv"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/igweb"
)

// TestSocialEventMapping locks the #49 post/story → canonical social_activity mapping:
// correct kind/chat-type/feed-bucket, stable idempotency id, author attribution, and the
// Social payload (sub/url/engagement/expiry).
func TestSocialEventMapping(t *testing.T) {
	ts := time.Date(2026, 6, 13, 9, 0, 0, 0, time.UTC)

	p := igweb.FeedPost{Shortcode: "Cxyz", URL: "https://instagram.com/p/Cxyz", Author: "caseyexample", AuthorID: "777", Caption: "gm", MediaType: "image", MediaURL: "https://cdn/x.jpg", Likes: 12, Comments: 3, TS: ts}
	m := socialPostEvent(p)
	if m.Kind != event.KindSocialActivity || m.ChatType != event.ChatFeed {
		t.Fatalf("post kind/type: %q/%q", m.Kind, m.ChatType)
	}
	if m.ID != "instagram:post:Cxyz" {
		t.Fatalf("post id = %q, want instagram:post:Cxyz", m.ID)
	}
	if m.ChatID != "instagram:feed:777" || m.Sender.PlatformUserID != "777" || m.Sender.Handle != "caseyexample" {
		t.Fatalf("post attribution: chat=%q sender=%+v", m.ChatID, m.Sender)
	}
	if m.Text != "gm" || m.Social == nil || m.Social.Sub != "post" || m.Social.Likes != 12 || m.Social.Comments != 3 || m.Social.URL == "" {
		t.Fatalf("post social: text=%q social=%+v", m.Text, m.Social)
	}
	if len(m.Attachments) != 1 || m.Attachments[0].Ref != "https://cdn/x.jpg" {
		t.Fatalf("post media: %+v", m.Attachments)
	}

	u := igweb.StoryUser{UserID: "888", Handle: "alice", Name: "Alice", LatestTS: ts, MediaCount: 2}
	sm := socialStoryEvent(u)
	if sm.Kind != event.KindSocialActivity || sm.Social == nil || sm.Social.Sub != "story" {
		t.Fatalf("story kind/social: %q %+v", sm.Kind, sm.Social)
	}
	if sm.ID != "instagram:story:888:"+strconv.FormatInt(ts.Unix(), 10) {
		t.Fatalf("story id = %q", sm.ID)
	}
	if sm.Social.ExpiresAt == nil || !sm.Social.ExpiresAt.Equal(ts.Add(24*time.Hour)) {
		t.Fatalf("story expiry: %v", sm.Social.ExpiresAt)
	}

	// Follow-identity event (#49): durable node + classify signals, no activity payload.
	f := igweb.FollowEntry{UserID: "999", Handle: "casey", Name: "Casey", Mutual: true, IsBusiness: false, Followers: 312, Category: "Friend"}
	fm := socialFollowEvent(f)
	if fm.Kind != event.KindSocialActivity || fm.ChatType != event.ChatFeed {
		t.Fatalf("follow kind/type: %q/%q", fm.Kind, fm.ChatType)
	}
	if fm.ID != "instagram:follow:999" {
		t.Fatalf("follow id = %q, want instagram:follow:999", fm.ID)
	}
	if fm.ChatID != "instagram:feed:999" || fm.Sender.PlatformUserID != "999" || fm.Sender.Handle != "casey" || fm.Sender.DisplayName != "Casey" {
		t.Fatalf("follow attribution: chat=%q sender=%+v", fm.ChatID, fm.Sender)
	}
	if fm.Social == nil || fm.Social.Sub != "follow" {
		t.Fatalf("follow social sub: %+v", fm.Social)
	}
	if fm.Social.Mutual == nil || !*fm.Social.Mutual || fm.Social.IsBusiness == nil || *fm.Social.IsBusiness || fm.Social.Followers != 312 || fm.Social.Category != "Friend" {
		t.Fatalf("follow classify signals: %+v", fm.Social)
	}
	// Identity-only: no text/media/engagement.
	if fm.Text != "" || len(fm.Attachments) != 0 || fm.Social.Likes != 0 {
		t.Fatalf("follow should carry no activity payload: text=%q att=%d social=%+v", fm.Text, len(fm.Attachments), fm.Social)
	}
}
