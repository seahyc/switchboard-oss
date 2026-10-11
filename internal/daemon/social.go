package daemon

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/igweb"
)

// IngestSocialActivity pulls the user's Instagram following-feed posts + story tray via
// the existing igweb reads (#25) and writes each as a canonical KindSocialActivity event
// to the log, attributed to the author (#49). It's the persistence layer the daily
// briefing reads to answer "what are my friends up to" (people-layer classifies each
// author friend-vs-creator + merge-proposes friends to their canonical person).
//
// On-demand (a manual trigger); the recurring story poll (stories expire ~24h) is a
// separate go/no-go and is NOT wired here. Idempotent: posts key on shortcode, stories on
// the user's latest-reel-media id, so re-ingest is a no-op via the pipeline's INSERT-OR-
// IGNORE. Returns the count of post + story events enqueued. limit bounds the feed pull.
func (d *Daemon) IngestSocialActivity(ctx context.Context, limit int) (posts, stories, follows int, err error) {
	if d.ig == nil {
		return 0, 0, 0, fmt.Errorf("instagram browser disabled")
	}
	if limit <= 0 {
		limit = 50
	}
	feed, ferr := d.ig.FollowingFeed(ctx, limit)
	if ferr != nil {
		return 0, 0, 0, fmt.Errorf("following feed: %w", ferr)
	}
	for _, p := range feed.Posts {
		if _, e := d.pipe.Ingest(ctx, socialPostEvent(p)); e != nil {
			d.log.Warn("social ingest: post", "shortcode", p.Shortcode, "err", e)
			continue
		}
		posts++
	}
	tray, terr := d.ig.Stories(ctx)
	if terr != nil {
		// Posts already ingested; surface the story error but don't discard the posts.
		return posts, 0, 0, fmt.Errorf("stories: %w", terr)
	}
	for _, u := range tray.Users {
		if _, e := d.pipe.Ingest(ctx, socialStoryEvent(u)); e != nil {
			d.log.Warn("social ingest: story", "user", u.UserID, "err", e)
			continue
		}
		stories++
	}

	// Follow-identity ingestion (#49): the post/story passes above only surface
	// follows who've been ACTIVE — a follow who hasn't posted never becomes an
	// identity node, so people-layer's apply can't classify/merge them. Pull the
	// complete follow-list and emit a lightweight identity-establishing event per
	// follow (Sub="follow", no post/media), carrying the friend-vs-creator signals
	// so the projection-time classifier needs no network call. Best-effort: a
	// follow-list error doesn't discard the posts/stories already ingested.
	if fl, lerr := d.ig.Following(ctx); lerr != nil {
		d.log.Warn("social ingest: following-list", "err", lerr)
	} else {
		for _, f := range fl.Follows {
			if f.UserID == "" {
				continue // can't form a stable identity without the numeric id
			}
			if _, e := d.pipe.Ingest(ctx, socialFollowEvent(f)); e != nil {
				d.log.Warn("social ingest: follow", "user", f.UserID, "handle", f.Handle, "err", e)
				continue
			}
			follows++
		}
	}

	d.log.Info("social activity ingested", "posts", posts, "stories", stories, "follows", follows)
	return posts, stories, follows, nil
}

// socialPostEvent maps an igweb following-feed post to a canonical social_activity event.
func socialPostEvent(p igweb.FeedPost) event.Message {
	now := time.Now().UTC()
	m := event.Message{
		Platform:   event.PlatformInstagram,
		ChatID:     event.PlatformInstagram + ":feed:" + p.AuthorID,
		ChatType:   event.ChatFeed,
		Sender:     event.Sender{PlatformUserID: p.AuthorID, Handle: p.Author, DisplayName: p.Author},
		TS:         p.TS,
		Kind:       event.KindSocialActivity,
		Text:       p.Caption,
		Source:     event.SourceLive,
		IngestedAt: now,
		Social:     &event.Social{Sub: "post", URL: p.URL, Likes: p.Likes, Comments: p.Comments},
	}
	if p.MediaURL != "" {
		mt := p.MediaType
		if mt == "" {
			mt = "image"
		}
		m.Attachments = []event.Attachment{{Type: mt, Ref: p.MediaURL}}
	}
	// Stable idempotency key: the post shortcode (one event per post, forever).
	m.SetID("post:" + p.Shortcode)
	return m
}

// socialStoryEvent maps an igweb story-tray user to a canonical social_activity event.
// v1 granularity is per story-SESSION (one event per user's current story batch), keyed
// on the latest-reel-media timestamp — it changes when the user posts new stories, so it
// won't collide across days like userid+date would. True per-FRAME events need an igweb
// reel-items fetch (meta's lane); deferred.
func socialStoryEvent(u igweb.StoryUser) event.Message {
	now := time.Now().UTC()
	expires := u.LatestTS.Add(24 * time.Hour)
	m := event.Message{
		Platform:   event.PlatformInstagram,
		ChatID:     event.PlatformInstagram + ":feed:" + u.UserID,
		ChatType:   event.ChatFeed,
		Sender:     event.Sender{PlatformUserID: u.UserID, Handle: u.Handle, DisplayName: u.Name},
		TS:         u.LatestTS,
		Kind:       event.KindSocialActivity,
		Source:     event.SourceLive,
		IngestedAt: now,
		Social:     &event.Social{Sub: "story", ExpiresAt: &expires},
	}
	m.SetID("story:" + u.UserID + ":" + strconv.FormatInt(u.LatestTS.Unix(), 10))
	return m
}

// socialFollowEvent maps a follow-list entry to a lightweight identity-establishing
// social_activity event (#49). Unlike posts/stories this is NOT activity — it exists
// so a followed account becomes a durable identity node (via the gbrain projector's
// ResolveSocialAuthor) even if they've never posted, and so people-layer's
// projection-time ClassifyFollow has the friend-vs-creator signals (mutual/is_business/
// followers/category) WITHOUT a network call. No Text/media/engagement. Idempotent on
// the user id — one stable "follow:" event per account, re-ingest is a no-op, and a
// later post by the same account is a separate (post:) event. The TS is set at ingest
// (a follow has no intrinsic timestamp); since the id is stable, re-ingest won't move it.
func socialFollowEvent(f igweb.FollowEntry) event.Message {
	now := time.Now().UTC()
	mutual, isBusiness := f.Mutual, f.IsBusiness
	m := event.Message{
		Platform:   event.PlatformInstagram,
		ChatID:     event.PlatformInstagram + ":feed:" + f.UserID,
		ChatType:   event.ChatFeed,
		Sender:     event.Sender{PlatformUserID: f.UserID, Handle: f.Handle, DisplayName: f.Name},
		TS:         now,
		Kind:       event.KindSocialActivity,
		Source:     event.SourceLive,
		IngestedAt: now,
		Social: &event.Social{
			Sub:        "follow",
			Mutual:     &mutual,
			IsBusiness: &isBusiness,
			Followers:  f.Followers,
			Category:   f.Category,
		},
	}
	m.SetID("follow:" + f.UserID)
	return m
}
