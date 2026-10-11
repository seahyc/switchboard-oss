package daemon

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/xweb"
)

// #55 — X (Twitter) ingestion. Switchboard's job is pure: pull the following-feed + the
// trending sidebar into the canonical social_activity log as RAW events (engagement as
// plain fields on the event), exactly like the IG #49 ingestion. NO judgment here —
// importance / ranking / "what's important to the user" / the briefing all live in HERMES,
// which owns the user-model (USER.md + Honcho) and the morning-briefing. Hermes reads
// social_activity via the MCP and judges it itself. Clean boundary: switchboard =
// ingest → canonical log → raw exposure; Hermes = judgment.

// xPostEvent maps an X following-feed post to a canonical social_activity event (#55),
// same envelope/pattern as the IG post mapper (#49). Engagement (likes/replies/retweets/
// quotes/views) rides as raw snapshot fields; consumers rank/judge on them. Idempotent on
// the tweet id.
func xPostEvent(p xweb.Post) event.Message {
	now := time.Now().UTC()
	sub := "post"
	if p.IsRepost {
		sub = "repost"
	}
	m := event.Message{
		Platform:   event.PlatformX,
		ChatID:     event.PlatformX + ":feed:" + p.AuthorID,
		ChatType:   event.ChatFeed,
		Sender:     event.Sender{PlatformUserID: p.AuthorID, Handle: p.Author, DisplayName: p.AuthorName},
		TS:         p.TS,
		Kind:       event.KindSocialActivity,
		Text:       p.Content,
		Source:     event.SourceLive,
		IngestedAt: now,
		Social: &event.Social{
			Sub:      sub,
			URL:      p.URL,
			Likes:    p.Likes,
			Comments: p.Replies,
			Retweets: p.Retweets,
			Quotes:   p.Quotes,
			Views:    p.Views,
		},
	}
	for _, u := range p.MediaURLs {
		if u != "" {
			m.Attachments = append(m.Attachments, event.Attachment{Type: "image", Ref: u})
		}
	}
	m.SetID("post:" + p.ID)
	return m
}

// IngestXActivity pulls the X following-feed and writes each post as a social_activity
// event (#55), then folds in the trending sidebar. Ingests everything to the log raw;
// consumers (Hermes) filter/judge at read time. Idempotent via the tweet-id event id.
// Returns the post count enqueued.
func (d *Daemon) IngestXActivity(ctx context.Context, limit int) (int, error) {
	if d.x == nil {
		return 0, fmt.Errorf("x browser disabled")
	}
	if limit <= 0 {
		limit = 50
	}
	feed, err := d.x.FollowingFeed(ctx, limit)
	if err != nil {
		return 0, fmt.Errorf("x following feed: %w", err)
	}
	n := 0
	for _, p := range feed.Posts {
		if p.ID == "" || p.AuthorID == "" {
			continue
		}
		if _, e := d.pipe.Ingest(ctx, xPostEvent(p)); e != nil {
			d.log.Warn("x ingest: post", "tweet", p.ID, "author", p.Author, "err", e)
			continue
		}
		n++
	}
	// Trending sidebar (#55 surface B): best-effort, never fails the feed ingest.
	if tn, terr := d.IngestXTrends(ctx); terr != nil {
		d.log.Warn("x ingest: trends", "err", terr)
	} else if tn > 0 {
		d.log.Info("x trends ingested", "trends", tn)
	}
	d.log.Info("x activity ingested", "posts", n)
	return n, nil
}

// xTrendEvent maps an X trending-sidebar entry to a canonical social_activity event
// (#55 surface B). A trend isn't authored by a person → synthetic sender + the shared
// x:trends feed chat. Volume (post_count) rides in Social.Views as a raw field.
// Idempotent per trend per UTC day so a daily read carries one row per trend.
func xTrendEvent(t xweb.Trend, now time.Time) event.Message {
	text := t.Name
	if t.Context != "" {
		text = t.Name + " — " + t.Context
	}
	m := event.Message{
		Platform:   event.PlatformX,
		ChatID:     event.PlatformX + ":trends",
		ChatType:   event.ChatFeed,
		Sender:     event.Sender{PlatformUserID: "trends", DisplayName: "X Trends"},
		TS:         now,
		Kind:       event.KindSocialActivity,
		Text:       text,
		Source:     event.SourceLive,
		IngestedAt: now,
		Social:     &event.Social{Sub: "trend", Views: t.PostCount},
	}
	m.SetID(fmt.Sprintf("trend:%s:%d", trendSlug(t.Name), now.Unix()/86400))
	return m
}

func trendSlug(name string) string {
	s := strings.ToLower(strings.TrimSpace(name))
	s = strings.TrimPrefix(s, "#")
	return strings.ReplaceAll(s, " ", "_")
}

// IngestXTrends pulls the trending sidebar and writes each trend as a raw social_activity
// event (#55). Everything to the log; Hermes judges. Returns the count enqueued.
func (d *Daemon) IngestXTrends(ctx context.Context) (int, error) {
	if d.x == nil {
		return 0, fmt.Errorf("x browser disabled")
	}
	trends, err := d.x.Trends(ctx)
	if err != nil {
		return 0, fmt.Errorf("x trends: %w", err)
	}
	now := time.Now().UTC()
	n := 0
	for _, t := range trends {
		if strings.TrimSpace(t.Name) == "" {
			continue
		}
		if _, e := d.pipe.Ingest(ctx, xTrendEvent(t, now)); e != nil {
			d.log.Warn("x ingest: trend", "trend", t.Name, "err", e)
			continue
		}
		n++
	}
	return n, nil
}
