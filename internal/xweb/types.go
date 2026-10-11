package xweb

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// Post is one X post mapped to the social_activity shape (platform=x).
type Post struct {
	Platform   string    `json:"platform"` // always "x"
	ID         string    `json:"id"`       // tweet rest_id
	URL        string    `json:"url"`
	Author     string    `json:"author"`      // @screen_name
	AuthorID   string    `json:"author_id"`   // user rest_id
	AuthorName string    `json:"author_name"` // display name
	Content    string    `json:"content"`     // full_text
	MediaURLs  []string  `json:"media_urls,omitempty"`
	Likes      int       `json:"likes"`
	Retweets   int       `json:"retweets"`
	Replies    int       `json:"replies"`
	Quotes     int       `json:"quotes"`
	Views      int       `json:"views,omitempty"`
	IsRepost   bool      `json:"is_repost,omitempty"`
	TS         time.Time `json:"ts"`
}

// FeedResult is a page of posts.
type FeedResult struct {
	Count      int    `json:"count"`
	Posts      []Post `json:"posts"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// ---- timeline decode (shared by HomeLatestTimeline + UserTweets) ----

type timelineURT struct {
	Instructions []tlInstruction `json:"instructions"`
}

type tlInstruction struct {
	Type    string    `json:"type"`
	Entries []tlEntry `json:"entries"`
}

type tlEntry struct {
	EntryID string `json:"entryId"`
	Content struct {
		EntryType   string `json:"entryType"`
		CursorType  string `json:"cursorType"`
		Value       string `json:"value"`
		ItemContent struct {
			ItemType         string          `json:"itemType"`
			TweetResults     tweetResultWrap `json:"tweet_results"`
			PromotedMetadata json.RawMessage `json:"promotedMetadata"`
		} `json:"itemContent"`
	} `json:"content"`
}

type tweetResultWrap struct {
	Result tweetResult `json:"result"`
}

// tweetResult handles both __typename "Tweet" (fields inline) and
// "TweetWithVisibilityResults" (fields under .tweet).
type tweetResult struct {
	Typename string     `json:"__typename"`
	Tweet    *tweetNode `json:"tweet"`
	tweetNode
}

func (t tweetResult) node() tweetNode {
	if t.Tweet != nil {
		return *t.Tweet
	}
	return t.tweetNode
}

type tweetNode struct {
	RestID string `json:"rest_id"`
	Core   struct {
		UserResults struct {
			Result struct {
				RestID string `json:"rest_id"`
				Core   struct {
					ScreenName string `json:"screen_name"`
					Name       string `json:"name"`
				} `json:"core"`
			} `json:"result"`
		} `json:"user_results"`
	} `json:"core"`
	Legacy tweetLegacy `json:"legacy"`
	Views  struct {
		Count string `json:"count"`
	} `json:"views"`
}

type tweetLegacy struct {
	FullText      string `json:"full_text"`
	CreatedAt     string `json:"created_at"`
	FavoriteCount int    `json:"favorite_count"`
	RetweetCount  int    `json:"retweet_count"`
	ReplyCount    int    `json:"reply_count"`
	QuoteCount    int    `json:"quote_count"`
	Entities      struct {
		Media []struct {
			MediaURLHTTPS string `json:"media_url_https"`
		} `json:"media"`
	} `json:"entities"`
	RetweetedStatusResult *struct {
		Result *tweetResult `json:"result"`
	} `json:"retweeted_status_result"`
}

// xTimeFormat is X's created_at format, e.g. "Sat Jun 13 10:04:00 +0000 2026".
const xTimeFormat = "Mon Jan 02 15:04:05 -0700 2006"

// mapTimeline walks timeline instructions → Posts, dropping promoted/ad entries
// and non-tweet modules (who-to-follow etc.) so the result is followed-authors
// only. Returns the posts (timeline order) and the bottom cursor for pagination.
func mapTimeline(urt *timelineURT) ([]Post, string) {
	var posts []Post
	var cursor string
	for _, ins := range urt.Instructions {
		if ins.Type != "TimelineAddEntries" && ins.Type != "TimelineReplaceEntry" {
			continue
		}
		for _, e := range ins.Entries {
			c := e.Content
			if c.EntryType == "TimelineTimelineCursor" && c.CursorType == "Bottom" {
				cursor = c.Value
				continue
			}
			if c.EntryType != "TimelineTimelineItem" || c.ItemContent.ItemType != "TimelineTweet" {
				continue // skip modules (who-to-follow), cursors, etc.
			}
			if len(c.ItemContent.PromotedMetadata) > 0 || strings.Contains(e.EntryID, "promoted") {
				continue // drop ads/promoted
			}
			if p, ok := mapTweet(c.ItemContent.TweetResults.Result.node()); ok {
				posts = append(posts, p)
			}
		}
	}
	return posts, cursor
}

func mapTweet(n tweetNode) (Post, bool) {
	u := n.Core.UserResults.Result
	if u.Core.ScreenName == "" || n.RestID == "" {
		return Post{}, false
	}
	p := Post{
		Platform:   "x",
		ID:         n.RestID,
		Author:     u.Core.ScreenName,
		AuthorID:   u.RestID,
		AuthorName: u.Core.Name,
		Content:    n.Legacy.FullText,
		Likes:      n.Legacy.FavoriteCount,
		Retweets:   n.Legacy.RetweetCount,
		Replies:    n.Legacy.ReplyCount,
		Quotes:     n.Legacy.QuoteCount,
		URL:        "https://x.com/" + u.Core.ScreenName + "/status/" + n.RestID,
	}
	if v, err := strconv.Atoi(n.Views.Count); err == nil {
		p.Views = v
	}
	if t, err := time.Parse(xTimeFormat, n.Legacy.CreatedAt); err == nil {
		p.TS = t.UTC()
	}
	for _, m := range n.Legacy.Entities.Media {
		if m.MediaURLHTTPS != "" {
			p.MediaURLs = append(p.MediaURLs, m.MediaURLHTTPS)
		}
	}
	// A retweet: surface the original author's text but flag it.
	if rs := n.Legacy.RetweetedStatusResult; rs != nil && rs.Result != nil {
		p.IsRepost = true
		orig := rs.Result.node()
		if ou := orig.Core.UserResults.Result; ou.Core.ScreenName != "" {
			if orig.Legacy.FullText != "" {
				p.Content = "RT @" + ou.Core.ScreenName + ": " + orig.Legacy.FullText
			}
		}
	}
	return p, true
}
