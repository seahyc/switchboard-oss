package igweb

import (
	"context"
	"encoding/json"
	"net/url"
	"time"
)

// IGPost is one recent post on a profile.
type IGPost struct {
	Shortcode string    `json:"shortcode"`
	URL       string    `json:"url"` // https://instagram.com/p/<shortcode>
	Caption   string    `json:"caption,omitempty"`
	MediaURL  string    `json:"media_url,omitempty"` // thumbnail/display image
	IsVideo   bool      `json:"is_video,omitempty"`
	Likes     int       `json:"likes"`
	Comments  int       `json:"comments,omitempty"`
	TS        time.Time `json:"ts"`
}

// ProfileCard is the structured "what an IG user checks when visiting a profile".
type ProfileCard struct {
	Handle     string `json:"handle"`
	Name       string `json:"name,omitempty"`
	UserID     string `json:"user_id,omitempty"`
	Bio        string `json:"bio,omitempty"`
	Verified   bool   `json:"verified,omitempty"`
	Private    bool   `json:"private,omitempty"`
	IsBusiness bool   `json:"is_business,omitempty"`
	Category   string `json:"category,omitempty"`

	Followers int `json:"followers"`
	Following int `json:"following"`
	Posts     int `json:"posts"`

	// Mutual-follow status relative to the logged-in user.
	YouFollow  bool `json:"you_follow"`
	FollowsYou bool `json:"follows_you"`
	Mutual     bool `json:"mutual"`

	HighlightCount int      `json:"highlight_count,omitempty"`
	RecentPosts    []IGPost `json:"recent_posts,omitempty"`
}

// StoryUser is one person in the viewer's story tray with an active story.
type StoryUser struct {
	UserID     string    `json:"user_id"`
	Handle     string    `json:"handle"`
	Name       string    `json:"name,omitempty"`
	LatestTS   time.Time `json:"latest_ts"`
	MediaCount int       `json:"media_count,omitempty"`
	Seen       bool      `json:"seen"`
}

// StoriesResult is the current story tray (who in the network has a live story).
type StoriesResult struct {
	Count int         `json:"count"`
	Users []StoryUser `json:"users"`
}

// --- IG private-web response shapes (only the fields we use) ---

type igProfileResp struct {
	Data struct {
		User struct {
			ID                 string `json:"id"`
			Username           string `json:"username"`
			FullName           string `json:"full_name"`
			Biography          string `json:"biography"`
			IsVerified         bool   `json:"is_verified"`
			IsPrivate          bool   `json:"is_private"`
			IsBusiness         bool   `json:"is_business_account"`
			CategoryName       string `json:"category_name"`
			FollowedByViewer   bool   `json:"followed_by_viewer"`
			FollowsViewer      bool   `json:"follows_viewer"`
			HighlightReelCount int    `json:"highlight_reel_count"`
			EdgeFollowedBy     struct {
				Count int `json:"count"`
			} `json:"edge_followed_by"`
			EdgeFollow struct {
				Count int `json:"count"`
			} `json:"edge_follow"`
			EdgeTimeline struct {
				Count int `json:"count"`
				Edges []struct {
					Node struct {
						Shortcode   string `json:"shortcode"`
						DisplayURL  string `json:"display_url"`
						IsVideo     bool   `json:"is_video"`
						TakenAt     int64  `json:"taken_at_timestamp"`
						EdgeLikedBy struct {
							Count int `json:"count"`
						} `json:"edge_liked_by"`
						EdgeComments struct {
							Count int `json:"count"`
						} `json:"edge_media_to_comment"`
						Caption struct {
							Edges []struct {
								Node struct {
									Text string `json:"text"`
								} `json:"node"`
							} `json:"edges"`
						} `json:"edge_media_to_caption"`
					} `json:"node"`
				} `json:"edges"`
			} `json:"edge_owner_to_timeline_media"`
		} `json:"user"`
	} `json:"data"`
}

type igTrayResp struct {
	Tray []struct {
		ID   string `json:"id"`
		User struct {
			PK       json.Number `json:"pk"`
			Username string      `json:"username"`
			FullName string      `json:"full_name"`
		} `json:"user"`
		LatestReelMedia int64 `json:"latest_reel_media"`
		MediaCount      int   `json:"media_count"`
		Seen            int64 `json:"seen"`
	} `json:"tray"`
}

// Profile reads a profile card for the given handle (egress-verified, read-only).
func (c *Client) Profile(ctx context.Context, handle string) (ProfileCard, error) {
	var r igProfileResp
	u := "https://www.instagram.com/api/v1/users/web_profile_info/?username=" + url.QueryEscape(handle)
	if err := c.get(ctx, u, &r); err != nil {
		return ProfileCard{}, err
	}
	if r.Data.User.Username == "" {
		return ProfileCard{}, ErrLoggedOut // empty payload usually means an auth wall
	}
	return mapProfile(handle, &r), nil
}

// Stories reads the current story tray (who in your network has a live story).
func (c *Client) Stories(ctx context.Context) (StoriesResult, error) {
	var r igTrayResp
	if err := c.get(ctx, "https://www.instagram.com/api/v1/feed/reels_tray/", &r); err != nil {
		return StoriesResult{}, err
	}
	return mapStories(&r), nil
}

// --- pure mappers (unit-tested against canned JSON) ---

func mapProfile(handle string, r *igProfileResp) ProfileCard {
	u := r.Data.User
	card := ProfileCard{
		Handle:         u.Username,
		Name:           u.FullName,
		UserID:         u.ID,
		Bio:            u.Biography,
		Verified:       u.IsVerified,
		Private:        u.IsPrivate,
		IsBusiness:     u.IsBusiness,
		Category:       u.CategoryName,
		Followers:      u.EdgeFollowedBy.Count,
		Following:      u.EdgeFollow.Count,
		Posts:          u.EdgeTimeline.Count,
		YouFollow:      u.FollowedByViewer,
		FollowsYou:     u.FollowsViewer,
		Mutual:         u.FollowedByViewer && u.FollowsViewer,
		HighlightCount: u.HighlightReelCount,
	}
	if card.Handle == "" {
		card.Handle = handle
	}
	for _, e := range u.EdgeTimeline.Edges {
		n := e.Node
		p := IGPost{
			Shortcode: n.Shortcode,
			IsVideo:   n.IsVideo,
			MediaURL:  n.DisplayURL,
			Likes:     n.EdgeLikedBy.Count,
			Comments:  n.EdgeComments.Count,
		}
		if n.Shortcode != "" {
			p.URL = "https://www.instagram.com/p/" + n.Shortcode + "/"
		}
		if n.TakenAt > 0 {
			p.TS = time.Unix(n.TakenAt, 0).UTC()
		}
		if len(n.Caption.Edges) > 0 {
			p.Caption = n.Caption.Edges[0].Node.Text
		}
		card.RecentPosts = append(card.RecentPosts, p)
	}
	return card
}

func mapStories(r *igTrayResp) StoriesResult {
	out := StoriesResult{}
	for _, t := range r.Tray {
		// A tray entry with no media isn't an active story.
		if t.MediaCount <= 0 && t.LatestReelMedia == 0 {
			continue
		}
		su := StoryUser{
			UserID:     t.User.PK.String(),
			Handle:     t.User.Username,
			Name:       t.User.FullName,
			MediaCount: t.MediaCount,
			Seen:       t.Seen >= t.LatestReelMedia && t.LatestReelMedia > 0,
		}
		if t.LatestReelMedia > 0 {
			su.LatestTS = time.Unix(t.LatestReelMedia, 0).UTC()
		}
		if su.UserID == "" {
			su.UserID = t.ID
		}
		out.Users = append(out.Users, su)
	}
	out.Count = len(out.Users)
	return out
}
