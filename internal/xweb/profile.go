package xweb

import (
	"context"
	"fmt"
)

// ProfileCard is a read-only X profile card + the account's recent posts.
type ProfileCard struct {
	Handle      string `json:"handle"`
	Name        string `json:"name"`
	UserID      string `json:"user_id"`
	Bio         string `json:"bio,omitempty"`
	Followers   int    `json:"followers"`
	Following   int    `json:"following"`
	Tweets      int    `json:"tweets"`
	Verified    bool   `json:"verified,omitempty"`
	RecentPosts []Post `json:"recent_posts,omitempty"`
}

type userByScreenNameResp struct {
	Data struct {
		User struct {
			Result struct {
				RestID string `json:"rest_id"`
				Core   struct {
					ScreenName string `json:"screen_name"`
					Name       string `json:"name"`
				} `json:"core"`
				IsBlueVerified bool `json:"is_blue_verified"`
				Legacy         struct {
					Description    string `json:"description"`
					FollowersCount int    `json:"followers_count"`
					FriendsCount   int    `json:"friends_count"`
					StatusesCount  int    `json:"statuses_count"`
				} `json:"legacy"`
			} `json:"result"`
		} `json:"user"`
	} `json:"data"`
}

type userTweetsResp struct {
	Data struct {
		User struct {
			Result struct {
				TimelineV2 struct {
					Timeline timelineURT `json:"timeline"`
				} `json:"timeline_v2"`
			} `json:"result"`
		} `json:"user"`
	} `json:"data"`
}

// Profile resolves a handle to its profile card + recent posts (egress-verified,
// read-only): UserByScreenName → rest_id, then UserTweets for the recent posts.
func (c *Client) Profile(ctx context.Context, handle string, limit int) (ProfileCard, error) {
	if limit <= 0 {
		limit = 10
	}
	handle = trimAt(handle)
	var pr userByScreenNameResp
	if err := c.graphqlGet(ctx, opUserByScreenName, map[string]any{"screen_name": handle}, &pr); err != nil {
		return ProfileCard{}, err
	}
	u := pr.Data.User.Result
	if u.RestID == "" || u.Core.ScreenName == "" {
		return ProfileCard{}, fmt.Errorf("xweb: user %q not found", handle)
	}
	card := ProfileCard{
		Handle:    u.Core.ScreenName,
		Name:      u.Core.Name,
		UserID:    u.RestID,
		Bio:       u.Legacy.Description,
		Followers: u.Legacy.FollowersCount,
		Following: u.Legacy.FriendsCount,
		Tweets:    u.Legacy.StatusesCount,
		Verified:  u.IsBlueVerified,
	}

	if err := c.pace(ctx); err != nil {
		return card, err
	}
	vars := map[string]any{
		"userId":                                 u.RestID,
		"count":                                  limit + 6,
		"includePromotedContent":                 false,
		"withQuickPromoteEligibilityTweetFields": false,
		"withVoice":                              false,
	}
	var tr userTweetsResp
	if err := c.graphqlGet(ctx, opUserTweets, vars, &tr); err != nil {
		return card, nil // profile still useful even if tweets fail
	}
	posts, _ := mapTimeline(&tr.Data.User.Result.TimelineV2.Timeline)
	// UserTweets is the target user only; keep just their authored posts.
	for _, p := range posts {
		if len(card.RecentPosts) >= limit {
			break
		}
		card.RecentPosts = append(card.RecentPosts, p)
	}
	return card, nil
}

func trimAt(s string) string {
	if len(s) > 0 && s[0] == '@' {
		return s[1:]
	}
	return s
}
