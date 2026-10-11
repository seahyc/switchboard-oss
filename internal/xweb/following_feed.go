package xweb

import "context"

const followingDefaultLimit = 20

// homeLatestResp decodes HomeLatestTimeline: data.home.home_timeline_urt.
type homeLatestResp struct {
	Data struct {
		Home struct {
			HomeTimelineUrt timelineURT `json:"home_timeline_urt"`
		} `json:"home"`
	} `json:"data"`
}

// FollowingFeed returns the user's chronological "Following" timeline — recent
// posts from accounts the user follows, newest-first, with promoted/ads dropped
// (egress-verified, read-only). limit<=0 uses followingDefaultLimit.
func (c *Client) FollowingFeed(ctx context.Context, limit int) (FeedResult, error) {
	if limit <= 0 {
		limit = followingDefaultLimit
	}
	// Ask for a bit more than limit since some entries get filtered (promoted).
	count := limit + 10
	if count > 100 {
		count = 100
	}
	vars := map[string]any{
		"count":                  count,
		"includePromotedContent": false,
		"latestControlAvailable": true,
		"requestContext":         "launch",
	}
	var r homeLatestResp
	if err := c.graphqlGet(ctx, opHomeLatestTimeline, vars, &r); err != nil {
		return FeedResult{}, err
	}
	posts, cursor := mapTimeline(&r.Data.Home.HomeTimelineUrt)
	if limit > 0 && len(posts) > limit {
		posts = posts[:limit]
	}
	return FeedResult{Count: len(posts), Posts: posts, NextCursor: cursor}, nil
}
