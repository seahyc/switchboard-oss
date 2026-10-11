package igweb

import (
	"context"
	"time"
)

// FollowEntry is one account the user follows, enriched with the friend-vs-creator
// classification signals (#49): follower scale, business flag, bio. The bare
// follow-list (friendships/following) lacks these, so each entry is enriched with
// one profile read.
type FollowEntry struct {
	Handle     string `json:"handle"`
	Name       string `json:"name"`
	UserID     string `json:"user_id"`
	IsPrivate  bool   `json:"is_private"`
	IsVerified bool   `json:"is_verified"`
	Mutual     bool   `json:"mutual"` // they follow the user back
	Followers  int    `json:"followers"`
	Following  int    `json:"following"`
	Bio        string `json:"bio,omitempty"`
	IsBusiness bool   `json:"is_business"`
	Category   string `json:"category,omitempty"`
	Enriched   bool   `json:"enriched"` // false if the per-account profile read failed
}

// FollowingList is the COMPLETE set of accounts the user follows.
type FollowingList struct {
	Count   int           `json:"count"`
	Follows []FollowEntry `json:"follows"`
}

const followingListCacheTTL = 10 * time.Minute

// Following returns the complete follow-list (egress-verified, read-only): it
// enumerates the accounts the user follows (1 call) then enriches each with a
// profile read (followers/bio/is_business — the friend-vs-creator signals #49's
// classifier needs). Paced across the fan-out; cached briefly so repeated classify
// runs don't re-fan-out. An account whose enrichment fails is still returned with
// the enumeration fields and Enriched=false.
func (c *Client) Following(ctx context.Context) (FollowingList, error) {
	c.followMu.Lock()
	if !c.followListAt.IsZero() && time.Since(c.followListAt) < followingListCacheTTL && len(c.followList) > 0 {
		cached := c.followList
		c.followMu.Unlock()
		return FollowingList{Count: len(cached), Follows: cached}, nil
	}
	c.followMu.Unlock()

	users, err := c.following(ctx)
	if err != nil {
		return FollowingList{}, err
	}
	out := make([]FollowEntry, 0, len(users))
	for i, u := range users {
		e := FollowEntry{
			Handle:     u.Username,
			Name:       u.FullName,
			UserID:     u.PK.String(),
			IsPrivate:  u.IsPrivate,
			IsVerified: u.IsVerified,
			Mutual:     u.Friendship.FollowedBy,
		}
		if i > 0 {
			if err := c.pace(ctx); err != nil {
				return FollowingList{}, err
			}
		}
		if card, err := c.Profile(ctx, u.Username); err == nil {
			e.Followers = card.Followers
			e.Following = card.Following
			e.Bio = card.Bio
			e.IsBusiness = card.IsBusiness
			e.Category = card.Category
			if card.FollowsYou {
				e.Mutual = true
			}
			e.Enriched = true
		}
		out = append(out, e)
	}

	c.followMu.Lock()
	c.followList = out
	c.followListAt = time.Now()
	c.followMu.Unlock()

	return FollowingList{Count: len(out), Follows: out}, nil
}
