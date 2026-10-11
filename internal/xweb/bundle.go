package xweb

import (
	"context"
	"io"
	"net/http"
	"regexp"
	"time"
)

// bundleMeta is the per-build GraphQL metadata X embeds in its JS bundle: the
// operation→queryId map (rotates per build — the X analog of IG's doc_id), the
// public Bearer, and the feature-switch list the API requires on each call.
type bundleMeta struct {
	queryIDs map[string]string
	bearer   string
	features []string
}

const bundleTTL = 30 * time.Minute

// Operations the connector uses.
const (
	opHomeLatestTimeline = "HomeLatestTimeline" // chronological Following timeline
	opHomeTimeline       = "HomeTimeline"       // algorithmic For You
	opUserByScreenName   = "UserByScreenName"   // handle → rest_id + profile
	opUserTweets         = "UserTweets"         // a user's recent tweets
	opFollowing          = "Following"          // the following list
)

// bakedBundle holds values captured live (2026-06) as a fallback when runtime
// harvesting fails. The live harvest (harvestBundle) is the durable primary path.
var bakedBundle = bundleMeta{
	queryIDs: map[string]string{
		opHomeLatestTimeline: "n2m8OTpLdsM3Zhv33ljKoA",
		opHomeTimeline:       "MP5Mn45hEc4i_q_UwIHBkw",
		opUserByScreenName:   "681MIj51w00Aj6dY0GXnHw",
		opUserTweets:         "RyDU3I9VJtPF-Pnl6vrRlw",
		opFollowing:          "OLm4oHZBfqWx8jbcEhWoFw",
	},
	bearer: publicBearer,
	features: []string{
		"rweb_video_screen_enabled",
		"profile_label_improvements_pcf_label_in_post_enabled",
		"rweb_tipjar_consumption_enabled",
		"responsive_web_graphql_exclude_directive_enabled",
		"verified_phone_label_enabled",
		"creator_subscriptions_tweet_preview_api_enabled",
		"responsive_web_graphql_timeline_navigation_enabled",
		"responsive_web_graphql_skip_user_profile_image_extensions_enabled",
		"premium_content_api_read_enabled",
		"communities_web_enable_tweet_community_results_fetch",
		"c9s_tweet_anatomy_moderator_badge_enabled",
		"responsive_web_grok_analyze_button_fetch_trends_enabled",
		"responsive_web_grok_analyze_post_followups_enabled",
		"responsive_web_jetfuel_frame",
		"responsive_web_grok_share_attachment_enabled",
		"articles_preview_enabled",
		"responsive_web_edit_tweet_api_enabled",
		"graphql_is_translatable_rweb_tweet_is_translatable_enabled",
		"view_counts_everywhere_api_enabled",
		"longform_notetweets_consumption_enabled",
		"responsive_web_twitter_article_tweet_consumption_enabled",
		"tweet_awards_web_tipping_enabled",
		"responsive_web_grok_show_grok_translated_post",
		"responsive_web_grok_analysis_button_from_backend",
		"creator_subscriptions_quote_tweet_preview_enabled",
		"freedom_of_speech_not_reach_fetch_enabled",
		"standardized_nudges_misinfo",
		"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled",
		"longform_notetweets_rich_text_read_enabled",
		"longform_notetweets_inline_media_enabled",
		"responsive_web_grok_image_annotation_enabled",
		"responsive_web_grok_community_note_auto_translation_is_enabled",
		"responsive_web_enhance_cards_enabled",
	},
}

var (
	reScriptURL = regexp.MustCompile(`https://abs\.twimg\.com/responsive-web/client-web[^"']+?\.js`)
	reQueryID   = regexp.MustCompile(`queryId:"([^"]+)",operationName:"([^"]+)"`)
	reBearer    = regexp.MustCompile(`Bearer ([A-Za-z0-9%]{80,})`)
)

// bundleInfo returns the harvested GraphQL metadata, cached for bundleTTL. On a
// harvest failure it falls back to the baked values so a single bad fetch doesn't
// break reads; callers also force-refresh on a queryId miss.
func (c *Client) bundleInfo(ctx context.Context, ck map[string]string) (bundleMeta, error) {
	c.bundleMu.Lock()
	if !c.bundleAt.IsZero() && time.Since(c.bundleAt) < bundleTTL && len(c.bundle.queryIDs) > 0 {
		bm := c.bundle
		c.bundleMu.Unlock()
		return bm, nil
	}
	c.bundleMu.Unlock()

	bm := c.harvestBundle(ctx, ck)
	c.bundleMu.Lock()
	c.bundle = bm
	c.bundleAt = time.Now()
	c.bundleMu.Unlock()
	return bm, nil
}

// harvestBundle fetches x.com + its client-web JS chunks (through the proxy, with
// the session) and extracts the live queryId map / bearer / feature list. Anything
// it can't find falls back to the baked value, so the result is always usable.
func (c *Client) harvestBundle(ctx context.Context, ck map[string]string) bundleMeta {
	out := bundleMeta{queryIDs: map[string]string{}, bearer: bakedBundle.bearer, features: bakedBundle.features}
	for k, v := range bakedBundle.queryIDs {
		out.queryIDs[k] = v
	}
	html := c.fetchText(ctx, "https://x.com/home", ck)
	if html == "" {
		return out // keep baked
	}
	scripts := uniq(reScriptURL.FindAllString(html, -1))
	// Bound the number of chunk fetches; the main + on-demand home/endpoints chunks
	// referenced by the shell HTML carry the operation map.
	if len(scripts) > 24 {
		scripts = scripts[:24]
	}
	wanted := map[string]bool{
		opHomeLatestTimeline: true, opHomeTimeline: true,
		opUserByScreenName: true, opUserTweets: true, opFollowing: true,
	}
	got := map[string]bool{}
	for _, u := range scripts {
		body := c.fetchText(ctx, u, ck)
		if body == "" {
			continue
		}
		if out.bearer == bakedBundle.bearer {
			if m := reBearer.FindStringSubmatch(body); m != nil {
				out.bearer = m[1]
			}
		}
		for _, m := range reQueryID.FindAllStringSubmatch(body, -1) {
			if wanted[m[2]] && !got[m[2]] {
				out.queryIDs[m[2]] = m[1]
				got[m[2]] = true
			}
		}
		if len(got) >= len(wanted) {
			break
		}
	}
	return out
}

// fetchText GETs a URL through the proxy with the session cookies (used for the
// JS bundle harvest; egress is assumed already verified by the caller's read).
func (c *Client) fetchText(ctx context.Context, rawURL string, ck map[string]string) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("User-Agent", webUA)
	req.Header.Set("Cookie", cookieHeader(ck))
	resp, err := c.http.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 24<<20))
	return string(body)
}

func uniq(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
