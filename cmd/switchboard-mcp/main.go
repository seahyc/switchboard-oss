// Command switchboard-mcp is the stdio MCP server the agent (Hermes) talks to.
// It is a thin translation layer: each MCP tool call becomes an HTTP request to
// the local switchboardd daemon, which owns the store + connectors. No state,
// no LLM — pure capture+index surface.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/seahyc/switchboard-oss/internal/api"
)

func main() {
	// Drop the inherited +200 oom_score_adj. Hermes spawns this stdio server as a
	// child of the systemd --user manager, whose DefaultOOMScoreAdjust=200 makes
	// every child a PREFERRED OOM victim. An unprivileged process can lower its own
	// score to 0 (not negative — that needs root), which clears the footgun. This is
	// a disposable, per-session process (it auto-respawns), so 0 — neutral, killed
	// after the daemon (-1000) + the user's persistent criticals (-500) — is the
	// right level; it just must not be a +200 preferred target. Best-effort.
	_ = os.WriteFile("/proc/self/oom_score_adj", []byte("0\n"), 0o644)

	addr := os.Getenv("SWITCHBOARD_ADDR")
	if addr == "" {
		addr = api.DefaultAddr
	}
	client := api.NewClient(addr)

	s := server.NewMCPServer("switchboard", "0.1.0",
		server.WithToolCapabilities(true),
	)
	registerTools(s, client)

	if err := server.ServeStdio(s); err != nil {
		log.Fatalf("switchboard-mcp: %v", err)
	}
}

func registerTools(s *server.MCPServer, c *api.Client) {
	s.AddTool(mcp.NewTool("list_chats",
		mcp.WithDescription("List chats from the index, newest activity first."),
		mcp.WithString("platform", mcp.Description("Filter: whatsapp|telegram|lark")),
		mcp.WithString("since", mcp.Description("RFC3339; only chats active since")),
		mcp.WithNumber("limit", mcp.Description("Max chats (default 100)")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		chats, err := c.ListChats(ctx, r.GetString("platform", ""), ts(r.GetString("since", "")), r.GetInt("limit", 100))
		return jsonResult(chats, err)
	})

	s.AddTool(mcp.NewTool("read",
		mcp.WithDescription("Read messages in a chat from the index. Default is oldest→newest; set "+
			"desc=true for the NEWEST messages (recent context / 'what was just said') — without it, "+
			"limit returns the OLDEST messages, which after a history backfill are ancient. Use "+
			"before for backward pagination (page older than a ts)."),
		mcp.WithString("chat_id", mcp.Required(), mcp.Description("Canonical chat id, e.g. whatsapp:<jid>")),
		mcp.WithString("since", mcp.Description("RFC3339 inclusive lower ts bound")),
		mcp.WithString("before", mcp.Description("RFC3339 exclusive upper ts bound (backward-pagination cursor)")),
		mcp.WithBoolean("desc", mcp.Description("Newest-first: return the newest `limit` messages by ts (default false = oldest-first)")),
		mcp.WithNumber("limit", mcp.Description("Max messages (default 100)")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		chatID, err := r.RequireString("chat_id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		msgs, err := c.ReadWindow(ctx, chatID, ts(r.GetString("since", "")), ts(r.GetString("before", "")),
			r.GetInt("limit", 100), r.GetBool("desc", false))
		return jsonResult(msgs, err)
	})

	s.AddTool(mcp.NewTool("search",
		mcp.WithDescription("Search the message firehose, newest-first. Provide query (full-text), "+
			"person (whose messages — resolved by NAME to their identities across platforms), or both. "+
			"person ALONE lists that person's recent messages with no keyword needed — the way to answer "+
			"'what's X's latest / show me X's recent messages'. query alone is full-text across everyone."),
		mcp.WithString("query", mcp.Description("FTS query (optional if person is set)")),
		mcp.WithString("platform", mcp.Description("Filter: whatsapp|telegram|lark|instagram")),
		mcp.WithString("person", mcp.Description("Whose messages — a NAME (e.g. 'Alex Example'); resolved to their identities, then filtered by author. Works alone (no query) for 'X's latest'.")),
		mcp.WithString("since", mcp.Description("RFC3339 lower bound")),
		mcp.WithNumber("limit", mcp.Description("Max hits (default 50)")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		query := r.GetString("query", "")
		person := r.GetString("person", "")
		if query == "" && person == "" {
			return mcp.NewToolResultError("provide query and/or person"), nil
		}
		msgs, err := c.Search(ctx, query, r.GetString("platform", ""), person, ts(r.GetString("since", "")), r.GetInt("limit", 50))
		return jsonResult(msgs, err)
	})

	s.AddTool(mcp.NewTool("grep",
		mcp.WithDescription("Regex-search the message firehose (RE2 dialect), newest-first — the regex counterpart to `search`'s full-text. "+
			"Use it for patterns FTS can't express: invoice/order ids, phone/email/URL shapes, code snippets, structured tokens, exact substrings. "+
			"DIALECT: Go RE2 — character classes, anchors (^$\\b), alternation, quantifiers, groups, and INLINE FLAGS like (?i) case-insensitive and (?s) dotall ALL work. "+
			"NOT supported (RE2 has no): lookbehind/lookahead, backreferences — an invalid pattern returns a clear 'invalid regex' error, not a crash. "+
			"Optionally narrow with person (a NAME, resolved to their identities then author-filtered — same as search), platform, and since. "+
			"The result is {matches, scanned, truncated, hint}: TRUNCATED=true means the scan cap was hit BEFORE the whole history was searched, so older matches may exist — "+
			"do NOT treat it as exhaustive; narrow with person/platform/since or tighten the pattern (the hint says so)."),
		mcp.WithString("pattern", mcp.Required(), mcp.Description(`RE2 regex, e.g. 'INV-\d{4}-\d+', '(?i)urgent', or '\b\+?\d[\d -]{7,}\d\b' for phone-ish`)),
		mcp.WithString("platform", mcp.Description("Filter: whatsapp|telegram|lark|instagram|gmail")),
		mcp.WithString("person", mcp.Description("Whose messages — a NAME (e.g. 'Alex Example'); resolved to identities then author-filtered")),
		mcp.WithString("since", mcp.Description("RFC3339 lower bound")),
		mcp.WithNumber("limit", mcp.Description("Max matches (default 50)")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		pattern := r.GetString("pattern", "")
		if pattern == "" {
			return mcp.NewToolResultError("pattern required"), nil
		}
		res, err := c.Grep(ctx, pattern, r.GetString("platform", ""), r.GetString("person", ""), ts(r.GetString("since", "")), r.GetInt("limit", 50))
		return jsonResult(res, err)
	})

	s.AddTool(mcp.NewTool("query_sql",
		mcp.WithDescription("Run a READ-ONLY SQL SELECT over the message firehose (SQLite dialect) — for structured questions grep/search can't express: counts, GROUP BY, JOINs, time-bucketing, per-sender/per-chat aggregates, reply-graph. "+
			"HARD RULES: a SINGLE SELECT (or WITH … SELECT) only — no INSERT/UPDATE/DELETE/DROP/PRAGMA/ATTACH, no multiple statements (the connection is physically read-only). 5s timeout, 1000-row cap (result.truncated=true if hit — narrow with WHERE/LIMIT). "+
			"SCHEMA — table messages(id TEXT, platform TEXT [whatsapp|telegram|lark|instagram|gmail], account_id TEXT, chat_id TEXT, chat_type TEXT [dm|group], sender_id TEXT, sender_name TEXT, sender_handle TEXT, sender_phone TEXT, sender_email TEXT, is_self INT, ts TEXT [RFC3339], kind TEXT, text TEXT, reply_to TEXT, thread_id TEXT, mentions TEXT[json], mentions_self INT, deleted INT, edited_ts TEXT, source TEXT, body TEXT[full canonical JSON]); "+
			"table chats(chat_id TEXT, platform TEXT, chat_type TEXT, title TEXT, derived_name TEXT, last_ts TEXT, last_text TEXT, msg_count INT, importance TEXT [important|normal|low], archived INT, muted INT, pinned INT, labels TEXT[json], backfilled_at TEXT). "+
			"Result: {columns, rows (row-major, aligned to columns), truncated, elapsed_ms}. NOTE attribution is by sender_id (sender_name is often blank by design — resolve names via the resolve_person tool, not WHERE sender_name=)."),
		mcp.WithString("sql", mcp.Required(), mcp.Description("A single read-only SELECT/WITH statement, SQLite dialect")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		sql := r.GetString("sql", "")
		if sql == "" {
			return mcp.NewToolResultError("sql required"), nil
		}
		res, err := c.QuerySQL(ctx, sql)
		return jsonResult(res, err)
	})

	s.AddTool(mcp.NewTool("mentions",
		mcp.WithDescription("Every @-mention of the user across all platforms."),
		mcp.WithString("since", mcp.Description("RFC3339 lower bound")),
		mcp.WithNumber("limit", mcp.Description("Max (default 100)")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		msgs, err := c.Mentions(ctx, ts(r.GetString("since", "")), r.GetInt("limit", 100))
		return jsonResult(msgs, err)
	})

	s.AddTool(mcp.NewTool("send",
		mcp.WithDescription("Send a message to a chat or a person. To DM a person, pass an id from "+
			"resolve_person: their p2p chat_id (e.g. lark:oc_…) OR their open_id (lark:ou_…) — both route "+
			"to the 1:1 DM. Do NOT pass a group chat or a bare name. Text is sent as the user."),
		mcp.WithString("chat_id", mcp.Required(), mcp.Description("Target: a chat_id (lark:oc_…/whatsapp:…/instagram:…) or, for Lark, a person's open_id (lark:ou_…) → their DM")),
		mcp.WithString("text", mcp.Required()),
		mcp.WithString("reply_to", mcp.Description("Canonical event id to reply to")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		chatID, err := r.RequireString("chat_id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		text, err := r.RequireString("text")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		sentID, err := c.Send(ctx, api.SendBody{ChatID: chatID, Text: text, ReplyTo: r.GetString("reply_to", "")})
		return jsonResult(map[string]string{"sent_id": sentID}, err)
	})

	s.AddTool(mcp.NewTool("send_media",
		mcp.WithDescription("Send a captured attachment to a chat. media_id is from get_media/search/read attachments, e.g. \"whatsapp:ABC#0\". v1 re-uploads stored bytes as a document; mark_forwarded only adds WhatsApp forwarded metadata and is not a native original-message forward."),
		mcp.WithString("chat_id", mcp.Required()),
		mcp.WithString("media_id", mcp.Required()),
		mcp.WithString("text", mcp.Description("Optional caption")),
		mcp.WithString("reply_to", mcp.Description("Canonical event id to reply to")),
		mcp.WithString("filename", mcp.Description("Optional filename override")),
		mcp.WithString("mime", mcp.Description("Optional MIME override")),
		mcp.WithBoolean("as_document", mcp.Description("Send as a document (default true for v1)")),
		mcp.WithBoolean("mark_forwarded", mcp.Description("Set platform forwarded metadata when supported")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		chatID, err := r.RequireString("chat_id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		mediaID, err := r.RequireString("media_id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		sentID, err := c.SendMedia(ctx, api.SendMediaBody{
			ChatID:        chatID,
			MediaID:       mediaID,
			Text:          r.GetString("text", ""),
			ReplyTo:       r.GetString("reply_to", ""),
			Filename:      r.GetString("filename", ""),
			MIME:          r.GetString("mime", ""),
			AsDocument:    r.GetBool("as_document", true),
			MarkForwarded: r.GetBool("mark_forwarded", false),
		})
		return jsonResult(map[string]string{"sent_id": sentID}, err)
	})

	s.AddTool(mcp.NewTool("react",
		mcp.WithDescription("React to a message with an emoji."),
		mcp.WithString("chat_id", mcp.Required()),
		mcp.WithString("message_id", mcp.Required()),
		mcp.WithString("emoji", mcp.Required()),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		chatID, _ := r.RequireString("chat_id")
		msgID, _ := r.RequireString("message_id")
		emoji, err := r.RequireString("emoji")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		err = c.React(ctx, api.ReactBody{ChatID: chatID, MessageID: msgID, Emoji: emoji})
		return jsonResult(map[string]bool{"ok": err == nil}, err)
	})

	s.AddTool(mcp.NewTool("start_chat",
		mcp.WithDescription("Start a NEW conversation with a contact you've never messaged "+
			"(resolves a handle/username to a chat_id, creating the DM). Use this before `send` "+
			"when no existing chat exists. Returns {chat_id, resolved_name} — confirm resolved_name "+
			"is the right account, then `send`. NOTE (Instagram): a cold DM lands in the recipient's "+
			"message-requests until they accept, and new-contact DMs are rate-limited — send one, then observe."),
		mcp.WithString("platform", mcp.Required(), mcp.Description("e.g. instagram, messenger")),
		mcp.WithString("identifier", mcp.Required(), mcp.Description("a @username/handle or numeric id")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		platform, err := r.RequireString("platform")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		identifier, err := r.RequireString("identifier")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		res, err := c.StartChat(ctx, api.StartChatBody{Platform: platform, Identifier: identifier})
		return jsonResult(map[string]string{"chat_id": res.ChatID, "resolved_name": res.ResolvedName}, err)
	})

	s.AddTool(mcp.NewTool("ig_profile",
		mcp.WithDescription("Browse an Instagram profile as the user (READ-ONLY) — the structured card you'd "+
			"see visiting their page: name, bio, verified, public/private, follower/following/post counts, "+
			"MUTUAL-FOLLOW status (you_follow / follows_you / mutual), business + category, and recent posts "+
			"(caption, like count, link, time). Use to scope a person or vendor (e.g. a shop's bio/contact). "+
			"All traffic egresses the user's home IP via the residential proxy; nothing is written (no like/follow/DM)."),
		mcp.WithString("handle", mcp.Required(), mcp.Description("the @username (with or without @)")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		handle, err := r.RequireString("handle")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		card, err := c.IGProfile(ctx, handle)
		return jsonResult(card, err)
	})

	s.AddTool(mcp.NewTool("ig_stories",
		mcp.WithDescription("List who in the user's Instagram network has a LIVE story right now (READ-ONLY): "+
			"handle, name, when it was posted, media count, and whether the user has seen it. Stories expire in 24h. "+
			"Home-IP proxied; read-only."),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		res, err := c.IGStories(ctx)
		return jsonResult(res, err)
	})

	s.AddTool(mcp.NewTool("ig_feed",
		mcp.WithDescription("Read the user's Instagram HOME FEED — recent posts from people they follow (READ-ONLY): "+
			"author handle, caption, media type, like/comment counts, permalink, and timestamp. "+
			"limit (default 10) auto-paginates internally to return ~that many real posts (IG's first page is "+
			"short after ads/seen are filtered). Optional max_id resumes from a prior result's next_max_id. "+
			"Home-IP proxied; read-only."),
		mcp.WithNumber("limit", mcp.Description("How many real posts to return (auto-paginated, bounded). Default 10.")),
		mcp.WithString("max_id", mcp.Description("Pagination cursor from a prior result's next_max_id; omit for the first page.")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		maxID := r.GetString("max_id", "")
		limit := r.GetInt("limit", 10)
		res, err := c.IGFeed(ctx, maxID, limit)
		return jsonResult(res, err)
	})

	s.AddTool(mcp.NewTool("ig_following_feed",
		mcp.WithDescription("Recent posts from ONLY the accounts the user follows, newest-first, with ZERO "+
			"recommended/suggested posts or ads (READ-ONLY). This is the 'what have the people I follow "+
			"posted lately' feed — unlike ig_feed (the algorithmic home feed, which interleaves "+
			"recommendations). Returns the same cards: author handle, caption, media type, like/comment "+
			"counts, permalink, timestamp. Built by fanning out across the following list + merging by time; "+
			"results are briefly cached. Home-IP proxied; read-only."),
		mcp.WithNumber("limit", mcp.Description("How many posts to return, newest-first. Default 12.")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		res, err := c.IGFollowingFeed(ctx, r.GetInt("limit", 12))
		return jsonResult(res, err)
	})

	s.AddTool(mcp.NewTool("ig_following",
		mcp.WithDescription("The COMPLETE list of Instagram accounts the user follows (READ-ONLY), each enriched "+
			"with friend-vs-creator signals: handle, name, user_id, mutual-follow, follower/following counts, bio, "+
			"is_business, category. Use this (not ig_following_feed, which only shows recent posters) when you need "+
			"the full follow set — e.g. to classify which follows are friends vs creators/brands. Home-IP proxied; read-only."),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		res, err := c.IGFollowing(ctx)
		return jsonResult(res, err)
	})

	s.AddTool(mcp.NewTool("x_following_feed",
		mcp.WithDescription("The user's X.com (Twitter) chronological 'Following' timeline — recent posts from "+
			"ONLY the accounts they follow, newest-first, with promoted/ads dropped (READ-ONLY). Returns posts "+
			"as social_activity cards: author handle, display name, content, media, engagement (likes/retweets/"+
			"replies/quotes/views), permalink, timestamp. Home-IP proxied; read-only."),
		mcp.WithNumber("limit", mcp.Description("How many posts to return, newest-first. Default 20.")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		res, err := c.XFollowingFeed(ctx, r.GetInt("limit", 20))
		return jsonResult(res, err)
	})

	s.AddTool(mcp.NewTool("x_profile",
		mcp.WithDescription("A read-only X.com profile card for a handle — name, bio, follower/following/tweet "+
			"counts, verified flag, and the account's recent tweets (as social_activity cards). Home-IP proxied; read-only."),
		mcp.WithString("handle", mcp.Required(), mcp.Description("X handle, with or without @ (e.g. levelsio)")),
		mcp.WithNumber("limit", mcp.Description("How many recent tweets to include. Default 10.")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		handle, err := r.RequireString("handle")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		res, err := c.XProfile(ctx, handle, r.GetInt("limit", 10))
		return jsonResult(res, err)
	})

	s.AddTool(mcp.NewTool("tap_button",
		mcp.WithDescription("Tap an inline/card button on a message (platform-dependent)."),
		mcp.WithString("chat_id", mcp.Required()),
		mcp.WithString("message_id", mcp.Required()),
		mcp.WithString("button_id", mcp.Required()),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		chatID, _ := r.RequireString("chat_id")
		msgID, _ := r.RequireString("message_id")
		buttonID, err := r.RequireString("button_id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		result, err := c.TapButton(ctx, api.TapBody{ChatID: chatID, MessageID: msgID, ButtonID: buttonID})
		return jsonResult(map[string]string{"result": result}, err)
	})

	s.AddTool(mcp.NewTool("vote_poll",
		mcp.WithDescription("Vote on a poll option in a message (platform-dependent)."),
		mcp.WithString("chat_id", mcp.Required()),
		mcp.WithString("message_id", mcp.Required()),
		mcp.WithString("option", mcp.Required()),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		chatID, _ := r.RequireString("chat_id")
		msgID, _ := r.RequireString("message_id")
		option, err := r.RequireString("option")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		err = c.VotePoll(ctx, api.VoteBody{ChatID: chatID, MessageID: msgID, Option: option})
		return jsonResult(map[string]bool{"ok": err == nil}, err)
	})

	s.AddTool(mcp.NewTool("backfill",
		mcp.WithDescription("Drive history-sync for a chat (or 'all') through the pipeline."),
		mcp.WithString("chat_id", mcp.Required(), mcp.Description("Canonical chat id, or 'all'")),
		mcp.WithString("since", mcp.Description("RFC3339 lower bound")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		chatID, err := r.RequireString("chat_id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		err = c.Backfill(ctx, api.BackfillBody{ChatID: chatID, Since: ts(r.GetString("since", ""))})
		return jsonResult(map[string]bool{"ok": err == nil}, err)
	})

	// --- People review flow (human-in-the-loop persona mapping) ---
	// The agent surfaces these to the user ("Review my contacts"): list the
	// pending fuzzy-merge proposals, resolve each by id (approve→merge,
	// reject→never re-propose), and set a confirmed name on one canonical
	// person. Strong-signal merges already happen automatically and never appear
	// here.

	s.AddTool(mcp.NewTool("list_person_proposals",
		mcp.WithDescription("List a PAGE of pending people-merge proposals awaiting the user's review — fuzzy name matches the system won't auto-merge. There can be hundreds, so this returns a batch (default 15); present them to the user a batch at a time and use offset to page ('show me the next ones'). Each has a stable id plus the two canonical persons (a/b with a_name/b_name), a score and a reason. Resolve each with resolve_person_proposal."),
		mcp.WithNumber("limit", mcp.Description("Max proposals to return (default 15). Keep it small enough to present at once.")),
		mcp.WithNumber("offset", mcp.Description("Skip this many (for paging to the next batch; default 0)")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ps, err := c.PendingProposals(ctx, r.GetInt("limit", 15), r.GetInt("offset", 0))
		return jsonResult(ps, err)
	})

	s.AddTool(mcp.NewTool("resolve_person_proposal",
		mcp.WithDescription("Resolve one pending people-merge proposal by id. decision=\"approve\" merges the two canonical persons into one; decision=\"reject\" records they are NOT the same and never proposes the pair again."),
		mcp.WithString("id", mcp.Required(), mcp.Description("Proposal id from list_person_proposals")),
		mcp.WithString("decision", mcp.Required(), mcp.Description(`"approve" (same person, merge) or "reject" (not the same)`)),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id, err := r.RequireString("id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		decision, err := r.RequireString("decision")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		outcome, err := c.ResolveProposal(ctx, id, decision)
		if err != nil {
			return jsonResult(nil, err)
		}
		if outcome != nil {
			// approve → tell the agent the survivor (id+name) so it can confirm
			// "now shown as «name»" and rename the survivor by id.
			return jsonResult(map[string]any{"ok": true, "merged": true, "survivor": outcome}, nil)
		}
		return jsonResult(map[string]any{"ok": true, "merged": false}, nil)
	})

	s.AddTool(mcp.NewTool("get_media",
		mcp.WithDescription("Retrieve a captured attachment's bytes by media id (\"<event_id>#<attachment_index>\", e.g. \"whatsapp:ABC#0\"). Returns the local file path the bytes are stored at (same host) plus type/mime/name/size. If the media was evicted under the size cap, it is re-downloaded on demand. Only media from important chats is kept on disk."),
		mcp.WithString("id", mcp.Required(), mcp.Description("Media id: the event id + '#' + attachment index")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id, err := r.RequireString("id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		meta, err := c.MediaInfo(ctx, id)
		return jsonResult(meta, err)
	})

	s.AddTool(mcp.NewTool("set_person_name",
		mcp.WithDescription("Apply a user-confirmed display name to a single canonical person (correct or fill in a name). Independent of proposals — targets one canonical id."),
		mcp.WithString("canon_id", mcp.Required(), mcp.Description("Canonical person id to name")),
		mcp.WithString("name", mcp.Required(), mcp.Description("The confirmed display name")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		canonID, err := r.RequireString("canon_id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		name, err := r.RequireString("name")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		err = c.RenameCanonical(ctx, canonID, name)
		return jsonResult(map[string]bool{"ok": err == nil}, err)
	})

	// resolve_person: name → canonical identity card(s). The reliable way to
	// answer "who is X / how do I reach X" without grepping messages. Matching is
	// accent- and case-insensitive (the daemon folds diacritics, e.g. "Bình An Đỗ"
	// == "Binh An Do"), so the agent can pass the name exactly as written.
	s.AddTool(mcp.NewTool("resolve_person",
		mcp.WithDescription("Resolve a person by name to their canonical identity card(s) across all platforms — use this to answer 'who is <name>', 'what's <name>'s WhatsApp/Lark', 'how do I reach <name>' INSTEAD of searching messages. Matching is accent- and case-insensitive (e.g. 'Binh An Do' resolves 'Bình An Đỗ'). Returns {query, in_graph, hits:[...]}: in_graph=false means no one matched the people graph; hits are ranked best-first. Each card has the canonical id, name, the platforms + per-identity {platform,user_id,display_name,handle}, any emails/phones/handles, Lark open_ids, salience tier, and what the query matched on. When several people match an ambiguous name, present them so the user can pick."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Person's name, alias, or handle to resolve (pass it exactly as written — diacritics are folded automatically).")),
	), func(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		name, err := r.RequireString("name")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return jsonResult(c.ResolvePerson(ctx, name))
	})
}

func ts(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func jsonResult(v any, err error) (*mcp.CallToolResult, error) {
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal: %v", err)), nil
	}
	return mcp.NewToolResultText(string(b)), nil
}
