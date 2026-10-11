package idfeeds

import (
	"context"
	"database/sql"
	"sort"
	"time"

	"github.com/seahyc/switchboard-oss/internal/gbrainfeed"
)

// parseTS parses a stored RFC3339(Nano) timestamp, returning the zero time on
// failure (an unparseable/absent ts simply yields unknown-recency downstream).
func parseTS(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// IdentityFeeds is the store-backed implementation of the people-layer's
// PersonStatsProvider (#15 salience) AND IdentityContextProvider (#19 fuzzy
// matcher). Both are derived from the SQLite firehose with a small set of
// GROUP-BY passes keyed by (platform, sender_id == identity UserID), so the
// projector can roll several identities up into one canonical person.
//
// It is read-only over the store and runs off the ingest path (the projector
// pulls it on a background tick), so it never blocks live capture.
type IdentityFeeds struct {
	db *sql.DB
}

// NewIdentityFeeds builds the feeds over the shared store DB handle.
func NewIdentityFeeds(db *sql.DB) *IdentityFeeds { return &IdentityFeeds{db: db} }

// compile-time assertions: this satisfies both provider interfaces.
var (
	_ gbrainfeed.PersonStatsProvider     = (*IdentityFeeds)(nil)
	_ gbrainfeed.IdentityContextProvider = (*IdentityFeeds)(nil)
)

type idKey struct{ platform, sender string }

// PersonStats returns per-identity interaction aggregates for the salience layer.
// Derived from a few GROUP-BY passes:
//   - their own messages (count, dm/group split, mentions-of-you, last ts)
//   - your messages in chats where they participate (reciprocity numerator)
//   - whether you started the conversation (earliest message is yours)
//   - whether they replied to one of your messages
//   - DM curation flags (pinned/muted/archived) from the chats table
func (f *IdentityFeeds) PersonStats(ctx context.Context) ([]gbrainfeed.PersonStat, error) {
	acc := map[idKey]*gbrainfeed.PersonStat{}
	get := func(platform, sender string) *gbrainfeed.PersonStat {
		k := idKey{platform, sender}
		s := acc[k]
		if s == nil {
			s = &gbrainfeed.PersonStat{Platform: platform, SenderID: sender}
			acc[k] = s
		}
		return s
	}

	// 1. Their own messages: counts + dm/group split + mentions-of-you + last ts.
	rows, err := f.db.QueryContext(ctx, `
		SELECT platform, sender_id,
		       COUNT(*),
		       SUM(CASE WHEN chat_type='dm' THEN 1 ELSE 0 END),
		       SUM(CASE WHEN chat_type IN ('group','channel') THEN 1 ELSE 0 END),
		       SUM(mentions_self),
		       MAX(ts)
		FROM messages
		WHERE is_self=0 AND sender_id<>''
		GROUP BY platform, sender_id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var plat, sid, lastTS string
		var msg, dm, grp, ment int
		if err := rows.Scan(&plat, &sid, &msg, &dm, &grp, &ment, &lastTS); err != nil {
			rows.Close()
			return nil, err
		}
		s := get(plat, sid)
		s.MsgCount = msg
		s.DMMsgCount = dm
		s.GroupMsgCount = grp
		s.MentionsYou = ment
		s.LastTS = parseTS(lastTS)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 2. Your messages in chats where they participate (reciprocity numerator).
	if err := f.scanInto(ctx, `
		SELECT p.platform, p.sender_id, COALESCE(SUM(sc.self_cnt),0)
		FROM (SELECT DISTINCT platform, sender_id, chat_id FROM messages WHERE is_self=0 AND sender_id<>'') p
		JOIN (SELECT chat_id, COUNT(*) self_cnt FROM messages WHERE is_self=1 GROUP BY chat_id) sc
		  ON sc.chat_id = p.chat_id
		GROUP BY p.platform, p.sender_id`,
		func(plat, sid string, v int) { get(plat, sid).YourMsgCount = v }); err != nil {
		return nil, err
	}

	// 3. Conversations you started: in a chat they're in, the earliest message is yours.
	if err := f.scanInto(ctx, `
		WITH firsts AS (
			SELECT chat_id, is_self FROM (
				SELECT chat_id, is_self, ROW_NUMBER() OVER (PARTITION BY chat_id ORDER BY ts ASC, id ASC) rn
				FROM messages
			) WHERE rn=1
		)
		SELECT p.platform, p.sender_id, SUM(CASE WHEN fr.is_self=1 THEN 1 ELSE 0 END)
		FROM (SELECT DISTINCT platform, sender_id, chat_id FROM messages WHERE is_self=0 AND sender_id<>'') p
		JOIN firsts fr ON fr.chat_id = p.chat_id
		GROUP BY p.platform, p.sender_id`,
		func(plat, sid string, v int) { get(plat, sid).YouInitiated = v }); err != nil {
		return nil, err
	}

	// 4. They replied to one of your messages.
	if err := f.scanFlag(ctx, `
		SELECT DISTINCT m.platform, m.sender_id
		FROM messages m JOIN messages t ON t.id = m.reply_to
		WHERE m.is_self=0 AND m.sender_id<>'' AND t.is_self=1`,
		func(plat, sid string) { get(plat, sid).RepliedToYou = true }); err != nil {
		return nil, err
	}

	// 5. DM presence + curation flags. A DM chat has one non-self participant, so
	// the chat's pinned/muted/archived attribute to that identity.
	rows, err = f.db.QueryContext(ctx, `
		SELECT DISTINCT m.platform, m.sender_id, c.pinned, c.muted, c.archived
		FROM messages m JOIN chats c ON c.chat_id = m.chat_id
		WHERE m.is_self=0 AND m.sender_id<>'' AND c.chat_type='dm'`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var plat, sid string
		var pinned, muted, archived int
		if err := rows.Scan(&plat, &sid, &pinned, &muted, &archived); err != nil {
			rows.Close()
			return nil, err
		}
		s := get(plat, sid)
		s.HasDM = true
		s.DMPinned = s.DMPinned || pinned != 0
		s.DMMuted = s.DMMuted || muted != 0
		s.DMArchived = s.DMArchived || archived != 0
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]gbrainfeed.PersonStat, 0, len(acc))
	keys := make([]idKey, 0, len(acc))
	for k := range acc {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].platform != keys[j].platform {
			return keys[i].platform < keys[j].platform
		}
		return keys[i].sender < keys[j].sender
	})
	for _, k := range keys {
		out = append(out, *acc[k])
	}
	return out, nil
}

// snippetsPerIdentity is how many recent message excerpts to surface for the LLM.
const snippetsPerIdentity = 8

// IdentityContext returns per-identity context for the fuzzy matcher: handles,
// the group/channel chats they're in (shared-group blocking), and a few recent
// message snippets. Bio is left empty (the store carries none; the prompt is
// empty-bio-robust). DMs are excluded from ChatIDs since a DM is 1:1 and useless
// as a shared-group block.
func (f *IdentityFeeds) IdentityContext(ctx context.Context) ([]gbrainfeed.IdentityContext, error) {
	acc := map[idKey]*gbrainfeed.IdentityContext{}
	seenHandle := map[idKey]map[string]bool{}
	get := func(platform, sender string) *gbrainfeed.IdentityContext {
		k := idKey{platform, sender}
		c := acc[k]
		if c == nil {
			c = &gbrainfeed.IdentityContext{Platform: platform, SenderID: sender}
			acc[k] = c
			seenHandle[k] = map[string]bool{}
		}
		return c
	}

	// Handles.
	rows, err := f.db.QueryContext(ctx, `
		SELECT DISTINCT platform, sender_id, sender_handle
		FROM messages WHERE is_self=0 AND sender_id<>'' AND sender_handle<>''`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var plat, sid, h string
		if err := rows.Scan(&plat, &sid, &h); err != nil {
			rows.Close()
			return nil, err
		}
		c := get(plat, sid)
		k := idKey{plat, sid}
		if !seenHandle[k][h] {
			seenHandle[k][h] = true
			c.Handles = append(c.Handles, h)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Group/channel chats they participate in (shared-group blocking key).
	rows, err = f.db.QueryContext(ctx, `
		SELECT DISTINCT platform, sender_id, chat_id
		FROM messages
		WHERE is_self=0 AND sender_id<>'' AND chat_type IN ('group','channel')
		ORDER BY platform, sender_id, chat_id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var plat, sid, chatID string
		if err := rows.Scan(&plat, &sid, &chatID); err != nil {
			rows.Close()
			return nil, err
		}
		c := get(plat, sid)
		c.ChatIDs = append(c.ChatIDs, chatID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Recent message snippets (top-N by recency per identity) — the primary
	// corroborating signal when bio is absent.
	rows, err = f.db.QueryContext(ctx, `
		SELECT platform, sender_id, text FROM (
			SELECT platform, sender_id, text,
			       ROW_NUMBER() OVER (PARTITION BY platform, sender_id ORDER BY ts DESC, id DESC) rn
			FROM messages
			WHERE is_self=0 AND sender_id<>'' AND text<>'' AND kind='text'
		) WHERE rn <= ?
		ORDER BY platform, sender_id, rn`, snippetsPerIdentity)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var plat, sid, text string
		if err := rows.Scan(&plat, &sid, &text); err != nil {
			rows.Close()
			return nil, err
		}
		c := get(plat, sid)
		c.Snippets = append(c.Snippets, text)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]gbrainfeed.IdentityContext, 0, len(acc))
	keys := make([]idKey, 0, len(acc))
	for k := range acc {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].platform != keys[j].platform {
			return keys[i].platform < keys[j].platform
		}
		return keys[i].sender < keys[j].sender
	})
	for _, k := range keys {
		out = append(out, *acc[k])
	}
	return out, nil
}

// scanInto runs a (platform, sender_id, int) query and applies set to each row.
func (f *IdentityFeeds) scanInto(ctx context.Context, q string, set func(platform, sender string, v int)) error {
	rows, err := f.db.QueryContext(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var plat, sid string
		var v int
		if err := rows.Scan(&plat, &sid, &v); err != nil {
			return err
		}
		set(plat, sid, v)
	}
	return rows.Err()
}

// scanFlag runs a (platform, sender_id) query and applies set to each row.
func (f *IdentityFeeds) scanFlag(ctx context.Context, q string, set func(platform, sender string)) error {
	rows, err := f.db.QueryContext(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var plat, sid string
		if err := rows.Scan(&plat, &sid); err != nil {
			return err
		}
		set(plat, sid)
	}
	return rows.Err()
}
