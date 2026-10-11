package comprehend

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Reader is a READ-ONLY view over the canonical switchboard event store. It opens
// switchboard.db with mode=ro so a stray bug can never mutate the sacred capture
// spine, and runs alongside the live switchboardd writer (SQLite WAL supports
// concurrent readers).
type Reader struct {
	db *sql.DB
}

// OpenReader opens switchboard.db read-only. path ":memory:" is accepted for
// tests (opened read-write there so a test can seed rows on the same handle).
func OpenReader(path string) (*Reader, error) {
	dsn := path
	if path != ":memory:" {
		dsn = "file:" + path + "?mode=ro&_pragma=busy_timeout(5000)&_pragma=foreign_keys(0)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open switchboard store (ro): %w", err)
	}
	if path == ":memory:" {
		db.SetMaxOpenConns(1)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping switchboard store: %w", err)
	}
	return &Reader{db: db}, nil
}

// DB exposes the raw handle (tests seed the store through it).
func (r *Reader) DB() *sql.DB { return r.db }

// Close closes the underlying handle.
func (r *Reader) Close() error { return r.db.Close() }

// lastMsg is the newest non-deleted message in a chat plus the chat's metadata.
type lastMsg struct {
	chatID, id, text, sender string
	ts                       time.Time
	isSelf                   bool
	chatType                 string
	title, derived           string
	importance               string
	muted, archived          bool
}

const parseLayout = time.RFC3339Nano

func parseTS(s string) time.Time {
	t, _ := time.Parse(parseLayout, s)
	return t
}

// lastMessagePerChat returns the newest non-deleted message in every chat, joined
// with the chat's metadata. One row per chat.
func (r *Reader) lastMessagePerChat(ctx context.Context) ([]lastMsg, error) {
	const q = `
SELECT m.chat_id, m.id, m.ts, m.is_self, m.text, m.sender_name,
       COALESCE(c.chat_type,''), COALESCE(c.title,''), COALESCE(c.derived_name,''),
       COALESCE(c.importance,''), COALESCE(c.muted,0), COALESCE(c.archived,0)
FROM messages m
JOIN (SELECT chat_id, MAX(ts) AS mts FROM messages WHERE deleted=0 GROUP BY chat_id) L
  ON L.chat_id = m.chat_id AND m.ts = L.mts
LEFT JOIN chats c ON c.chat_id = m.chat_id
WHERE m.deleted = 0
GROUP BY m.chat_id`
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("lastMessagePerChat: %w", err)
	}
	defer rows.Close()
	var out []lastMsg
	for rows.Next() {
		var lm lastMsg
		var tsStr string
		var isSelf, muted, archived int
		if err := rows.Scan(&lm.chatID, &lm.id, &tsStr, &isSelf, &lm.text, &lm.sender,
			&lm.chatType, &lm.title, &lm.derived, &lm.importance, &muted, &archived); err != nil {
			return nil, err
		}
		lm.ts = parseTS(tsStr)
		lm.isSelf = isSelf != 0
		lm.muted = muted != 0
		lm.archived = archived != 0
		out = append(out, lm)
	}
	return out, rows.Err()
}

// lastSelfTSPerChat maps chat_id → ts of the most recent message the user sent.
func (r *Reader) lastSelfTSPerChat(ctx context.Context) (map[string]time.Time, error) {
	const q = `SELECT chat_id, MAX(ts) FROM messages WHERE is_self=1 AND deleted=0 GROUP BY chat_id`
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("lastSelfTSPerChat: %w", err)
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var chatID, tsStr string
		if err := rows.Scan(&chatID, &tsStr); err != nil {
			return nil, err
		}
		out[chatID] = parseTS(tsStr)
	}
	return out, rows.Err()
}

// latestInboundSenderPerChat maps chat_id → display name of the most recent
// non-self sender (the counterpart). Used to name a ghosting thread whose last
// message is the user's own.
func (r *Reader) latestInboundSenderPerChat(ctx context.Context) (map[string]string, error) {
	const q = `
SELECT m.chat_id, m.sender_name
FROM messages m
JOIN (SELECT chat_id, MAX(ts) AS mts FROM messages WHERE is_self=0 AND deleted=0 GROUP BY chat_id) L
  ON L.chat_id = m.chat_id AND m.ts = L.mts
WHERE m.is_self = 0
GROUP BY m.chat_id`
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("latestInboundSenderPerChat: %w", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var chatID, name string
		if err := rows.Scan(&chatID, &name); err != nil {
			return nil, err
		}
		out[chatID] = name
	}
	return out, rows.Err()
}

// chatsMentioningSelf returns the set of chat_ids that have at least one message
// @-mentioning the user (used to admit group threads into waiting-on-me).
func (r *Reader) chatsMentioningSelf(ctx context.Context) (map[string]bool, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT DISTINCT chat_id FROM messages WHERE mentions_self=1`)
	if err != nil {
		return nil, fmt.Errorf("chatsMentioningSelf: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var chatID string
		if err := rows.Scan(&chatID); err != nil {
			return nil, err
		}
		out[chatID] = true
	}
	return out, rows.Err()
}

// Candidate is a message admitted to the extraction funnel, with the context the
// extractor needs to resolve who/to_whom ("me" vs the counterpart).
type Candidate struct {
	ID          string
	ChatID      string
	Platform    string
	ChatType    string
	TS          time.Time
	Text        string
	Sender      string
	IsSelf      bool
	Importance  string
	Counterpart string // most recent inbound sender name in the chat (the "other side")
}

// CandidateMessages returns text messages eligible for commitment extraction:
// importance-gated (important|normal — never low), not muted/archived, non-empty
// text, strictly newer than `since`, oldest-first, capped at limit. This is the
// incremental funnel input — the existing importance tier is the gate, so bulk
// noise never reaches the (scarce) LLM.
func (r *Reader) CandidateMessages(ctx context.Context, since time.Time, limit int) ([]Candidate, error) {
	counterpart, err := r.latestInboundSenderPerChat(ctx)
	if err != nil {
		return nil, err
	}
	sinceStr := ""
	if !since.IsZero() {
		sinceStr = since.UTC().Format(parseLayout)
	}
	// Exclusion is pushed into SQL (not filtered post-LIMIT in Go) so a burst of
	// excluded messages — Mamba's own bot chatter, E2E test probes — never consumes
	// the batch budget or stalls the cursor: the watermark advances over them via
	// the genuine candidates the LIMIT then returns.
	args := []any{sinceStr}
	var exConds []string
	for chatID := range excludedChats() {
		exConds = append(exConds, "m.chat_id != ?")
		args = append(args, chatID)
	}
	if TestMarker != "" {
		exConds = append(exConds, "m.text NOT LIKE ?")
		args = append(args, "%"+TestMarker+"%")
	}
	exClause := ""
	if len(exConds) > 0 {
		exClause = " AND " + strings.Join(exConds, " AND ")
	}
	q := `
SELECT m.id, m.chat_id, m.platform, COALESCE(c.chat_type,''), m.ts, m.text, m.sender_name, m.is_self,
       COALESCE(c.importance,'')
FROM messages m
JOIN chats c ON c.chat_id = m.chat_id
WHERE m.deleted = 0 AND m.kind = 'text' AND TRIM(m.text) != ''
  AND m.ts > ?
  AND COALESCE(c.importance,'') IN ('important','normal')
  AND COALESCE(c.muted,0) = 0 AND COALESCE(c.archived,0) = 0` + exClause + `
ORDER BY m.ts ASC
LIMIT ?`
	args = append(args, limit)
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("CandidateMessages: %w", err)
	}
	defer rows.Close()
	var out []Candidate
	for rows.Next() {
		var cd Candidate
		var tsStr string
		var isSelf int
		if err := rows.Scan(&cd.ID, &cd.ChatID, &cd.Platform, &cd.ChatType, &tsStr, &cd.Text,
			&cd.Sender, &isSelf, &cd.Importance); err != nil {
			return nil, err
		}
		cd.TS = parseTS(tsStr)
		cd.IsSelf = isSelf != 0
		cd.Counterpart = counterpart[cd.ChatID]
		out = append(out, cd)
	}
	return out, rows.Err()
}

// inboundStreakStart returns the start of the current unanswered inbound streak in
// a chat: the earliest non-self message ts that is strictly newer than the user's
// last sent message (afterTS). If the user never sent in the chat, pass the zero
// time and it returns the earliest inbound ts.
func (r *Reader) inboundStreakStart(ctx context.Context, chatID string, afterTS time.Time) (time.Time, error) {
	after := ""
	if !afterTS.IsZero() {
		after = afterTS.UTC().Format(parseLayout)
	}
	var tsStr sql.NullString
	err := r.db.QueryRowContext(ctx,
		`SELECT MIN(ts) FROM messages WHERE chat_id=? AND is_self=0 AND deleted=0 AND ts > ?`,
		chatID, after).Scan(&tsStr)
	if err != nil {
		return time.Time{}, err
	}
	if !tsStr.Valid {
		return time.Time{}, nil
	}
	return parseTS(tsStr.String), nil
}

// ThreadMsg is one line of conversation context for draft generation.
type ThreadMsg struct {
	Sender string
	IsSelf bool
	Text   string
	TS     time.Time
}

// RecentThread returns the last `limit` text messages of a chat, oldest→newest,
// for draft-reply context. Excludes deleted/empty and the bot's own chatter.
func (r *Reader) RecentThread(ctx context.Context, chatID string, limit int) ([]ThreadMsg, error) {
	if limit <= 0 {
		limit = 8
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT sender_name, is_self, text, ts FROM messages
		 WHERE chat_id = ? AND deleted = 0 AND kind = 'text' AND TRIM(text) != ''
		 ORDER BY ts DESC LIMIT ?`, chatID, limit)
	if err != nil {
		return nil, fmt.Errorf("RecentThread: %w", err)
	}
	defer rows.Close()
	var rev []ThreadMsg
	for rows.Next() {
		var tm ThreadMsg
		var isSelf int
		var tsStr string
		if err := rows.Scan(&tm.Sender, &isSelf, &tm.Text, &tsStr); err != nil {
			return nil, err
		}
		tm.IsSelf = isSelf != 0
		tm.TS = parseTS(tsStr)
		rev = append(rev, tm)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// reverse to chronological order
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	return rev, nil
}

// MessageByID returns a stored message's text + ts by canonical id. Used by the
// due-date backfill to re-parse a commitment's source text.
func (r *Reader) MessageByID(ctx context.Context, id string) (text string, ts time.Time, ok bool, err error) {
	var tsStr string
	row := r.db.QueryRowContext(ctx, `SELECT text, ts FROM messages WHERE id = ?`, id)
	if err := row.Scan(&text, &tsStr); err != nil {
		if err == sql.ErrNoRows {
			return "", time.Time{}, false, nil
		}
		return "", time.Time{}, false, err
	}
	return text, parseTS(tsStr), true, nil
}
