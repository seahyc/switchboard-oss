// Package store is the SQLite+FTS5 projection over the canonical event log.
//
// It holds two things in one pure-Go SQLite database (modernc.org/sqlite, no cgo):
//
//  1. event_log  — the append-only spine. Every canonical event.Message lands
//     here as JSON, ordered by a monotonic seq. Idempotent on the global id, so
//     replays/overlaps never duplicate. This is the source of truth; everything
//     else is a projection rebuildable from it.
//  2. messages / messages_fts / chats / cursors — query projections derived from
//     the log: structured rows, full-text search, per-chat summaries, and the
//     per-chat watermark cursor that lets backfill and live meet without gaps.
//
// All writes go through Append, which is idempotent and updates the projections
// in the same transaction.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/privatefs"
)

// Store wraps the SQLite database.
type Store struct {
	db    *sql.DB
	lease *os.File
	// roDB is a dedicated READ-ONLY connection pool (mode=ro + query_only) for the
	// API lookups and background scoring snapshots. It cannot write and stays
	// separate from the serialized writer so ingest and interactive reads do not
	// queue behind expensive scans.
	// For :memory: (tests) it aliases db (a second :memory: conn would be a new empty
	// DB); the parse-guard still blocks writes there, and the mode=ro hard layer is
	// exercised by a file-backed test.
	roDB *sql.DB
}

// Open opens (creating if needed) the SQLite database at path and applies the
// schema. Use path ":memory:" for tests.
func Open(path string) (*Store, error) {
	var lease *os.File
	var err error
	opened := false
	if path != ":memory:" {
		lease, err = acquireLease(path)
		if err != nil {
			return nil, err
		}
	}
	defer func() {
		if !opened && lease != nil {
			lease.Close()
		}
	}()
	dsn := path
	if path != ":memory:" {
		// Pre-create database and SQLite sidecars as private leaf files. This
		// secures existing files too and rejects symlinks before SQLite opens them.
		// Do not reopen/close these files after SQLite starts: POSIX close can drop
		// SQLite's process-scoped locks and invalidate WAL coordination.
		for _, p := range []string{path, path + "-wal", path + "-shm"} {
			if err := privatefs.PrepareFile(p); err != nil {
				return nil, fmt.Errorf("secure sqlite file %q: %w", p, err)
			}
		}
		// Pragmas for a long-running, concurrent daemon: WAL + reasonable sync.
		dsn = path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=secure_delete(1)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// SQLite is single-writer. Serialize ALL access through ONE connection so writes
	// can never SQLITE_BUSY against each other — database/sql queues at the Go layer
	// instead. Without this, the file-backed pool opened multiple connections and a
	// write (e.g. the #13 backfill-sweep convergence marker) raced live-ingest writes
	// and failed with "database is locked (5)", silently breaking convergence. The
	// non-mutating API reads and expensive scoring scans use the read-only pool
	// instead, so neither waits for this writer connection. :memory: also needs a
	// single connection because each connection otherwise gets a separate database.
	db.SetMaxOpenConns(1)
	s := &Store{db: db, lease: lease}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	// Open the dedicated read-only pool AFTER migrate (so the file exists). mode=ro +
	// query_only make it physically unable to write — the hard guarantee behind the
	// #36 query_sql parse-guard. :memory: can't share across connections, so alias db.
	if path == ":memory:" {
		s.roDB = db
	} else {
		roDSN := "file:" + path + "?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(5000)"
		roDB, rerr := sql.Open("sqlite", roDSN)
		if rerr != nil {
			db.Close()
			return nil, fmt.Errorf("open sqlite (ro): %w", rerr)
		}
		s.roDB = roDB
	}
	opened = true
	return s, nil
}

// Close closes the underlying database.
func (s *Store) Close() error {
	if s.lease != nil {
		defer s.lease.Close()
	}
	if s.roDB != nil && s.roDB != s.db {
		s.roDB.Close()
	}
	return s.db.Close()
}

// DB exposes the raw handle (for advanced/other-package queries).
func (s *Store) DB() *sql.DB { return s.db }

const schema = `
CREATE TABLE IF NOT EXISTS event_log (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    id          TEXT NOT NULL UNIQUE,           -- canonical "<platform>:<msg_id>"
    platform    TEXT NOT NULL,
    chat_id     TEXT NOT NULL,
    ts          TEXT NOT NULL,                  -- RFC3339
    source      TEXT NOT NULL,                  -- live|backfill
    body        TEXT NOT NULL,                  -- full canonical event.Message JSON
    ingested_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_log_chat_ts ON event_log(chat_id, ts);

-- Structured projection for queries.
CREATE TABLE IF NOT EXISTS messages (
    id            TEXT PRIMARY KEY,
    platform      TEXT NOT NULL,
    account_id    TEXT NOT NULL,
    chat_id       TEXT NOT NULL,
    chat_type     TEXT NOT NULL,
    sender_id     TEXT NOT NULL,
    sender_name   TEXT NOT NULL,
    sender_handle TEXT NOT NULL,
    sender_phone  TEXT NOT NULL,
    sender_email  TEXT NOT NULL,
    is_self       INTEGER NOT NULL,
    ts            TEXT NOT NULL,
    kind          TEXT NOT NULL,
    text          TEXT NOT NULL,
    reply_to      TEXT NOT NULL,
    thread_id     TEXT NOT NULL,
    mentions      TEXT NOT NULL,                -- JSON array
    mentions_self INTEGER NOT NULL DEFAULT 0,
    deleted       INTEGER NOT NULL DEFAULT 0,
    edited_ts     TEXT,
    source        TEXT NOT NULL,
    body          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages(chat_id, ts);
CREATE INDEX IF NOT EXISTS idx_messages_platform_ts ON messages(platform, ts);
CREATE INDEX IF NOT EXISTS idx_messages_mentions_self ON messages(mentions_self, ts);
-- (kind, ts) so the social_activity / kind-filtered briefing read (#49) seeks straight
-- to its rows already in ts order, instead of full-scanning the whole firehose + sorting
-- (the GET /social/activity 16s regression: 56 matching rows hidden behind a 100k-row
-- table scan because no index led with kind, and none led with ts for the ORDER BY).
CREATE INDEX IF NOT EXISTS idx_messages_kind_ts ON messages(kind, ts);

-- Full-text index over message text.
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    text,
    id UNINDEXED,
    chat_id UNINDEXED,
    platform UNINDEXED,
    sender_name UNINDEXED,
    tokenize = 'unicode61'
);

-- Per-chat summary projection (for list_chats). Beyond activity, it carries
-- human-legible names + normalized org signals so the agent can prioritize
-- (archived/muted/pinned/labels/importance). title is a connector-supplied
-- display name (group subject / DM counterpart); derived_name is a fallback
-- learned from message senders when no explicit title is provided.
CREATE TABLE IF NOT EXISTS chats (
    chat_id      TEXT PRIMARY KEY,
    platform     TEXT NOT NULL,
    chat_type    TEXT NOT NULL,
    title        TEXT NOT NULL DEFAULT '',
    derived_name TEXT NOT NULL DEFAULT '',
    last_ts      TEXT NOT NULL,
    last_text    TEXT NOT NULL DEFAULT '',
    msg_count    INTEGER NOT NULL DEFAULT 0,
    archived     INTEGER NOT NULL DEFAULT 0,
    muted        INTEGER NOT NULL DEFAULT 0,
    muted_until  TEXT NOT NULL DEFAULT '',      -- RFC3339 mute expiry, if known
    pinned       INTEGER NOT NULL DEFAULT 0,
    labels       TEXT NOT NULL DEFAULT '[]',   -- JSON array of folder/label strings
    importance          TEXT NOT NULL DEFAULT '',  -- computed: important|normal|low
    importance_reason   TEXT NOT NULL DEFAULT '',  -- transparent: why it scored that tier
    importance_override TEXT NOT NULL DEFAULT ''    -- Tier-0 user override (sticky); set via #12
);
CREATE INDEX IF NOT EXISTS idx_chats_platform ON chats(platform, last_ts);

-- Per-chat watermark cursor: where backfill/live have reached.
CREATE TABLE IF NOT EXISTS cursors (
    chat_id      TEXT PRIMARY KEY,
    platform     TEXT NOT NULL,
    watermark_ts TEXT NOT NULL,                 -- newest ts ingested for the chat
    last_msg_id  TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

-- Small durable key/value scratch for connector/daemon cursors that aren't per-chat
-- (e.g. the #31 Lark p2p discovery seed-window offset, rotated across runs).
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

`

func (s *Store) migrate() error {
	if _, err := s.db.Exec(purgeSchema); err != nil {
		return fmt.Errorf("migrate purge: %w", err)
	}
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if _, err := s.db.Exec(lifecycleSchema); err != nil {
		return fmt.Errorf("migrate lifecycle: %w", err)
	}
	if _, err := s.db.Exec(mediaSchema); err != nil {
		return fmt.Errorf("migrate media: %w", err)
	}
	// Additive column migrations for databases created before the chat-metadata
	// fields existed (task #11). SQLite lacks ADD COLUMN IF NOT EXISTS, so add
	// only the columns that are missing.
	chatCols := map[string]string{
		"derived_name":        "TEXT NOT NULL DEFAULT ''",
		"archived":            "INTEGER NOT NULL DEFAULT 0",
		"muted":               "INTEGER NOT NULL DEFAULT 0",
		"muted_until":         "TEXT NOT NULL DEFAULT ''",
		"pinned":              "INTEGER NOT NULL DEFAULT 0",
		"labels":              "TEXT NOT NULL DEFAULT '[]'",
		"importance":          "TEXT NOT NULL DEFAULT ''",
		"importance_reason":   "TEXT NOT NULL DEFAULT ''",
		"importance_override": "TEXT NOT NULL DEFAULT ''",
		// #13 backfill-sweep convergence marker: set once a chat's full history has
		// been swept, so it drops out of the candidate set (deterministic convergence).
		"backfilled_at": "TEXT NOT NULL DEFAULT ''",
	}
	if err := s.addMissingColumns("chats", chatCols); err != nil {
		return err
	}
	return nil
}

// addMissingColumns ALTERs table to add any of cols not already present.
func (s *Store) addMissingColumns(table string, cols map[string]string) error {
	rows, err := s.db.Query("SELECT name FROM pragma_table_info(?)", table)
	if err != nil {
		return fmt.Errorf("table_info %s: %w", table, err)
	}
	existing := map[string]bool{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		existing[n] = true
	}
	rows.Close()
	for col, def := range cols {
		if existing[col] {
			continue
		}
		if _, err := s.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, col, def)); err != nil {
			return fmt.Errorf("add column %s.%s: %w", table, col, err)
		}
	}
	return nil
}

// AppendResult reports what Append did.
type AppendResult struct {
	Inserted bool // false if the id already existed (idempotent no-op)
}

// Append idempotently writes a canonical event to the log and updates all
// projections in a single transaction. Re-appending the same id is a no-op,
// so replays and backfill/live overlap never duplicate.
//
// selfIDs is the set of the user's own platform_user_ids (for computing
// mentions_self / "@me anywhere"); it may be nil.
func (s *Store) Append(ctx context.Context, m event.Message, selfIDs map[string]bool) (AppendResult, error) {
	m = event.NormalizeMutation(m)
	if m.TargetID != "" && m.ChatID == "" {
		return AppendResult{}, fmt.Errorf("append: mutation requires a resolved chat id")
	}
	if m.ID == "" {
		return AppendResult{}, fmt.Errorf("append: empty event id (use event.MakeID)")
	}
	body, err := json.Marshal(m)
	if err != nil {
		return AppendResult{}, fmt.Errorf("append: marshal: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AppendResult{}, err
	}
	defer tx.Rollback()
	var blocked int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM purged_chats WHERE chat_id=?`, m.ChatID).Scan(&blocked); err != nil {
		return AppendResult{}, err
	}
	if blocked != 0 {
		return AppendResult{}, ErrChatPurged
	}

	tsStr := m.TS.UTC().Format(time.RFC3339Nano)
	ingStr := m.IngestedAt.UTC().Format(time.RFC3339Nano)
	if m.IngestedAt.IsZero() {
		ingStr = tsStr
	}

	// 1. Append to the log, idempotent on id.
	res, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO event_log(id, platform, chat_id, ts, source, body, ingested_at)
		 VALUES (?,?,?,?,?,?,?)`,
		m.ID, m.Platform, m.ChatID, tsStr, m.Source, string(body), ingStr)
	if err != nil {
		return AppendResult{}, fmt.Errorf("append log: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		// Already ingested — idempotent no-op. Do not touch projections.
		if err := tx.Commit(); err != nil {
			return AppendResult{}, err
		}
		return AppendResult{Inserted: false}, nil
	}

	if err := projectLifecycle(ctx, tx, m, selfIDs); err != nil {
		return AppendResult{}, err
	}
	if err := bumpCursor(ctx, tx, m, tsStr); err != nil {
		return AppendResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return AppendResult{}, err
	}
	return AppendResult{Inserted: true}, nil
}

func projectMessage(ctx context.Context, tx *sql.Tx, m event.Message, body []byte, selfIDs map[string]bool) error {
	mentionsJSON, _ := json.Marshal(m.Mentions)
	mentionsSelf := 0
	for _, mid := range m.Mentions {
		if selfIDs[mid] {
			mentionsSelf = 1
			break
		}
	}
	var editedTS any
	if m.EditedTS != nil {
		editedTS = m.EditedTS.UTC().Format(time.RFC3339Nano)
	}
	_, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO messages(
			id, platform, account_id, chat_id, chat_type,
			sender_id, sender_name, sender_handle, sender_phone, sender_email, is_self,
			ts, kind, text, reply_to, thread_id, mentions, mentions_self, deleted, edited_ts, source, body)
		 VALUES (?,?,?,?,?, ?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?)`,
		m.ID, m.Platform, m.AccountID, m.ChatID, m.ChatType,
		m.Sender.PlatformUserID, m.Sender.DisplayName, m.Sender.Handle, m.Sender.Phone, m.Sender.Email, boolInt(m.Sender.IsSelf),
		m.TS.UTC().Format(time.RFC3339Nano), m.Kind, m.Text, m.ReplyTo, m.ThreadID, string(mentionsJSON), mentionsSelf, boolInt(m.Deleted), editedTS, m.Source, string(body))
	if err != nil {
		return fmt.Errorf("project message: %w", err)
	}
	// Keep FTS in sync (delete-then-insert covers edits/replaces).
	if _, err := tx.ExecContext(ctx, `DELETE FROM messages_fts WHERE id = ?`, m.ID); err != nil {
		return fmt.Errorf("fts delete: %w", err)
	}
	if m.Text != "" {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO messages_fts(text, id, chat_id, platform, sender_name) VALUES (?,?,?,?,?)`,
			m.Text, m.ID, m.ChatID, m.Platform, m.Sender.DisplayName); err != nil {
			return fmt.Errorf("fts insert: %w", err)
		}
	}
	return nil
}

func projectChat(ctx context.Context, tx *sql.Tx, m event.Message) error {
	tsStr := m.TS.UTC().Format(time.RFC3339Nano)
	// Learn a fallback display name: for a DM, the counterpart is the non-self
	// sender. Groups/channels get their name from connector-supplied metadata
	// (UpsertChatMeta), so derived_name stays empty for them here.
	derived := ""
	if m.ChatType == event.ChatDM && !m.Sender.IsSelf {
		derived = m.Sender.DisplayName
	}
	// Upsert chat summary; only advance last_ts/last_text when this message is
	// newer, and only fill derived_name when we learned a non-empty one.
	_, err := tx.ExecContext(ctx,
		`INSERT INTO chats(chat_id, platform, chat_type, derived_name, last_ts, last_text, msg_count)
		 VALUES (?,?,?,?,?,?,1)
		 ON CONFLICT(chat_id) DO UPDATE SET
		    msg_count    = chats.msg_count + 1,
		    chat_type    = excluded.chat_type,
		    derived_name = CASE WHEN excluded.derived_name != '' THEN excluded.derived_name ELSE chats.derived_name END,
		    last_ts      = CASE WHEN excluded.last_ts > chats.last_ts THEN excluded.last_ts ELSE chats.last_ts END,
		    last_text    = CASE WHEN excluded.last_ts > chats.last_ts THEN excluded.last_text ELSE chats.last_text END`,
		m.ChatID, m.Platform, m.ChatType, derived, tsStr, m.Text)
	if err != nil {
		return fmt.Errorf("project chat: %w", err)
	}
	return nil
}

// ChatMeta is connector-supplied, human-legible + organizational metadata for a
// chat. It is upserted independently of message ingestion (task #11). Title is
// the authoritative display name (group subject / DM counterpart); the org
// signals are normalized across platforms.
type ChatMeta struct {
	ChatID     string     `json:"chat_id"`
	Platform   string     `json:"platform"`
	Title      string     `json:"title"`
	Archived   bool       `json:"archived"`
	Muted      bool       `json:"muted"`
	MutedUntil *time.Time `json:"muted_until,omitempty"`
	Pinned     bool       `json:"pinned"`
	Labels     []string   `json:"labels"`
}

// UpsertChatMeta writes connector-supplied chat metadata, creating the chat row
// if it doesn't exist yet (metadata can arrive before the first message). It
// never touches activity fields (last_ts/last_text/msg_count) or derived_name.
func (s *Store) UpsertChatMeta(ctx context.Context, cm ChatMeta) error {
	if cm.ChatID == "" {
		return fmt.Errorf("UpsertChatMeta: empty chat_id")
	}
	var blocked int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM purged_chats WHERE chat_id=?`, cm.ChatID).Scan(&blocked); err != nil {
		return err
	}
	if blocked != 0 {
		return nil
	}
	if cm.Labels == nil {
		cm.Labels = []string{}
	}
	labelsJSON, _ := json.Marshal(cm.Labels)
	platform := cm.Platform
	if platform == "" {
		platform = platformPrefix(cm.ChatID)
	}
	mutedUntil := ""
	if cm.MutedUntil != nil && !cm.MutedUntil.IsZero() {
		mutedUntil = cm.MutedUntil.UTC().Format(time.RFC3339Nano)
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO chats(chat_id, platform, chat_type, title, last_ts, archived, muted, muted_until, pinned, labels)
		 VALUES (?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(chat_id) DO UPDATE SET
		    title       = CASE WHEN excluded.title != '' THEN excluded.title ELSE chats.title END,
		    archived    = excluded.archived,
		    muted       = excluded.muted,
		    muted_until = excluded.muted_until,
		    pinned      = excluded.pinned,
		    labels      = excluded.labels`,
		cm.ChatID, platform, "", cm.Title, "", boolInt(cm.Archived), boolInt(cm.Muted), mutedUntil, boolInt(cm.Pinned), string(labelsJSON))
	if err != nil {
		return fmt.Errorf("upsert chat meta: %w", err)
	}
	return nil
}

// SetImportanceOverride sets the Tier-0 sticky user override for a chat
// (important|low|"" to clear). Set via the #12 review flow; the scorer respects
// it above all other signals.
func (s *Store) SetImportanceOverride(ctx context.Context, chatID, override string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE chats SET importance_override = ? WHERE chat_id = ?`, override, chatID)
	return err
}

// ChatImportance returns a chat's computed importance tier (important|normal|
// low). Empty string means the chat isn't scored yet (or is unknown) — callers
// that gate on importance should treat "" as "defer", not "exclude". Used by the
// media worker's download gate.
func (s *Store) ChatImportance(ctx context.Context, chatID string) (string, error) {
	var imp string
	err := s.db.QueryRowContext(ctx, `SELECT importance FROM chats WHERE chat_id = ?`, chatID).Scan(&imp)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return imp, err
}

// RecomputeImportance recomputes the importance tier (important|normal|low) for
// every chat from a uniform, deterministic, LLM-free scorer, and stores the tier
//   - a human-readable reason. Inputs are normalized across platforms:
//     Tier 0  user override (sticky) — trumps all
//     Tier 1  curation: pinned (+), muted (−), archived (−)
//     Tier 2  engagement: does the user send here? (self-sent count)
//     Tier 3  structural: dm/group > channel; @mentions of the user (+)
//
// (Tier-3 known-person boost is added once the #12 people graph exists.)
func (s *Store) RecomputeImportance(ctx context.Context) (int, error) {
	rows, err := s.roDB.QueryContext(ctx, `
		SELECT c.chat_id, c.chat_type, c.pinned, c.muted, c.archived, c.importance_override,
		       c.msg_count, c.last_ts,
		       (SELECT COUNT(*) FROM messages m WHERE m.chat_id=c.chat_id AND m.is_self=1),
		       (SELECT COUNT(*) FROM messages m WHERE m.chat_id=c.chat_id AND m.is_self=0),
		       (SELECT COUNT(*) FROM messages m WHERE m.chat_id=c.chat_id AND m.mentions_self=1)
		FROM chats c`)
	if err != nil {
		return 0, err
	}
	type row struct {
		chatID, chatType, override     string
		pinned, muted, archived        int
		msgCount                       int
		lastTS                         string
		selfSent, othersSent, mentions int
	}
	var rs []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.chatID, &r.chatType, &r.pinned, &r.muted, &r.archived, &r.override,
			&r.msgCount, &r.lastTS, &r.selfSent, &r.othersSent, &r.mentions); err != nil {
			rows.Close()
			return 0, err
		}
		rs = append(rs, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	now := time.Now().UTC()
	n := 0
	for _, r := range rs {
		ageDays := -1 // unknown (no/blank last_ts) → neutral, don't penalize missing data
		if t, perr := time.Parse(time.RFC3339Nano, r.lastTS); perr == nil {
			ageDays = int(now.Sub(t.UTC()).Hours() / 24)
		}
		tier, reason := scoreImportance(r.chatType, r.pinned != 0, r.muted != 0, r.archived != 0, r.selfSent, r.othersSent, r.mentions, r.msgCount, ageDays, r.override)
		if _, err := s.db.ExecContext(ctx,
			`UPDATE chats SET importance = ?, importance_reason = ? WHERE chat_id = ?`,
			tier, reason, r.chatID); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// scoreImportance is the pure, deterministic scorer (testable in isolation). Beyond
// curation (pinned/muted/archived) + engagement (self-sent/mentions) + structure
// (dm/group/channel), it weighs REAL activity signals so a genuine chat isn't dumped to
// 'low' just because the user rarely posts there (#15 fix — the sweep exposed real
// people landing in 'low'): RECIPROCITY (both sides talk = a real conversation, not a
// broadcast), VOLUME (msgCount), and RECENCY (ageDays; -1 = unknown/no messages →
// neutral, so a not-yet-backfilled shell chat isn't penalized for missing data).
func scoreImportance(chatType string, pinned, muted, archived bool, selfSent, othersSent, mentions, msgCount, ageDays int, override string) (tier, reason string) {
	if override == "important" || override == "low" {
		return override, "user override"
	}
	score := 0
	var why []string
	if pinned {
		score += 3
		why = append(why, "pinned")
	}
	if mentions > 0 {
		score += 2
		why = append(why, "mentions you")
	}
	// Engagement: a two-way conversation (you AND they post) is the strongest "real
	// relationship" signal — it rescues real chats the old self-sent-only scorer missed.
	switch {
	case selfSent > 0 && othersSent > 0:
		score += 3
		why = append(why, "two-way conversation")
		if selfSent >= 5 {
			score++
		}
	case selfSent > 0:
		score += 2
		why = append(why, "you reply here")
	}
	// Volume: a high-traffic chat matters even if the user mostly lurks.
	switch {
	case msgCount >= 200:
		score += 2
		why = append(why, "high volume")
	case msgCount >= 30:
		score++
		why = append(why, "active")
	}
	// Recency: recent activity boosts; long-dead chats sink (but unknown age is neutral).
	switch {
	case ageDays >= 0 && ageDays <= 45:
		score++
		why = append(why, "recent")
	case ageDays > 365:
		score--
		why = append(why, "stale")
	}
	switch chatType {
	case "dm":
		score += 2
		why = append(why, "DM")
	case "channel":
		score -= 2
		why = append(why, "channel")
	}
	if muted {
		score -= 3
		why = append(why, "muted")
	}
	if archived {
		score -= 2
		why = append(why, "archived")
	}
	switch {
	case score >= 3:
		tier = "important"
	case score <= -2:
		tier = "low"
	default:
		tier = "normal"
	}
	if len(why) == 0 {
		why = append(why, "no strong signals")
	}
	return tier, strings.Join(why, ", ")
}

func platformPrefix(chatID string) string {
	if i := indexByte(chatID, ':'); i >= 0 {
		return chatID[:i]
	}
	return ""
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func bumpCursor(ctx context.Context, tx *sql.Tx, m event.Message, tsStr string) error {
	now := tsStr // watermark advances to the newest ingested ts
	_, err := tx.ExecContext(ctx,
		`INSERT INTO cursors(chat_id, platform, watermark_ts, last_msg_id, updated_at)
		 VALUES (?,?,?,?,?)
		 ON CONFLICT(chat_id) DO UPDATE SET
		    watermark_ts = CASE WHEN excluded.watermark_ts > cursors.watermark_ts THEN excluded.watermark_ts ELSE cursors.watermark_ts END,
		    last_msg_id  = CASE WHEN excluded.watermark_ts > cursors.watermark_ts THEN excluded.last_msg_id ELSE cursors.last_msg_id END,
		    updated_at   = excluded.updated_at`,
		m.ChatID, m.Platform, tsStr, m.ID, now)
	if err != nil {
		return fmt.Errorf("bump cursor: %w", err)
	}
	return nil
}

// SenderInfo is a distinct message sender (for re-projecting the gbrain
// identity registry from the durable store after a projection-queue drop).
type SenderInfo struct {
	Platform    string
	UserID      string
	DisplayName string
	Handle      string
	Phone       string
	Email       string
	IsSelf      bool
}

// DistinctSenders returns one row per distinct (platform, sender) across all
// stored messages — the full identity set, recoverable independent of the
// (lossy-under-burst) live projection queue.
func (s *Store) DistinctSenders(ctx context.Context) ([]SenderInfo, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT platform, sender_id,
		        MAX(sender_name), MAX(sender_handle), MAX(sender_phone), MAX(sender_email), MAX(is_self)
		 FROM messages WHERE sender_id != ''
		 GROUP BY platform, sender_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SenderInfo
	for rows.Next() {
		var si SenderInfo
		var isSelf int
		if err := rows.Scan(&si.Platform, &si.UserID, &si.DisplayName, &si.Handle, &si.Phone, &si.Email, &isSelf); err != nil {
			return nil, err
		}
		si.IsSelf = isSelf != 0
		out = append(out, si)
	}
	return out, rows.Err()
}

// MessagesWithAttachments returns logged events that carry at least one
// attachment (decoded from the canonical body), NEWEST FIRST so the media worker
// fills its recency budget with the most-recent media and never downloads media
// it would immediately evict. Used to retro-enqueue the historical backlog when
// the media store is first enabled (ingest only registers go-forward media). If
// chatID is non-empty the scan is scoped to that chat (bounded rollout). The
// LIKE pre-filter keeps this to the media-bearing subset.
func (s *Store) MessagesWithAttachments(ctx context.Context, chatID string) ([]event.Message, error) {
	q := `SELECT body FROM event_log WHERE body LIKE '%"attachments":[{%'`
	var args []any
	if chatID != "" {
		q += ` AND chat_id = ?`
		args = append(args, chatID)
	}
	q += ` ORDER BY ts DESC`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []event.Message
	for rows.Next() {
		var body string
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var m event.Message
		if json.Unmarshal([]byte(body), &m) != nil {
			continue
		}
		if len(m.Attachments) > 0 {
			out = append(out, m)
		}
	}
	return out, rows.Err()
}

// OldestMessage returns the earliest stored message for a chat — the anchor an
// AnchoredBackfiller (WhatsApp) needs to page further back than the push window.
func (s *Store) OldestMessage(ctx context.Context, chatID string) (id string, isSelf bool, ts time.Time, ok bool, err error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, is_self, ts FROM messages WHERE chat_id = ? ORDER BY ts ASC LIMIT 1`, chatID)
	var tsStr string
	var self int
	if err := row.Scan(&id, &self, &tsStr); err != nil {
		if err == sql.ErrNoRows {
			return "", false, time.Time{}, false, nil
		}
		return "", false, time.Time{}, false, err
	}
	ts, _ = time.Parse(time.RFC3339Nano, tsStr)
	return id, self != 0, ts, true, nil
}

// MessageRef returns the chat_id and sender platform_user_id for a stored
// message id. Used to (a) resolve a reaction/edit/delete event whose own
// chat_id is empty by joining on the target message, and (b) recover the target
// participant for platform reactions that need the original sender.
// RawByEventID returns the untouched connector raw payload (event.Message.Raw) for a
// logged event — used to re-derive media download refs from history without re-hitting
// the platform for metadata (#48 ciphertext recovery).
func (s *Store) RawByEventID(ctx context.Context, eventID string) ([]byte, error) {
	var body string
	if err := s.db.QueryRowContext(ctx, `SELECT body FROM event_log WHERE id = ?`, eventID).Scan(&body); err != nil {
		return nil, err
	}
	var m event.Message
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		return nil, err
	}
	return []byte(m.Raw), nil
}

func (s *Store) MessageRef(ctx context.Context, msgID string) (chatID, senderID string, ok bool, err error) {
	row := s.roDB.QueryRowContext(ctx, `SELECT chat_id, sender_id FROM messages WHERE id = ?`, msgID)
	if err := row.Scan(&chatID, &senderID); err != nil {
		if err == sql.ErrNoRows {
			return "", "", false, nil
		}
		return "", "", false, err
	}
	return chatID, senderID, true, nil
}

// Cursor returns the per-chat watermark, or zero time if none yet.
func (s *Store) Cursor(ctx context.Context, chatID string) (watermark time.Time, lastMsgID string, err error) {
	row := s.roDB.QueryRowContext(ctx, `SELECT watermark_ts, last_msg_id FROM cursors WHERE chat_id = ?`, chatID)
	var ws string
	if err := row.Scan(&ws, &lastMsgID); err != nil {
		if err == sql.ErrNoRows {
			return time.Time{}, "", nil
		}
		return time.Time{}, "", err
	}
	watermark, _ = time.Parse(time.RFC3339Nano, ws)
	return watermark, lastMsgID, nil
}

// --- Query API (backs the MCP tools) ---

// ChatSummary is a row for list_chats. Title is the effective human-legible
// name (connector title → derived counterpart name → chat_id fallback).
type ChatSummary struct {
	ChatID           string    `json:"chat_id"`
	Platform         string    `json:"platform"`
	ChatType         string    `json:"chat_type"`
	Title            string    `json:"title"`
	LastTS           time.Time `json:"last_ts"`
	LastText         string    `json:"last_text"`
	MsgCount         int       `json:"msg_count"`
	Archived         bool      `json:"archived"`
	Muted            bool      `json:"muted"`
	Pinned           bool      `json:"pinned"`
	Labels           []string  `json:"labels"`
	Importance       string    `json:"importance,omitempty"`        // important|normal|low
	ImportanceReason string    `json:"importance_reason,omitempty"` // why it scored that tier
}

// ListChats returns chats from the index, newest activity first. platform=="" for all.
func (s *Store) ListChats(ctx context.Context, platform string, since time.Time, limit int) ([]ChatSummary, error) {
	q := `SELECT chat_id, platform, chat_type, title, derived_name, last_ts, last_text, msg_count,
	             archived, muted, pinned, labels, importance, importance_reason FROM chats`
	var conds []string
	var args []any
	if platform != "" {
		conds = append(conds, "platform = ?")
		args = append(args, platform)
	}
	if !since.IsZero() {
		conds = append(conds, "last_ts >= ?")
		args = append(args, since.UTC().Format(time.RFC3339Nano))
	}
	if len(conds) > 0 {
		q += " WHERE " + strings.Join(conds, " AND ")
	}
	// Pinned first, then most-recent activity — matches how a human triages.
	q += " ORDER BY pinned DESC, last_ts DESC"
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := s.roDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChatSummary
	for rows.Next() {
		var c ChatSummary
		var lastTS, title, derived, labels string
		var archived, muted, pinned int
		if err := rows.Scan(&c.ChatID, &c.Platform, &c.ChatType, &title, &derived, &lastTS, &c.LastText, &c.MsgCount,
			&archived, &muted, &pinned, &labels, &c.Importance, &c.ImportanceReason); err != nil {
			return nil, err
		}
		c.LastTS, _ = time.Parse(time.RFC3339Nano, lastTS)
		// Effective display name: connector title → derived counterpart → id.
		c.Title = title
		if c.Title == "" {
			c.Title = derived
		}
		if c.Title == "" {
			c.Title = c.ChatID
		}
		c.Archived, c.Muted, c.Pinned = archived != 0, muted != 0, pinned != 0
		c.Labels = []string{}
		_ = json.Unmarshal([]byte(labels), &c.Labels)
		out = append(out, c)
	}
	return out, rows.Err()
}

// Read returns messages for a chat, oldest→newest, since the given time.
func (s *Store) Read(ctx context.Context, chatID string, since time.Time, limit int) ([]event.Message, error) {
	return s.ReadWindow(ctx, chatID, ReadOpts{Since: since, Limit: limit})
}

// MessageByID is a single-message point lookup by canonical id (the PK) — no window scan
// (#53). Lets the send-gate reply-quote resolver fetch the exact quoted message however
// far back it is, instead of falling back when it's outside the recent /read window.
// ok=false if no such id.
func (s *Store) MessageByID(ctx context.Context, id string) (event.Message, bool, error) {
	var body string
	err := s.roDB.QueryRowContext(ctx, `SELECT body FROM messages WHERE id = ?`, id).Scan(&body)
	if err == sql.ErrNoRows {
		return event.Message{}, false, nil
	}
	if err != nil {
		return event.Message{}, false, err
	}
	var m event.Message
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		return event.Message{}, false, err
	}
	return m, true, nil
}

// BlankSenderIDs returns the distinct sender_ids on a platform whose messages have a
// blank sender_name — the work-list for the #54 people-graph name backfill (people-layer
// resolves each id → name → SetSenderNameBySender). Skips empty sender_ids.
func (s *Store) BlankSenderIDs(ctx context.Context, platform string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT sender_id FROM messages
		   WHERE platform = ? AND sender_id != '' AND (sender_name = '' OR sender_name IS NULL)`, platform)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// SetSenderNameBySender fills sender_name/sender_handle for ALL of a sender's messages
// that are still blank (#54 backfill writer). Blank-guarded → idempotent + never clobbers
// a real name already present (e.g. from the forward ingest fix). Bulk-by-sender (one call
// per distinct id, not per message). Returns rows updated.
func (s *Store) SetSenderNameBySender(ctx context.Context, platform, senderID, name, handle string) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE messages SET sender_name = ?, sender_handle = ?
		   WHERE platform = ? AND sender_id = ? AND (sender_name = '' OR sender_name IS NULL)`,
		name, handle, platform, senderID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ReadOpts parameterizes ReadWindow. Since (inclusive lower ts bound) and Before
// (exclusive upper ts bound) define the window; Desc flips ordering to newest-first.
// All ordering is strictly by message ts — NOT rowid/ingestion order — so a recent
// backfill of OLD history sorts by its real timestamp, not by when it was ingested (#45).
type ReadOpts struct {
	Since  time.Time // ts >= Since (zero = no lower bound)
	Before time.Time // ts <  Before (zero = no upper bound) — the backward-pagination cursor
	Limit  int
	Desc   bool // true → newest-first (ts DESC); false → oldest-first (ts ASC, default/back-compat)
}

// ReadWindow returns a chat's messages within [Since, Before) ordered by ts, newest-first
// when Desc. The default (Desc=false, no Before) is the historical oldest-first behavior,
// so existing callers are unchanged. Desc=true + Limit=N is the clean "newest N" call for
// recent-context consumers (send-gate thread lookback, morning briefing) — oldest-first +
// limit returned the OLDEST N, which surfaced ancient/backfilled messages as the head (#45).
func (s *Store) ReadWindow(ctx context.Context, chatID string, o ReadOpts) ([]event.Message, error) {
	q := `SELECT body FROM messages WHERE chat_id = ?`
	args := []any{chatID}
	if !o.Since.IsZero() {
		q += " AND ts >= ?"
		args = append(args, o.Since.UTC().Format(time.RFC3339Nano))
	}
	if !o.Before.IsZero() {
		q += " AND ts < ?"
		args = append(args, o.Before.UTC().Format(time.RFC3339Nano))
	}
	if o.Desc {
		q += " ORDER BY ts DESC"
	} else {
		q += " ORDER BY ts ASC"
	}
	if o.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", o.Limit)
	}
	return s.scanMessages(ctx, q, args...)
}

// Search runs FTS over the firehose. platform/person optional filters; person
// matches sender_name. Returns newest-first.
// searchDefaultLimit / searchHardMaxLimit bound the result set so an FTS MATCH on a
// COMMON token (e.g. "the") can never trigger an unbounded ORDER BY ts sort over the
// whole match set — limit<=0 (incl. an explicit ?limit=0) is a footgun that drops the
// LIMIT clause entirely. We always emit a bounded LIMIT (#39 latency hardening).
const (
	searchDefaultLimit = 50
	searchHardMaxLimit = 1000
)

// clampSearchLimit normalizes a caller-supplied limit into [1, searchHardMaxLimit],
// defaulting <=0 to searchDefaultLimit, so every FTS query carries a bounded LIMIT.
func clampSearchLimit(limit int) int {
	if limit <= 0 {
		return searchDefaultLimit
	}
	if limit > searchHardMaxLimit {
		return searchHardMaxLimit
	}
	return limit
}

// ftsWord matches any Unicode letter or digit — used to drop pure-punctuation
// tokens that would tokenize to nothing inside an FTS5 phrase.
var ftsWord = regexp.MustCompile(`[\p{L}\p{N}]`)

// ftsQuote turns an arbitrary user/agent search string into a syntactically safe
// FTS5 MATCH query. Each whitespace-separated token is wrapped as a quoted phrase
// (internal " doubled) and the tokens are AND'd (space-joined). This makes
// punctuation (@ . ? : -), FTS5 operators (AND/OR/NOT/NEAR) and column filters
// (foo:bar) LITERAL, so a raw query like "user@x.com from:acct?" can no longer
// throw `fts5: syntax error`. Pure-punctuation tokens are dropped. Returns ""
// when nothing usable remains — callers then skip the MATCH clause entirely.
func ftsQuote(query string) string {
	quoted := make([]string, 0, 8)
	for _, f := range strings.Fields(query) {
		if !ftsWord.MatchString(f) {
			continue // pure punctuation — skip
		}
		quoted = append(quoted, `"`+strings.ReplaceAll(f, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, " ")
}

func (s *Store) Search(ctx context.Context, query, platform, person string, since time.Time, limit int) ([]event.Message, error) {
	limit = clampSearchLimit(limit)
	fts := ftsQuote(query)
	var q string
	var args []any
	if fts != "" {
		q = `SELECT m.body FROM messages_fts f JOIN messages m ON m.id = f.id WHERE messages_fts MATCH ?`
		args = append(args, fts)
	} else {
		// No usable search term (empty or punctuation-only) — don't require an FTS
		// MATCH; fall back to a plain scan filtered by the other predicates.
		q = `SELECT m.body FROM messages m WHERE 1=1`
	}
	if platform != "" {
		q += " AND m.platform = ?"
		args = append(args, platform)
	}
	if person != "" {
		q += " AND m.sender_name LIKE ?"
		args = append(args, "%"+person+"%")
	}
	if !since.IsZero() {
		q += " AND m.ts >= ?"
		args = append(args, since.UTC().Format(time.RFC3339Nano))
	}
	q += " ORDER BY m.ts DESC"
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	return s.scanMessages(ctx, q, args...)
}

// SocialActivity returns recent KindSocialActivity feed events (#49), newest-first —
// the briefing's "what friends are up to" source. The friend-vs-creator / tier filter is
// applied by the CALLER joining each event's author (sender_id) to its canonical node via
// the people-graph resolver; this just returns the raw feed events (optionally since a ts).
// subs optionally restricts to specific Social.Sub values (e.g. "post","story"
// for the "what friends are up to" briefing, excluding identity-only "follow"
// rows). Empty subs → all social_activity events. Sub-filtering uses json_extract
// over the event body since Sub lives in the JSON payload, not a column; the
// (kind, ts) index still drives the kind-filter + newest-first ordering.
// platform optionally restricts to one platform (e.g. "x", "instagram"); empty = all.
func (s *Store) SocialActivity(ctx context.Context, platform string, since time.Time, limit int, subs ...string) ([]event.Message, error) {
	q := `SELECT body FROM messages WHERE kind = ?`
	args := []any{event.KindSocialActivity}
	if platform != "" {
		q += " AND platform = ?"
		args = append(args, platform)
	}
	if !since.IsZero() {
		q += " AND ts >= ?"
		args = append(args, since.UTC().Format(time.RFC3339Nano))
	}
	if len(subs) > 0 {
		ph := make([]string, len(subs))
		for i, sub := range subs {
			ph[i] = "?"
			args = append(args, sub)
		}
		q += " AND json_extract(body, '$.social.sub') IN (" + strings.Join(ph, ",") + ")"
	}
	q += " ORDER BY ts DESC"
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	return s.scanMessages(ctx, q, args...)
}

// SenderRef is a (platform, sender_id) message-author key — the resolved
// identity set a person maps to. sender_id is the canonical per-platform author
// id stored on each message (lark open_id, whatsapp jid, telegram id).
type SenderRef struct {
	Platform string
	SenderID string
}

// SearchBySender runs FTS over the firehose filtered to messages authored by ANY
// of the given (platform, sender_id) pairs — the #32 "search person=<name>" path,
// where <name> is first resolved to its identity set (sender_ids) rather than
// matched against the by-design-blank sender_name. Empty senders → no rows (the
// person resolved to nobody). Returns newest-first.
func (s *Store) SearchBySender(ctx context.Context, query, platform string, senders []SenderRef, since time.Time, limit int) ([]event.Message, error) {
	if len(senders) == 0 {
		return nil, nil
	}
	limit = clampSearchLimit(limit)
	fts := ftsQuote(query)
	var q string
	var args []any
	if fts != "" {
		q = `SELECT m.body FROM messages_fts f JOIN messages m ON m.id = f.id WHERE messages_fts MATCH ?`
		args = append(args, fts)
	} else {
		// Person-only ("Casey's latest?", no keyword): author-filter the messages
		// directly — no FTS MATCH to require. This is the dominant query, so it must
		// not need a search term.
		q = `SELECT m.body FROM messages m WHERE 1=1`
	}
	if platform != "" {
		q += " AND m.platform = ?"
		args = append(args, platform)
	}
	// (m.platform, m.sender_id) IN <set>, expressed as OR'd pairs (a person has a
	// handful of platform identities, so this stays small).
	clauses := make([]string, 0, len(senders))
	for _, sr := range senders {
		clauses = append(clauses, "(m.platform = ? AND m.sender_id = ?)")
		args = append(args, sr.Platform, sr.SenderID)
	}
	q += " AND (" + strings.Join(clauses, " OR ") + ")"
	if !since.IsZero() {
		q += " AND m.ts >= ?"
		args = append(args, since.UTC().Format(time.RFC3339Nano))
	}
	q += " ORDER BY m.ts DESC"
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	return s.scanMessages(ctx, q, args...)
}

// PlatformCoverage is one connector's ingestion-coverage row for the #13 scorecard:
// how many chats we KNOW about (chat index) vs how many actually have messages
// (backfilled), the message count, and the history depth (oldest→newest ts).
// ChatsEmpty = known-but-no-messages "shell" chats (discovered but not backfilled,
// or genuinely empty).
type PlatformCoverage struct {
	Platform          string    `json:"platform"`
	ChatsKnown        int       `json:"chats_known"`
	ChatsWithMessages int       `json:"chats_with_messages"`
	ChatsEmpty        int       `json:"chats_empty"`
	Messages          int       `json:"messages"`
	OldestTS          time.Time `json:"oldest_ts"`
	NewestTS          time.Time `json:"newest_ts"`
}

// Coverage returns per-platform ingestion coverage (#13 scorecard): chats known (the
// chat index) vs chats with messages (actually backfilled), total messages, and the
// history depth (oldest→newest message ts). One row per platform that has either a
// known chat or a message. Read-only aggregate.
func (s *Store) Coverage(ctx context.Context) ([]PlatformCoverage, error) {
	byPlat := map[string]*PlatformCoverage{}
	get := func(p string) *PlatformCoverage {
		if byPlat[p] == nil {
			byPlat[p] = &PlatformCoverage{Platform: p}
		}
		return byPlat[p]
	}

	// Known chats + covered chats, both straight off the small chats index.
	// ChatsWithMessages comes from chats.msg_count>0 rather than a
	// COUNT(DISTINCT chat_id) over the whole firehose: msg_count is the
	// projection's per-chat message tally and is ALREADY the system's
	// source-of-truth for coverage (the #42 breadth-first BackfillCandidates
	// ordering keys on msg_count=0 = "never covered"). Counting it here keeps
	// /observability off the messages full-scan that made it time out (16s),
	// and stays consistent with what the sweep itself considers covered.
	crows, err := s.db.QueryContext(ctx,
		`SELECT platform, COUNT(*), SUM(CASE WHEN msg_count > 0 THEN 1 ELSE 0 END) FROM chats GROUP BY platform`)
	if err != nil {
		return nil, err
	}
	for crows.Next() {
		var p string
		var known, withMsgs int
		if err := crows.Scan(&p, &known, &withMsgs); err != nil {
			crows.Close()
			return nil, err
		}
		c := get(p)
		c.ChatsKnown = known
		c.ChatsWithMessages = withMsgs
	}
	crows.Close()
	if err := crows.Err(); err != nil {
		return nil, err
	}

	// Messages: count + depth (min/max ts) per platform. No COUNT(DISTINCT
	// chat_id) here — this aggregate is now served entirely from
	// idx_messages_platform_ts (platform,ts): grouped COUNT + min/max ts are
	// index endpoints, no row lookups and no temp sort.
	mrows, err := s.db.QueryContext(ctx,
		`SELECT platform, COUNT(*), MIN(ts), MAX(ts) FROM messages GROUP BY platform`)
	if err != nil {
		return nil, err
	}
	for mrows.Next() {
		var p string
		var msgs int
		var oldest, newest string
		if err := mrows.Scan(&p, &msgs, &oldest, &newest); err != nil {
			mrows.Close()
			return nil, err
		}
		c := get(p)
		c.Messages = msgs
		if t, e := time.Parse(time.RFC3339Nano, oldest); e == nil {
			c.OldestTS = t.UTC()
		}
		if t, e := time.Parse(time.RFC3339Nano, newest); e == nil {
			c.NewestTS = t.UTC()
		}
	}
	mrows.Close()
	if err := mrows.Err(); err != nil {
		return nil, err
	}

	out := make([]PlatformCoverage, 0, len(byPlat))
	for _, c := range byPlat {
		// Clamp: messages in a chat not in the index shouldn't happen, but guard.
		c.ChatsEmpty = max(0, c.ChatsKnown-c.ChatsWithMessages)
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Platform < out[j].Platform })
	return out, nil
}

// BackfillCandidate is one chat the salience-ordered backfill sweep should pull (#13).
type BackfillCandidate struct {
	ChatID   string
	Platform string
	ChatType string
}

// importanceRankSQL orders the salience tiers for the sweep: important → normal →
// unscored (”) → low. 'low' is swept LAST, not skipped — validation showed importance
// is under-populated (real chats/people defaulting to 'low'), and the user wants
// maximal ingestion, so everything gets swept; tiering just sets priority (#13/#15).
const importanceRankSQL = `CASE importance WHEN 'important' THEN 0 WHEN 'normal' THEN 1 WHEN 'low' THEN 3 ELSE 2 END`

// BackfillCandidates returns chats that still need a full-history backfill (#13):
// every chat with backfilled_at unset, capped at limit. platform=="" spans all platforms.
//
// Ordering is BREADTH-FIRST (#42): never-covered chats (msg_count=0) sort ahead of
// already-covered ones, THEN by importance tier (important→normal→unscored→low), then
// recency. Covering a chat we have ZERO history for (0→N) is strictly higher value than
// deepening one we already have (N→N+more), so the daily sweep fills every empty chat
// before it spends budget re-deepening covered ones — which is the only way the coverage
// % actually climbs. Without this, a platform with more covered-but-unmarked chats than
// the per-fire cap (e.g. Lark: 86 covered > cap 25) would burn every fire deepening the
// covered set and never reach the ~2,275 empty chats (they sort last on blank last_ts).
// Importance still orders WITHIN each breadth class, so real DMs fill before dead noise.
func (s *Store) BackfillCandidates(ctx context.Context, platform string, limit int) ([]BackfillCandidate, error) {
	q := `SELECT chat_id, platform, chat_type FROM chats
	      WHERE (backfilled_at IS NULL OR backfilled_at = '')`
	var args []any
	if platform != "" {
		q += " AND platform = ?"
		args = append(args, platform)
	}
	q += " ORDER BY (msg_count = 0) DESC, " + importanceRankSQL + ", last_ts DESC"
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BackfillCandidate
	for rows.Next() {
		var c BackfillCandidate
		if err := rows.Scan(&c.ChatID, &c.Platform, &c.ChatType); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// LowTierBackfillStats reports the explicit 'low'-importance chats the sweep GATES
// (#13): count + a small title sample, so we can eyeball how many are bot/broadcast/
// dead-shell vs real before deciding whether to sweep them too. platform=="" spans all.
func (s *Store) LowTierBackfillStats(ctx context.Context, platform string, sampleN int) (count int, sample []string, err error) {
	base := `FROM chats WHERE (backfilled_at IS NULL OR backfilled_at = '') AND importance = 'low'`
	var args []any
	if platform != "" {
		base += " AND platform = ?"
		args = append(args, platform)
	}
	if err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) "+base, args...).Scan(&count); err != nil {
		return 0, nil, err
	}
	sq := "SELECT COALESCE(NULLIF(title,''), NULLIF(derived_name,''), chat_id) " + base + " ORDER BY last_ts DESC"
	if sampleN > 0 {
		sq += fmt.Sprintf(" LIMIT %d", sampleN)
	}
	rows, err := s.db.QueryContext(ctx, sq, args...)
	if err != nil {
		return count, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return count, sample, err
		}
		sample = append(sample, t)
	}
	return count, sample, rows.Err()
}

// MarkChatBackfilled stamps a chat's backfilled_at so it drops out of the sweep
// candidate set (#13 deterministic convergence — each chat backfilled once).
func (s *Store) MarkChatBackfilled(ctx context.Context, chatID string, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE chats SET backfilled_at = ? WHERE chat_id = ?`,
		at.UTC().Format(time.RFC3339Nano), chatID)
	return err
}

// PlatformNewestAndAccount returns the newest message ts + its account_id for a
// platform (#37 connector health). Zero time + "" when the platform has no messages.
func (s *Store) PlatformNewestAndAccount(ctx context.Context, platform string) (newest time.Time, account string, err error) {
	var ts string
	row := s.roDB.QueryRowContext(ctx,
		`SELECT ts, account_id FROM messages WHERE platform = ? ORDER BY ts DESC LIMIT 1`, platform)
	if err := row.Scan(&ts, &account); err != nil {
		if err == sql.ErrNoRows {
			return time.Time{}, "", nil
		}
		return time.Time{}, "", err
	}
	if t, perr := time.Parse(time.RFC3339Nano, ts); perr == nil {
		newest = t.UTC()
	}
	return newest, account, nil
}

// GetMeta reads a durable scratch value by key; missing key → ("", nil).
func (s *Store) GetMeta(ctx context.Context, key string) (string, error) {
	var v string
	err := s.roDB.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return v, err
}

// SetMeta upserts a durable scratch value.
func (s *Store) SetMeta(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO meta(key, value) VALUES(?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// BackfillRun is one recorded backfill-sweep result for ONE platform (phase-2
// convergence observability): when it ran, how many chats it swept/failed, whether a
// flood-wait aborted it, and the chats-with-messages coverage before→after. Persisted as
// a capped JSON ring in the meta kv (no schema migration) so /observability can show the
// convergence trend without a dedicated table.
type BackfillRun struct {
	TS             time.Time `json:"ts"`
	Platform       string    `json:"platform"`
	Swept          int       `json:"swept"`
	Failed         int       `json:"failed"`
	Flooded        bool      `json:"flooded"`
	IngestedBefore int       `json:"ingested_before"`
	IngestedAfter  int       `json:"ingested_after"`
	ChatsKnown     int       `json:"chats_known"`
}

// backfillRunsKey + backfillRunsCap: the meta-kv ring for recorded sweep runs.
const backfillRunsKey = "backfill:runs"
const backfillRunsCap = 50

// RecordBackfillRun appends a sweep-run record to the durable ring (newest last, capped
// at backfillRunsCap). Best-effort: a malformed existing value is replaced rather than
// failing the sweep.
func (s *Store) RecordBackfillRun(ctx context.Context, run BackfillRun) error {
	runs, _ := s.BackfillRuns(ctx) // ignore parse error → start fresh
	runs = append(runs, run)
	if len(runs) > backfillRunsCap {
		runs = runs[len(runs)-backfillRunsCap:]
	}
	b, err := json.Marshal(runs)
	if err != nil {
		return err
	}
	return s.SetMeta(ctx, backfillRunsKey, string(b))
}

// BackfillRuns returns the recorded sweep runs oldest→newest (empty if none/unparseable).
func (s *Store) BackfillRuns(ctx context.Context) ([]BackfillRun, error) {
	v, err := s.GetMeta(ctx, backfillRunsKey)
	if err != nil || v == "" {
		return nil, err
	}
	var runs []BackfillRun
	if uerr := json.Unmarshal([]byte(v), &runs); uerr != nil {
		return nil, uerr
	}
	return runs, nil
}

// LastBackfillRun returns the most-recent recorded run for a platform, or (zero,false).
func (s *Store) LastBackfillRun(ctx context.Context, platform string) (BackfillRun, bool) {
	runs, _ := s.BackfillRuns(ctx)
	for i := len(runs) - 1; i >= 0; i-- {
		if runs[i].Platform == platform {
			return runs[i], true
		}
	}
	return BackfillRun{}, false
}

// RecentChat is a lightweight pointer to one conversation a resolved person has been
// active in: the chat to open, the title (chat title → derived name fallback), when
// they last spoke there, and a snippet of that last message. Neutral store type —
// the daemon maps it to api.ChatRef (store must not import api).
type RecentChat struct {
	ChatID   string
	ChatType string // stored "dm"/"group" label — UNRELIABLE: some genuine 1:1s are mislabeled "group" (#31)
	IsDM     bool   // robust 1:1 signal: chat_type=="dm" OR exactly 2 distinct senders incl. self
	Title    string
	LastTS   time.Time
	Snippet  string
}

// RecentChatsForSenders returns the most-recently-active chats in which ANY of the
// given (platform, sender_id) authors posted — newest-first, DM-inclusive. This is
// the #35 / #32 resolve_person "RecentChats" population: it turns a resolved person's
// identity set into directly-openable chat pointers, so RecentChats[0].ChatID is the
// person's most-recent conversation — a directly-sendable target (e.g. a Lark p2p
// oc_) the agent can send/read without guessing a chat_id. For each chat it carries
// the chat title (falling back to derived_name) and a snippet (the author's most-
// recent message text there, via SQLite's bare-column-with-MAX(ts) selection).
// Empty senders → no rows (the person resolved to nobody).
func (s *Store) RecentChatsForSenders(ctx context.Context, senders []SenderRef, limit int) ([]RecentChat, error) {
	if len(senders) == 0 {
		return nil, nil
	}
	var args []any
	// (m.platform, m.sender_id) IN <set>, expressed as OR'd pairs (mirrors
	// SearchBySender — a person has only a handful of platform identities).
	clauses := make([]string, 0, len(senders))
	for _, sr := range senders {
		clauses = append(clauses, "(m.platform = ? AND m.sender_id = ?)")
		args = append(args, sr.Platform, sr.SenderID)
	}
	// participants/has_self subqueries scan the FULL chat (not just this person's
	// rows) so we can detect a genuine 1:1 by cardinality — exactly 2 distinct
	// senders, one of them self — even when chat_type is mislabeled "group" (#31).
	q := `SELECT m.chat_id, m.chat_type, COALESCE(c.title, ''), COALESCE(c.derived_name, ''), MAX(m.ts) AS lt, m.text,
	             (SELECT COUNT(DISTINCT m2.sender_id) FROM messages m2 WHERE m2.chat_id = m.chat_id) AS participants,
	             (SELECT MAX(m2.is_self) FROM messages m2 WHERE m2.chat_id = m.chat_id) AS has_self
	      FROM messages m
	      LEFT JOIN chats c ON c.chat_id = m.chat_id
	      WHERE (` + strings.Join(clauses, " OR ") + `)
	      GROUP BY m.chat_id
	      ORDER BY lt DESC`
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RecentChat
	for rows.Next() {
		var rc RecentChat
		var title, derived, lt string
		var participants, hasSelf int
		if err := rows.Scan(&rc.ChatID, &rc.ChatType, &title, &derived, &lt, &rc.Snippet, &participants, &hasSelf); err != nil {
			return nil, err
		}
		// Robust 1:1 detection: trust the stored "dm" label, but ALSO treat a chat
		// with exactly 2 distinct senders (one self) as a DM — catches genuine 1:1s
		// mislabeled "group" (#31), so the resolve layer can title them by counterparty.
		rc.IsDM = rc.ChatType == event.ChatDM || (participants == 2 && hasSelf == 1)
		if title != "" {
			rc.Title = title
		} else {
			rc.Title = derived // p2p titles are blank until #31 p2p-titling lands
		}
		if t, perr := time.Parse(time.RFC3339Nano, lt); perr == nil {
			rc.LastTS = t.UTC()
		}
		rc.Snippet = snippetOf(rc.Snippet)
		out = append(out, rc)
	}
	return out, rows.Err()
}

// snippetOf trims a message body to a short, single-line preview for ChatRef.
func snippetOf(s string) string {
	s = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\r", " "))
	const max = 160
	if r := []rune(s); len(r) > max {
		return strings.TrimSpace(string(r[:max])) + "…"
	}
	return s
}

// ErrInvalidPattern wraps a regexp compile failure so callers (the API handler) can
// map a bad user-supplied pattern to a 400, not a 500.
var ErrInvalidPattern = errors.New("invalid regex")

// ErrUnsafeSQL rejects a query_sql passthrough that isn't a single read-only SELECT/CTE
// (#36). The parse-guard is the first line; the mode=ro+query_only connection is the
// hard guarantee. Mapped to a 400 by the handler.
var ErrUnsafeSQL = errors.New("query_sql: only a single read-only SELECT/WITH statement is allowed")

// QuerySQLResult is the #36 query_sql response: columns + rows (each row aligned to
// columns), with caps surfaced — Truncated=true when the row cap was hit (no silent
// cap, same principle as grep). ElapsedMs is the query wall-time.
type QuerySQLResult struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	Truncated bool     `json:"truncated"`
	ElapsedMs int64    `json:"elapsed_ms"`
}

// sqlGuardRe rejects anything but a statement that STARTS with SELECT or WITH.
var sqlGuardRe = regexp.MustCompile(`(?is)^\s*(select|with)\b`)

// sqlForbiddenRe rejects dangerous tokens even inside an otherwise-SELECT statement
// (belt-and-suspenders on top of the mode=ro conn): ATTACH (could reach another db),
// PRAGMA (settings), and write DML keywords.
var sqlForbiddenRe = regexp.MustCompile(`(?is)\b(attach|detach|pragma|insert|update|delete|drop|create|alter|replace|vacuum|reindex|truncate)\b`)

// guardReadOnlySQL validates a query_sql passthrough is a SINGLE read-only SELECT/CTE
// (#36): exactly one statement (no embedded ';'), starting with SELECT/WITH, with no
// forbidden write/PRAGMA/ATTACH tokens. Returns the trimmed query or ErrUnsafeSQL.
func guardReadOnlySQL(q string) (string, error) {
	t := strings.TrimSpace(q)
	t = strings.TrimSuffix(t, ";")
	t = strings.TrimSpace(t)
	if t == "" {
		return "", ErrUnsafeSQL
	}
	if strings.Contains(t, ";") {
		return "", ErrUnsafeSQL // multiple statements
	}
	if !sqlGuardRe.MatchString(t) {
		return "", ErrUnsafeSQL // must start with SELECT or WITH
	}
	if sqlForbiddenRe.MatchString(t) {
		return "", ErrUnsafeSQL // write/PRAGMA/ATTACH token present
	}
	return t, nil
}

// QuerySQL runs an arbitrary READ-ONLY SELECT over the firehose (#36) — the "let the
// agent SQL the message history" primitive, complementing grep/search. Three hard
// guards: (1) parse-guard (single SELECT/CTE, no writes/PRAGMA/ATTACH/multi-stmt),
// (2) the dedicated mode=ro+query_only connection (physically can't write), (3) the
// caller's context timeout + the maxRows cap → Truncated. A rejected/ bad query is a
// clean error (ErrUnsafeSQL → 400), never a write or a 500.
func (s *Store) QuerySQL(ctx context.Context, query string, maxRows int) (QuerySQLResult, error) {
	clean, gerr := guardReadOnlySQL(query)
	if gerr != nil {
		return QuerySQLResult{}, gerr
	}
	if maxRows <= 0 {
		maxRows = 1000
	}
	start := time.Now()
	rows, err := s.roDB.QueryContext(ctx, clean)
	if err != nil {
		return QuerySQLResult{}, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return QuerySQLResult{}, err
	}
	res := QuerySQLResult{Columns: cols, Rows: [][]any{}}
	for rows.Next() {
		if len(res.Rows) >= maxRows {
			res.Truncated = true // row cap hit — more rows exist (no silent cap)
			break
		}
		holders := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range holders {
			ptrs[i] = &holders[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return res, err
		}
		for i, v := range holders {
			if b, ok := v.([]byte); ok {
				holders[i] = string(b) // []byte → string for JSON legibility
			}
		}
		res.Rows = append(res.Rows, holders)
	}
	if err := rows.Err(); err != nil {
		return res, err
	}
	res.ElapsedMs = time.Since(start).Milliseconds()
	return res, nil
}

// grepMaxScan bounds how many rows a single Grep examines, so a rare pattern over the
// whole firehose can't run away. Newest-first ordering + the result limit mean common
// patterns return well before this; it only caps the pathological case.
const grepMaxScan = 200_000

// GrepResult carries Grep matches plus the scan accounting, so the caller can tell an
// EXHAUSTIVE "no more matches" from a "stopped early at the scan cap" (#16: no silent
// cap). Truncated is true only when the scan cap was hit before the candidate set was
// exhausted — i.e. there may be older matches this run didn't reach.
type GrepResult struct {
	Matches   []event.Message
	Scanned   int
	Truncated bool
}

// Grep runs a Go-regexp (RE2) scan over message text — the #16 firehose grep, the
// regex counterpart to FTS Search. The pattern is full RE2 (charclasses/anchors/
// alternation/quantifiers + inline flags like (?i)/(?s); NO lookbehind/backrefs). It
// streams rows newest-first (never loading all), pre-filtered by platform + author
// (senders, the #32 person→sender_id set) + since, matches text in Go, and returns up
// to limit matches — unmarshalling the full body ONLY for matches (the perf win). A
// bad pattern is a clean error. Bounded by grepMaxScan rows examined; if that cap is
// hit before the candidate set is exhausted, GrepResult.Truncated is set (NOT a silent
// stop). A result limit hit is NOT truncation — it's the requested cap.
func (s *Store) Grep(ctx context.Context, pattern, platform string, senders []SenderRef, since time.Time, limit int) (GrepResult, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return GrepResult{}, fmt.Errorf("%w: %v", ErrInvalidPattern, err)
	}
	q := `SELECT text, body FROM messages WHERE 1=1`
	var args []any
	if platform != "" {
		q += " AND platform = ?"
		args = append(args, platform)
	}
	if len(senders) > 0 {
		clauses := make([]string, 0, len(senders))
		for _, sr := range senders {
			clauses = append(clauses, "(platform = ? AND sender_id = ?)")
			args = append(args, sr.Platform, sr.SenderID)
		}
		q += " AND (" + strings.Join(clauses, " OR ") + ")"
	}
	if !since.IsZero() {
		q += " AND ts >= ?"
		args = append(args, since.UTC().Format(time.RFC3339Nano))
	}
	q += " ORDER BY ts DESC"

	rows, err := s.roDB.QueryContext(ctx, q, args...)
	if err != nil {
		return GrepResult{}, err
	}
	defer rows.Close()
	res := GrepResult{}
	for rows.Next() {
		if res.Scanned >= grepMaxScan {
			res.Truncated = true // hit the scan cap with rows still pending — not exhaustive
			break
		}
		res.Scanned++
		var text string
		var body []byte
		if err := rows.Scan(&text, &body); err != nil {
			return res, err
		}
		if !re.MatchString(text) {
			continue
		}
		var m event.Message
		if json.Unmarshal(body, &m) != nil {
			continue
		}
		res.Matches = append(res.Matches, m)
		if limit > 0 && len(res.Matches) >= limit {
			break // result limit reached — expected, NOT a scan-cap truncation
		}
	}
	return res, rows.Err()
}

// ReextractSenderPhones recovers a blank sender_phone by re-parsing each affected
// @lid sender's stored Raw (via the connector-supplied extract fn) and UPDATEing
// the column — the #33 historical recovery: WhatsApp @lid messages ingested before
// the SenderAlt capture left sender_phone blank, but the phone was in Raw (body)
// all along. Scoped to one platform + @lid sender_ids with a blank phone; updates
// ALL of a recovered sender's blank-phone rows. Returns rows updated.
func (s *Store) ReextractSenderPhones(ctx context.Context, platform string, extract func(event.Message) (string, bool)) (int, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT sender_id, MAX(body) FROM messages
		 WHERE platform = ? AND (sender_phone IS NULL OR sender_phone = '') AND sender_id LIKE '%@lid'
		 GROUP BY sender_id`, platform)
	if err != nil {
		return 0, err
	}
	type pending struct{ senderID, phone string }
	var todo []pending
	for rows.Next() {
		var senderID string
		var body []byte
		if err := rows.Scan(&senderID, &body); err != nil {
			rows.Close()
			return 0, err
		}
		var m event.Message
		if json.Unmarshal(body, &m) != nil {
			continue
		}
		if phone, ok := extract(m); ok && phone != "" {
			todo = append(todo, pending{senderID, phone})
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	total := 0
	for _, p := range todo {
		res, err := s.db.ExecContext(ctx,
			`UPDATE messages SET sender_phone = ?
			 WHERE platform = ? AND sender_id = ? AND (sender_phone IS NULL OR sender_phone = '')`,
			p.phone, platform, p.senderID)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += int(n)
	}
	return total, nil
}

// Mentions returns every message that @-mentions the user, newest-first.
func (s *Store) Mentions(ctx context.Context, since time.Time, limit int) ([]event.Message, error) {
	q := `SELECT body FROM messages WHERE mentions_self = 1`
	var args []any
	if !since.IsZero() {
		q += " AND ts >= ?"
		args = append(args, since.UTC().Format(time.RFC3339Nano))
	}
	q += " ORDER BY ts DESC"
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	return s.scanMessages(ctx, q, args...)
}

func (s *Store) scanMessages(ctx context.Context, q string, args ...any) ([]event.Message, error) {
	rows, err := s.roDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []event.Message
	for rows.Next() {
		var body string
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var m event.Message
		if err := json.Unmarshal([]byte(body), &m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
