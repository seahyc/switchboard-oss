package comprehend

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Store is comprehend's OWN database (comprehend.db) — the only thing this package
// writes. It holds extracted commitments and the extraction content-hash cache.
// The canonical switchboard.db is never touched (see Reader).
type Store struct {
	db *sql.DB
}

const storeSchema = `
CREATE TABLE IF NOT EXISTS commitments (
    id            TEXT PRIMARY KEY,        -- hash(source_msg + what)
    who           TEXT NOT NULL,
    to_whom       TEXT NOT NULL,
    what          TEXT NOT NULL,
    due           TEXT NOT NULL DEFAULT '',-- RFC3339 or ''
    status        TEXT NOT NULL DEFAULT 'open',
    source_msg    TEXT NOT NULL,
    chat_id       TEXT NOT NULL DEFAULT '',
    platform      TEXT NOT NULL DEFAULT '',
    direction     TEXT NOT NULL DEFAULT 'mine',
    confidence    REAL NOT NULL DEFAULT 0,
    extractor     TEXT NOT NULL DEFAULT '',
    source_ts     TEXT NOT NULL DEFAULT '',
    extracted_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_commitments_due ON commitments(due, status);
CREATE INDEX IF NOT EXISTS idx_commitments_source ON commitments(source_msg);

-- Extraction content-hash cache: a message whose salient content we've already
-- processed never re-hits the (scarce) LLM. Keyed by a hash of the model+prompt
-- shape + message text, so a prompt change re-extracts but identical inputs don't.
CREATE TABLE IF NOT EXISTS extract_cache (
    cache_key   TEXT PRIMARY KEY,   -- hash(extractor + msg text)
    source_msg  TEXT NOT NULL,
    n_found     INTEGER NOT NULL DEFAULT 0,
    processed_at TEXT NOT NULL
);

-- High-water cursor for the incremental extraction funnel (per scope key).
CREATE TABLE IF NOT EXISTS extract_cursor (
    scope         TEXT PRIMARY KEY,
    watermark_ts  TEXT NOT NULL DEFAULT '',
    updated_at    TEXT NOT NULL
);

-- Pre-written reply drafts for who's-waiting threads (T2.3). SUGGEST-ONLY: a
-- draft is STORED here and surfaced on the waiting card's "draft" field; it is
-- NEVER sent without an explicit tap. Keyed by chat_id; last_msg_id ties the
-- draft to the message it answers so a newer message invalidates it.
CREATE TABLE IF NOT EXISTS drafts (
    chat_id      TEXT PRIMARY KEY,
    last_msg_id  TEXT NOT NULL,
    draft        TEXT NOT NULL,
    lang         TEXT NOT NULL DEFAULT '',
    model        TEXT NOT NULL DEFAULT '',
    generated_at TEXT NOT NULL
);
`

// OpenStore opens (creating if needed) comprehend.db and applies the schema.
// ":memory:" is accepted for tests.
func OpenStore(path string) (*Store, error) {
	dsn := path
	if path != ":memory:" {
		dsn = "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open comprehend store: %w", err)
	}
	if path == ":memory:" {
		db.SetMaxOpenConns(1)
	}
	if _, err := db.Exec(storeSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("comprehend migrate: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// CommitmentID is the stable idempotency key for a commitment.
func CommitmentID(sourceMsg, what string) string {
	sum := sha256.Sum256([]byte(sourceMsg + "\x00" + what))
	return hex.EncodeToString(sum[:8])
}

// CacheKey is the content-hash key for the extraction cache.
func CacheKey(extractor, text string) string {
	sum := sha256.Sum256([]byte(extractor + "\x00" + text))
	return hex.EncodeToString(sum[:12])
}

// PutCommitment idempotently upserts a commitment. Re-running extraction
// converges: an existing open commitment can transition to resolved/slipped, but
// provenance fields are stable on the idempotency key.
func (s *Store) PutCommitment(ctx context.Context, c Commitment) error {
	if c.ID == "" {
		c.ID = CommitmentID(c.SourceMsgID, c.What)
	}
	if c.Status == "" {
		c.Status = CommitmentOpen
	}
	if c.ExtractedAt.IsZero() {
		c.ExtractedAt = time.Now()
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO commitments(id, who, to_whom, what, due, status, source_msg, chat_id, platform, direction, confidence, extractor, source_ts, extracted_at)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET
    due        = excluded.due,
    status     = excluded.status,
    confidence = excluded.confidence,
    extractor  = excluded.extractor,
    extracted_at = excluded.extracted_at`,
		c.ID, c.Who, c.ToWhom, c.What, c.Due, c.Status, c.SourceMsgID, c.ChatID, c.Platform,
		c.Direction, c.Confidence, c.Extractor, fmtTS(c.SourceTS), fmtTS(c.ExtractedAt))
	if err != nil {
		return fmt.Errorf("put commitment: %w", err)
	}
	return nil
}

// Commitments returns all commitments with the given status ("" = any),
// newest-source first.
func (s *Store) Commitments(ctx context.Context, status string) ([]Commitment, error) {
	q := `SELECT id, who, to_whom, what, due, status, source_msg, chat_id, platform, direction, confidence, extractor, source_ts, extracted_at FROM commitments`
	var args []any
	if status != "" {
		q += ` WHERE status = ?`
		args = append(args, status)
	}
	q += ` ORDER BY source_ts DESC`
	return s.scanCommitments(ctx, q, args...)
}

// OpenCommitments returns open commitments for the brief's "on your plate"
// section, sorted dated-soonest-first then undated by most-recently-captured, so
// a freshly-extracted commitment surfaces in the next brief even if its due date
// is days out (G3 needs the captured commitment visible, not just today's).
func (s *Store) OpenCommitments(ctx context.Context, limit int) ([]Commitment, error) {
	q := `SELECT id, who, to_whom, what, due, status, source_msg, chat_id, platform, direction, confidence, extractor, source_ts, extracted_at
	      FROM commitments WHERE status = 'open'
	      ORDER BY (due = '') ASC, due ASC, source_ts DESC`
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	return s.scanCommitments(ctx, q)
}

// DueOn returns open commitments whose due date falls on the calendar day of `day`
// (in loc), or are overdue. Implements CommitmentSource for the brief.
func (s *Store) DueOn(ctx context.Context, day time.Time, loc *time.Location) ([]Commitment, error) {
	if loc == nil {
		loc = time.UTC
	}
	end := time.Date(day.In(loc).Year(), day.In(loc).Month(), day.In(loc).Day(), 23, 59, 59, 0, loc)
	all, err := s.Commitments(ctx, CommitmentOpen)
	if err != nil {
		return nil, err
	}
	var out []Commitment
	for _, c := range all {
		if c.Due == "" {
			continue
		}
		due, err := time.Parse(parseLayout, c.Due)
		if err != nil {
			// date-only form
			due, err = time.ParseInLocation("2006-01-02", c.Due, loc)
			if err != nil {
				continue
			}
		}
		if !due.After(end) {
			out = append(out, c)
		}
	}
	sortCommitmentsByDue(out)
	return out, nil
}

// OpenCommitmentsMissingDue returns open commitments with no due date set —
// candidates for the deterministic due-date backfill.
func (s *Store) OpenCommitmentsMissingDue(ctx context.Context) ([]Commitment, error) {
	return s.scanCommitments(ctx,
		`SELECT id, who, to_whom, what, due, status, source_msg, chat_id, platform, direction, confidence, extractor, source_ts, extracted_at
		 FROM commitments WHERE status='open' AND due=''`)
}

// SetDue updates a commitment's due date by id.
func (s *Store) SetDue(ctx context.Context, id, due string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE commitments SET due=? WHERE id=?`, due, id)
	return err
}

// Seen reports whether a message's content has already been processed by the given
// extractor pass (content-hash cache hit).
func (s *Store) Seen(ctx context.Context, extractor, text string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM extract_cache WHERE cache_key = ?`,
		CacheKey(extractor, text)).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// MarkSeen records that a message was processed (even if it yielded no commitment),
// so it never re-hits the LLM.
func (s *Store) MarkSeen(ctx context.Context, extractor, text, sourceMsg string, found int) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO extract_cache(cache_key, source_msg, n_found, processed_at) VALUES (?,?,?,?)
ON CONFLICT(cache_key) DO UPDATE SET n_found = excluded.n_found, processed_at = excluded.processed_at`,
		CacheKey(extractor, text), sourceMsg, found, fmtTS(time.Now()))
	return err
}

// Cursor returns the extraction watermark for a scope (zero time if none).
func (s *Store) Cursor(ctx context.Context, scope string) (time.Time, error) {
	var ts sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT watermark_ts FROM extract_cursor WHERE scope = ?`, scope).Scan(&ts)
	if err == sql.ErrNoRows || !ts.Valid {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return parseTS(ts.String), nil
}

// SetCursor advances the extraction watermark for a scope.
func (s *Store) SetCursor(ctx context.Context, scope string, watermark time.Time) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO extract_cursor(scope, watermark_ts, updated_at) VALUES (?,?,?)
ON CONFLICT(scope) DO UPDATE SET watermark_ts = excluded.watermark_ts, updated_at = excluded.updated_at`,
		scope, fmtTS(watermark), fmtTS(time.Now()))
	return err
}

// PutDraft stores (idempotent upsert) a suggest-only reply draft for a thread.
func (s *Store) PutDraft(ctx context.Context, chatID, lastMsgID, draft, lang, model string) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO drafts(chat_id, last_msg_id, draft, lang, model, generated_at) VALUES (?,?,?,?,?,?)
ON CONFLICT(chat_id) DO UPDATE SET
    last_msg_id = excluded.last_msg_id,
    draft       = excluded.draft,
    lang        = excluded.lang,
    model       = excluded.model,
    generated_at = excluded.generated_at`,
		chatID, lastMsgID, draft, lang, model, fmtTS(time.Now()))
	return err
}

// draftRow is a stored draft.
type draftRow struct {
	draft     string
	lastMsgID string
}

// draftsByChat loads all stored drafts keyed by chat_id.
func (s *Store) draftsByChat(ctx context.Context) (map[string]draftRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT chat_id, last_msg_id, draft FROM drafts`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]draftRow{}
	for rows.Next() {
		var chatID, lastMsgID, draft string
		if err := rows.Scan(&chatID, &lastMsgID, &draft); err != nil {
			return nil, err
		}
		out[chatID] = draftRow{draft: draft, lastMsgID: lastMsgID}
	}
	return out, rows.Err()
}

// AttachDrafts fills each card's Draft field from the store, but ONLY when the
// stored draft answers the card's CURRENT last message — a newer inbound message
// leaves Draft nil (stale draft suppressed) until regeneration. Mutates in place.
func (s *Store) AttachDrafts(ctx context.Context, cards []WaitingCard) error {
	if len(cards) == 0 {
		return nil
	}
	drafts, err := s.draftsByChat(ctx)
	if err != nil {
		return err
	}
	for i := range cards {
		if d, ok := drafts[cards[i].ChatID]; ok && d.lastMsgID == cards[i].LastMsgID && d.draft != "" {
			text := d.draft
			cards[i].Draft = &text
		}
	}
	return nil
}

func (s *Store) scanCommitments(ctx context.Context, q string, args ...any) ([]Commitment, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Commitment
	for rows.Next() {
		var c Commitment
		var srcTS, extAt string
		if err := rows.Scan(&c.ID, &c.Who, &c.ToWhom, &c.What, &c.Due, &c.Status, &c.SourceMsgID,
			&c.ChatID, &c.Platform, &c.Direction, &c.Confidence, &c.Extractor, &srcTS, &extAt); err != nil {
			return nil, err
		}
		c.SourceTS = parseTS(srcTS)
		c.ExtractedAt = parseTS(extAt)
		out = append(out, c)
	}
	return out, rows.Err()
}

func fmtTS(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(parseLayout)
}
