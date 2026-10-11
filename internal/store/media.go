package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// The media store tracks per-attachment download state over a content-addressed
// blob store on disk. The store layer here is pure bookkeeping (no filesystem):
// it records which attachments are pending/stored/evicted, the content hash that
// dedups identical bytes, and drives eviction by MESSAGE recency so the on-disk
// set is always the most-recent N bytes of important media. The daemon's media
// worker owns the actual download + file IO and calls these methods.
//
// Dedup is content-addressed: a blob's on-disk path is derived from its sha256,
// so identical bytes from many messages map to ONE file. Multiple media rows can
// share a hash; the bytes for that hash are present on disk as long as ≥1 row
// with that hash is `stored`. Total stored bytes counts each hash once.

const mediaSchema = `
CREATE TABLE IF NOT EXISTS media (
    id          TEXT PRIMARY KEY,           -- "<event_id>#<attachment_idx>"
    event_id    TEXT NOT NULL,
    chat_id     TEXT NOT NULL,
    platform    TEXT NOT NULL,
    idx         INTEGER NOT NULL,           -- attachment index within the message
    type        TEXT NOT NULL DEFAULT '',   -- image|video|audio|voice|document|sticker|...
    mime        TEXT NOT NULL DEFAULT '',
    name        TEXT NOT NULL DEFAULT '',
    fetch_ref   TEXT NOT NULL DEFAULT '',   -- connector handle to (re)download the bytes
    msg_ts      TEXT NOT NULL,              -- the message timestamp (eviction PRIMARY key)
    hash        TEXT NOT NULL DEFAULT '',   -- sha256 hex, set once downloaded (dedup key)
    size        INTEGER NOT NULL DEFAULT 0, -- bytes
    stored      INTEGER NOT NULL DEFAULT 0, -- 1 = bytes on disk under the hash path
    status      TEXT NOT NULL DEFAULT 'pending', -- pending|stored|evicted|skipped|failed
    last_access TEXT NOT NULL DEFAULT '',   -- get_media warm-bump (secondary signal)
    err         TEXT NOT NULL DEFAULT '',   -- last download/skip reason (visibility)
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_media_status ON media(status);
CREATE INDEX IF NOT EXISTS idx_media_evict ON media(stored, msg_ts);
CREATE INDEX IF NOT EXISTS idx_media_hash ON media(hash);
`

// MediaStatus values.
const (
	MediaPending = "pending" // registered, not yet downloaded
	MediaStored  = "stored"  // bytes on disk under the hash path
	MediaEvicted = "evicted" // bytes removed under the cap; re-fetchable via fetch_ref
	MediaSkipped = "skipped" // gated out / too large — intentionally not downloaded
	MediaFailed  = "failed"  // download failed; eligible for retry
)

// MediaItem registers one attachment for the media worker. id is
// "<event_id>#<idx>"; FetchRef is the connector handle used to download.
type MediaItem struct {
	ID       string
	EventID  string
	ChatID   string
	Platform string
	Idx      int
	Type     string
	MIME     string
	Name     string
	FetchRef string
	MsgTS    time.Time
}

// MediaRow is a media record as stored.
type MediaRow struct {
	ID       string
	EventID  string
	ChatID   string
	Platform string
	Idx      int
	Type     string
	MIME     string
	Name     string
	FetchRef string
	MsgTS    time.Time
	Hash     string
	Size     int64
	Stored   bool
	Status   string
}

// BlobRef identifies a content blob to unlink from disk (hash + size freed).
type BlobRef struct {
	Hash string
	Size int64
}

// EnqueueMedia registers attachments as pending download. Idempotent on id for
// already-stored media, but replay/reprojection may refresh the fetch_ref for
// non-stored rows when a connector can now derive a richer ref. Failed rows are
// deliberately moved back to pending only when the ref changes; chat-importance
// skips are retried only if the chat is now important. Returns the number newly
// enqueued.
func (s *Store) EnqueueMedia(ctx context.Context, items []MediaItem) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	n := 0
	for _, it := range items {
		res, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO media(id, event_id, chat_id, platform, idx, type, mime, name, fetch_ref, msg_ts, status, created_at)
			 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
			it.ID, it.EventID, it.ChatID, it.Platform, it.Idx, it.Type, it.MIME, it.Name, it.FetchRef,
			it.MsgTS.UTC().Format(time.RFC3339Nano), MediaPending, now)
		if err != nil {
			return 0, fmt.Errorf("enqueue media: %w", err)
		}
		if a, _ := res.RowsAffected(); a > 0 {
			n++
			continue
		}
		if err := refreshExistingMediaTx(ctx, tx, it); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

func refreshExistingMediaTx(ctx context.Context, tx *sql.Tx, it MediaItem) error {
	if it.FetchRef == "" {
		return nil
	}
	var status, oldRef, errText string
	var stored int
	err := tx.QueryRowContext(ctx,
		`SELECT status, stored, fetch_ref, err FROM media WHERE id=?`, it.ID).
		Scan(&status, &stored, &oldRef, &errText)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return fmt.Errorf("refresh media %s: %w", it.ID, err)
	}
	if stored == 1 {
		return nil
	}

	newStatus := status
	newErr := errText
	switch status {
	case MediaPending, MediaEvicted:
		if oldRef == it.FetchRef {
			return nil
		}
	case MediaFailed:
		if oldRef == it.FetchRef {
			return nil
		}
		newStatus = MediaPending
		newErr = ""
	case MediaSkipped:
		if !isChatImportanceSkip(errText) {
			return nil
		}
		imp, err := chatImportanceTx(ctx, tx, it.ChatID)
		if err != nil {
			return err
		}
		if imp == "important" {
			newStatus = MediaPending
			newErr = ""
		} else if oldRef == it.FetchRef {
			return nil
		}
	default:
		return nil
	}

	_, err = tx.ExecContext(ctx,
		`UPDATE media
		    SET event_id=?, chat_id=?, platform=?, idx=?, type=?, mime=?, name=?,
		        fetch_ref=?, msg_ts=?, status=?, err=?
		  WHERE id=? AND stored=0`,
		it.EventID, it.ChatID, it.Platform, it.Idx, it.Type, it.MIME, it.Name,
		it.FetchRef, it.MsgTS.UTC().Format(time.RFC3339Nano), newStatus, newErr, it.ID)
	if err != nil {
		return fmt.Errorf("refresh media %s: %w", it.ID, err)
	}
	return nil
}

func isChatImportanceSkip(reason string) bool {
	return len(reason) >= len("chat importance=") && reason[:len("chat importance=")] == "chat importance="
}

func chatImportanceTx(ctx context.Context, tx *sql.Tx, chatID string) (string, error) {
	var imp string
	err := tx.QueryRowContext(ctx, `SELECT importance FROM chats WHERE chat_id = ?`, chatID).Scan(&imp)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return imp, err
}

// NextPendingMedia returns up to limit attachments awaiting download, newest
// message first (so recent media is fetched before older backlog).
func (s *Store) NextPendingMedia(ctx context.Context, limit int) ([]MediaRow, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, event_id, chat_id, platform, idx, type, mime, name, fetch_ref, msg_ts, hash, size, stored, status
		   FROM media WHERE status = ? ORDER BY msg_ts DESC LIMIT ?`, MediaPending, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMediaRows(rows)
}

func scanMediaRows(rows *sql.Rows) ([]MediaRow, error) {
	var out []MediaRow
	for rows.Next() {
		var m MediaRow
		var tsStr string
		var stored int
		if err := rows.Scan(&m.ID, &m.EventID, &m.ChatID, &m.Platform, &m.Idx, &m.Type, &m.MIME,
			&m.Name, &m.FetchRef, &tsStr, &m.Hash, &m.Size, &stored, &m.Status); err != nil {
			return nil, err
		}
		m.MsgTS, _ = time.Parse(time.RFC3339Nano, tsStr)
		m.Stored = stored == 1
		out = append(out, m)
	}
	return out, rows.Err()
}

// StoredImageMedia returns currently-stored image/sticker rows for a platform — the
// candidate set for the #48 ciphertext re-validation sweep (re-check the bytes on disk
// against their declared type).
func (s *Store) StoredImageMedia(ctx context.Context, platform string) ([]MediaRow, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, event_id, chat_id, platform, idx, type, mime, name, fetch_ref, msg_ts, hash, size, stored, status
		   FROM media WHERE status=? AND platform=? AND type IN ('image','sticker')`, MediaStored, platform)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMediaRows(rows)
}

// ResetMediaForRedownload un-stores a row (clears stored/hash/size) and sets it pending
// with a (possibly re-derived) fetch_ref, so the worker / on-view ensure re-downloads it.
// Used by the #48 ciphertext re-validation sweep to recover rows whose stored bytes were
// undecrypted ciphertext.
func (s *Store) ResetMediaForRedownload(ctx context.Context, id, fetchRef string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE media SET stored=0, hash='', size=0, status=?, err='', fetch_ref=? WHERE id=?`,
		MediaPending, fetchRef, id)
	return err
}

// MarkMediaStored records a successful download: the bytes are on disk under the
// content-addressed hash path. Dedup is implicit — multiple rows may share a
// hash, all marked stored, but the file exists once. Sets the canonical fields.
func (s *Store) MarkMediaStored(ctx context.Context, id, hash string, size int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE media SET hash=?, size=?, stored=1, status=?, err='' WHERE id=?`,
		hash, size, MediaStored, id)
	return err
}

// MarkMediaSkipped records an intentional non-download (gated out, too large).
func (s *Store) MarkMediaSkipped(ctx context.Context, id, reason string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE media SET status=?, err=? WHERE id=?`, MediaSkipped, reason, id)
	return err
}

// MarkMediaFailed records a failed download (eligible for later retry).
func (s *Store) MarkMediaFailed(ctx context.Context, id, reason string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE media SET status=?, err=? WHERE id=?`, MediaFailed, reason, id)
	return err
}

// OldestStoredMediaTS returns the message timestamp of the oldest currently-
// stored media (the eviction frontier). The cap-aware drain uses it to decide
// whether a pending item is worth downloading: at/over cap, only media NEWER
// than this frontier should be fetched (it will evict something older); older
// backlog media would be evicted immediately, so it's left pending instead of
// burning a download. ok=false when nothing is stored yet.
func (s *Store) OldestStoredMediaTS(ctx context.Context) (time.Time, bool, error) {
	var tsStr sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT MIN(msg_ts) FROM media WHERE stored=1`).Scan(&tsStr)
	if err != nil {
		return time.Time{}, false, err
	}
	if !tsStr.Valid || tsStr.String == "" {
		return time.Time{}, false, nil
	}
	t, _ := time.Parse(time.RFC3339Nano, tsStr.String)
	return t, true, nil
}

// StoredBytes returns the total on-disk media bytes, counting each content hash
// ONCE (dedup-aware) — this is what the 40GB cap is measured against.
func (s *Store) StoredBytes(ctx context.Context) (int64, error) {
	var total sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(size),0) FROM (SELECT DISTINCT hash, size FROM media WHERE stored=1 AND hash != '')`).Scan(&total)
	if err != nil {
		return 0, err
	}
	return total.Int64, nil
}

// MediaByID returns a single media row.
func (s *Store) MediaByID(ctx context.Context, id string) (MediaRow, bool, error) {
	rows, err := s.roDB.QueryContext(ctx,
		`SELECT id, event_id, chat_id, platform, idx, type, mime, name, fetch_ref, msg_ts, hash, size, stored, status
		   FROM media WHERE id=?`, id)
	if err != nil {
		return MediaRow{}, false, err
	}
	defer rows.Close()
	list, err := scanMediaRows(rows)
	if err != nil || len(list) == 0 {
		return MediaRow{}, false, err
	}
	return list[0], true, nil
}

// TouchMediaAccess warm-bumps last_access on a hash (secondary signal; primary
// eviction ordering stays message recency). No-op if hash is empty.
func (s *Store) TouchMediaAccess(ctx context.Context, hash string) error {
	if hash == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE media SET last_access=? WHERE hash=?`, time.Now().UTC().Format(time.RFC3339Nano), hash)
	return err
}

// EvictToTarget marks the OLDEST stored media (by message timestamp) as evicted
// until the dedup-aware stored-byte total is at or below targetBytes, and
// returns the content blobs whose bytes can now be unlinked from disk (a hash is
// only returned once NO stored row references it any longer). The caller unlinks
// the files. msg_ts is the primary key so the store always retains the most
// recent media; last_access is a deliberate non-factor here (warm reads don't
// rescue stale media past the recency budget — they only annotate).
func (s *Store) EvictToTarget(ctx context.Context, targetBytes int64) ([]BlobRef, int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()

	cur, err := distinctStoredBytesTx(ctx, tx)
	if err != nil {
		return nil, 0, err
	}
	if cur <= targetBytes {
		return nil, 0, nil
	}

	// Oldest stored rows first. Read fully into memory before issuing UPDATEs
	// (SQLite dislikes interleaving a live cursor with writes on the same conn).
	rows, err := tx.QueryContext(ctx,
		`SELECT id, hash, size FROM media WHERE stored=1 AND hash != '' ORDER BY msg_ts ASC, id ASC`)
	if err != nil {
		return nil, 0, err
	}
	type cand struct {
		id, hash string
		size     int64
	}
	var cands []cand
	for rows.Next() {
		var c cand
		if err := rows.Scan(&c.id, &c.hash, &c.size); err != nil {
			rows.Close()
			return nil, 0, err
		}
		cands = append(cands, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	var unlink []BlobRef
	var freed int64
	for _, c := range cands {
		if cur <= targetBytes {
			break
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE media SET stored=0, status=? WHERE id=?`, MediaEvicted, c.id); err != nil {
			return nil, 0, err
		}
		// Is this hash still held by any stored row? If not, its bytes are freed.
		var remaining int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM media WHERE stored=1 AND hash=?`, c.hash).Scan(&remaining); err != nil {
			return nil, 0, err
		}
		if remaining == 0 {
			unlink = append(unlink, BlobRef{Hash: c.hash, Size: c.size})
			freed += c.size
			cur -= c.size
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, err
	}
	return unlink, freed, nil
}

func distinctStoredBytesTx(ctx context.Context, tx *sql.Tx) (int64, error) {
	var total sql.NullInt64
	err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(size),0) FROM (SELECT DISTINCT hash, size FROM media WHERE stored=1 AND hash != '')`).Scan(&total)
	return total.Int64, err
}
