package gbrainfeed

import (
	"context"
	"database/sql"
	"log/slog"
)

// SQLitePageHashStore is a durable PageHashStore backed by the switchboard SQLite
// handle (store.DB()). It persists slug->content-hash so the projector's put-page skip
// cache survives a daemon restart (#62): without it, a restart re-writes ~every gbrain
// page (each PutPage is a gbrain subprocess shell-out) even though the graph is
// unchanged — the ~13min startup reproject. The hash is recorded only AFTER a
// successful write, so a persisted hash always means "this exact content is already in
// gbrain" → safe to skip. A content/format change yields a different hash → re-written
// (self-correcting; never serves stale content).
//
// Wire it from the daemon BEFORE the startup reproject:
//
//	phs, _ := gbrainfeed.NewSQLitePageHashStore(st.DB(), logger)
//	proj.SetPageHashStore(phs)
//	proj.LoadPageHashes(ctx)
type SQLitePageHashStore struct {
	db  *sql.DB
	log *slog.Logger
}

// NewSQLitePageHashStore opens/creates the page-hash table on the given DB.
func NewSQLitePageHashStore(db *sql.DB, logger *slog.Logger) (*SQLitePageHashStore, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS gbrain_page_hash (
		slug TEXT PRIMARY KEY,
		hash TEXT NOT NULL
	)`); err != nil {
		return nil, err
	}
	return &SQLitePageHashStore{db: db, log: logger}, nil
}

// PageHashes bulk-loads every persisted slug->hash in one query (startup seed).
func (s *SQLitePageHashStore) PageHashes(ctx context.Context) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT slug, hash FROM gbrain_page_hash`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var slug, hash string
		if err := rows.Scan(&slug, &hash); err != nil {
			return nil, err
		}
		out[slug] = hash
	}
	return out, rows.Err()
}

// UpsertPageHash records a page's content hash after a successful write.
func (s *SQLitePageHashStore) UpsertPageHash(ctx context.Context, slug, hash string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO gbrain_page_hash(slug, hash) VALUES(?, ?)
		 ON CONFLICT(slug) DO UPDATE SET hash=excluded.hash`, slug, hash)
	return err
}
