package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
)

var ErrChatPurged = errors.New("chat capture disabled by local purge")

const purgeSchema = `
CREATE TABLE IF NOT EXISTS purged_chats (
    chat_id TEXT PRIMARY KEY
);
`

// PurgeResult reports database rows removed by PurgeChat. OrphanHashes lists
// stored content hashes with no remaining stored media references. Filesystem
// cleanup is deliberately left to the caller.
type PurgeResult struct {
	Events       int      `json:"events"`
	Messages     int      `json:"messages"`
	Media        int      `json:"media"`
	OrphanHashes []string `json:"orphan_hashes"`
}

// PurgeChat removes this chat's locally stored message data and records a
// durable tombstone so callers can prevent future capture. It never unlinks
// media files; the caller may remove only the returned orphan hashes.
func (s *Store) PurgeChat(ctx context.Context, chatID string) (PurgeResult, error) {
	if chatID == "" {
		return PurgeResult{}, errors.New("purge chat: empty chat id")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PurgeResult{}, fmt.Errorf("purge chat: begin transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO purged_chats(chat_id) VALUES (?)`, chatID); err != nil {
		return PurgeResult{}, fmt.Errorf("purge chat: record tombstone: %w", err)
	}

	messageIDs, err := queryStrings(ctx, tx, `SELECT id FROM messages WHERE chat_id=?`, chatID)
	if err != nil {
		return PurgeResult{}, fmt.Errorf("purge chat: list message ids: %w", err)
	}
	hashes, err := queryStrings(ctx, tx, `SELECT DISTINCT hash FROM media WHERE chat_id=? AND stored=1 AND hash!=''`, chatID)
	if err != nil {
		return PurgeResult{}, fmt.Errorf("purge chat: list media hashes: %w", err)
	}
	result := PurgeResult{OrphanHashes: []string{}}

	if err := deleteCount(ctx, tx, &result.Events, `DELETE FROM event_log WHERE chat_id=?`, chatID); err != nil {
		return PurgeResult{}, fmt.Errorf("purge chat: delete events: %w", err)
	}
	if err := deleteCount(ctx, tx, nil, `DELETE FROM messages_fts WHERE chat_id=?`, chatID); err != nil {
		return PurgeResult{}, fmt.Errorf("purge chat: delete full-text rows: %w", err)
	}
	if exists, err := tableExists(ctx, tx, "message_versions"); err != nil {
		return PurgeResult{}, fmt.Errorf("purge chat: inspect message versions: %w", err)
	} else if exists && len(messageIDs) > 0 {
		if err := deleteIDs(ctx, tx, "message_versions", messageIDs); err != nil {
			return PurgeResult{}, fmt.Errorf("purge chat: delete message versions: %w", err)
		}
	}
	if err := deleteCount(ctx, tx, &result.Messages, `DELETE FROM messages WHERE chat_id=?`, chatID); err != nil {
		return PurgeResult{}, fmt.Errorf("purge chat: delete messages: %w", err)
	}
	if err := deleteCount(ctx, tx, &result.Media, `DELETE FROM media WHERE chat_id=?`, chatID); err != nil {
		return PurgeResult{}, fmt.Errorf("purge chat: delete media: %w", err)
	}
	if err := deleteCount(ctx, tx, nil, `DELETE FROM cursors WHERE chat_id=?`, chatID); err != nil {
		return PurgeResult{}, fmt.Errorf("purge chat: delete cursor: %w", err)
	}
	if err := deleteCount(ctx, tx, nil, `DELETE FROM chats WHERE chat_id=?`, chatID); err != nil {
		return PurgeResult{}, fmt.Errorf("purge chat: delete chat summary: %w", err)
	}

	for _, hash := range hashes {
		var remaining int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM media WHERE stored=1 AND hash=?`, hash).Scan(&remaining); err != nil {
			return PurgeResult{}, fmt.Errorf("purge chat: check media hash references: %w", err)
		}
		if remaining == 0 {
			result.OrphanHashes = append(result.OrphanHashes, hash)
		}
	}
	sort.Strings(result.OrphanHashes)

	if err := tx.Commit(); err != nil {
		return PurgeResult{}, fmt.Errorf("purge chat: commit: %w", err)
	}
	return result, nil
}

func queryStrings(ctx context.Context, tx *sql.Tx, query string, arg any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func deleteCount(ctx context.Context, tx *sql.Tx, count *int, query string, arg any) error {
	result, err := tx.ExecContext(ctx, query, arg)
	if err != nil {
		return err
	}
	if count != nil {
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		*count = int(n)
	}
	return nil
}

func tableExists(ctx context.Context, tx *sql.Tx, name string) (bool, error) {
	var found int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&found)
	return found > 0, err
}

func deleteIDs(ctx context.Context, tx *sql.Tx, table string, ids []string) error {
	// The table name is a fixed internal constant supplied by PurgeChat.
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE id=?`, id); err != nil {
			return err
		}
	}
	return nil
}
