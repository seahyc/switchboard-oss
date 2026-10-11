package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
)

const lifecycleSchema = `CREATE TABLE IF NOT EXISTS message_versions (
 id TEXT PRIMARY KEY, version_ts TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0
);`

func projectLifecycle(ctx context.Context, tx *sql.Tx, m event.Message, selfIDs map[string]bool) error {
	target := m.ID
	if m.TargetID != "" {
		target = m.TargetID
	}
	var version string
	var deleted int
	err := tx.QueryRowContext(ctx, `SELECT version_ts,deleted FROM message_versions WHERE id=?`, target).Scan(&version, &deleted)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	mutation := m.Kind == event.KindEdit || m.Kind == event.KindDelete
	if err == nil {
		if deleted != 0 || !mutation {
			return nil
		}
		old, _ := time.Parse(time.RFC3339Nano, version)
		changed := m.TS
		if m.EditedTS != nil {
			changed = *m.EditedTS
		}
		if m.Kind != event.KindDelete && !changed.After(old) {
			return nil
		}
	}
	var original event.Message
	var oldBody string
	originalExists := false
	err = tx.QueryRowContext(ctx, `SELECT body FROM messages WHERE id=?`, target).Scan(&oldBody)
	if err == nil {
		if err = json.Unmarshal([]byte(oldBody), &original); err != nil {
			return err
		}
		originalExists = true
	} else if err != sql.ErrNoRows {
		return err
	}
	projected := m
	projected.ID = target
	projected.TargetID = ""
	if mutation && originalExists {
		projected = original
		if m.Kind == event.KindEdit {
			projected.Text = m.Text
			projected.Attachments = m.Attachments
			projected.UI = m.UI
			projected.EditedTS = m.EditedTS
			projected.Raw = m.Raw
		}
	}
	if m.Kind == event.KindDelete {
		projected.Kind = event.KindDelete
		projected.Deleted = true
		projected.Text = ""
		projected.Raw = nil
		projected.Attachments = nil
		projected.UI = event.UI{}
		projected.Reactions = nil
		projected.Mentions = nil
	}
	if mutation {
		changed := m.TS
		if m.EditedTS != nil {
			changed = *m.EditedTS
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO message_versions(id,version_ts,deleted) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET version_ts=excluded.version_ts,deleted=excluded.deleted`, target, changed.UTC().Format(time.RFC3339Nano), boolInt(projected.Deleted)); err != nil {
			return err
		}
	}
	body, err := json.Marshal(projected)
	if err != nil {
		return err
	}
	if err = projectMessage(ctx, tx, projected, body, selfIDs); err != nil {
		return err
	}
	if !originalExists && !projected.Deleted {
		if err = projectChat(ctx, tx, projected); err != nil {
			return err
		}
	}
	// Edits and deletions must update the chat preview without adding a message.
	if mutation {
		_, err = tx.ExecContext(ctx, `UPDATE chats SET
   msg_count=(SELECT COUNT(*) FROM messages WHERE chat_id=? AND deleted=0),
   last_text=COALESCE((SELECT text FROM messages WHERE chat_id=? AND deleted=0 ORDER BY ts DESC LIMIT 1),'')
   WHERE chat_id=?`, projected.ChatID, projected.ChatID, projected.ChatID)
	}
	return err
}
