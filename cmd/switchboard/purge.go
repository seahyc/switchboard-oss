package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/seahyc/switchboard-oss/internal/api"
	"github.com/seahyc/switchboard-oss/internal/store"
)

func cmdPurgeChat(ctx context.Context, args []string) error {
	fs, _ := newFS("purge-chat")
	db := fs.String("db", filepath.Join(api.DefaultStateDir(), "switchboard.db"), "local database path; daemon must be stopped")
	chat := fs.String("chat-id", "", "chat to erase from the local archive and stop capturing")
	confirm := fs.String("confirm", "", "repeat the exact chat ID to confirm local deletion")
	root := fs.String("media-root", "", "configured blob root; required when this chat has stored media")
	fs.Parse(args)
	if *chat == "" || *confirm != *chat {
		return fmt.Errorf("purge requires --chat-id ID --confirm ID with the same nonempty ID")
	}
	if _, err := os.Stat(*db); err != nil {
		return fmt.Errorf("existing database required: %w", err)
	}
	st, err := store.Open(*db)
	if err != nil {
		return err
	}
	defer st.Close()
	var mediaCount int
	if err = st.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM media WHERE chat_id=? AND stored=1`, *chat).Scan(&mediaCount); err != nil {
		return err
	}
	if mediaCount > 0 && *root == "" {
		return fmt.Errorf("this chat has stored media; supply its configured --media-root before purging")
	}
	if *root != "" {
		info, err := os.Lstat(*root)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("media root must be a real directory")
		}
	}
	result, err := st.PurgeChat(ctx, *chat)
	if err != nil {
		return err
	}
	for _, hash := range result.OrphanHashes {
		if len(hash) != 64 {
			return fmt.Errorf("database purge committed; invalid orphan hash")
		}
		if _, err = hex.DecodeString(hash); err != nil {
			return fmt.Errorf("database purge committed; invalid orphan hash")
		}
		// Match the media manager's content-addressed layout.
		dir := filepath.Join(*root, hash[:2])
		if info, err := os.Lstat(dir); err == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
			return fmt.Errorf("database purge committed; unsafe blob directory")
		}
		if err = os.Remove(filepath.Join(dir, hash)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("database purge committed; blob cleanup failed: %w", err)
		}
	}
	if _, err = st.DB().ExecContext(ctx, "VACUUM"); err != nil {
		return fmt.Errorf("database purge committed; compaction failed: %w", err)
	}
	if _, err = st.DB().ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("database purge committed; checkpoint failed: %w", err)
	}
	return emit(result)
}
