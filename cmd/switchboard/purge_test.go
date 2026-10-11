package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/store"
)

func purgeTestDB(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "switchboard.db")
}

func seedPurgeEvent(t *testing.T, dbPath, chatID, id string) {
	t.Helper()
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	_, err = st.Append(context.Background(), event.Message{
		ID: id, Platform: "slack", AccountID: "slack:T:U", ChatID: chatID,
		ChatType: event.ChatGroup, TS: now, Kind: event.KindText, Text: "fixture",
		Source: event.SourceLive, IngestedAt: now,
	}, nil)
	if closeErr := st.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func seedPurgeMedia(t *testing.T, dbPath, chatID, eventID, hash string) {
	t.Helper()
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.DB().Exec(`INSERT INTO media(id,event_id,chat_id,platform,idx,msg_ts,hash,size,stored,status,created_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`, eventID+"#0", eventID, chatID, "slack", 0,
		time.Now().UTC().Format(time.RFC3339Nano), hash, 5, 1, store.MediaStored, time.Now().UTC().Format(time.RFC3339Nano))
	if closeErr := st.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func queryCount(t *testing.T, dbPath, query string, args ...any) int {
	t.Helper()
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var count int
	if err := st.DB().QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func invokePurge(dbPath, chatID, confirm, mediaRoot string) error {
	args := []string{"--db", dbPath, "--chat-id", chatID, "--confirm", confirm}
	if mediaRoot != "" {
		args = append(args, "--media-root", mediaRoot)
	}
	return cmdPurgeChat(context.Background(), args)
}

func TestPurgeChatMismatchedConfirmLeavesDatabaseUnchanged(t *testing.T) {
	dbPath := purgeTestDB(t)
	seedPurgeEvent(t, dbPath, "slack:T:C1", "slack:T:C1:message-1")
	if err := invokePurge(dbPath, "slack:T:C1", "slack:T:OTHER", ""); err == nil || !strings.Contains(err.Error(), "same nonempty ID") {
		t.Fatalf("purge error=%v", err)
	}
	if got := queryCount(t, dbPath, `SELECT COUNT(*) FROM event_log WHERE chat_id=?`, "slack:T:C1"); got != 1 {
		t.Fatalf("event count after rejected purge=%d", got)
	}
	if got := queryCount(t, dbPath, `SELECT COUNT(*) FROM purged_chats WHERE chat_id=?`, "slack:T:C1"); got != 0 {
		t.Fatalf("purge tombstones after rejected purge=%d", got)
	}
}

func TestPurgeChatRefusesLiveStoreWriterLease(t *testing.T) {
	dbPath := purgeTestDB(t)
	seedPurgeEvent(t, dbPath, "slack:T:C1", "slack:T:C1:message-1")
	writer, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	err = invokePurge(dbPath, "slack:T:C1", "slack:T:C1", "")
	if err == nil || !strings.Contains(err.Error(), "already open") {
		t.Fatalf("expected active writer lease refusal, got %v", err)
	}
	var got int
	if err := writer.DB().QueryRow(`SELECT COUNT(*) FROM event_log WHERE chat_id=?`, "slack:T:C1").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("event count after lease refusal=%d", got)
	}
}

func TestPurgeChatRequiresMediaRootForStoredMedia(t *testing.T) {
	dbPath := purgeTestDB(t)
	chatID, eventID := "slack:T:C1", "slack:T:C1:message-1"
	seedPurgeEvent(t, dbPath, chatID, eventID)
	seedPurgeMedia(t, dbPath, chatID, eventID, strings.Repeat("a", 64))
	err := invokePurge(dbPath, chatID, chatID, "")
	if err == nil || !strings.Contains(err.Error(), "supply its configured --media-root") {
		t.Fatalf("expected media-root requirement, got %v", err)
	}
	if got := queryCount(t, dbPath, `SELECT COUNT(*) FROM media WHERE chat_id=?`, chatID); got != 1 {
		t.Fatalf("media count after rejected purge=%d", got)
	}
	if got := queryCount(t, dbPath, `SELECT COUNT(*) FROM event_log WHERE chat_id=?`, chatID); got != 1 {
		t.Fatalf("event count after rejected purge=%d", got)
	}
}

func putTestBlob(t *testing.T, root, hash string) string {
	t.Helper()
	dir := filepath.Join(root, hash[:2])
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, hash)
	if err := os.WriteFile(path, []byte("blob:"+hash), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPurgeChatRemovesOrphanBlobAndPreservesSharedBlob(t *testing.T) {
	dbPath := purgeTestDB(t)
	mediaRoot := filepath.Join(t.TempDir(), "media")
	chatID, keepChatID := "slack:T:C1", "slack:T:C2"
	deleteEvent, keepEvent := "slack:T:C1:message-delete", "slack:T:C2:message-keep"
	seedPurgeEvent(t, dbPath, chatID, deleteEvent)
	seedPurgeEvent(t, dbPath, keepChatID, keepEvent)
	orphanHash, sharedHash := strings.Repeat("a", 64), strings.Repeat("b", 64)
	seedPurgeMedia(t, dbPath, chatID, deleteEvent, orphanHash)
	seedPurgeMedia(t, dbPath, chatID, deleteEvent+"-shared", sharedHash)
	seedPurgeMedia(t, dbPath, keepChatID, keepEvent, sharedHash)
	orphanPath := putTestBlob(t, mediaRoot, orphanHash)
	sharedPath := putTestBlob(t, mediaRoot, sharedHash)

	var stdout bytes.Buffer
	originalStdout := os.Stdout
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = write
	callErr := invokePurge(dbPath, chatID, chatID, mediaRoot)
	_ = write.Close()
	os.Stdout = originalStdout
	if _, err := stdout.ReadFrom(read); err != nil {
		t.Fatal(err)
	}
	_ = read.Close()
	if callErr != nil {
		t.Fatalf("purge failed: %v", callErr)
	}
	var result store.PurgeResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("purge output %q: %v", stdout.String(), err)
	}
	if result.Messages != 1 || result.Media != 2 || result.Events != 1 {
		t.Fatalf("purge result=%+v", result)
	}
	if len(result.OrphanHashes) != 1 || result.OrphanHashes[0] != orphanHash {
		t.Fatalf("orphan hashes=%v", result.OrphanHashes)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("orphan blob still exists or stat failed: %v", err)
	}
	if _, err := os.Stat(sharedPath); err != nil {
		t.Fatalf("shared blob was removed: %v", err)
	}
	if got := queryCount(t, dbPath, `SELECT COUNT(*) FROM event_log WHERE chat_id=?`, chatID); got != 0 {
		t.Fatalf("purged event count=%d", got)
	}
	if got := queryCount(t, dbPath, `SELECT COUNT(*) FROM media WHERE chat_id=?`, chatID); got != 0 {
		t.Fatalf("purged media count=%d", got)
	}
	if got := queryCount(t, dbPath, `SELECT COUNT(*) FROM media WHERE hash=? AND stored=1`, sharedHash); got != 1 {
		t.Fatalf("shared media references=%d", got)
	}
}
