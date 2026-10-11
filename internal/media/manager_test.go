package media

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/store"
)

// fakeDownloader serves canned bytes per fetchRef.
type fakeDownloader struct {
	data  map[string][]byte
	calls int
}

func (f *fakeDownloader) DownloadMedia(_ context.Context, ref string) (io.ReadCloser, int64, error) {
	f.calls++
	b, ok := f.data[ref]
	if !ok {
		return nil, 0, io.ErrUnexpectedEOF
	}
	return io.NopCloser(bytes.NewReader(b)), int64(len(b)), nil
}

func newMgr(t *testing.T, cfg Config) (*Manager, *store.Store, *fakeDownloader) {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Root = t.TempDir()
	m, err := NewManager(st, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	fd := &fakeDownloader{data: map[string][]byte{}}
	m.RegisterDownloader("whatsapp", fd)
	return m, st, fd
}

func seedChat(t *testing.T, st *store.Store, chatID, importance string) {
	t.Helper()
	_, err := st.DB().Exec(
		`INSERT INTO chats(chat_id, platform, chat_type, last_ts, importance) VALUES (?,?,?,?,?)`,
		chatID, "whatsapp", "dm", time.Now().UTC().Format(time.RFC3339), importance)
	if err != nil {
		t.Fatal(err)
	}
}

func enqueue(t *testing.T, st *store.Store, id, chatID, ref string, ts time.Time) {
	t.Helper()
	// Type "file" (not image) so the cap/gate/dedup tests — which use arbitrary canned
	// bytes — aren't subject to the #48 image content-magic guard (tested separately in
	// TestContentMagicGuard). These tests exercise capacity/gating logic, not content.
	_, err := st.EnqueueMedia(context.Background(), []store.MediaItem{{
		ID: id, EventID: "e-" + id, ChatID: chatID, Platform: "whatsapp", Idx: 0,
		Type: "file", MIME: "application/octet-stream", Name: id + ".bin", FetchRef: ref, MsgTS: ts,
	}})
	if err != nil {
		t.Fatal(err)
	}
}

// TestContentMagicGuard proves the #48 guard: declared-image bytes that aren't a real
// image (undecrypted ciphertext) are rejected (failed, NOT stored), while valid image
// magic is stored — and the guard only applies to images (a "file" with binary bytes is
// stored untouched).
func TestContentMagicGuard(t *testing.T) {
	m, st, fd := newMgr(t, Config{CapBytes: 1 << 20})
	ctx := context.Background()
	seedChat(t, st, "whatsapp:imp", "important")

	// Ciphertext-like bytes declared as image → rejected.
	fd.data["r-cipher"] = []byte{0xA3, 0x98, 0xA6, 0x52, 0x01, 0x02, 0x03, 0x04}
	_, _ = st.EnqueueMedia(ctx, []store.MediaItem{{ID: "cipher", EventID: "e1", ChatID: "whatsapp:imp", Platform: "whatsapp", Idx: 0, Type: "image", MIME: "image/jpeg", FetchRef: "r-cipher", MsgTS: time.Now().UTC()}})
	// Real JPEG magic declared as image → stored.
	fd.data["r-jpeg"] = append([]byte{0xFF, 0xD8, 0xFF, 0xE0}, bytes.Repeat([]byte{0}, 16)...)
	_, _ = st.EnqueueMedia(ctx, []store.MediaItem{{ID: "jpeg", EventID: "e2", ChatID: "whatsapp:imp", Platform: "whatsapp", Idx: 0, Type: "image", MIME: "image/jpeg", FetchRef: "r-jpeg", MsgTS: time.Now().UTC()}})

	m.drain(ctx)

	cipher, _, _ := st.MediaByID(ctx, "cipher")
	if cipher.Stored {
		t.Error("ciphertext declared as image must NOT be stored")
	}
	jpeg, _, _ := st.MediaByID(ctx, "jpeg")
	if !jpeg.Stored {
		t.Error("valid JPEG magic must be stored")
	}
}

func TestPrivateRootAndTemporaryBlobModes(t *testing.T) {
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	root := filepath.Join(t.TempDir(), "owned-media")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(st, Config{Root: root}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(root); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("root mode/info = %v, %v", info, err)
	}
	_, _, _, tmp, err := m.streamToTemp(bytes.NewReader([]byte("private media")))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp)
	if info, err := os.Stat(tmp); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("temporary blob mode/info = %v, %v", info, err)
	}
}

func TestManagerGateSkipsUnimportant(t *testing.T) {
	m, st, fd := newMgr(t, Config{})
	ctx := context.Background()
	seedChat(t, st, "whatsapp:low", "low")
	fd.data["r1"] = []byte("hello")
	enqueue(t, st, "m1", "whatsapp:low", "r1", time.Now())

	m.drain(ctx)

	if fd.calls != 0 {
		t.Fatalf("low-importance chat must not download, got %d calls", fd.calls)
	}
	row, _, _ := st.MediaByID(ctx, "m1")
	if row.Status != store.MediaSkipped {
		t.Fatalf("want skipped, got %q", row.Status)
	}
}

func TestManagerDefersUnscored(t *testing.T) {
	m, st, fd := newMgr(t, Config{})
	ctx := context.Background()
	// No chat row at all → importance "" → defer (stay pending, no download).
	fd.data["r1"] = []byte("hello")
	enqueue(t, st, "m1", "whatsapp:unscored", "r1", time.Now())

	m.drain(ctx)

	if fd.calls != 0 {
		t.Fatalf("unscored chat must defer, got %d calls", fd.calls)
	}
	row, _, _ := st.MediaByID(ctx, "m1")
	if row.Status != store.MediaPending {
		t.Fatalf("want still pending, got %q", row.Status)
	}
}

func TestManagerOnIngestRefreshesFailedDerivedRefAndRetries(t *testing.T) {
	m, st, fd := newMgr(t, Config{})
	ctx := context.Background()
	seedChat(t, st, "whatsapp:imp", "important")
	now := time.Now()

	enqueue(t, st, "msg1#0", "whatsapp:imp", "legacy-path", now)
	if err := st.MarkMediaFailed(ctx, "msg1#0", "download: stale path"); err != nil {
		t.Fatal(err)
	}
	fd.data["rich-ref"] = append([]byte{0xFF, 0xD8, 0xFF, 0xE0}, []byte("downloadable")...) // valid JPEG magic for the #48 content-guard
	m.RegisterRefDeriver("whatsapp", func(raw []byte) string {
		if string(raw) != `{"media":"raw"}` {
			t.Fatalf("unexpected raw payload: %s", raw)
		}
		return "rich-ref"
	})

	m.OnIngest(event.Message{
		ID:       "msg1",
		Platform: "whatsapp",
		ChatID:   "whatsapp:imp",
		TS:       now,
		Raw:      []byte(`{"media":"raw"}`),
		Attachments: []event.Attachment{{
			Type: "image",
			MIME: "image/jpeg",
			Ref:  "legacy-path",
			Name: "old.jpg",
		}},
	})
	row, _, _ := st.MediaByID(ctx, "msg1#0")
	if row.Status != store.MediaPending || row.FetchRef != "rich-ref" {
		t.Fatalf("ingest should refresh failed media to pending with rich ref, got %+v", row)
	}

	m.drain(ctx)

	row, _, _ = st.MediaByID(ctx, "msg1#0")
	if row.Status != store.MediaStored || row.FetchRef != "rich-ref" || !row.Stored {
		t.Fatalf("refreshed failed media should download, got %+v", row)
	}
	if fd.calls != 1 {
		t.Fatalf("download calls=%d, want 1", fd.calls)
	}
}

func TestManagerOnIngestDoesNotRetryLowImportanceSkippedMedia(t *testing.T) {
	m, st, fd := newMgr(t, Config{})
	ctx := context.Background()
	seedChat(t, st, "whatsapp:low", "low")
	now := time.Now()

	enqueue(t, st, "msg1#0", "whatsapp:low", "legacy-path", now)
	if err := st.MarkMediaSkipped(ctx, "msg1#0", "chat importance=low"); err != nil {
		t.Fatal(err)
	}
	fd.data["rich-ref"] = append([]byte{0xFF, 0xD8, 0xFF, 0xE0}, []byte("downloadable")...) // valid JPEG magic for the #48 content-guard
	m.RegisterRefDeriver("whatsapp", func(raw []byte) string { return "rich-ref" })

	m.OnIngest(event.Message{
		ID:       "msg1",
		Platform: "whatsapp",
		ChatID:   "whatsapp:low",
		TS:       now,
		Raw:      []byte(`{"media":"raw"}`),
		Attachments: []event.Attachment{{
			Type: "image",
			MIME: "image/jpeg",
			Ref:  "legacy-path",
			Name: "old.jpg",
		}},
	})
	m.drain(ctx)

	if fd.calls != 0 {
		t.Fatalf("low-importance skipped media must not download, got %d calls", fd.calls)
	}
	row, _, _ := st.MediaByID(ctx, "msg1#0")
	if row.Status != store.MediaSkipped || row.FetchRef != "rich-ref" {
		t.Fatalf("low-importance skipped row should refresh ref only, got %+v", row)
	}
}

func TestManagerDownloadsAndDedups(t *testing.T) {
	m, st, fd := newMgr(t, Config{})
	ctx := context.Background()
	seedChat(t, st, "whatsapp:imp", "important")
	// Two attachments, IDENTICAL bytes (different refs) → one blob on disk.
	fd.data["r1"] = []byte("same-content")
	fd.data["r2"] = []byte("same-content")
	fd.data["r3"] = []byte("other-content")
	now := time.Now()
	enqueue(t, st, "m1", "whatsapp:imp", "r1", now)
	enqueue(t, st, "m2", "whatsapp:imp", "r2", now)
	enqueue(t, st, "m3", "whatsapp:imp", "r3", now)

	m.drain(ctx)

	for _, id := range []string{"m1", "m2", "m3"} {
		row, _, _ := st.MediaByID(ctx, id)
		if row.Status != store.MediaStored || !row.Stored || row.Hash == "" {
			t.Fatalf("%s not stored: %+v", id, row)
		}
	}
	// Dedup: m1 and m2 share a hash; 2 distinct blobs on disk total.
	blobs := countBlobs(t, m.cfg.Root)
	if blobs != 2 {
		t.Fatalf("want 2 deduped blobs on disk, got %d", blobs)
	}
	// Dedup-aware byte accounting: 12 + 13 = 25 (not 12+12+13).
	got, _ := st.StoredBytes(ctx)
	if got != int64(len("same-content")+len("other-content")) {
		t.Fatalf("stored bytes = %d, want %d", got, len("same-content")+len("other-content"))
	}
}

func TestManagerSkipsOversize(t *testing.T) {
	m, st, fd := newMgr(t, Config{PerFileMax: 4})
	ctx := context.Background()
	seedChat(t, st, "whatsapp:imp", "important")
	fd.data["r1"] = []byte("waytoobig")
	enqueue(t, st, "m1", "whatsapp:imp", "r1", time.Now())

	m.drain(ctx)

	row, _, _ := st.MediaByID(ctx, "m1")
	if row.Status != store.MediaSkipped {
		t.Fatalf("oversize want skipped, got %q", row.Status)
	}
	if countBlobs(t, m.cfg.Root) != 0 {
		t.Fatal("oversize media must not be written to disk")
	}
}

func TestManagerEvictsAndUnlinks(t *testing.T) {
	m, st, fd := newMgr(t, Config{CapBytes: 20}) // tiny cap to force eviction
	ctx := context.Background()
	seedChat(t, st, "whatsapp:imp", "important")
	base := time.Date(2026, 6, 10, 8, 0, 0, 0, time.UTC)
	// 3 distinct 10-byte blobs, oldest→newest.
	for i, id := range []string{"old", "mid", "new"} {
		ref := "r-" + id
		fd.data[ref] = []byte("0123456789") // 10 bytes, distinct refs but same content...
		// make content distinct so they're 3 blobs:
		fd.data[ref] = bytes.Repeat([]byte{byte('a' + i)}, 10)
		enqueue(t, st, id, "whatsapp:imp", ref, base.Add(time.Duration(i)*time.Hour))
	}
	m.drain(ctx)           // stores all 3 = 30 bytes
	m.enforceCapacity(ctx) // cap 20 → evict oldest until <=20 → "old" goes

	if b, _ := st.StoredBytes(ctx); b > 20 {
		t.Fatalf("over cap after evict: %d", b)
	}
	oldRow, _, _ := st.MediaByID(ctx, "old")
	if oldRow.Status != store.MediaEvicted || oldRow.Stored {
		t.Fatalf("oldest should be evicted, got %+v", oldRow)
	}
	// The evicted blob's file must be unlinked; newer ones remain.
	if countBlobs(t, m.cfg.Root) != 2 {
		t.Fatalf("want 2 blobs after evicting 1, got %d", countBlobs(t, m.cfg.Root))
	}
	// Re-fetch the evicted one via Get → re-downloads (gate bypassed) + serves.
	preCalls := fd.calls
	row, rc, err := m.Get(ctx, "old")
	if err != nil {
		t.Fatalf("get evicted: %v", err)
	}
	defer rc.Close()
	if fd.calls != preCalls+1 {
		t.Fatal("Get on evicted media should re-download")
	}
	b, _ := io.ReadAll(rc)
	if len(b) != 10 || !row.Stored {
		t.Fatalf("re-fetched bytes wrong: len=%d stored=%v", len(b), row.Stored)
	}
}

func TestManagerCapAwareSkipsOlderWhenFull(t *testing.T) {
	m, st, fd := newMgr(t, Config{CapBytes: 20})
	ctx := context.Background()
	seedChat(t, st, "whatsapp:imp", "important")
	base := time.Date(2026, 6, 10, 8, 0, 0, 0, time.UTC)
	// Fill the cap with two NEWER 10-byte blobs.
	for i, id := range []string{"new1", "new2"} {
		ref := "r-" + id
		fd.data[ref] = bytes.Repeat([]byte{byte('a' + i)}, 10)
		enqueue(t, st, id, "whatsapp:imp", ref, base.Add(time.Duration(10+i)*time.Hour))
	}
	m.drain(ctx)
	if b, _ := st.StoredBytes(ctx); b != 20 {
		t.Fatalf("setup: stored=%d want 20", b)
	}
	callsAfterFill := fd.calls

	// Now enqueue an OLDER item. At cap + older than the frontier, it must NOT be
	// downloaded (would be evicted instantly) — no platform call, stays pending.
	fd.data["r-old"] = bytes.Repeat([]byte{'z'}, 10)
	enqueue(t, st, "old", "whatsapp:imp", "r-old", base) // oldest
	m.drain(ctx)

	if fd.calls != callsAfterFill {
		t.Fatalf("older media at cap must NOT download (no platform call); calls %d→%d", callsAfterFill, fd.calls)
	}
	row, _, _ := st.MediaByID(ctx, "old")
	if row.Status != store.MediaPending {
		t.Fatalf("older media should stay pending (not skipped/failed), got %q", row.Status)
	}
}

func countBlobs(t *testing.T, root string) int {
	t.Helper()
	n := 0
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		// ignore leftover temp files
		if filepath.Base(p)[0] == '.' {
			return nil
		}
		n++
		return nil
	})
	return n
}
