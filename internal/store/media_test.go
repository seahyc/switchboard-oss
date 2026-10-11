package store

import (
	"context"
	"testing"
	"time"
)

func mediaItem(id string, ts time.Time) MediaItem {
	return MediaItem{
		ID: id, EventID: "evt-" + id, ChatID: "whatsapp:c1", Platform: "whatsapp",
		Idx: 0, Type: "image", MIME: "image/jpeg", Name: id + ".jpg",
		FetchRef: "ref-" + id, MsgTS: ts,
	}
}

func TestMediaEnqueueIdempotent(t *testing.T) {
	st, _ := Open(":memory:")
	defer st.Close()
	ctx := context.Background()
	t0 := time.Date(2026, 6, 10, 8, 0, 0, 0, time.UTC)

	n, err := st.EnqueueMedia(ctx, []MediaItem{mediaItem("a", t0), mediaItem("b", t0)})
	if err != nil || n != 2 {
		t.Fatalf("first enqueue: n=%d err=%v", n, err)
	}
	// Re-enqueue same ids + one new → only the new one counts; existing untouched.
	n, err = st.EnqueueMedia(ctx, []MediaItem{mediaItem("a", t0), mediaItem("c", t0)})
	if err != nil || n != 1 {
		t.Fatalf("idempotent enqueue: n=%d err=%v", n, err)
	}
	pend, err := st.NextPendingMedia(ctx, 10)
	if err != nil || len(pend) != 3 {
		t.Fatalf("pending: %d err=%v", len(pend), err)
	}
}

func TestMediaEnqueueRefreshesFailedWithNewRef(t *testing.T) {
	st, _ := Open(":memory:")
	defer st.Close()
	ctx := context.Background()
	t0 := time.Date(2026, 6, 10, 8, 0, 0, 0, time.UTC)

	it := mediaItem("a", t0)
	if _, err := st.EnqueueMedia(ctx, []MediaItem{it}); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMediaFailed(ctx, "a", "download: stale path"); err != nil {
		t.Fatal(err)
	}
	it.FetchRef = "rich-ref"
	n, err := st.EnqueueMedia(ctx, []MediaItem{it})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("refresh should not count as a new enqueue, got %d", n)
	}
	row, ok, err := st.MediaByID(ctx, "a")
	if err != nil || !ok {
		t.Fatalf("row: ok=%v err=%v", ok, err)
	}
	if row.Status != MediaPending || row.FetchRef != "rich-ref" {
		t.Fatalf("failed row should be refreshed and retried, got %+v", row)
	}
}

func TestMediaEnqueueDoesNotRetryFailedWithSameRef(t *testing.T) {
	st, _ := Open(":memory:")
	defer st.Close()
	ctx := context.Background()
	t0 := time.Date(2026, 6, 10, 8, 0, 0, 0, time.UTC)

	it := mediaItem("a", t0)
	if _, err := st.EnqueueMedia(ctx, []MediaItem{it}); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMediaFailed(ctx, "a", "download: transient"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.EnqueueMedia(ctx, []MediaItem{it}); err != nil {
		t.Fatal(err)
	}
	row, _, _ := st.MediaByID(ctx, "a")
	if row.Status != MediaFailed {
		t.Fatalf("same-ref failed row should stay failed, got %+v", row)
	}
}

func TestMediaEnqueueRefreshesPendingAndEvictedRefs(t *testing.T) {
	st, _ := Open(":memory:")
	defer st.Close()
	ctx := context.Background()
	t0 := time.Date(2026, 6, 10, 8, 0, 0, 0, time.UTC)

	pending := mediaItem("pending", t0)
	evicted := mediaItem("evicted", t0.Add(time.Hour))
	if _, err := st.EnqueueMedia(ctx, []MediaItem{pending, evicted}); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMediaStored(ctx, "evicted", "hash-evicted", 100); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.EvictToTarget(ctx, 0); err != nil {
		t.Fatal(err)
	}

	pending.FetchRef = "rich-pending"
	evicted.FetchRef = "rich-evicted"
	if _, err := st.EnqueueMedia(ctx, []MediaItem{pending, evicted}); err != nil {
		t.Fatal(err)
	}
	row, _, _ := st.MediaByID(ctx, "pending")
	if row.Status != MediaPending || row.FetchRef != "rich-pending" {
		t.Fatalf("pending row not refreshed: %+v", row)
	}
	row, _, _ = st.MediaByID(ctx, "evicted")
	if row.Status != MediaEvicted || row.FetchRef != "rich-evicted" {
		t.Fatalf("evicted row should refresh ref without becoming pending: %+v", row)
	}
}

func TestMediaEnqueueDoesNotTouchStoredRows(t *testing.T) {
	st, _ := Open(":memory:")
	defer st.Close()
	ctx := context.Background()
	t0 := time.Date(2026, 6, 10, 8, 0, 0, 0, time.UTC)

	it := mediaItem("stored", t0)
	if _, err := st.EnqueueMedia(ctx, []MediaItem{it}); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMediaStored(ctx, "stored", "hash-stored", 100); err != nil {
		t.Fatal(err)
	}
	it.FetchRef = "rich-ref"
	if _, err := st.EnqueueMedia(ctx, []MediaItem{it}); err != nil {
		t.Fatal(err)
	}
	row, _, _ := st.MediaByID(ctx, "stored")
	if row.Status != MediaStored || !row.Stored || row.FetchRef != "ref-stored" {
		t.Fatalf("stored row should be untouched, got %+v", row)
	}
}

func TestMediaEnqueueSkippedChatImportanceRetryOnlyWhenImportant(t *testing.T) {
	st, _ := Open(":memory:")
	defer st.Close()
	ctx := context.Background()
	t0 := time.Date(2026, 6, 10, 8, 0, 0, 0, time.UTC)

	if _, err := st.DB().Exec(
		`INSERT INTO chats(chat_id, platform, chat_type, last_ts, importance) VALUES (?,?,?,?,?)`,
		"whatsapp:low", "whatsapp", "dm", t0.Format(time.RFC3339Nano), "low"); err != nil {
		t.Fatal(err)
	}
	it := mediaItem("skipped", t0)
	it.ChatID = "whatsapp:low"
	if _, err := st.EnqueueMedia(ctx, []MediaItem{it}); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMediaSkipped(ctx, "skipped", "chat importance=low"); err != nil {
		t.Fatal(err)
	}

	it.FetchRef = "rich-ref"
	if _, err := st.EnqueueMedia(ctx, []MediaItem{it}); err != nil {
		t.Fatal(err)
	}
	row, _, _ := st.MediaByID(ctx, "skipped")
	if row.Status != MediaSkipped || row.FetchRef != "rich-ref" {
		t.Fatalf("low-importance skip should refresh ref but not retry, got %+v", row)
	}

	if _, err := st.DB().Exec(`UPDATE chats SET importance=? WHERE chat_id=?`, "important", "whatsapp:low"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.EnqueueMedia(ctx, []MediaItem{it}); err != nil {
		t.Fatal(err)
	}
	row, _, _ = st.MediaByID(ctx, "skipped")
	if row.Status != MediaPending {
		t.Fatalf("now-important chat skip should retry, got %+v", row)
	}
}

func TestMediaDedupBytesCountedOnce(t *testing.T) {
	st, _ := Open(":memory:")
	defer st.Close()
	ctx := context.Background()
	t0 := time.Date(2026, 6, 10, 8, 0, 0, 0, time.UTC)

	// Two attachments with IDENTICAL content (same hash) — bytes counted once.
	_, _ = st.EnqueueMedia(ctx, []MediaItem{mediaItem("a", t0), mediaItem("b", t0)})
	if err := st.MarkMediaStored(ctx, "a", "deadbeef", 1000); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMediaStored(ctx, "b", "deadbeef", 1000); err != nil {
		t.Fatal(err)
	}
	got, err := st.StoredBytes(ctx)
	if err != nil || got != 1000 {
		t.Fatalf("dedup bytes: want 1000 once, got %d err=%v", got, err)
	}
}

func TestMediaEvictByMessageRecency(t *testing.T) {
	st, _ := Open(":memory:")
	defer st.Close()
	ctx := context.Background()
	base := time.Date(2026, 6, 10, 8, 0, 0, 0, time.UTC)

	// 4 distinct blobs, 1000 bytes each, oldest→newest by msg_ts.
	for i, id := range []string{"old1", "old2", "new1", "new2"} {
		it := mediaItem(id, base.Add(time.Duration(i)*time.Hour))
		if _, err := st.EnqueueMedia(ctx, []MediaItem{it}); err != nil {
			t.Fatal(err)
		}
		if err := st.MarkMediaStored(ctx, id, "hash-"+id, 1000); err != nil {
			t.Fatal(err)
		}
	}
	if b, _ := st.StoredBytes(ctx); b != 4000 {
		t.Fatalf("pre-evict bytes = %d, want 4000", b)
	}

	// Evict down to 2000 → the 2 OLDEST by msg_ts (old1, old2) go.
	unlink, freed, err := st.EvictToTarget(ctx, 2000)
	if err != nil || freed != 2000 || len(unlink) != 2 {
		t.Fatalf("evict: freed=%d unlink=%d err=%v", freed, len(unlink), err)
	}
	gotHashes := map[string]bool{unlink[0].Hash: true, unlink[1].Hash: true}
	if !gotHashes["hash-old1"] || !gotHashes["hash-old2"] {
		t.Fatalf("evicted wrong blobs: %+v (should be the oldest)", unlink)
	}
	if b, _ := st.StoredBytes(ctx); b != 2000 {
		t.Fatalf("post-evict bytes = %d, want 2000", b)
	}
	// Evicted rows keep their fetch_ref (re-fetchable) and status=evicted.
	row, ok, _ := st.MediaByID(ctx, "old1")
	if !ok || row.Status != MediaEvicted || row.Stored || row.FetchRef == "" {
		t.Fatalf("evicted row state wrong: %+v ok=%v", row, ok)
	}
}

func TestMediaEvictKeepsSharedBlob(t *testing.T) {
	st, _ := Open(":memory:")
	defer st.Close()
	ctx := context.Background()
	base := time.Date(2026, 6, 10, 8, 0, 0, 0, time.UTC)

	// Same content (shared hash) referenced by an OLD and a NEW message.
	old := mediaItem("shared-old", base)
	newer := mediaItem("shared-new", base.Add(5*time.Hour))
	filler := mediaItem("filler", base.Add(1*time.Hour))
	_, _ = st.EnqueueMedia(ctx, []MediaItem{old, newer, filler})
	_ = st.MarkMediaStored(ctx, "shared-old", "shared", 1000)
	_ = st.MarkMediaStored(ctx, "shared-new", "shared", 1000) // same hash, dedup → 1000 total for it
	_ = st.MarkMediaStored(ctx, "filler", "filler", 1000)
	if b, _ := st.StoredBytes(ctx); b != 2000 {
		t.Fatalf("pre bytes = %d, want 2000 (shared counted once)", b)
	}

	// Evict to 1000: the oldest row "shared-old" is marked evicted, but its hash
	// is STILL held by "shared-new" → the blob must NOT be unlinked. The next
	// oldest unique blob ("filler") is what actually frees bytes.
	unlink, freed, err := st.EvictToTarget(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range unlink {
		if u.Hash == "shared" {
			t.Fatalf("unlinked a blob still referenced by a newer message: %+v", unlink)
		}
	}
	if freed != 1000 {
		t.Fatalf("freed=%d, want 1000 (only filler's bytes)", freed)
	}
	if b, _ := st.StoredBytes(ctx); b != 1000 {
		t.Fatalf("post bytes = %d, want 1000", b)
	}
	// The shared blob's bytes survive because the newer message still holds them.
	row, _, _ := st.MediaByID(ctx, "shared-new")
	if !row.Stored {
		t.Fatal("newer shared ref should still be stored")
	}
}
