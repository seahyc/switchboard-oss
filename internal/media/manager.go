// Package media is the importance-gated, content-addressed media store. It runs
// strictly OFF the ingest path: a background worker pulls pending attachments
// (registered by the pipeline), downloads only those in IMPORTANT chats (reusing
// the deterministic importance scorer), content-addresses + dedups the bytes on
// disk, and evicts the oldest-by-message media once the size cap is exceeded —
// replacing evicted bytes with a re-fetchable reference. A download stall on one
// attachment can never back up live ingest: the worker is a separate goroutine
// with a bounded pool and a per-download timeout.
package media

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seahyc/switchboard-oss/internal/connectors"
	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/privatefs"
	"github.com/seahyc/switchboard-oss/internal/store"
)

const (
	defaultCapBytes   = 40 << 30  // 40 GiB
	defaultPerFileMax = 512 << 20 // 512 MiB — one giant file can't evict a pile of small media
	defaultTimeout    = 2 * time.Minute
	defaultConc       = 4
	drainInterval     = 15 * time.Second
	drainBatch        = 200
)

var errTooLarge = errors.New("media exceeds per-file ceiling")

// Config tunes the media store. Zero values fall back to sane defaults.
type Config struct {
	Root          string          // blob root dir (e.g. ~/switchboard/media)
	CapBytes      int64           // total stored-blob cap (dedup-aware); default 40GiB
	PerFileMax    int64           // skip any single file larger than this; default 512MiB
	IncludedTypes map[string]bool // if non-empty, only these attachment types are downloaded
	Timeout       time.Duration   // per-download timeout; default 2m
	Concurrency   int             // max concurrent downloads; default 4
	RatePerMin    int             // throttle: max downloads/min (0 = unlimited). Gentle pace
	//                              for the historical drain so we don't hammer a
	//                              platform's media CDN (WhatsApp anti-abuse).
}

// Manager owns the blob filesystem + the gated download worker.
type Manager struct {
	st  *store.Store
	cfg Config
	log *slog.Logger

	mu       sync.RWMutex
	dls      map[string]connectors.MediaDownloader // platform -> downloader
	derivers map[string]func(raw []byte) string    // platform -> raw->Ref recovery

	rate time.Duration // min interval between downloads (0 = unlimited)
	rl   chan struct{} // token bucket, refilled by the limiter goroutine
}

// NewManager creates the media store rooted at cfg.Root (created if missing).
func NewManager(st *store.Store, cfg Config, log *slog.Logger) (*Manager, error) {
	if cfg.Root == "" {
		return nil, fmt.Errorf("media: root required")
	}
	if cfg.CapBytes <= 0 {
		cfg.CapBytes = defaultCapBytes
	}
	if cfg.PerFileMax <= 0 {
		cfg.PerFileMax = defaultPerFileMax
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = defaultConc
	}
	if err := privatefs.EnsureDir(cfg.Root); err != nil {
		return nil, fmt.Errorf("media root: %w", err)
	}
	m := &Manager{st: st, cfg: cfg, log: log, dls: map[string]connectors.MediaDownloader{}, derivers: map[string]func([]byte) string{}}
	if cfg.RatePerMin > 0 {
		m.rate = time.Minute / time.Duration(cfg.RatePerMin)
		burst := cfg.Concurrency
		if burst < 1 {
			burst = 1
		}
		m.rl = make(chan struct{}, burst)
	}
	return m, nil
}

// acquire blocks until a download token is available (throttle). No-op when
// rate-limiting is disabled. Returns false if ctx is cancelled while waiting.
func (m *Manager) acquire(ctx context.Context) bool {
	if m.rl == nil {
		return true
	}
	select {
	case <-m.rl:
		return true
	case <-ctx.Done():
		return false
	}
}

// RegisterDownloader wires a connector's MediaDownloader under its platform.
func (m *Manager) RegisterDownloader(platform string, d connectors.MediaDownloader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dls[platform] = d
}

func (m *Manager) downloader(platform string) (connectors.MediaDownloader, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	d, ok := m.dls[platform]
	return d, ok
}

// RegisterRefDeriver wires a connector's raw->Ref recovery under its platform.
func (m *Manager) RegisterRefDeriver(platform string, fn func(raw []byte) string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.derivers[platform] = fn
}

func (m *Manager) deriver(platform string) func([]byte) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.derivers[platform]
}

// blobPath is the content-addressed location for a hash: root/<hash[:2]>/<hash>.
func (m *Manager) blobPath(hash string) string {
	return filepath.Join(m.cfg.Root, hash[:2], hash)
}

// isFullRef reports whether a Ref already carries the full serialized download
// tuple (a JSON object) vs a legacy path-only string that needs raw recovery.
func isFullRef(ref string) bool { return strings.HasPrefix(ref, "{") }

// OnIngest implements pipeline.Sink: as messages flow, it registers any
// re-fetchable attachments as pending media (a fast local insert; the gated
// download happens later on the worker, off this path). Attachments without a
// Ref can't be re-fetched, so they're skipped. Enqueue is idempotent, so live/
// backfill overlap never double-registers.
func (m *Manager) OnIngest(msg event.Message) {
	if len(msg.Attachments) == 0 {
		return
	}
	var items []store.MediaItem
	for i, a := range msg.Attachments {
		ref := a.Ref
		// Replay-safe Ref recovery: if the stored Ref is empty or path-only
		// (predates full-tuple serialization), rebuild a complete, downloadable
		// Ref from the untouched raw payload. This keeps media capture correct
		// across reprojection without re-hitting the platform for metadata.
		if !isFullRef(ref) {
			if d := m.deriver(msg.Platform); d != nil {
				if derived := d([]byte(msg.Raw)); derived != "" {
					ref = derived
				}
			}
		}
		if ref == "" {
			continue // not re-fetchable — nothing to download
		}
		items = append(items, store.MediaItem{
			ID:       msg.ID + "#" + strconv.Itoa(i),
			EventID:  msg.ID,
			ChatID:   msg.ChatID,
			Platform: msg.Platform,
			Idx:      i,
			Type:     a.Type,
			MIME:     a.MIME,
			Name:     a.Name,
			FetchRef: ref,
			MsgTS:    msg.TS,
		})
	}
	if len(items) == 0 {
		return
	}
	if _, err := m.st.EnqueueMedia(context.Background(), items); err != nil {
		m.log.Warn("media: enqueue on ingest", "err", err)
	}
}

// Run is the worker loop — call in its own goroutine. It is fully decoupled from
// ingest: it polls the pending queue, downloads gated media with a bounded pool,
// and enforces the cap. Returns when ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	// Throttle: refill the download token bucket at the configured rate so the
	// historical drain trickles rather than floods the platform's media CDN.
	if m.rl != nil {
		go func() {
			tk := time.NewTicker(m.rate)
			defer tk.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tk.C:
					select {
					case m.rl <- struct{}{}:
					default: // bucket full
					}
				}
			}
		}()
	}
	t := time.NewTicker(drainInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.drain(ctx)
			m.enforceCapacity(ctx)
		}
	}
}

// drain processes one batch of pending media with a bounded worker pool.
func (m *Manager) drain(ctx context.Context) {
	rows, err := m.st.NextPendingMedia(ctx, drainBatch)
	if err != nil {
		m.log.Warn("media: next pending", "err", err)
		return
	}
	if len(rows) == 0 {
		return
	}
	sem := make(chan struct{}, m.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, r := range rows {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(row store.MediaRow) {
			defer wg.Done()
			defer func() { <-sem }()
			m.processOne(ctx, row)
		}(r)
	}
	wg.Wait()
}

// processOne applies the importance + type gate, then downloads & stores.
func (m *Manager) processOne(ctx context.Context, row store.MediaRow) {
	imp, err := m.st.ChatImportance(ctx, row.ChatID)
	if err != nil {
		m.log.Warn("media: importance lookup", "err", err)
		return
	}
	switch imp {
	case "important":
		// gate passed
	case "":
		return // chat not scored yet — leave pending, retry next tick
	default: // normal | low
		_ = m.st.MarkMediaSkipped(ctx, row.ID, "chat importance="+imp)
		return
	}
	if len(m.cfg.IncludedTypes) > 0 && !m.cfg.IncludedTypes[row.Type] {
		_ = m.st.MarkMediaSkipped(ctx, row.ID, "type excluded: "+row.Type)
		return
	}
	// Cap-aware: at/over cap, only download media NEWER than the eviction
	// frontier (it will displace something older). Older backlog media would be
	// evicted the instant it landed, so don't burn a download (or a platform API
	// call) on it — leave it pending; a future eviction may make room.
	if total, err := m.st.StoredBytes(ctx); err == nil && total >= m.cfg.CapBytes {
		if oldest, ok, _ := m.st.OldestStoredMediaTS(ctx); ok && !row.MsgTS.After(oldest) {
			return
		}
	}
	if err := m.fetchStore(ctx, row, false); err != nil {
		m.log.Debug("media: fetch failed", "id", row.ID, "err", err)
	}
}

// isPermanentMediaErr reports whether a download error means the media is gone
// for good (expired/removed on the platform's servers) — so we must NOT retry it
// (a retry-storm on permanent failures is exactly what trips anti-abuse). We
// keep the ref either way so the user can see the attachment existed.
func isPermanentMediaErr(err error) bool {
	s := strings.ToLower(err.Error())
	for _, sig := range []string{"404", "410", "not found", "no longer", "expired", "gone"} {
		if strings.Contains(s, sig) {
			return true
		}
	}
	return false
}

// fetchStore downloads the attachment's bytes, content-addresses + dedups them,
// and records the result. Records skip/fail status on the row rather than
// returning hard errors (the worker is best-effort). Used by both the gated
// worker and the on-demand Get (which bypasses the gate). onDemand=true skips the
// bulk rate-limiter: an on-view fetch (the send-gate card requesting /media/{id}/meta)
// is user-initiated + must be PROMPT — otherwise, under worker-drain throttle
// saturation, acquire() blocks until the HTTP ctx times out and the row is left
// pending (blank-forever for a gated backlog item that only fetches on view).
func (m *Manager) fetchStore(ctx context.Context, row store.MediaRow, onDemand bool) error {
	dl, ok := m.downloader(row.Platform)
	if !ok || row.FetchRef == "" {
		_ = m.st.MarkMediaFailed(ctx, row.ID, "no downloader or empty ref")
		return fmt.Errorf("no downloader/ref for %s", row.Platform)
	}
	// Throttle (gentle pace; protects against platform anti-abuse on the drain) — but
	// NOT for on-demand: a user-facing view must fetch promptly, not queue behind the drain.
	if !onDemand && !m.acquire(ctx) {
		return ctx.Err()
	}
	dctx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	defer cancel()
	rc, _, err := dl.DownloadMedia(dctx, row.FetchRef)
	if err != nil {
		reason := "download: " + err.Error()
		if isPermanentMediaErr(err) {
			reason = "permanent (media gone): " + err.Error()
		}
		// Either way mark failed (terminal) — the worker only re-picks pending
		// rows, so failures never retry-storm. Transient failures can still be
		// re-fetched on demand via get_media.
		_ = m.st.MarkMediaFailed(ctx, row.ID, reason)
		return err
	}
	defer rc.Close()

	hash, size, head, tmp, err := m.streamToTemp(rc)
	if tmp != "" && (err != nil) {
		os.Remove(tmp)
	}
	if err != nil {
		if errors.Is(err, errTooLarge) {
			_ = m.st.MarkMediaSkipped(ctx, row.ID, fmt.Sprintf("too large (> %d bytes)", m.cfg.PerFileMax))
			return nil
		}
		_ = m.st.MarkMediaFailed(ctx, row.ID, "read: "+err.Error())
		return err
	}
	// Content-magic guard (#48): reject bytes that don't match the declared media kind
	// before marking stored — catches UNDECRYPTED ciphertext (e.g. a WhatsApp .enc blob
	// fetched without the mediaKey via the path-only fallback) being served as image/jpeg
	// → browser DECODE_FAIL → blank preview. Failed (not stored) so the recovery can
	// re-derive a full ref + re-download decrypted.
	if !looksDecodable(head, row.MIME, row.Type) {
		os.Remove(tmp)
		_ = m.st.MarkMediaFailed(ctx, row.ID, fmt.Sprintf("content mismatch: declared %s/%s but bytes sniff as %s (likely undecrypted)", row.Type, row.MIME, http.DetectContentType(head)))
		return fmt.Errorf("content mismatch for %s", row.ID)
	}

	dst := m.blobPath(hash)
	if _, statErr := os.Lstat(dst); statErr == nil {
		if err := privatefs.PrepareFile(dst); err != nil {
			os.Remove(tmp)
			_ = m.st.MarkMediaFailed(ctx, row.ID, "secure existing blob: "+err.Error())
			return err
		}
		os.Remove(tmp) // dedup: identical bytes already on disk
	} else {
		if err := privatefs.EnsureDir(filepath.Dir(dst)); err != nil {
			os.Remove(tmp)
			_ = m.st.MarkMediaFailed(ctx, row.ID, "mkdir: "+err.Error())
			return err
		}
		if err := os.Rename(tmp, dst); err != nil {
			os.Remove(tmp)
			_ = m.st.MarkMediaFailed(ctx, row.ID, "rename: "+err.Error())
			return err
		}
		if err := privatefs.PrepareFile(dst); err != nil {
			_ = m.st.MarkMediaFailed(ctx, row.ID, "secure blob: "+err.Error())
			return err
		}
	}
	return m.st.MarkMediaStored(ctx, row.ID, hash, size)
}

// streamToTemp streams rc into a temp file under root, computing sha256 and
// aborting with errTooLarge past the per-file ceiling. Returns hash, size, and
// the temp path (always returned when a temp file was created, so callers can
// clean it up on error).
func (m *Manager) streamToTemp(rc io.Reader) (hash string, size int64, head []byte, tmpPath string, err error) {
	tmp, err := os.CreateTemp(m.cfg.Root, ".dl-*")
	if err != nil {
		return "", 0, nil, "", err
	}
	tmpPath = tmp.Name()
	h := sha256.New()
	buf := make([]byte, 64*1024)
	for {
		n, rerr := rc.Read(buf)
		if n > 0 {
			size += int64(n)
			if size > m.cfg.PerFileMax {
				tmp.Close()
				return "", 0, head, tmpPath, errTooLarge
			}
			// Capture up to the first 512 bytes for content-type sniffing.
			if len(head) < 512 {
				head = append(head, buf[:min(n, 512-len(head))]...)
			}
			h.Write(buf[:n])
			if _, werr := tmp.Write(buf[:n]); werr != nil {
				tmp.Close()
				return "", 0, head, tmpPath, werr
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			tmp.Close()
			return "", 0, head, tmpPath, rerr
		}
	}
	if cerr := tmp.Close(); cerr != nil {
		return "", 0, head, tmpPath, cerr
	}
	return hex.EncodeToString(h.Sum(nil)), size, head, tmpPath, nil
}

// looksDecodable guards against storing bytes that don't match their declared media
// kind (#48) — specifically undecrypted ciphertext served as an image. We HARD-reject
// only the clear, preview-critical case: declared image/sticker but the bytes don't
// sniff as a known image type (http.DetectContentType → not image/*; ciphertext sniffs
// as application/octet-stream). Non-image kinds + empty heads pass — we don't want
// false rejects on odd-but-valid documents/audio/video.
func looksDecodable(head []byte, mime, typ string) bool {
	if len(head) == 0 {
		return true
	}
	declaredImage := strings.HasPrefix(mime, "image/") || typ == "image" || typ == "sticker"
	if !declaredImage {
		return true
	}
	return strings.HasPrefix(http.DetectContentType(head), "image/")
}

// RevalidateStored re-checks every stored image/sticker blob for a platform against its
// declared type and recovers the undecrypted ones (#48): if the stored bytes don't sniff
// as a real image (the ciphertext class — WhatsApp media stored before the content guard,
// via the no-mediaKey path-only download), re-derive a full decrypt-capable ref from the
// message's stored Raw and reset the row to pending so the worker / on-view ensure
// re-downloads + decrypts it. Returns (checked, reset). Idempotent: valid blobs are left
// untouched; re-running only re-resets ones still bad.
func (m *Manager) RevalidateStored(ctx context.Context, platform string) (checked, decrypted, reset int, err error) {
	rows, err := m.st.StoredImageMedia(ctx, platform)
	if err != nil {
		return 0, 0, 0, err
	}
	d := m.deriver(platform)
	dl, _ := m.downloader(platform)
	dec, _ := dl.(connectors.MediaLocalDecryptor)
	for _, row := range rows {
		if ctx.Err() != nil {
			return checked, decrypted, reset, ctx.Err()
		}
		checked++
		blob := m.blobPath(row.Hash)
		head, herr := readBlobHead(blob, 512)
		if herr != nil {
			continue // blob missing (evicted/never-written) — leave it for ensure/eviction
		}
		if looksDecodable(head, row.MIME, row.Type) {
			continue // genuinely an image — keep
		}
		// Undecrypted ciphertext. PREFER local decrypt-in-place: we already have the .enc
		// bytes + the mediaKey is in the ref, so decrypt directly — zero network, and it
		// recovers even media whose CDN URL has since expired (a re-download would lose).
		if dec != nil {
			if enc, rerr := os.ReadFile(blob); rerr == nil {
				if plain, derr := dec.DecryptLocalMedia(row.FetchRef, enc); derr == nil &&
					looksDecodable(plain[:min(len(plain), 512)], row.MIME, row.Type) {
					if serr := m.storeBytes(ctx, row.ID, plain); serr == nil {
						decrypted++
						continue
					}
				}
			}
		}
		// Fallback: re-derive a full (mediaKey-bearing) ref from stored Raw + reset to
		// pending for re-download (used when there's no local decryptor, the ref lacks the
		// key, or the on-disk bytes weren't recoverable).
		ref := row.FetchRef
		if d != nil {
			if raw, rerr := m.st.RawByEventID(ctx, row.EventID); rerr == nil {
				if derived := d(raw); derived != "" {
					ref = derived
				}
			}
		}
		if rerr := m.st.ResetMediaForRedownload(ctx, row.ID, ref); rerr != nil {
			m.log.Warn("media revalidate: reset failed", "id", row.ID, "err", rerr)
			continue
		}
		reset++
	}
	m.log.Info("media revalidate complete", "platform", platform,
		"checked", checked, "decrypted_in_place", decrypted, "reset_for_redownload", reset)
	return checked, decrypted, reset, nil
}

// storeBytes content-addresses + writes plaintext bytes to the blob store and marks the
// row stored (used by the in-place decrypt recovery). Reuses the streaming hash/temp path.
func (m *Manager) storeBytes(ctx context.Context, id string, data []byte) error {
	hash, size, _, tmp, err := m.streamToTemp(bytes.NewReader(data))
	if tmp != "" && err != nil {
		os.Remove(tmp)
	}
	if err != nil {
		return err
	}
	dst := m.blobPath(hash)
	if _, statErr := os.Lstat(dst); statErr == nil {
		if err := privatefs.PrepareFile(dst); err != nil {
			os.Remove(tmp)
			return err
		}
		os.Remove(tmp) // dedup
	} else {
		if err := privatefs.EnsureDir(filepath.Dir(dst)); err != nil {
			os.Remove(tmp)
			return err
		}
		if err := os.Rename(tmp, dst); err != nil {
			os.Remove(tmp)
			return err
		}
		if err := privatefs.PrepareFile(dst); err != nil {
			return err
		}
	}
	return m.st.MarkMediaStored(ctx, id, hash, size)
}

// readBlobHead reads up to n leading bytes of a file for content sniffing.
func readBlobHead(path string, n int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, n)
	r, _ := io.ReadFull(f, buf)
	return buf[:r], nil
}

// enforceCapacity evicts oldest-by-message media until the dedup-aware stored
// total is under the cap, unlinking the freed blob files.
func (m *Manager) enforceCapacity(ctx context.Context) {
	total, err := m.st.StoredBytes(ctx)
	if err != nil {
		m.log.Warn("media: stored bytes", "err", err)
		return
	}
	if total <= m.cfg.CapBytes {
		return
	}
	unlink, freed, err := m.st.EvictToTarget(ctx, m.cfg.CapBytes)
	if err != nil {
		m.log.Warn("media: evict", "err", err)
		return
	}
	for _, b := range unlink {
		if err := os.Remove(m.blobPath(b.Hash)); err != nil && !os.IsNotExist(err) {
			m.log.Warn("media: unlink blob", "hash", b.Hash, "err", err)
		}
	}
	if freed > 0 {
		m.log.Info("media evicted under cap", "freed_bytes", freed, "cap_bytes", m.cfg.CapBytes)
	}
}

// ensure guarantees the bytes for a media id are on disk, re-fetching on demand
// (bypassing the importance gate — the user explicitly asked) if the row is
// ref-only (evicted / never-downloaded). Returns the up-to-date row.
func (m *Manager) ensure(ctx context.Context, id string) (store.MediaRow, error) {
	row, ok, err := m.st.MediaByID(ctx, id)
	if err != nil {
		return store.MediaRow{}, err
	}
	if !ok {
		return store.MediaRow{}, fmt.Errorf("media %q not found", id)
	}
	if row.Stored && row.Hash != "" {
		if _, statErr := os.Stat(m.blobPath(row.Hash)); statErr == nil {
			_ = m.st.TouchMediaAccess(ctx, row.Hash)
			return row, nil
		}
		// File missing despite stored flag — fall through to re-fetch.
	}
	if err := m.fetchStore(ctx, row, true); err != nil {
		return row, fmt.Errorf("re-fetch: %w", err)
	}
	row, _, err = m.st.MediaByID(ctx, id)
	if err != nil {
		return store.MediaRow{}, err
	}
	_ = m.st.TouchMediaAccess(ctx, row.Hash)
	// NB: do NOT enforce capacity inline here. An on-demand re-fetch of the
	// oldest media would otherwise be evicted again immediately (it's oldest by
	// message-ts) and unlinked before we can serve it. The worker's periodic
	// enforceCapacity reconciles the (transient) overage on its next tick.
	return row, nil
}

// Get returns a reader for a media id's bytes (for GET /media/{id}), re-fetching
// ref-only media on demand. Caller closes the reader.
func (m *Manager) Get(ctx context.Context, id string) (store.MediaRow, io.ReadCloser, error) {
	row, err := m.ensure(ctx, id)
	if err != nil {
		return row, nil, err
	}
	rc, err := os.Open(m.blobPath(row.Hash))
	return row, rc, err
}

// LocalPath ensures the bytes are on disk and returns the absolute blob path
// (for the get_media MCP tool — the same-VM agent can open the file directly).
func (m *Manager) LocalPath(ctx context.Context, id string) (store.MediaRow, string, error) {
	row, err := m.ensure(ctx, id)
	if err != nil {
		return row, "", err
	}
	abs, err := filepath.Abs(m.blobPath(row.Hash))
	if err != nil {
		return row, m.blobPath(row.Hash), nil
	}
	return row, abs, nil
}
