// Package whatsapp implements the switchboard Connector for WhatsApp on top of
// go.mau.fi/whatsmeow (multidevice). It captures live messages, drives history
// backfill, sends messages/reactions, and exposes a QR-code login the daemon
// renders to PNG and pushes to Telegram for the user to scan.
//
// Design notes:
//   - The whatsmeow device session is persisted in a SQLite store (sqlstore) so
//     auth survives restarts — no re-scan on every boot.
//   - Live messages map to event.Message with source=live; history-sync messages
//     (both the post-pairing dump and explicit Backfill requests) map with
//     source=backfill. Both paths share the same pure mapper (mapping.go).
//   - The whatsmeow event goroutine must never block on slow projection, so emit
//     races the connector's context and drops to the floor only on shutdown.
package whatsapp

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/skip2/go-qrcode"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"go.mau.fi/whatsmeow/util/cbcutil"
	"go.mau.fi/whatsmeow/util/hkdfutil"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"

	"github.com/seahyc/switchboard-oss/internal/connectors"
	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/privatefs"
)

// qrPNGSize is the pixel size of the rendered login QR PNG.
const qrPNGSize = 512

// deviceName is how switchboard's device appears in the user's WhatsApp Linked
// Devices list — distinct from any other linked client (e.g. the old Baileys
// "hermes agent" entry) so the user can identify and manage it unambiguously.
const deviceName = "Switchboard"

const (
	mediaRetrySendTimeout = 5 * time.Second
	mediaRetryWaitTimeout = 20 * time.Second
)

// Connector is the WhatsApp implementation of connectors.Connector.
type Connector struct {
	container *sqlstore.Container
	client    *whatsmeow.Client
	log       waLog.Logger

	mu           sync.Mutex
	ctx          context.Context              // lifetime of Connect; nil until Connect runs
	out          chan<- event.Message         // primary live sink (set by Connect)
	sinks        map[int]chan<- event.Message // extra backfill sinks
	nextSink     int
	qrPNG        []byte // latest login QR PNG, if awaiting scan
	qrDetail     string
	qrExpiresAt  time.Time
	loginRunning bool // true while the self-renewing QR loop is active

	// WhatsApp Business label registry, built from app-state events (whatsmeow
	// has no queryable label store). labelNames maps labelID→display name;
	// chatLabels maps canonical chat_id→set of labelIDs currently applied.
	labelNames map[string]string
	chatLabels map[string]map[string]bool

	mediaRetryWaiters map[types.MessageID][]chan *events.MediaRetry
}

// compile-time interface checks.
var (
	_ connectors.Connector            = (*Connector)(nil)
	_ connectors.ParticipantReactor   = (*Connector)(nil)
	_ connectors.ChatMetadataProvider = (*Connector)(nil)
	_ connectors.AnchoredBackfiller   = (*Connector)(nil)
	_ connectors.MediaSender          = (*Connector)(nil)
)

// New opens (or creates) the whatsmeow session store at dbPath and builds the
// client. The QR/login flow is driven later via Login. dbPath is a SQLite file,
// e.g. "/var/lib/switchboard/whatsapp.db".
func New(ctx context.Context, dbPath string, logger waLog.Logger) (*Connector, error) {
	if logger == nil {
		logger = waLog.Noop
	}
	versionOnce.Do(func() {
		if err := prepareWAVersion(ctx); err != nil {
			logger.Warnf("Could not fetch current WhatsApp client version; using bundled version: %v", err)
		} else {
			logger.Infof("Using WhatsApp client version %s", store.GetWAVersion())
		}
	})

	// Register under an explicit, recognizable device name/platform so it shows
	// as "Switchboard" (desktop) in the user's Linked Devices instead of the
	// default "whatsmeow". Read by whatsmeow at pairing time; safe to set once.
	store.DeviceProps.Os = proto.String(deviceName)
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_DESKTOP.Enum()
	// Secure the database and sidecars before SQLite opens them. Reopening and
	// closing these paths afterward can drop SQLite's process-scoped POSIX locks.
	for _, p := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
		if err := privatefs.PrepareFile(p); err != nil {
			return nil, fmt.Errorf("whatsapp: secure session database file %q: %w", p, err)
		}
	}

	dsn := fmt.Sprintf("file:%s?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)", dbPath)
	container, err := sqlstore.New(ctx, "sqlite", dsn, logger.Sub("sqlstore"))
	if err != nil {
		return nil, fmt.Errorf("whatsapp: open session store: %w", err)
	}
	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		return nil, fmt.Errorf("whatsapp: load device: %w", err)
	}

	c := &Connector{
		container:         container,
		client:            whatsmeow.NewClient(device, logger.Sub("client")),
		log:               logger,
		sinks:             make(map[int]chan<- event.Message),
		labelNames:        make(map[string]string),
		chatLabels:        make(map[string]map[string]bool),
		mediaRetryWaiters: make(map[types.MessageID][]chan *events.MediaRetry),
	}
	// DESKTOP is not mapped by whatsmeow's automatic QR client-type selection.
	c.client.QRClientType = whatsmeow.PairClientElectron
	c.client.AddEventHandler(c.handleEvent)
	return c, nil
}

// Name implements connectors.Connector.
func (c *Connector) Name() string { return event.PlatformWhatsApp }

// waMediaType maps our short attachment-type tag to a whatsmeow MediaType.
var waMediaType = map[string]whatsmeow.MediaType{
	"image":    whatsmeow.MediaImage,
	"video":    whatsmeow.MediaVideo,
	"audio":    whatsmeow.MediaAudio,
	"voice":    whatsmeow.MediaAudio,
	"document": whatsmeow.MediaDocument,
	"sticker":  whatsmeow.MediaImage,
}

// DeriveMediaRef implements connectors.RefDeriver: rebuild the full media Ref
// tuple from a stored raw event payload (replay-safe historical recovery).
func (c *Connector) DeriveMediaRef(raw []byte) string { return RefFromRaw(raw) }

// DownloadMedia implements connectors.MediaDownloader: it reconstructs the
// download parameters from the serialized Attachment.Ref (see encodeMediaRef)
// and fetches + decrypts the bytes via whatsmeow. Called only off the ingest
// path by the gated media worker. Returns the bytes as a reader.
func (c *Connector) DownloadMedia(ctx context.Context, ref string) (io.ReadCloser, int64, error) {
	var r mediaRefV1
	if err := json.Unmarshal([]byte(ref), &r); err != nil || r.Path == "" {
		// Legacy / path-only ref (or unencrypted): best-effort path download.
		path := ref
		if r.Path != "" {
			path = r.Path
		}
		data, derr := c.client.DownloadMediaWithOnlyPath(ctx, path)
		if derr != nil {
			return nil, 0, fmt.Errorf("whatsapp download (path-only): %w", derr)
		}
		return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
	}
	mt, ok := waMediaType[r.MT]
	if !ok {
		mt = whatsmeow.MediaImage
	}
	data, err := c.client.DownloadMediaWithPath(ctx, r.Path, r.Enc, r.SHA, r.Key, mt, "", false)
	if err != nil {
		if shouldTryMediaRetry(err, r) {
			freshPath, retryErr := c.requestFreshMediaPath(ctx, r)
			if retryErr == nil {
				r.Path = freshPath
				data, err = c.client.DownloadMediaWithPath(ctx, r.Path, r.Enc, r.SHA, r.Key, mt, "", false)
				if err == nil {
					return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
				}
			}
			if retryErr != nil {
				return nil, 0, fmt.Errorf("whatsapp download: %w (media retry failed: %v)", err, retryErr)
			}
		}
		return nil, 0, fmt.Errorf("whatsapp download: %w", err)
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

func shouldTryMediaRetry(err error, r mediaRefV1) bool {
	if r.Path == "" || r.ID == "" || r.Chat == "" || len(r.Key) == 0 {
		return false
	}
	return errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410)
}

func (r mediaRefV1) messageInfo() (*types.MessageInfo, error) {
	chat, err := types.ParseJID(r.Chat)
	if err != nil {
		return nil, fmt.Errorf("bad chat jid %q: %w", r.Chat, err)
	}
	info := &types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     chat,
			IsFromMe: r.FromMe,
			IsGroup:  r.IsGroup,
		},
		ID: types.MessageID(r.ID),
	}
	if r.Participant != "" {
		sender, err := types.ParseJID(r.Participant)
		if err != nil {
			return nil, fmt.Errorf("bad participant jid %q: %w", r.Participant, err)
		}
		info.Sender = sender
	}
	return info, nil
}

func (c *Connector) requestFreshMediaPath(ctx context.Context, r mediaRefV1) (string, error) {
	info, err := r.messageInfo()
	if err != nil {
		return "", err
	}
	// whatsmeow correlates MediaRetry responses by message ID only. WhatsApp
	// message IDs are expected to be globally unique enough for this short,
	// single-download wait; the encrypted notification is still validated with
	// the attachment's media key before the fresh direct path is trusted.
	ch, cleanup := c.addMediaRetryWaiter(types.MessageID(r.ID))
	defer cleanup()

	sendCtx, cancelSend := context.WithTimeout(ctx, mediaRetrySendTimeout)
	err = c.client.SendMediaRetryReceipt(sendCtx, info, r.Key)
	cancelSend()
	if err != nil {
		return "", fmt.Errorf("send media retry receipt: %w", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, mediaRetryWaitTimeout)
	defer cancelWait()
	select {
	case evt := <-ch:
		return freshDirectPathFromMediaRetry(evt, r.Key)
	case <-waitCtx.Done():
		return "", waitCtx.Err()
	}
}

func freshDirectPathFromMediaRetry(evt *events.MediaRetry, mediaKey []byte) (string, error) {
	notif, err := whatsmeow.DecryptMediaRetryNotification(evt, mediaKey)
	if err != nil {
		return "", fmt.Errorf("decrypt media retry: %w", err)
	}
	if notif.GetResult() != waMmsRetry.MediaRetryNotification_SUCCESS {
		return "", fmt.Errorf("media retry result: %s", notif.GetResult())
	}
	if notif.GetDirectPath() == "" {
		return "", errors.New("media retry response had no direct path")
	}
	return notif.GetDirectPath(), nil
}

func (c *Connector) addMediaRetryWaiter(id types.MessageID) (<-chan *events.MediaRetry, func()) {
	ch := make(chan *events.MediaRetry, 1)
	c.mu.Lock()
	if c.mediaRetryWaiters == nil {
		c.mediaRetryWaiters = make(map[types.MessageID][]chan *events.MediaRetry)
	}
	c.mediaRetryWaiters[id] = append(c.mediaRetryWaiters[id], ch)
	c.mu.Unlock()

	return ch, func() {
		c.mu.Lock()
		waiters := c.mediaRetryWaiters[id]
		for i, waiter := range waiters {
			if waiter == ch {
				waiters = append(waiters[:i], waiters[i+1:]...)
				break
			}
		}
		if len(waiters) == 0 {
			delete(c.mediaRetryWaiters, id)
		} else {
			c.mediaRetryWaiters[id] = waiters
		}
		c.mu.Unlock()
	}
}

func (c *Connector) handleMediaRetry(evt *events.MediaRetry) {
	if evt == nil || evt.MessageID == "" {
		return
	}
	c.mu.Lock()
	waiters := append([]chan *events.MediaRetry(nil), c.mediaRetryWaiters[evt.MessageID]...)
	c.mu.Unlock()
	for _, ch := range waiters {
		select {
		case ch <- evt:
		default:
		}
	}
}

// accountID derives the user's canonical account id from the logged-in JID.
func (c *Connector) accountID() string {
	if id := c.client.Store.ID; id != nil {
		return "wa:" + id.User
	}
	return "wa:unknown"
}

// Connect ensures the client is connected and streams live + history events to
// out until ctx is cancelled. The session must already be authenticated (call
// Login first); otherwise this returns an error.
func (c *Connector) Connect(ctx context.Context, out chan<- event.Message) error {
	if c.client.Store.ID == nil {
		return errors.New("whatsapp: not authenticated — call Login first")
	}

	c.mu.Lock()
	c.ctx = ctx
	c.out = out
	c.mu.Unlock()

	if !c.client.IsConnected() {
		if err := c.client.Connect(); err != nil {
			return fmt.Errorf("whatsapp: connect: %w", err)
		}
	}

	<-ctx.Done()
	c.client.Disconnect()

	c.mu.Lock()
	c.out = nil
	c.ctx = nil
	c.mu.Unlock()
	return ctx.Err()
}

// Backfill routes WhatsApp history into out (source=backfill) until ctx is
// cancelled.
//
// WhatsApp history sync is PUSH-based: WhatsApp streams bulk history as
// events.HistorySync after pairing (and after on-demand requests), which
// handleHistorySync normalizes and fans to every registered sink. This method
// registers out as a sink and waits, so the caller receives that stream.
//
// It does NOT issue an on-demand request here: whatsmeow's
// BuildHistorySyncRequest needs a per-chat anchor (the oldest known message —
// chat JID + msg id + timestamp) to fetch the page immediately before it, which
// the connector alone doesn't have (it doesn't keep a decrypted message store).
// Passing nil panics inside whatsmeow, so the previous code crashed on the
// `backfill all` path. For precise per-chat paging the daemon supplies the
// anchor via BackfillBefore (connectors.AnchoredBackfiller).
func (c *Connector) Backfill(ctx context.Context, chatID string, since time.Time, out chan<- event.Message) error {
	if c.client.Store.ID == nil {
		return errors.New("whatsapp: not authenticated — call Login first")
	}
	if !c.client.IsConnected() {
		return errors.New("whatsapp: not connected — call Connect first")
	}

	// Register a sink so any history events (push or on-demand) reach the caller.
	id := c.addSink(out)
	defer c.removeSink(id)

	// Bulk history is delivered via the push stream; just route it until done.
	<-ctx.Done()
	return ctx.Err()
}

// BackfillBefore implements connectors.AnchoredBackfiller: it issues an on-demand
// history-sync request for older messages in a single chat, anchored at the
// oldest message the daemon currently has (oldestMsgID + oldestTS). WhatsApp
// returns up to `count` messages immediately before the anchor as an
// events.HistorySync, which is normalized (source=backfill) and delivered to out.
// Without a valid anchor (oldestMsgID == "") this is a no-op, since whatsmeow
// cannot build a request without one.
func (c *Connector) BackfillBefore(ctx context.Context, chatID, oldestMsgID string, oldestFromMe bool, oldestTS time.Time, count int, out chan<- event.Message) error {
	if c.client.Store.ID == nil {
		return errors.New("whatsapp: not authenticated — call Login first")
	}
	if !c.client.IsConnected() {
		return errors.New("whatsapp: not connected — call Connect first")
	}
	chat, err := parseChatID(chatID)
	if err != nil {
		return err
	}
	if oldestMsgID == "" {
		return nil // no anchor → nothing to request (bulk history is push-based)
	}

	id := c.addSink(out)
	defer c.removeSink(id)

	anchor := &types.MessageInfo{
		MessageSource: types.MessageSource{Chat: chat, IsFromMe: oldestFromMe},
		ID:            stripPlatformPrefix(oldestMsgID),
		Timestamp:     oldestTS,
	}
	req := c.client.BuildHistorySyncRequest(anchor, clampCount(count))
	if req == nil {
		return errors.New("whatsapp: could not build history sync request")
	}
	if _, err := c.client.SendMessage(ctx, c.client.Store.ID.ToNonAD(), req, whatsmeow.SendRequestExtra{Peer: true}); err != nil {
		return fmt.Errorf("whatsapp: history sync request: %w", err)
	}

	// On-demand history sync is ASYNC: WhatsApp streams the requested messages back as
	// HistorySync notifications over the next several seconds, which our event handler
	// feeds into the sink (out). There's no synchronous "done" signal, so we hold the
	// sink open until the collection window (the per-chat ctx) elapses, draining whatever
	// arrives. Hitting that deadline is the EXPECTED end of collection — NOT a failure:
	// returning ctx.Err() here made the backfill-sweep mark every WA chat failed (swept=0)
	// and re-request the same chats every fire forever, never stamping backfilled_at, so
	// convergence stalled (#41). Treat DeadlineExceeded as success (we made a best-effort
	// request + drained the window); only a parent cancellation (daemon shutdown) is a
	// real error to propagate.
	<-ctx.Done()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return nil
	}
	return ctx.Err()
}

// DecryptLocalMedia implements connectors.MediaLocalDecryptor: decrypt the on-disk
// WhatsApp ciphertext bytes IN PLACE using the mediaKey carried in the stored ref —
// NO network. This is the #48 recovery for the ~432 .enc blobs that were stored
// undecrypted (path-only fallback = nil mediaKey). Because we already have the bytes +
// the key, it recovers even media whose CDN URL has since expired (which a re-download
// would lose). Mirrors whatsmeow's downloadAndDecrypt using its exported primitives:
// HKDF-expand the mediaKey → iv/cipherKey/macKey, verify the trailing-10-byte HMAC, then
// AES-CBC decrypt. enc is the full .enc file (ciphertext || mac10).
func (c *Connector) DecryptLocalMedia(ref string, enc []byte) ([]byte, error) {
	var r mediaRefV1
	if err := json.Unmarshal([]byte(ref), &r); err != nil || len(r.Key) == 0 {
		return nil, fmt.Errorf("whatsapp: ref has no mediaKey for local decrypt")
	}
	mt, ok := waMediaType[r.MT]
	if !ok {
		mt = whatsmeow.MediaImage
	}
	if len(enc) < 10 {
		return nil, fmt.Errorf("whatsapp: ciphertext too short")
	}
	expanded := hkdfutil.SHA256(r.Key, nil, []byte(mt), 112)
	iv, cipherKey, macKey := expanded[:16], expanded[16:48], expanded[48:80]
	file, mac := enc[:len(enc)-10], enc[len(enc)-10:]
	h := hmac.New(sha256.New, macKey)
	h.Write(iv)
	h.Write(file)
	if !hmac.Equal(h.Sum(nil)[:10], mac) {
		return nil, fmt.Errorf("whatsapp: media MAC mismatch (wrong key or not WA ciphertext)")
	}
	plain, err := cbcutil.Decrypt(cipherKey, iv, file)
	if err != nil {
		return nil, fmt.Errorf("whatsapp: cbc decrypt: %w", err)
	}
	return plain, nil
}

// clampCount keeps the on-demand page size in WhatsApp's sensible range.
func clampCount(count int) int {
	if count <= 0 {
		return 50
	}
	if count > 500 {
		return 500
	}
	return count
}

// GroupInviteLink returns the WhatsApp invite link for a group chat (#52). reset=true
// rotates the link. Errors if the chat isn't a group or the user lacks permission.
func (c *Connector) GroupInviteLink(ctx context.Context, chatID string, reset bool) (string, error) {
	jid, err := parseChatID(chatID)
	if err != nil {
		return "", err
	}
	if jid.Server != types.GroupServer {
		return "", fmt.Errorf("whatsapp: %q is not a group chat", chatID)
	}
	return c.client.GetGroupInviteLink(ctx, jid, reset)
}

// Send delivers an outbound message and returns the canonical sent id.
func (c *Connector) Send(ctx context.Context, req connectors.SendRequest) (string, error) {
	jid, err := parseChatID(req.ChatID)
	if err != nil {
		return "", err
	}
	if len(req.Attachments) > 0 {
		return "", errors.New("whatsapp: media send not implemented in v1")
	}

	var msg *waE2E.Message
	if req.ReplyTo != "" {
		msg = &waE2E.Message{
			ExtendedTextMessage: &waE2E.ExtendedTextMessage{
				Text: proto.String(req.Text),
				ContextInfo: &waE2E.ContextInfo{
					StanzaID: proto.String(stripPlatformPrefix(req.ReplyTo)),
				},
			},
		}
	} else {
		msg = &waE2E.Message{Conversation: proto.String(req.Text)}
	}

	resp, err := c.client.SendMessage(ctx, jid, msg)
	if err != nil {
		return "", fmt.Errorf("whatsapp: send: %w", err)
	}
	return event.MakeID(event.PlatformWhatsApp, resp.ID), nil
}

// SendMedia uploads a captured attachment's bytes and sends it to the target
// WhatsApp chat. v1 sends all media as a document, which preserves PDFs/files
// predictably and avoids pretending to be a native provenance-preserving forward.
func (c *Connector) SendMedia(ctx context.Context, req connectors.MediaSendRequest) (string, error) {
	jid, err := parseChatID(req.ChatID)
	if err != nil {
		return "", err
	}
	if req.Body == nil {
		return "", errors.New("whatsapp: media body required")
	}
	resp, err := c.client.UploadReader(ctx, req.Body, nil, whatsmeow.MediaDocument)
	if err != nil {
		return "", fmt.Errorf("whatsapp: upload media: %w", err)
	}

	mime := req.MIME
	if mime == "" {
		mime = "application/octet-stream"
	}
	name := req.Name
	if name == "" {
		name = "attachment"
	}
	keyTS := time.Now().Unix()
	doc := &waE2E.DocumentMessage{
		URL:               proto.String(resp.URL),
		DirectPath:        proto.String(resp.DirectPath),
		MediaKey:          resp.MediaKey,
		FileEncSHA256:     resp.FileEncSHA256,
		FileSHA256:        resp.FileSHA256,
		FileLength:        proto.Uint64(resp.FileLength),
		MediaKeyTimestamp: proto.Int64(keyTS),
		Mimetype:          proto.String(mime),
		FileName:          proto.String(name),
		Title:             proto.String(name),
	}
	if req.Text != "" {
		doc.Caption = proto.String(req.Text)
	}
	if ci := mediaSendContextInfo(req); ci != nil {
		doc.ContextInfo = ci
	}

	sent, err := c.client.SendMessage(ctx, jid, &waE2E.Message{DocumentMessage: doc})
	if err != nil {
		return "", fmt.Errorf("whatsapp: send media: %w", err)
	}
	return event.MakeID(event.PlatformWhatsApp, sent.ID), nil
}

func mediaSendContextInfo(req connectors.MediaSendRequest) *waE2E.ContextInfo {
	if req.ReplyTo == "" && !req.MarkForwarded {
		return nil
	}
	ci := &waE2E.ContextInfo{}
	if req.ReplyTo != "" {
		ci.StanzaID = proto.String(stripPlatformPrefix(req.ReplyTo))
	}
	if req.MarkForwarded {
		ci.IsForwarded = proto.Bool(true)
		ci.ForwardingScore = proto.Uint32(1)
	}
	return ci
}

// React adds an emoji reaction to a message. Passing an empty emoji removes a
// previously sent reaction (whatsmeow convention).
//
// Limitation: the canonical msgID alone does not carry the target message's
// sender. For DMs we use the chat JID as the participant; for groups the
// reaction key's participant defaults to the chat, which is correct for DMs and
// best-effort for groups. (Tracked with core as a possible interface refinement.)
func (c *Connector) React(ctx context.Context, chatID, msgID, emoji string) error {
	chat, err := parseChatID(chatID)
	if err != nil {
		return err
	}
	sender := chat
	if c.client.Store.ID != nil {
		// Reacting to our own last message is common; whatsmeow flips FromMe
		// based on sender == own JID.
		sender = c.client.Store.ID.ToNonAD()
	}
	reaction := c.client.BuildReaction(chat, sender, stripPlatformPrefix(msgID), emoji)
	if _, err := c.client.SendMessage(ctx, chat, reaction); err != nil {
		return fmt.Errorf("whatsapp: react: %w", err)
	}
	return nil
}

// ChatMetadata implements connectors.ChatMetadataProvider (task #11): it
// enumerates the user's chats with human-legible titles + normalized org signals
// (archived/muted/pinned) so list_chats shows names instead of "whatsapp:<jid>".
//
// Sources: joined groups (subject) + contacts (DM display name), each enriched
// from the chat-settings store. These reflect app-state, so values are only
// populated after the initial app-state sync lands; the daemon re-polls.
//
// WhatsApp Business labels are captured from app-state events into an in-memory
// registry (whatsmeow has no queryable label store) and resolved to names here;
// they appear once the label app-state collection has synced (Business accounts
// only — empty for personal accounts).
func (c *Connector) ChatMetadata(ctx context.Context) ([]connectors.ChatMeta, error) {
	if c.client.Store.ID == nil {
		return nil, errors.New("whatsapp: not authenticated")
	}

	now := time.Now()
	seen := make(map[string]bool)
	var metas []connectors.ChatMeta

	add := func(jid types.JID, title string) {
		id := chatIDForJID(jid)
		if seen[id] {
			return
		}
		seen[id] = true
		cm := connectors.ChatMeta{ChatID: id, Title: title, Labels: c.labelsForChat(id)}
		if s, err := c.client.Store.ChatSettings.GetChatSettings(ctx, jid); err == nil && s.Found {
			cm.Archived = s.Archived
			cm.Pinned = s.Pinned
			cm.Muted = s.MutedUntil.After(now)
			if !s.MutedUntil.IsZero() {
				mutedUntil := s.MutedUntil
				cm.MutedUntil = &mutedUntil
			}
		}
		metas = append(metas, cm)
	}

	groups, gErr := c.client.GetJoinedGroups(ctx)
	for _, g := range groups {
		add(g.JID, g.Name)
	}

	contacts, cErr := c.client.Store.Contacts.GetAllContacts(ctx)
	for jid, info := range contacts {
		add(jid, contactTitle(info))
	}

	if len(metas) == 0 && gErr != nil && cErr != nil {
		return nil, fmt.Errorf("whatsapp: chat metadata: groups: %v; contacts: %w", gErr, cErr)
	}
	return metas, nil
}

// Contacts implements connectors.ContactsProvider from the whatsmeow contact
// store — the authoritative WhatsApp name source (FullName/FirstName/PushName/
// BusinessName per JID), INCLUDING @lid contacts whose messages carry no name.
// UserID matches event.Sender.PlatformUserID exactly (jid.ToNonAD().String()).
func (c *Connector) Contacts(ctx context.Context) ([]connectors.Contact, error) {
	if c.client == nil || c.client.Store == nil || c.client.Store.Contacts == nil {
		return nil, nil
	}
	all, err := c.client.Store.Contacts.GetAllContacts(ctx)
	if err != nil {
		return nil, err
	}
	self := ""
	if c.client.Store.ID != nil {
		self = c.client.Store.ID.ToNonAD().String()
	}
	out := make([]connectors.Contact, 0, len(all))
	for jid, info := range all {
		ct := connectors.Contact{
			UserID:       userIDForJID(jid),
			FullName:     info.FullName,
			FirstName:    info.FirstName,
			PushName:     info.PushName,
			BusinessName: info.BusinessName,
			IsSelf:       userIDForJID(jid) == self,
		}
		// Only phone-based JIDs carry a real phone; @lid User is NOT a phone, so
		// leave Phone empty there (a bogus phone would wrongly auto-merge people).
		if jid.Server == types.DefaultUserServer && jid.User != "" {
			ct.Phone = jid.User
		}
		out = append(out, ct)
	}
	return out, nil
}

// contactTitle picks the best human-legible name for a DM contact.
func contactTitle(info types.ContactInfo) string {
	switch {
	case info.FullName != "":
		return info.FullName
	case info.PushName != "":
		return info.PushName
	case info.BusinessName != "":
		return info.BusinessName
	default:
		return ""
	}
}

// ReactAs implements connectors.ParticipantReactor. The daemon resolves the
// target message's original sender from the store and passes it here, so the
// reaction key carries the exact participant JID — making group reactions
// correct (whatsmeow derives Participant + FromMe from this sender). An empty
// emoji removes a previous reaction.
func (c *Connector) ReactAs(ctx context.Context, chatID, msgID, senderID, emoji string) error {
	chat, err := parseChatID(chatID)
	if err != nil {
		return err
	}
	sender, err := types.ParseJID(stripPlatformPrefix(senderID))
	if err != nil {
		return fmt.Errorf("whatsapp: bad sender id %q: %w", senderID, err)
	}
	reaction := c.client.BuildReaction(chat, sender, stripPlatformPrefix(msgID), emoji)
	if _, err := c.client.SendMessage(ctx, chat, reaction); err != nil {
		return fmt.Errorf("whatsapp: react: %w", err)
	}
	return nil
}

// Login drives QR-code authentication. If already paired it just (re)connects.
// Otherwise it starts a self-renewing background QR loop and returns the first
// QR as a PNG; the daemon renders/pushes it to Telegram for the user to scan.
//
// The loop keeps minting fresh codes across whatsmeow's QR rotation/timeout
// (~160s) — when a QR window expires without a scan it disconnects and opens a
// new GetQRChannel, so there is no hard deadline: the user can scan whenever and
// the daemon always has a current PNG via Status(). On success it flips to
// Connected so the daemon's driver proceeds to live ingest.
func (c *Connector) Login(ctx context.Context) (connectors.LoginStatus, error) {
	if c.client.Store.ID != nil {
		if !c.client.IsConnected() {
			if err := c.client.Connect(); err != nil {
				return connectors.LoginStatus{}, fmt.Errorf("whatsapp: connect: %w", err)
			}
		}
		return connectors.LoginStatus{Connected: true, Detail: "already authenticated"}, nil
	}

	// Start the renew loop once; concurrent Login calls just return cached state.
	c.mu.Lock()
	if c.loginRunning {
		st := connectors.LoginStatus{NeedsAuth: true, QRPNG: c.qrPNG, Detail: c.qrDetail}
		c.mu.Unlock()
		return st, nil
	}
	c.loginRunning = true
	c.mu.Unlock()

	first := make(chan connectors.LoginStatus, 1)
	go c.runQRLoop(ctx, first)

	select {
	case st := <-first:
		return st, nil
	case <-ctx.Done():
		return connectors.LoginStatus{}, ctx.Err()
	case <-time.After(30 * time.Second):
		return connectors.LoginStatus{NeedsAuth: true, Detail: "waiting for QR code"}, nil
	}
}

// runQRLoop keeps generating QR codes until paired or ctx is cancelled. Each
// expired window is disconnected and a fresh GetQRChannel is opened, removing
// the scan deadline entirely.
func (c *Connector) runQRLoop(ctx context.Context, first chan<- connectors.LoginStatus) {
	defer func() {
		c.mu.Lock()
		c.loginRunning = false
		c.mu.Unlock()
	}()

	var once sync.Once
	report := func(st connectors.LoginStatus) {
		once.Do(func() {
			select {
			case first <- st:
			default:
			}
		})
	}

	for {
		if ctx.Err() != nil {
			return
		}
		if c.client.Store.ID != nil { // paired in a prior iteration
			report(connectors.LoginStatus{Connected: true, Detail: "authenticated"})
			return
		}

		qrChan, err := c.client.GetQRChannel(ctx)
		if err != nil {
			if errors.Is(err, whatsmeow.ErrQRStoreContainsID) {
				report(connectors.LoginStatus{Connected: true, Detail: "authenticated"})
				return
			}
			c.setQRDetail("qr channel error: " + err.Error())
			return
		}
		if err := c.client.Connect(); err != nil {
			c.setQRDetail("connect error: " + err.Error())
			return
		}

		paired, retry := c.consumeQR(qrChan, report)
		if paired {
			return
		}
		if !retry {
			c.client.Disconnect()
			return
		}

		// Window expired without a scan — reset and regenerate a fresh code.
		c.client.Disconnect()
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// consumeQR drains one QR channel: each new code is rendered to PNG and cached
// (and the very first across the whole loop is reported via report). Returns
// whether pairing succeeded and whether the caller should renew an expired QR.
// Pairing failures remain visible until login is explicitly retried.
func (c *Connector) consumeQR(qrChan <-chan whatsmeow.QRChannelItem, report func(connectors.LoginStatus)) (paired, retry bool) {
	for item := range qrChan {
		switch item.Event {
		case "code":
			png, err := qrcode.Encode(item.Code, qrcode.Medium, qrPNGSize)
			st := connectors.LoginStatus{NeedsAuth: true, Detail: "scan QR code in WhatsApp"}
			if err == nil {
				st.QRPNG = png
				c.mu.Lock()
				c.qrPNG = png
				c.qrDetail = st.Detail
				c.qrExpiresAt = time.Now().Add(item.Timeout)
				c.mu.Unlock()
			} else {
				c.setQRDetail("QR rendering failed")
				c.log.Errorf("QR rendering failed: %v", err)
				report(connectors.LoginStatus{NeedsAuth: true, Detail: "QR rendering failed"})
				return false, false
			}
			report(st)
		case "success":
			c.mu.Lock()
			c.qrPNG = nil
			c.qrDetail = "authenticated"
			c.mu.Unlock()
			report(connectors.LoginStatus{Connected: true, Detail: "authenticated"})
			return true, false
		case "timeout":
			c.setQRDetail("qr timeout")
			return false, true
		default:
			detail := "qr " + item.Event
			if item.Error != nil {
				detail += ": " + pairingErrorDetail(item.Error)
			}
			c.setQRDetail(detail)
			c.log.Errorf("WhatsApp pairing failed: %s", detail)
			report(connectors.LoginStatus{NeedsAuth: true, Detail: detail})
			return false, false
		}
	}
	return false, true
}

// pairingErrorDetail keeps protocol payloads and credentials out of status/logs.
func pairingErrorDetail(err error) string {
	var database *whatsmeow.PairDatabaseError
	var protocol *whatsmeow.PairProtoError
	switch {
	case errors.As(err, &database):
		return database.Message
	case errors.As(err, &protocol):
		return protocol.Message
	case errors.Is(err, whatsmeow.ErrPairInvalidDeviceIdentityHMAC):
		return "device identity verification failed"
	case errors.Is(err, whatsmeow.ErrPairInvalidDeviceSignature):
		return "device signature verification failed"
	case errors.Is(err, whatsmeow.ErrPairRejectedLocally):
		return "pairing rejected locally"
	default:
		return fmt.Sprintf("pairing failure (%T)", err)
	}
}

// setQRDetail updates the cached human-readable login detail under lock.
func (c *Connector) setQRDetail(detail string) {
	c.mu.Lock()
	c.qrDetail = detail
	c.qrPNG = nil
	c.qrExpiresAt = time.Time{}
	c.mu.Unlock()
}

// Status reports current auth/connection state without driving login.
func (c *Connector) Status(_ context.Context) (connectors.LoginStatus, error) {
	c.mu.Lock()
	if !c.qrExpiresAt.IsZero() && !time.Now().Before(c.qrExpiresAt) {
		c.qrPNG = nil
	}
	qr := c.qrPNG
	detail := c.qrDetail
	c.mu.Unlock()

	st := connectors.LoginStatus{
		Connected: c.client.IsConnected() && c.client.IsLoggedIn(),
		NeedsAuth: c.client.Store.ID == nil,
		QRPNG:     qr,
		Detail:    detail,
	}
	if st.Connected && detail == "" {
		st.Detail = "connected"
	}
	return st, nil
}

// handleEvent is the single whatsmeow event handler. It normalizes messages and
// history into canonical events and fans them out without blocking the socket.
func (c *Connector) handleEvent(evt any) {
	switch v := evt.(type) {
	case *events.Message:
		c.emit(mapMessage(v, c.accountID(), event.SourceLive, nowUTC()))

	case *events.HistorySync:
		c.handleHistorySync(v)

	case *events.MediaRetry:
		c.handleMediaRetry(v)

	case *events.LabelEdit:
		c.handleLabelEdit(v)

	case *events.LabelAssociationChat:
		c.handleLabelAssociation(v)
	}
}

// handleLabelEdit records/updates/removes a Business label's display name.
func (c *Connector) handleLabelEdit(v *events.LabelEdit) {
	if v == nil || v.LabelID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if v.Action.GetDeleted() {
		delete(c.labelNames, v.LabelID)
		return
	}
	if name := v.Action.GetName(); name != "" {
		c.labelNames[v.LabelID] = name
	}
}

// handleLabelAssociation adds/removes a label from a chat.
func (c *Connector) handleLabelAssociation(v *events.LabelAssociationChat) {
	if v == nil || v.LabelID == "" {
		return
	}
	id := chatIDForJID(v.JID)
	c.mu.Lock()
	defer c.mu.Unlock()
	set := c.chatLabels[id]
	if v.Action.GetLabeled() {
		if set == nil {
			set = make(map[string]bool)
			c.chatLabels[id] = set
		}
		set[v.LabelID] = true
	} else if set != nil {
		delete(set, v.LabelID)
		if len(set) == 0 {
			delete(c.chatLabels, id)
		}
	}
}

// labelsForChat resolves a chat's applied label IDs to display names.
func (c *Connector) labelsForChat(chatID string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := c.chatLabels[chatID]
	if len(ids) == 0 {
		return nil
	}
	labels := make([]string, 0, len(ids))
	for id := range ids {
		if name := c.labelNames[id]; name != "" {
			labels = append(labels, name)
		} else {
			labels = append(labels, id)
		}
	}
	return labels
}

// handleHistorySync flattens a history-sync payload into canonical backfill
// events. Each stored WebMessageInfo is parsed via the client (reusing the live
// decode path) so the same mapper applies.
func (c *Connector) handleHistorySync(v *events.HistorySync) {
	if v == nil || v.Data == nil {
		return
	}
	acct := c.accountID()
	for _, conv := range v.Data.GetConversations() {
		chatJID, err := types.ParseJID(conv.GetID())
		if err != nil {
			continue
		}
		for _, hmsg := range conv.GetMessages() {
			webMsg := hmsg.GetMessage()
			if webMsg == nil {
				continue
			}
			parsed, err := c.client.ParseWebMessage(chatJID, webMsg)
			if err != nil {
				continue
			}
			c.emit(mapMessage(parsed, acct, event.SourceBackfill, nowUTC()))
		}
	}
}

// emit fans a canonical message out to the live sink and any backfill sinks,
// racing the connector context so a shutdown never deadlocks the socket goroutine.
func (c *Connector) emit(m event.Message) {
	c.mu.Lock()
	ctx := c.ctx
	out := c.out
	sinks := make([]chan<- event.Message, 0, len(c.sinks))
	for _, s := range c.sinks {
		sinks = append(sinks, s)
	}
	c.mu.Unlock()

	send := func(ch chan<- event.Message) {
		if ch == nil {
			return
		}
		if ctx == nil {
			// No active Connect context; best-effort non-blocking send.
			select {
			case ch <- m:
			default:
			}
			return
		}
		select {
		case ch <- m:
		case <-ctx.Done():
		}
	}

	send(out)
	for _, s := range sinks {
		send(s)
	}
}

func (c *Connector) addSink(out chan<- event.Message) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextSink
	c.nextSink++
	c.sinks[id] = out
	return id
}

func (c *Connector) removeSink(id int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.sinks, id)
}

// parseChatID turns a canonical "whatsapp:<jid>" chat id into a whatsmeow JID.
func parseChatID(chatID string) (types.JID, error) {
	raw := stripPlatformPrefix(chatID)
	if raw == "" {
		return types.JID{}, errors.New("whatsapp: empty chat id")
	}
	jid, err := types.ParseJID(raw)
	if err != nil {
		return types.JID{}, fmt.Errorf("whatsapp: bad chat id %q: %w", chatID, err)
	}
	return jid, nil
}

// stripPlatformPrefix removes a leading "whatsapp:" if present.
func stripPlatformPrefix(s string) string {
	return strings.TrimPrefix(s, event.PlatformWhatsApp+":")
}

// nowUTC returns the current UTC time (indirection kept tiny for clarity).
func nowUTC() time.Time { return time.Now().UTC() }
