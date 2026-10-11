package meta

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/seahyc/switchboard-oss/internal/connectors"
	"github.com/seahyc/switchboard-oss/internal/event"
)

// Config configures the shared Meta client.
type Config struct {
	Homeserver string // Conduit C-S URL, e.g. http://127.0.0.1:6167
	UserID     string // the switchboard Matrix account, e.g. @sb:meta.local
	Token      string // access token for UserID
	StatePath  string // room↔chat index + watermark file
	// GhostPrefixes are the bridges' username-template prefixes used to parse
	// remote user ids from ghost mxids (default: meta_, instagram_, messenger_).
	GhostPrefixes []string
	// Self alias signals stamped on is_self messages for the gbrain registry.
	SelfHandle, SelfPhone, SelfEmail string
	Logger                           *slog.Logger
}

// RoomInfo is the bridge-derived metadata for a portal room.
type RoomInfo struct {
	Network        string
	IsGroup        bool
	Title          string
	RemoteThreadID string
	Archived       bool
	Muted          bool
}

// matrixBackend is the narrow Matrix surface the connector needs. The real
// implementation (backend_mautrix.go) wraps mautrix-go; tests use a fake. This
// is the seam that keeps all connector logic unit-testable without a live
// homeserver.
type matrixBackend interface {
	SelfMXID() string
	// Sync runs the live sync loop, invoking onEvent for each decoded timeline
	// event until ctx is cancelled. Events carry their owning Network.
	Sync(ctx context.Context, onEvent func(MatrixEvent)) error
	// Paginate fetches up to limit historical events in a room, newest→oldest,
	// older than fromToken (""=from the latest). Returns events + the next
	// pagination token ("" when history is exhausted).
	Paginate(ctx context.Context, roomID, fromToken string, limit int) ([]MatrixEvent, string, error)
	// JoinedRooms lists portal room ids the account is in.
	JoinedRooms(ctx context.Context) ([]string, error)
	// RoomInfo returns bridge-derived metadata for a room.
	RoomInfo(ctx context.Context, roomID string) (RoomInfo, error)
	// SendText sends a (optionally reply) text message, returning the Matrix event id.
	SendText(ctx context.Context, roomID, text, replyToEventID string) (string, error)
	// SendMedia uploads bytes and sends a media message.
	SendMedia(ctx context.Context, roomID string, att event.Attachment, data []byte) (string, error)
	// React sends an emoji annotation on a target event.
	React(ctx context.Context, roomID, targetEventID, emoji string) (string, error)
	// Download fetches the bytes behind an mxc:// URI.
	Download(ctx context.Context, mxc string) (io.ReadCloser, int64, error)
	// LoggedIn reports whether the given network is authenticated on the bridge.
	LoggedIn(ctx context.Context, network string) (bool, string, error)
	// StartChat resolves an identifier (a @username or numeric remote id) to a
	// DM portal on the given network, creating the conversation via the bridge
	// if it doesn't exist yet. Returns the portal room id, the remote thread id
	// (for the chat_id), and the resolved display name. Does NOT send a message.
	StartChat(ctx context.Context, network, identifier string) (roomID, threadID, resolvedName string, err error)
}

// Client is the shared session: one backend, one sync loop, fanned out to the
// per-network Connector facades.
type Client struct {
	backend       matrixBackend
	st            *state
	self          SelfSignals
	ghostPrefixes []string
	log           *slog.Logger

	mu          sync.Mutex
	syncStarted bool
	subs        map[string][]chan<- event.Message // network → live subscribers
}

// NewClient builds the shared client from a config + a backend. The daemon's
// main.go constructs the real mautrix backend; tests inject a fake.
func NewClient(cfg Config, backend matrixBackend) *Client {
	prefixes := cfg.GhostPrefixes
	if len(prefixes) == 0 {
		prefixes = []string{"meta_", "instagram_", "messenger_"}
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Client{
		backend:       backend,
		st:            newState(cfg.StatePath),
		self:          SelfSignals{MXID: backend.SelfMXID(), Handle: cfg.SelfHandle, Phone: cfg.SelfPhone, Email: cfg.SelfEmail},
		ghostPrefixes: prefixes,
		log:           log,
		subs:          map[string][]chan<- event.Message{},
	}
}

// Instagram returns the Instagram-facing connector facade.
func (c *Client) Instagram() *Connector { return &Connector{client: c, network: PlatformInstagram} }

// Messenger returns the Messenger-facing connector facade.
func (c *Client) Messenger() *Connector { return &Connector{client: c, network: PlatformMessenger} }

// Load restores persisted state (room index + watermarks). Best-effort.
func (c *Client) Load() error { return c.st.load() }

// subscribe registers a live out channel for a network and ensures the shared
// sync loop is running. It blocks until ctx is cancelled, then unsubscribes.
func (c *Client) subscribe(ctx context.Context, network string, out chan<- event.Message) error {
	c.mu.Lock()
	c.subs[network] = append(c.subs[network], out)
	start := !c.syncStarted
	c.syncStarted = true
	c.mu.Unlock()

	if start {
		go func() {
			if err := c.backend.Sync(ctx, c.onEvent); err != nil && ctx.Err() == nil {
				c.log.Error("meta sync loop exited", "err", err)
			}
		}()
	}
	<-ctx.Done()
	return ctx.Err()
}

// onEvent is the single sync callback: discover/refresh the room, map the event,
// dispatch to the owning network's subscribers, and advance the watermark.
func (c *Client) onEvent(ev MatrixEvent) {
	network := ev.Network
	if network != PlatformInstagram && network != PlatformMessenger {
		return // unknown/unsupported room
	}
	// Keep the room index fresh (chat_id resolution + Send routing).
	chatID := canonicalChatID(network, ev)
	c.st.upsertRoom(ev.RoomID, chatID, network, ev.IsGroup, "")

	m, ok := mapEvent(ev, network, c.self, c.ghostPrefixes, event.SourceLive, time.Now())
	if !ok {
		return
	}
	c.dispatch(network, m)
	c.st.advanceWatermark(ev.RoomID, ev.TSMillis)
}

func (c *Client) dispatch(network string, m event.Message) {
	c.mu.Lock()
	subs := append([]chan<- event.Message(nil), c.subs[network]...)
	c.mu.Unlock()
	for _, out := range subs {
		out <- m
	}
}

// backfill drives history for one network into out. chatID=="" = all that
// network's portals. Walks newest→oldest via the backend, bounded by since +
// each room's watermark already covered by the idempotent canonical id.
func (c *Client) backfill(ctx context.Context, network, chatID string, since time.Time, out chan<- event.Message) error {
	rooms, err := c.targetRooms(ctx, network, chatID)
	if err != nil {
		return err
	}
	sinceMs := since.UnixMilli()
	for _, roomID := range rooms {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.backfillRoom(ctx, network, roomID, sinceMs, out); err != nil {
			c.log.Warn("meta backfill room failed", "network", network, "room", roomID, "err", err)
		}
	}
	return nil
}

func (c *Client) backfillRoom(ctx context.Context, network, roomID string, sinceMs int64, out chan<- event.Message) error {
	const pageSize = 100
	token := ""
	var emitted, oldest int64
	oldest = 1<<63 - 1
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		evs, next, err := c.backend.Paginate(ctx, roomID, token, pageSize)
		if err != nil {
			return err
		}
		if len(evs) == 0 {
			break
		}
		stop := false
		for _, ev := range evs {
			if ev.Network == "" {
				ev.Network = network
			}
			if sinceMs > 0 && ev.TSMillis < sinceMs {
				stop = true
				break
			}
			m, ok := mapEvent(ev, network, c.self, c.ghostPrefixes, event.SourceBackfill, time.Now())
			if !ok {
				continue
			}
			out <- m
			emitted++
			if ev.TSMillis < oldest {
				oldest = ev.TSMillis
			}
		}
		if stop || next == "" {
			break
		}
		token = next
	}
	// Honest reporting: how much we actually got + the oldest ts reached. We never
	// claim "complete" — IG lazy-loads/expires history (see docs/META_CONNECTOR.md).
	if emitted > 0 {
		c.log.Info("meta backfill done", "network", network, "room", roomID,
			"emitted", emitted, "oldest_ts", time.UnixMilli(oldest).UTC())
	}
	return nil
}

// targetRooms resolves the rooms to backfill: a single chat or all of a network's.
func (c *Client) targetRooms(ctx context.Context, network, chatID string) ([]string, error) {
	if chatID != "" {
		if rid, ok := c.st.roomForChat(chatID); ok {
			return []string{rid}, nil
		}
		return nil, fmt.Errorf("meta: no room known for chat %q (not yet discovered via sync)", chatID)
	}
	// All portals for this network: refresh the index from the homeserver first.
	c.refreshRooms(ctx)
	recs := c.st.roomsForNetwork(network)
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.RoomID)
	}
	return out, nil
}

// refreshRooms enumerates joined portals and updates the index (network, title,
// remote thread id). Best-effort; used by Backfill-all + ChatMetadata/Contacts.
func (c *Client) refreshRooms(ctx context.Context) {
	roomIDs, err := c.backend.JoinedRooms(ctx)
	if err != nil {
		c.log.Warn("meta JoinedRooms failed", "err", err)
		return
	}
	for _, rid := range roomIDs {
		info, err := c.backend.RoomInfo(ctx, rid)
		if err != nil || (info.Network != PlatformInstagram && info.Network != PlatformMessenger) {
			continue
		}
		chatID := info.Network + ":room:" + rid
		if info.RemoteThreadID != "" {
			chatID = info.Network + ":" + info.RemoteThreadID
		}
		c.st.upsertRoom(rid, chatID, info.Network, info.IsGroup, info.Title)
	}
	_ = c.st.save()
}

// Connector is one per-network facade over the shared Client. It implements the
// full connectors.Connector interface plus the optional capability interfaces,
// so Instagram/Messenger are first-class channels identical to the others.
type Connector struct {
	client  *Client
	network string // instagram | messenger
}

// Compile-time assertions: the base interface + the capabilities we support.
var (
	_ connectors.Connector            = (*Connector)(nil)
	_ connectors.MediaDownloader      = (*Connector)(nil)
	_ connectors.ChatMetadataProvider = (*Connector)(nil)
	_ connectors.ContactsProvider     = (*Connector)(nil)
	_ connectors.ChatStarter          = (*Connector)(nil)
)

func (c *Connector) Name() string { return c.network }

func (c *Connector) Connect(ctx context.Context, out chan<- event.Message) error {
	return c.client.subscribe(ctx, c.network, out)
}

func (c *Connector) Backfill(ctx context.Context, chatID string, since time.Time, out chan<- event.Message) error {
	return c.client.backfill(ctx, c.network, chatID, since, out)
}

func (c *Connector) Send(ctx context.Context, req connectors.SendRequest) (string, error) {
	roomID, ok := c.client.st.roomForChat(req.ChatID)
	if !ok {
		return "", fmt.Errorf("meta: unknown chat %q", req.ChatID)
	}
	replyEvt := matrixEventIDFromCanonical(req.ReplyTo)

	// Media first if present (IG/Messenger DMs support photo/video/file/voice).
	if len(req.Attachments) > 0 {
		// Bytes resolution from the media store is the daemon's job; here we
		// forward the attachment ref/metadata. The backend uploads + sends.
		evtID, err := c.client.backend.SendMedia(ctx, roomID, req.Attachments[0], nil)
		if err != nil {
			return "", err
		}
		return event.MakeID(c.network, "evt:"+evtID), nil
	}

	if req.Poll != nil || req.Card != nil || req.Buttons != nil {
		return "", fmt.Errorf("meta: rich UI (poll/card/buttons) unsupported on %s DMs", c.network)
	}
	evtID, err := c.client.backend.SendText(ctx, roomID, req.Text, replyEvt)
	if err != nil {
		return "", err
	}
	return event.MakeID(c.network, "evt:"+evtID), nil
}

func (c *Connector) React(ctx context.Context, chatID, msgID, emoji string) error {
	roomID, ok := c.client.st.roomForChat(chatID)
	if !ok {
		return fmt.Errorf("meta: unknown chat %q", chatID)
	}
	target := matrixEventIDFromCanonical(msgID)
	if target == "" {
		return fmt.Errorf("meta: cannot resolve target event for %q", msgID)
	}
	_, err := c.client.backend.React(ctx, roomID, target, emoji)
	return err
}

// StartChat (ChatStarter): resolve identifier → DM portal → canonical chat_id,
// creating the conversation via the bridge if needed. No message is sent. The
// returned chat_id is then usable with Send for the cold-DM.
func (c *Connector) StartChat(ctx context.Context, identifier string) (chatID, resolvedName string, err error) {
	return c.client.startChat(ctx, c.network, identifier)
}

func (c *Client) startChat(ctx context.Context, network, identifier string) (chatID, resolvedName string, err error) {
	roomID, threadID, name, err := c.backend.StartChat(ctx, network, identifier)
	if err != nil {
		return "", "", err
	}
	if roomID == "" {
		return "", "", fmt.Errorf("meta: start_chat produced no portal for %q", identifier)
	}
	chatID = network + ":" + threadID
	if threadID == "" {
		chatID = network + ":room:" + roomID
	}
	// Register immediately so Send resolves the chat without waiting for the
	// next metadata refresh.
	c.st.upsertRoom(roomID, chatID, network, false, name)
	_ = c.st.save()
	return chatID, name, nil
}

// DownloadMedia (MediaDownloader): fetchRef is the mxc:// uri we stored on the
// attachment; fetch bytes via the Matrix media repo for the gated media worker.
func (c *Connector) DownloadMedia(ctx context.Context, fetchRef string) (io.ReadCloser, int64, error) {
	return c.client.backend.Download(ctx, fetchRef)
}

// ChatMetadata (ChatMetadataProvider): titles + group signals for this network's
// portals, into the same chat projection as the other channels.
func (c *Connector) ChatMetadata(ctx context.Context) ([]connectors.ChatMeta, error) {
	c.client.refreshRooms(ctx)
	var out []connectors.ChatMeta
	for _, r := range c.client.st.roomsForNetwork(c.network) {
		out = append(out, connectors.ChatMeta{
			ChatID: r.ChatID,
			Title:  r.Title,
		})
	}
	return out, nil
}

// Contacts (ContactsProvider): the DM counterparts as address-book entries so IG/
// Messenger people get named person pages + become cross-platform merge
// candidates. Names come from the portal/ghost profiles refreshed via RoomInfo.
func (c *Connector) Contacts(ctx context.Context) ([]connectors.Contact, error) {
	c.client.refreshRooms(ctx)
	var out []connectors.Contact
	for _, r := range c.client.st.roomsForNetwork(c.network) {
		if r.IsGroup || r.Title == "" {
			continue
		}
		out = append(out, connectors.Contact{
			FullName: r.Title,
		})
	}
	return out, nil
}

func (c *Connector) Login(ctx context.Context) (connectors.LoginStatus, error) {
	return c.status(ctx)
}

func (c *Connector) Status(ctx context.Context) (connectors.LoginStatus, error) {
	return c.status(ctx)
}

func (c *Connector) status(ctx context.Context) (connectors.LoginStatus, error) {
	ok, detail, err := c.client.backend.LoggedIn(ctx, c.network)
	if err != nil {
		return connectors.LoginStatus{Detail: err.Error()}, nil
	}
	if !ok {
		return connectors.LoginStatus{
			NeedsAuth: true,
			Detail:    fmt.Sprintf("%s not logged in on the bridge — provide cookies once (see docs/META_CONNECTOR.md §4). %s", c.network, detail),
		}, nil
	}
	return connectors.LoginStatus{Connected: true, Detail: detail}, nil
}

// matrixEventIDFromCanonical recovers the Matrix event id from a canonical id of
// the "<network>:evt:<matrixEventID>" form. Returns "" when the id is a remote-id
// form (we don't keep a reverse map yet; reply/react then no-op-fallback). The
// "evt:" form is what we always emit for our own sent messages, so reply-to-self
// + reacting to recently-seen messages resolves.
func matrixEventIDFromCanonical(canonicalID string) string {
	if canonicalID == "" {
		return ""
	}
	// strip "<network>:" prefix
	i := indexByte(canonicalID, ':')
	if i < 0 {
		return ""
	}
	rest := canonicalID[i+1:]
	const p = "evt:"
	if len(rest) > len(p) && rest[:len(p)] == p {
		return rest[len(p):]
	}
	return ""
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
