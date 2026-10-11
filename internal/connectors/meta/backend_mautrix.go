package meta

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix"
	mxevent "maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// mautrixBackend is the real matrixBackend, wrapping a mautrix-go client talking
// to the self-hosted Conduit homeserver. It is the ONLY file that imports
// mautrix-go; everything else in this package is library-agnostic and unit
// tested via fakeBackend.
//
// NOTE: bridge-specific extraction (network detection from room state, remote
// message/thread ids stamped by mautrix-meta) is marked where it must be
// confirmed against the LIVE bridge — that verification waits on the user's §10
// go-ahead (no live auth yet). The wiring compiles and is structurally complete.
type mautrixBackend struct {
	cli    *mautrix.Client
	selfID string
	logger *slog.Logger

	mu          sync.Mutex
	roomNetwork map[string]string      // roomID → instagram|messenger (cache)
	roomThread  map[string]string      // roomID → remote thread/channel id (cache)
	mgmtRoom    map[string]string      // network → management-room id (cache)
	waiters     map[string]chan string // roomID → channel for the next bot reply
}

// NewMautrixBackend constructs the live backend. It does NOT connect or sync —
// the daemon drives that via Connect after auth.
func NewMautrixBackend(cfg Config) (matrixBackend, error) {
	if cfg.Homeserver == "" || cfg.UserID == "" {
		return nil, errors.New("meta: homeserver + user id required")
	}
	cli, err := mautrix.NewClient(cfg.Homeserver, id.UserID(cfg.UserID), cfg.Token)
	if err != nil {
		return nil, fmt.Errorf("meta: new matrix client: %w", err)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &mautrixBackend{
		cli:         cli,
		selfID:      cfg.UserID,
		logger:      logger,
		roomNetwork: map[string]string{},
		roomThread:  map[string]string{},
		mgmtRoom:    map[string]string{},
		waiters:     map[string]chan string{},
	}, nil
}

func (b *mautrixBackend) SelfMXID() string { return b.selfID }

func (b *mautrixBackend) Sync(ctx context.Context, onEvent func(MatrixEvent)) error {
	syncer, ok := b.cli.Syncer.(*mautrix.DefaultSyncer)
	if !ok {
		return errors.New("meta: unexpected syncer type")
	}
	handle := func(_ mautrix.EventSource, evt *mxevent.Event) {
		// Deliver bridge-bot replies in the management room to a pending
		// start_chat waiter (the reliable, push-based correlation — the sync
		// stream definitely receives the reply).
		b.deliverReply(evt)
		me := b.decode(ctx, evt)
		if me.Type != "" {
			onEvent(me)
		}
	}
	for _, t := range []string{evTypeMessage, evTypeReaction, evTypeRedaction} {
		syncer.OnEventType(mxevent.NewEventType(t), handle)
	}
	// Member events: auto-join portal-room invites for our own account (the bridge
	// invites @sb to each portal it creates), then fall through to the normal
	// handler. Without this the connector stays invited-but-not-joined and never
	// sees the thread's messages.
	syncer.OnEventType(mxevent.NewEventType(evTypeMember), func(src mautrix.EventSource, evt *mxevent.Event) {
		if evt.GetStateKey() == b.selfID {
			if mem, _ := evt.Content.Raw["membership"].(string); mem == "invite" {
				if _, err := b.cli.JoinRoomByID(evt.RoomID); err != nil {
					b.logger.Warn("meta auto-join failed", "room", evt.RoomID.String(), "err", err)
				}
			}
		}
		handle(src, evt)
	})

	// Run the blocking sync; stop it when ctx is cancelled.
	errCh := make(chan error, 1)
	go func() { errCh <- b.cli.Sync() }()
	select {
	case <-ctx.Done():
		b.cli.StopSync()
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (b *mautrixBackend) Paginate(ctx context.Context, roomID, from string, limit int) ([]MatrixEvent, string, error) {
	// Conduit's /messages requires a real pagination token — an empty `from`
	// returns nothing. On the first page, start from the current sync position
	// (the syncer persists next_batch to the Store) and page backward from newest.
	if from == "" {
		from = b.cli.Store.LoadNextBatch(b.cli.UserID)
		if from == "" {
			// The live sync hasn't established a position yet; nothing to page from.
			return nil, "", nil
		}
	}
	// dir 'b' = backward (newest→oldest).
	resp, err := b.cli.Messages(id.RoomID(roomID), from, "", 'b', limit)
	if err != nil {
		return nil, "", err
	}
	out := make([]MatrixEvent, 0, len(resp.Chunk))
	for _, evt := range resp.Chunk {
		me := b.decode(ctx, evt)
		if me.Type != "" {
			out = append(out, me)
		}
	}
	return out, resp.End, nil
}

func (b *mautrixBackend) JoinedRooms(ctx context.Context) ([]string, error) {
	resp, err := b.cli.JoinedRooms()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(resp.JoinedRooms))
	for _, r := range resp.JoinedRooms {
		out = append(out, r.String())
	}
	return out, nil
}

func (b *mautrixBackend) RoomInfo(ctx context.Context, roomID string) (RoomInfo, error) {
	network, threadID := b.bridgeMeta(roomID)
	info := RoomInfo{Network: network, RemoteThreadID: threadID}
	// Room name (DM counterpart / group subject).
	var nameContent struct {
		Name string `json:"name"`
	}
	if err := b.cli.StateEvent(id.RoomID(roomID), mxevent.StateRoomName, "", &nameContent); err == nil {
		info.Title = nameContent.Name
	}
	return info, nil
}

func (b *mautrixBackend) SendText(ctx context.Context, roomID, text, replyToEventID string) (string, error) {
	content := map[string]any{"msgtype": msgText, "body": text}
	if replyToEventID != "" {
		content["m.relates_to"] = map[string]any{
			"m.in_reply_to": map[string]any{"event_id": replyToEventID},
		}
	}
	resp, err := b.cli.SendMessageEvent(id.RoomID(roomID), mxevent.EventMessage, content)
	if err != nil {
		return "", err
	}
	return resp.EventID.String(), nil
}

func (b *mautrixBackend) SendMedia(ctx context.Context, roomID string, att event.Attachment, data []byte) (string, error) {
	if len(data) == 0 {
		// The daemon resolves bytes from the media store before calling Send; until
		// that wiring lands, fail loudly rather than send an empty message.
		return "", errors.New("meta: media send requires resolved bytes (daemon media resolution pending)")
	}
	up, err := b.cli.UploadBytes(data, att.MIME)
	if err != nil {
		return "", err
	}
	msgtype := msgFile
	switch {
	case strings.HasPrefix(att.MIME, "image/"):
		msgtype = msgImage
	case strings.HasPrefix(att.MIME, "video/"):
		msgtype = msgVideo
	case strings.HasPrefix(att.MIME, "audio/"):
		msgtype = msgAudio
	}
	content := map[string]any{
		"msgtype": msgtype,
		"body":    att.Name,
		"url":     up.ContentURI.String(),
		"info":    map[string]any{"mimetype": att.MIME, "size": att.Size},
	}
	resp, err := b.cli.SendMessageEvent(id.RoomID(roomID), mxevent.EventMessage, content)
	if err != nil {
		return "", err
	}
	return resp.EventID.String(), nil
}

func (b *mautrixBackend) React(ctx context.Context, roomID, targetEventID, emoji string) (string, error) {
	resp, err := b.cli.SendReaction(id.RoomID(roomID), id.EventID(targetEventID), emoji)
	if err != nil {
		return "", err
	}
	return resp.EventID.String(), nil
}

func (b *mautrixBackend) Download(ctx context.Context, mxc string) (io.ReadCloser, int64, error) {
	uri, err := id.ParseContentURI(mxc)
	if err != nil {
		return nil, 0, fmt.Errorf("meta: bad mxc uri %q: %w", mxc, err)
	}
	rc, err := b.cli.Download(uri)
	if err != nil {
		return nil, 0, err
	}
	return rc, 0, nil // length unknown from the Matrix media repo
}

func (b *mautrixBackend) LoggedIn(ctx context.Context, network string) (bool, string, error) {
	// Session validity check.
	if _, err := b.cli.Whoami(); err != nil {
		return false, "matrix session invalid", err
	}
	// Proxy for "network bridged": at least one portal of this network is joined.
	// (A precise check would query the bridge bot's login list; refine once live.)
	rooms, err := b.JoinedRooms(ctx)
	if err != nil {
		return false, "", err
	}
	for _, r := range rooms {
		if b.networkOf(r) == network {
			return true, "portal(s) present", nil
		}
	}
	return false, "no portals yet — bridge not logged in to " + network, nil
}

// bridgeMeta reads the portal's `m.bridge` state event ONCE and returns +caches
// the owning network and the remote thread/channel id. mautrix-meta stamps the
// bridge info under state_key "<homeserver domain>/<network>" (verified live:
// "meta.local/instagram", content {"protocol":{"id":"instagramgo"},
// "channel":{"id":"<thread id>"}}). We MUST use that state_key — an empty key
// 404s, which previously left network="" and dropped every event. Cached per room.
func (b *mautrixBackend) bridgeMeta(roomID string) (network, threadID string) {
	b.mu.Lock()
	n, okN := b.roomNetwork[roomID]
	th := b.roomThread[roomID]
	b.mu.Unlock()
	if okN {
		return n, th
	}

	domain := b.domain()
	for _, cand := range []string{PlatformInstagram, PlatformMessenger} {
		var content map[string]any
		// BuildURL PathEscapes the "/" in the state key, so this resolves correctly.
		if err := b.cli.StateEvent(id.RoomID(roomID), mxevent.NewEventType("m.bridge"), domain+"/"+cand, &content); err == nil {
			n = cand
			if ch, ok := content["channel"].(map[string]any); ok {
				th, _ = ch["id"].(string)
			}
			break
		}
	}
	if n != "" {
		b.mu.Lock()
		b.roomNetwork[roomID] = n
		if th != "" {
			b.roomThread[roomID] = th
		}
		b.mu.Unlock()
	}
	return n, th
}

// domain extracts the homeserver domain from the account mxid ("@sb:meta.local"
// → "meta.local") for building the m.bridge state key.
func (b *mautrixBackend) domain() string {
	if i := strings.IndexByte(b.selfID, ':'); i >= 0 {
		return b.selfID[i+1:]
	}
	return ""
}

// networkOf returns just the owning network (instagram|messenger) for a room.
func (b *mautrixBackend) networkOf(roomID string) string {
	n, _ := b.bridgeMeta(roomID)
	return n
}

// StartChat drives the bridge bot's management commands to resolve an identifier
// and create a DM portal (mautrix-meta has no public username→id resolver, so we
// `search` for a username, then `start-chat <numeric id>`). No message is sent.
func (b *mautrixBackend) StartChat(ctx context.Context, network, identifier string) (roomID, threadID, resolvedName string, err error) {
	mgmt, botMXID, err := b.managementRoom(network)
	if err != nil {
		return "", "", "", err
	}
	remoteID := strings.TrimPrefix(strings.TrimSpace(identifier), "@")
	if remoteID == "" {
		return "", "", "", errors.New("meta: empty identifier")
	}
	if !allDigits(remoteID) {
		reply, serr := b.sendCommandAwait(ctx, mgmt, botMXID, "search "+remoteID)
		if serr != nil {
			return "", "", "", serr
		}
		hits := parseSearchHits(reply)
		switch {
		case len(hits) == 0:
			return "", "", "", fmt.Errorf("meta: no %s user found for %q", network, identifier)
		case len(hits) > 1:
			var names []string
			for _, h := range hits {
				names = append(names, fmt.Sprintf("%s (%s)", h.Name, h.ID))
			}
			return "", "", "", fmt.Errorf("meta: %q matched %d users — pass the exact numeric id of: %s",
				identifier, len(hits), strings.Join(names, ", "))
		}
		remoteID = hits[0].ID
		resolvedName = hits[0].Name
	}

	reply, serr := b.sendCommandAwait(ctx, mgmt, botMXID, "start-chat "+remoteID)
	if serr != nil {
		return "", "", "", serr
	}
	if isBridgeError(reply) {
		return "", "", "", fmt.Errorf("meta: start-chat failed: %s", reply)
	}
	roomID = parseCreatedRoomID(reply)
	if roomID == "" {
		return "", "", "", fmt.Errorf("meta: could not parse portal room from start-chat reply: %s", reply)
	}
	// Ensure we're joined (so a later Send can post into it), then read the
	// remote thread id. For 1:1 DMs the thread id equals the remote user id.
	_, _ = b.cli.JoinRoomByID(id.RoomID(roomID))
	if _, th := b.bridgeMeta(roomID); th != "" {
		threadID = th
	} else {
		threadID = remoteID
	}
	if resolvedName == "" {
		var nc struct {
			Name string `json:"name"`
		}
		if e := b.cli.StateEvent(id.RoomID(roomID), mxevent.StateRoomName, "", &nc); e == nil {
			resolvedName = nc.Name
		}
	}
	return roomID, threadID, resolvedName, nil
}

// managementRoom finds the @sb↔@<network>bot management DM. SAFETY-CRITICAL: we
// type bridge commands ("search", "start-chat") into this room, so it MUST be the
// real management DM and never a portal/space — typing a command into a portal
// would send that text to a real contact. We therefore require a room whose
// JOINED members are EXACTLY {our account, the bridge bot} (a clean 1:1 DM), that
// is NOT a portal (no m.bridge) and NOT a space. A real contact's portal has a
// ghost member (not @<network>bot) and carries m.bridge, so it can never match.
func (b *mautrixBackend) managementRoom(network string) (roomID, botMXID string, err error) {
	botMXID = "@" + network + "bot:" + b.domain()
	b.mu.Lock()
	cached := b.mgmtRoom[network]
	b.mu.Unlock()
	if cached != "" {
		return cached, botMXID, nil
	}
	rooms, err := b.JoinedRooms(context.Background())
	if err != nil {
		return "", "", err
	}
	for _, r := range rooms {
		if b.networkOf(r) != "" || b.isSpace(r) { // never a portal or a space
			continue
		}
		mem, e := b.cli.JoinedMembers(id.RoomID(r))
		if e != nil || mem == nil || len(mem.Joined) != 2 {
			continue
		}
		_, hasBot := mem.Joined[id.UserID(botMXID)]
		_, hasSelf := mem.Joined[id.UserID(b.selfID)]
		if hasBot && hasSelf {
			b.mu.Lock()
			b.mgmtRoom[network] = r
			b.mu.Unlock()
			return r, botMXID, nil
		}
	}
	return "", "", fmt.Errorf("meta: no management DM found for %s (bot %s)", network, botMXID)
}

// isSpace reports whether a room is a Matrix space (m.room.create type=m.space).
func (b *mautrixBackend) isSpace(roomID string) bool {
	var c struct {
		Type string `json:"type"`
	}
	if err := b.cli.StateEvent(id.RoomID(roomID), mxevent.NewEventType("m.room.create"), "", &c); err == nil {
		return c.Type == "m.space"
	}
	return false
}

// sendCommandAwait sends a bot command to the management room and waits (bounded)
// for the bot's reply, returning its body. It correlates by sending the command,
// then polling the room timeline for the newest bot message that arrived AFTER
// our command event.
func (b *mautrixBackend) sendCommandAwait(ctx context.Context, roomID, botMXID, command string) (string, error) {
	// Register a waiter for the next bot reply in this room BEFORE sending, so we
	// can't miss a fast reply. The sync handler (deliverReply) feeds it.
	ch := make(chan string, 4)
	b.mu.Lock()
	b.waiters[roomID] = ch
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.waiters, roomID)
		b.mu.Unlock()
	}()

	// Snapshot existing bot messages so the polling fallback can detect a NEW
	// reply (independent of the push stream).
	seen := map[string]bool{}
	for _, ev := range b.recentRoomMessages(roomID, 30) {
		if ev.Type.Type == evTypeMessage && strings.HasSuffix(localpart(ev.Sender.String()), "bot") {
			seen[ev.ID.String()] = true
		}
	}

	if _, err := b.cli.SendMessageEvent(id.RoomID(roomID), mxevent.EventMessage,
		map[string]any{"msgtype": msgText, "body": command}); err != nil {
		return "", fmt.Errorf("meta: send command %q: %w", command, err)
	}

	// Race two correlations for robustness: (1) the push waiter (fast, used in
	// production with a single sync); (2) a /messages poll (works even if the
	// sync stream is starved). Whichever finds the reply first wins.
	deadline := time.Now().Add(50 * time.Second)
	poll := time.NewTicker(3 * time.Second)
	defer poll.Stop()
	for {
		select {
		case body := <-ch:
			return body, nil
		case <-ctx.Done():
			return "", ctx.Err()
		case <-poll.C:
			for _, ev := range b.recentRoomMessages(roomID, 30) {
				if ev.Type.Type != evTypeMessage || seen[ev.ID.String()] {
					continue
				}
				if strings.HasSuffix(localpart(ev.Sender.String()), "bot") {
					if body, _ := ev.Content.Raw["body"].(string); body != "" {
						return body, nil
					}
				}
			}
			if time.Now().After(deadline) {
				return "", fmt.Errorf("meta: timed out waiting for bridge reply to %q", command)
			}
		}
	}
}

// recentRoomMessages reads recent timeline events in a room (newest-first) via
// /messages, starting from the live sync position. Empty if the sync hasn't
// established a position yet.
func (b *mautrixBackend) recentRoomMessages(roomID string, limit int) []*mxevent.Event {
	from := b.cli.Store.LoadNextBatch(b.cli.UserID)
	if from == "" {
		return nil
	}
	msgs, err := b.cli.Messages(id.RoomID(roomID), from, "", 'b', limit)
	if err != nil {
		return nil
	}
	return msgs.Chunk
}

// deliverReply routes a management-room bridge-bot message to a pending
// start_chat waiter (best-effort, non-blocking).
func (b *mautrixBackend) deliverReply(evt *mxevent.Event) {
	if evt.Type.Type != evTypeMessage {
		return
	}
	b.mu.Lock()
	ch := b.waiters[evt.RoomID.String()]
	b.mu.Unlock()
	if ch == nil {
		return
	}
	// Only bridge-bot messages (sender localpart ends with "bot").
	if !strings.HasSuffix(localpart(evt.Sender.String()), "bot") {
		return
	}
	if body, _ := evt.Content.Raw["body"].(string); body != "" {
		select {
		case ch <- body:
		default:
		}
	}
}

// normalizeProtocol maps a bridge protocol id to our network token.
func normalizeProtocol(pid string) string {
	switch strings.ToLower(pid) {
	case "instagram", "instagramgo", "meta-instagram":
		return PlatformInstagram
	case "facebook", "messenger", "facebookgo", "meta-facebook", "meta-messenger":
		return PlatformMessenger
	}
	return ""
}

// decode converts a mautrix event into our normalized MatrixEvent. Robust to
// missing fields; returns a zero-Type event for things we ignore.
func (b *mautrixBackend) decode(ctx context.Context, evt *mxevent.Event) MatrixEvent {
	t := evt.Type.Type
	network, threadID := b.bridgeMeta(evt.RoomID.String())
	me := MatrixEvent{
		RoomID:         evt.RoomID.String(),
		EventID:        evt.ID.String(),
		Sender:         evt.Sender.String(),
		Type:           t,
		Network:        network,
		RemoteThreadID: threadID, // → chat_id "instagram:<thread id>" (SPEC form)
		TSMillis:       evt.Timestamp,
		Redacts:        evt.Redacts.String(),
		Raw:            evt.Content.VeryRaw,
	}
	raw := evt.Content.Raw

	switch t {
	case evTypeMessage:
		me.MsgType, _ = raw["msgtype"].(string)
		me.Body, _ = raw["body"].(string)
		me.URL, _ = raw["url"].(string)
		me.FileName, _ = raw["filename"].(string)
		me.ExternalURL, _ = raw["external_url"].(string)
		if info, ok := raw["info"].(map[string]any); ok {
			me.MIME, _ = info["mimetype"].(string)
			me.Size = asInt64(info["size"])
		}
		if _, ok := raw["org.matrix.msc3245.voice"]; ok {
			me.IsVoice = true
		}
		decodeRelations(raw, &me)
		decodeMentions(raw, &me, b)
	case evTypeReaction:
		if rel, ok := raw["m.relates_to"].(map[string]any); ok {
			me.ReactionKey, _ = rel["key"].(string)
			me.ReactionTarget, _ = rel["event_id"].(string)
		}
	case evTypeMember:
		me.DisplayName, _ = raw["displayname"].(string)
	}
	return me
}

func decodeRelations(raw map[string]any, me *MatrixEvent) {
	rel, ok := raw["m.relates_to"].(map[string]any)
	if !ok {
		return
	}
	if rt, _ := rel["rel_type"].(string); rt == "m.replace" {
		me.IsEdit = true
		me.EditTarget, _ = rel["event_id"].(string)
		// The replacement body lives under m.new_content.
		if nc, ok := raw["m.new_content"].(map[string]any); ok {
			if body, ok := nc["body"].(string); ok {
				me.Body = body
			}
		}
	}
	if irt, ok := rel["m.in_reply_to"].(map[string]any); ok {
		me.ReplyToEvent, _ = irt["event_id"].(string)
	}
}

func decodeMentions(raw map[string]any, me *MatrixEvent, b *mautrixBackend) {
	m, ok := raw["m.mentions"].(map[string]any)
	if !ok {
		return
	}
	ids, ok := m["user_ids"].([]any)
	if !ok {
		return
	}
	for _, v := range ids {
		if mxid, ok := v.(string); ok {
			// Map ghost mxids → remote ids; keep the self mxid as-is so the
			// daemon's "@me" detection fires.
			if uid := remoteUserIDFromMXID(mxid, []string{"meta_", "instagram_", "messenger_"}); uid != "" {
				me.Mentions = append(me.Mentions, uid)
			} else {
				me.Mentions = append(me.Mentions, mxid)
			}
		}
	}
}

func asInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	}
	return 0
}
