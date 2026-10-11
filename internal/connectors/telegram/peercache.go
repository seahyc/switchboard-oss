package telegram

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/gotd/td/tg"
	"github.com/seahyc/switchboard-oss/internal/event"
)

// peerCache maps a canonical chat id ("telegram:user:123") to the InputPeer
// (with access hash) needed for outbound RPCs. Telegram requires the access hash
// to address a user/channel, and it only arrives via entities in updates,
// dialogs, or history — so we cache every peer we observe.
type peerCache struct {
	mu    sync.RWMutex
	m     map[string]tg.InputPeerClass
	names map[int64]senderName // user id -> cached display/handle/phone (#54)
}

func newPeerCache() *peerCache {
	return &peerCache{m: make(map[string]tg.InputPeerClass), names: map[int64]senderName{}}
}

// senderName is a cached human identity for a Telegram user id (#54): the display name
// (first+last / username), @username, and phone. Accumulated from every user entity we
// see (dialogs/history/updates) so a message whose own batch omits the sender entity
// still maps to a name — fixing blank DM sender_name/handle at ingest.
type senderName struct {
	display string
	handle  string
	phone   string
}

// rememberUser caches a user's identity if it carries any usable name field.
func (p *peerCache) rememberUser(u *tg.User) {
	if u == nil {
		return
	}
	n := senderName{display: displayName(u), handle: u.Username, phone: u.Phone}
	if n.display == "" && n.handle == "" && n.phone == "" {
		return
	}
	p.mu.Lock()
	p.names[u.ID] = n
	p.mu.Unlock()
}

// userName returns the cached identity for a user id (#54 blank-sender fallback).
func (p *peerCache) userName(id int64) (senderName, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n, ok := p.names[id]
	return n, ok
}

func (p *peerCache) put(chatID string, peer tg.InputPeerClass) {
	if chatID == "" || peer == nil {
		return
	}
	p.mu.Lock()
	p.m[chatID] = peer
	p.mu.Unlock()
}

func (p *peerCache) get(chatID string) (tg.InputPeerClass, bool) {
	p.mu.RLock()
	peer, ok := p.m[chatID]
	p.mu.RUnlock()
	return peer, ok
}

// learnFromEntities caches input peers for every user/chat/channel in an
// entity set (from updates, dialogs, or history results).
func (p *peerCache) learnFromEntities(users map[int64]*tg.User, chats map[int64]*tg.Chat, channels map[int64]*tg.Channel) {
	for id, u := range users {
		p.put(event.PlatformTelegram+":user:"+strconv.FormatInt(id, 10), u.AsInputPeer())
		p.rememberUser(u) // #54: cache the name so blank-batch senders still resolve
	}
	for id := range chats {
		p.put(event.PlatformTelegram+":chat:"+strconv.FormatInt(id, 10), &tg.InputPeerChat{ChatID: id})
	}
	for id, ch := range channels {
		p.put(event.PlatformTelegram+":channel:"+strconv.FormatInt(id, 10), ch.AsInputPeer())
	}
}

// learnFromClasses caches input peers from the []UserClass/[]ChatClass shapes
// returned by history/dialogs RPCs.
func (p *peerCache) learnFromClasses(users []tg.UserClass, chats []tg.ChatClass) {
	for _, uc := range users {
		if u, ok := uc.(*tg.User); ok {
			p.put(event.PlatformTelegram+":user:"+strconv.FormatInt(u.ID, 10), u.AsInputPeer())
			p.rememberUser(u) // #54
		}
	}
	for _, cc := range chats {
		switch c := cc.(type) {
		case *tg.Chat:
			p.put(event.PlatformTelegram+":chat:"+strconv.FormatInt(c.ID, 10), c.AsInputPeer())
		case *tg.Channel:
			p.put(event.PlatformTelegram+":channel:"+strconv.FormatInt(c.ID, 10), c.AsInputPeer())
		}
	}
}

// resolve returns the InputPeer for a canonical chat id. Basic chats need no
// access hash, so they resolve even if unseen; users/channels must have been
// observed (their access hash cached) or this errors.
func (p *peerCache) resolve(chatID string) (tg.InputPeerClass, error) {
	if peer, ok := p.get(chatID); ok {
		return peer, nil
	}
	kind, id, err := parseChatID(chatID)
	if err != nil {
		return nil, err
	}
	if kind == "chat" {
		return &tg.InputPeerChat{ChatID: id}, nil
	}
	return nil, fmt.Errorf("telegram: no cached access hash for %s; observe it via updates/backfill first", chatID)
}

// parseChatID splits "telegram:<kind>:<id>" into its parts.
func parseChatID(chatID string) (kind string, id int64, err error) {
	parts := strings.Split(chatID, ":")
	if len(parts) != 3 || parts[0] != event.PlatformTelegram {
		return "", 0, fmt.Errorf("telegram: malformed chat id %q (want telegram:<kind>:<id>)", chatID)
	}
	id, err = strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("telegram: bad numeric id in %q: %w", chatID, err)
	}
	switch parts[1] {
	case "user", "chat", "channel":
		return parts[1], id, nil
	default:
		return "", 0, fmt.Errorf("telegram: unknown peer kind %q in %q", parts[1], chatID)
	}
}

// botChatID converts a canonical chat id into the numeric chat id the Bot API
// expects: users → user id; basic chats → -id; channels/supergroups → -100<id>.
// (Best-effort: the bot can only message chats it shares with the user.)
func botChatID(chatID string) (int64, error) {
	kind, id, err := parseChatID(chatID)
	if err != nil {
		return 0, err
	}
	switch kind {
	case "user":
		return id, nil
	case "chat":
		return -id, nil
	case "channel":
		return -1_000_000_000_000 - id, nil
	default:
		return 0, fmt.Errorf("telegram: cannot map %q to a bot chat id", chatID)
	}
}
