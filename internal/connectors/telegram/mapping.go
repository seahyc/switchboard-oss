// Package telegram implements the switchboard Connector for Telegram using
// github.com/gotd/td (MTProto) as a USER account (userbot). It reads the user's
// personal DMs and groups — the gap the @example_bot Bot API cannot fill — and
// normalizes every update into the FROZEN canonical event.Message.
//
// Outbound is unified: normal sends go through the userbot; rich inline-keyboard
// UI sends route through the @example_bot Bot API (a user account cannot send
// inline keyboards).
package telegram

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/gotd/td/tg"
	"github.com/seahyc/switchboard-oss/internal/event"
)

// entityIndex holds the per-update entity maps gotd hands to update handlers,
// plus the authenticated user's id so we can flag is_self / @me mentions. It is
// the only context the pure mapping functions need, which keeps them unit
// testable without a live client.
type entityIndex struct {
	users    map[int64]*tg.User
	chats    map[int64]*tg.Chat
	channels map[int64]*tg.Channel
	selfID   int64
	cache    *peerCache // #54: persistent name fallback when a batch omits the sender entity
}

func newEntityIndex(e tg.Entities, selfID int64, cache *peerCache) entityIndex {
	return entityIndex{users: e.Users, chats: e.Chats, channels: e.Channels, selfID: selfID, cache: cache}
}

func (ix entityIndex) user(id int64) (*tg.User, bool) {
	if ix.users == nil {
		return nil, false
	}
	u, ok := ix.users[id]
	return u, ok
}

// peerKey returns a stable, id-space-qualified key for a Telegram peer:
// "user:123", "chat:45", "channel:678". Telegram message ids are only unique
// within a peer, so this key scopes them into a globally unique id.
func peerKey(p tg.PeerClass) string {
	switch v := p.(type) {
	case *tg.PeerUser:
		return "user:" + strconv.FormatInt(v.UserID, 10)
	case *tg.PeerChat:
		return "chat:" + strconv.FormatInt(v.ChatID, 10)
	case *tg.PeerChannel:
		return "channel:" + strconv.FormatInt(v.ChannelID, 10)
	default:
		return "unknown"
	}
}

// canonicalChatID builds the canonical chat id "telegram:<peerKey>".
func canonicalChatID(p tg.PeerClass) string {
	return event.PlatformTelegram + ":" + peerKey(p)
}

// platformMsgID scopes a message id by its peer so it is globally unique:
// "<peerKey>:<msgID>" (e.g. "user:123:456").
func platformMsgID(p tg.PeerClass, msgID int) string {
	return peerKey(p) + ":" + strconv.Itoa(msgID)
}

// chatType classifies a peer into the canonical dm|group|channel. Broadcast
// channels map to "channel"; megagroups/supergroups and basic chats map to
// "group"; users map to "dm".
func (ix entityIndex) chatType(p tg.PeerClass) string {
	switch v := p.(type) {
	case *tg.PeerUser:
		return event.ChatDM
	case *tg.PeerChat:
		return event.ChatGroup
	case *tg.PeerChannel:
		if ch, ok := ix.channels[v.ChannelID]; ok && ch.Broadcast {
			return event.ChatChannel
		}
		return event.ChatGroup
	default:
		return event.ChatGroup
	}
}

// senderFromMessage resolves the sender identity + alias signals.
func (ix entityIndex) senderFromMessage(m *tg.Message) event.Sender {
	// Determine the sender's user/peer id.
	var fromUserID int64
	var fromPeer tg.PeerClass

	switch {
	case m.FromID != nil:
		fromPeer = m.FromID
		if pu, ok := m.FromID.(*tg.PeerUser); ok {
			fromUserID = pu.UserID
		}
	case m.Out:
		fromUserID = ix.selfID
	default:
		// Incoming DM with no explicit FromID: sender is the dialog peer.
		fromPeer = m.PeerID
		if pu, ok := m.PeerID.(*tg.PeerUser); ok {
			fromUserID = pu.UserID
		}
	}

	s := event.Sender{}
	if fromUserID != 0 {
		s.PlatformUserID = strconv.FormatInt(fromUserID, 10)
		s.IsSelf = fromUserID == ix.selfID || m.Out
		if u, ok := ix.user(fromUserID); ok {
			s.Handle = u.Username
			s.Phone = u.Phone
			s.DisplayName = displayName(u)
		}
		// #54: DM sender entities are often absent from a message's own batch, leaving the
		// name blank. Fall back to the persistent name cache (every user we've ever seen).
		if ix.cache != nil && (s.DisplayName == "" || s.Handle == "") {
			if n, ok := ix.cache.userName(fromUserID); ok {
				if s.DisplayName == "" {
					s.DisplayName = n.display
				}
				if s.Handle == "" {
					s.Handle = n.handle
				}
				if s.Phone == "" {
					s.Phone = n.phone
				}
			}
		}
		return s
	}

	// Channel post or chat-as-sender: fall back to the peer title.
	if fromPeer != nil {
		s.PlatformUserID = peerKey(fromPeer)
		s.DisplayName = ix.peerTitle(fromPeer)
	}
	s.IsSelf = m.Out
	return s
}

func (ix entityIndex) peerTitle(p tg.PeerClass) string {
	switch v := p.(type) {
	case *tg.PeerUser:
		if u, ok := ix.user(v.UserID); ok {
			return displayName(u)
		}
	case *tg.PeerChat:
		if c, ok := ix.chats[v.ChatID]; ok {
			return c.Title
		}
	case *tg.PeerChannel:
		if c, ok := ix.channels[v.ChannelID]; ok {
			return c.Title
		}
	}
	return ""
}

func displayName(u *tg.User) string {
	name := strings.TrimSpace(strings.TrimSpace(u.FirstName) + " " + strings.TrimSpace(u.LastName))
	if name != "" {
		return name
	}
	if u.Username != "" {
		return u.Username
	}
	return ""
}

// kindOf classifies the message body into a canonical kind.
func kindOf(m *tg.Message) string {
	if m.Media == nil {
		return event.KindText
	}
	switch media := m.Media.(type) {
	case *tg.MessageMediaPoll:
		return event.KindPoll
	case *tg.MessageMediaPhoto:
		return event.KindMedia
	case *tg.MessageMediaDocument:
		if isSticker(media) {
			return event.KindSticker
		}
		return event.KindMedia
	default:
		return event.KindMedia
	}
}

func isSticker(media *tg.MessageMediaDocument) bool {
	doc, ok := media.Document.(*tg.Document)
	if !ok {
		return false
	}
	for _, a := range doc.Attributes {
		if _, ok := a.(*tg.DocumentAttributeSticker); ok {
			return true
		}
	}
	return false
}

// attachmentsOf extracts best-effort media references (not the bytes).
// tgMediaRef is the rich, self-contained handle for downloading a Telegram
// attachment's bytes later (#48). It carries the full InputFileLocation params —
// id, access_hash, file_reference, and (for photos) the largest PhotoSize type —
// plus the chat+msg so an EXPIRED file_reference can be refreshed by re-fetching
// the message. JSON-encoded onto event.Attachment.Ref; it REPLACES the old thin
// "telegram-photo:<id>" ref, which carried no access_hash/file_reference and so
// could never be downloaded ("no downloader or empty ref"). []byte FileRef
// marshals as base64 automatically.
type tgMediaRef struct {
	Kind       string `json:"k"` // "photo" | "doc"
	ID         int64  `json:"id"`
	AccessHash int64  `json:"ah"`
	FileRef    []byte `json:"fr"`
	ThumbSize  string `json:"ts,omitempty"` // photo: PhotoSize.Type of the largest size
	Chat       string `json:"c,omitempty"`  // canonical chat id (refresh: re-resolve peer)
	Msg        int    `json:"m,omitempty"`  // message id (refresh: re-fetch for fresh file_reference)
}

func (r tgMediaRef) encode() string {
	b, _ := json.Marshal(r)
	return string(b)
}

// decodeTGMediaRef parses a rich ref; ok=false for the legacy thin ref (or junk),
// which has no downloadable location and must be re-ingested to gain one.
func decodeTGMediaRef(s string) (tgMediaRef, bool) {
	var r tgMediaRef
	if err := json.Unmarshal([]byte(s), &r); err != nil || r.ID == 0 {
		return tgMediaRef{}, false
	}
	return r, true
}

// rawTGMedia mirrors the JSON shape gotd's json.Marshal emits for a tg.Message's
// media (capitalized Go field names). gotd's tg.Message.Media/PeerID are INTERFACE
// fields that DON'T round-trip through encoding/json (a full tg.Message unmarshal
// yields nil Media), so the #48 recovery parses this minimal shape directly to pull
// the download params out of a stored Raw.
type rawTGMedia struct {
	ID    int `json:"ID"` // the message id
	Media *struct {
		Photo *struct {
			ID            int64  `json:"ID"`
			AccessHash    int64  `json:"AccessHash"`
			FileReference []byte `json:"FileReference"`
			Sizes         []struct {
				Type string `json:"Type"`
				W    int    `json:"W"`
			} `json:"Sizes"`
		} `json:"Photo"`
		Document *struct {
			ID            int64  `json:"ID"`
			AccessHash    int64  `json:"AccessHash"`
			FileReference []byte `json:"FileReference"`
		} `json:"Document"`
	} `json:"Media"`
}

// ReextractMediaRef implements connectors.MediaRefReextractor: rebuild a rich
// downloadable tgMediaRef from a stored raw tg.Message payload + the canonical chat id
// (#48 recovery for the 583 old rows that hold the thin "telegram-photo:<id>" ref).
// Returns "" if the raw carries no photo/document. The embedded file_reference may be
// stale; DownloadMedia's refresh-on-expiry (which uses the chat+msg embedded here)
// fetches a fresh one at download time.
func (c *Connector) ReextractMediaRef(raw []byte, chatID string) string {
	var p rawTGMedia
	if err := json.Unmarshal(raw, &p); err != nil || p.Media == nil {
		return ""
	}
	if ph := p.Media.Photo; ph != nil {
		ts, bw := "x", -1
		for _, s := range ph.Sizes {
			if s.W > bw {
				bw, ts = s.W, s.Type
			}
		}
		return tgMediaRef{Kind: "photo", ID: ph.ID, AccessHash: ph.AccessHash, FileRef: ph.FileReference, ThumbSize: ts, Chat: chatID, Msg: p.ID}.encode()
	}
	if d := p.Media.Document; d != nil {
		return tgMediaRef{Kind: "doc", ID: d.ID, AccessHash: d.AccessHash, FileRef: d.FileReference, Chat: chatID, Msg: p.ID}.encode()
	}
	return ""
}

// location reconstructs the Telegram InputFileLocation for the downloader.
func (r tgMediaRef) location() tg.InputFileLocationClass {
	if r.Kind == "photo" {
		return &tg.InputPhotoFileLocation{
			ID: r.ID, AccessHash: r.AccessHash, FileReference: r.FileRef, ThumbSize: r.ThumbSize,
		}
	}
	return &tg.InputDocumentFileLocation{
		ID: r.ID, AccessHash: r.AccessHash, FileReference: r.FileRef,
	}
}

// largestPhotoSize returns the Type of the biggest downloadable PhotoSize (Telegram
// requires the size selector in InputPhotoFileLocation). Falls back to "x" (a
// standard large size) if none carry dimensions.
func largestPhotoSize(sizes []tg.PhotoSizeClass) string {
	best, bestType := -1, ""
	for _, s := range sizes {
		switch ps := s.(type) {
		case *tg.PhotoSize:
			if ps.W > best {
				best, bestType = ps.W, ps.Type
			}
		case *tg.PhotoSizeProgressive:
			if ps.W > best {
				best, bestType = ps.W, ps.Type
			}
		}
	}
	if bestType == "" {
		bestType = "x"
	}
	return bestType
}

func attachmentsOf(m *tg.Message) []event.Attachment {
	if m.Media == nil {
		return nil
	}
	chat := canonicalChatID(m.PeerID)
	switch media := m.Media.(type) {
	case *tg.MessageMediaPhoto:
		if p, ok := media.Photo.(*tg.Photo); ok {
			return []event.Attachment{{
				Type: "image",
				MIME: "image/jpeg",
				Ref: tgMediaRef{
					Kind: "photo", ID: p.ID, AccessHash: p.AccessHash,
					FileRef: p.FileReference, ThumbSize: largestPhotoSize(p.Sizes),
					Chat: chat, Msg: m.ID,
				}.encode(),
			}}
		}
		return []event.Attachment{{Type: "image"}}
	case *tg.MessageMediaDocument:
		doc, ok := media.Document.(*tg.Document)
		if !ok {
			return []event.Attachment{{Type: "file"}}
		}
		at := event.Attachment{
			Type: "file",
			MIME: doc.MimeType,
			Ref: tgMediaRef{
				Kind: "doc", ID: doc.ID, AccessHash: doc.AccessHash,
				FileRef: doc.FileReference, Chat: chat, Msg: m.ID,
			}.encode(),
			Size: doc.Size,
		}
		if isSticker(media) {
			at.Type = "sticker"
		}
		for _, a := range doc.Attributes {
			if fn, ok := a.(*tg.DocumentAttributeFilename); ok {
				at.Name = fn.FileName
			}
		}
		return []event.Attachment{at}
	default:
		return nil
	}
}

// mentionsOf collects mentioned platform user ids. Explicit MentionName entities
// resolve to numeric ids; the Mentioned flag (the user was @-mentioned anywhere,
// including via reply) adds self so the pipeline's "@me" projection fires.
func mentionsOf(m *tg.Message, selfID int64) []string {
	var out []string
	seen := map[string]bool{}
	add := func(id string) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, id)
	}
	for _, e := range m.Entities {
		switch ent := e.(type) {
		case *tg.MessageEntityMentionName:
			add(strconv.FormatInt(ent.UserID, 10))
		case *tg.InputMessageEntityMentionName:
			if iu, ok := ent.UserID.(*tg.InputUser); ok {
				add(strconv.FormatInt(iu.UserID, 10))
			}
		}
	}
	if m.Mentioned && selfID != 0 {
		add(strconv.FormatInt(selfID, 10))
	}
	return out
}

// uiOf preserves inline keyboards and polls so the agent can re-render or act.
func uiOf(m *tg.Message) event.UI {
	ui := event.UI{}
	if markup, ok := m.ReplyMarkup.(*tg.ReplyInlineMarkup); ok {
		for _, row := range markup.Rows {
			for _, b := range row.Buttons {
				if raw := encodeButton(b); raw != nil {
					ui.Buttons = append(ui.Buttons, raw)
				}
			}
		}
	}
	if media, ok := m.Media.(*tg.MessageMediaPoll); ok {
		ui.Poll = encodePoll(media)
	}
	return ui
}

// encodeButton serializes an inline button. For callback buttons the id is the
// base64 of the callback data so tap_button can round-trip it back to
// messages.getBotCallbackAnswer.
func encodeButton(b tg.KeyboardButtonClass) json.RawMessage {
	type btn struct {
		Text string `json:"text"`
		Type string `json:"type"`
		ID   string `json:"id,omitempty"`
		URL  string `json:"url,omitempty"`
	}
	var v btn
	switch t := b.(type) {
	case *tg.KeyboardButtonCallback:
		v = btn{Text: t.Text, Type: "callback", ID: base64.StdEncoding.EncodeToString(t.Data)}
	case *tg.KeyboardButtonURL:
		v = btn{Text: t.Text, Type: "url", URL: t.URL}
	case *tg.KeyboardButton:
		v = btn{Text: t.Text, Type: "text"}
	default:
		return nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return raw
}

func encodePoll(media *tg.MessageMediaPoll) json.RawMessage {
	type opt struct {
		Text   string `json:"text"`
		Option string `json:"option"`
	}
	type poll struct {
		ID       string `json:"id"`
		Question string `json:"question"`
		Options  []opt  `json:"options"`
	}
	p := poll{
		ID:       strconv.FormatInt(media.Poll.ID, 10),
		Question: media.Poll.Question.Text,
	}
	for _, a := range media.Poll.Answers {
		ans, ok := a.(*tg.PollAnswer)
		if !ok {
			continue
		}
		p.Options = append(p.Options, opt{
			Text:   ans.Text.Text,
			Option: base64.StdEncoding.EncodeToString(ans.Option),
		})
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return nil
	}
	return raw
}

func reactionsOf(m *tg.Message) []event.Reaction {
	if len(m.Reactions.Results) == 0 {
		return nil
	}
	var out []event.Reaction
	for _, rc := range m.Reactions.Results {
		switch r := rc.Reaction.(type) {
		case *tg.ReactionEmoji:
			out = append(out, event.Reaction{Emoji: r.Emoticon})
		case *tg.ReactionCustomEmoji:
			out = append(out, event.Reaction{Emoji: "custom:" + strconv.FormatInt(r.DocumentID, 10)})
		}
	}
	return out
}

// mapMessage converts a Telegram *tg.Message into the FROZEN canonical event.
// It is pure: all platform context arrives via entityIndex, so it is fully
// unit-testable without a live client.
func mapMessage(m *tg.Message, ix entityIndex, source string, ingestedAt time.Time) event.Message {
	msg := event.Message{
		Platform:    event.PlatformTelegram,
		AccountID:   event.PlatformTelegram + ":" + strconv.FormatInt(ix.selfID, 10),
		ChatID:      canonicalChatID(m.PeerID),
		ChatType:    ix.chatType(m.PeerID),
		Sender:      ix.senderFromMessage(m),
		TS:          time.Unix(int64(m.Date), 0).UTC(),
		Kind:        kindOf(m),
		Text:        m.Message,
		Mentions:    mentionsOf(m, ix.selfID),
		Attachments: attachmentsOf(m),
		UI:          uiOf(m),
		Reactions:   reactionsOf(m),
		Source:      source,
		IngestedAt:  ingestedAt.UTC(),
		Raw:         rawJSON(m),
	}
	msg.SetID(platformMsgID(m.PeerID, m.ID))

	if h, ok := m.ReplyTo.(*tg.MessageReplyHeader); ok && h.ReplyToMsgID != 0 {
		// Reply target lives in the same peer unless ReplyToPeerID says otherwise.
		peer := m.PeerID
		if h.ReplyToPeerID != nil {
			peer = h.ReplyToPeerID
		}
		msg.ReplyTo = event.MakeID(event.PlatformTelegram, platformMsgID(peer, h.ReplyToMsgID))
	}

	if m.EditDate > 0 {
		t := time.Unix(int64(m.EditDate), 0).UTC()
		msg.EditedTS = &t
	}

	return msg
}

// mapDelete builds canonical delete events for a set of deleted message ids in a
// peer. Telegram delete updates carry only ids, so text/sender are empty.
func mapDelete(peer tg.PeerClass, ids []int, selfID int64, ingestedAt time.Time) []event.Message {
	out := make([]event.Message, 0, len(ids))
	for _, id := range ids {
		m := event.Message{
			Platform:  event.PlatformTelegram,
			AccountID: event.PlatformTelegram + ":" + strconv.FormatInt(selfID, 10),
			ChatID:    canonicalChatID(peer),
			Kind:      event.KindDelete,
			Deleted:   true,
			// Telegram delete updates carry only ids — no event timestamp. Use the
			// observation time as TS so the row isn't stored with a zero (0001-01-01)
			// timestamp that skews oldest_ts / ordering (#13 correctness fix).
			TS:         ingestedAt.UTC(),
			Source:     event.SourceLive,
			IngestedAt: ingestedAt.UTC(),
		}
		m.SetID(platformMsgID(peer, id))
		out = append(out, m)
	}
	return out
}

func rawJSON(m *tg.Message) json.RawMessage {
	b, err := json.Marshal(m)
	if err != nil {
		return json.RawMessage(`null`)
	}
	return b
}
