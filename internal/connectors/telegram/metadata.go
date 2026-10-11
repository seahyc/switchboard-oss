package telegram

import (
	"context"
	"strconv"
	"time"

	"github.com/gotd/td/tg"
	"github.com/seahyc/switchboard-oss/internal/connectors"
)

// ChatMetadata enumerates every dialog (main + archive folders) and returns
// human-legible + organizational metadata for each — titles, archived/muted/
// pinned signals, and dialog-filter (folder) labels. The daemon polls this and
// upserts the chats projection so list_chats shows real names instead of
// "telegram:user:100000004". Implements connectors.ChatMetadataProvider.
func (c *Connector) ChatMetadata(ctx context.Context) ([]connectors.ChatMeta, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	api, err := c.awaitReady(ctx)
	if err != nil {
		return nil, err
	}

	// Dialog-filter (folder) labels are best-effort: a failure here must not
	// block the names/org-signals, which are the headline win.
	labels := c.fetchDialogFilterLabels(ctx, api)

	var metas []connectors.ChatMeta
	seen := map[string]bool{}
	for _, folder := range []int{0, 1} { // 0 = main, 1 = archive
		ms, err := c.chatMetaForFolder(ctx, api, folder, labels, seen)
		metas = append(metas, ms...)
		if err != nil {
			// Return the partial set we gathered plus the error so the daemon
			// can log it but still upsert what we have.
			c.log("telegram: chat metadata folder %d: %v", folder, err)
		}
	}
	return metas, nil
}

func (c *Connector) chatMetaForFolder(ctx context.Context, api *tg.Client, folder int, labels map[string][]string, seen map[string]bool) ([]connectors.ChatMeta, error) {
	var (
		out        []connectors.ChatMeta
		offsetDate int
		offsetID   int
		offsetPeer tg.InputPeerClass = &tg.InputPeerEmpty{}
		nowUnix                      = int(time.Now().Unix())
	)

	for page := 0; page < maxDialogPages; page++ {
		req := &tg.MessagesGetDialogsRequest{
			OffsetDate: offsetDate,
			OffsetID:   offsetID,
			OffsetPeer: offsetPeer,
			Limit:      dialogPageLimit,
		}
		if folder != 0 {
			req.SetFolderID(folder)
		}
		res, err := api.MessagesGetDialogs(ctx, req)
		if err != nil {
			return out, err
		}

		dialogs, messages, users, chats, isSlice := extractDialogs(res)
		if len(dialogs) == 0 {
			break
		}
		c.peers.learnFromClasses(users, chats)
		usersIdx := indexUsers(users)
		chatsIdx := indexChats(chats)
		chanIdx := indexChannels(chats)
		dateIdx := messageDateIndex(messages)

		var lastPeer tg.PeerClass
		var lastTop int
		for _, dc := range dialogs {
			d, ok := dc.(*tg.Dialog)
			if !ok {
				continue
			}
			cid := canonicalChatID(d.Peer)
			lastPeer = d.Peer
			lastTop = d.TopMessage
			if seen[cid] {
				continue
			}
			seen[cid] = true
			out = append(out, connectors.ChatMeta{
				ChatID:   cid,
				Title:    titleForPeer(d.Peer, usersIdx, chatsIdx, chanIdx),
				Archived: folder == 1,
				Muted:    isMuted(d.NotifySettings, nowUnix),
				Pinned:   d.Pinned,
				Labels:   labels[cid],
			})
		}

		if !isSlice || len(dialogs) < dialogPageLimit {
			break
		}
		offsetID = lastTop
		offsetDate = dateIdx[peerKey(lastPeer)+":"+strconv.Itoa(lastTop)]
		if ip := dialogInputPeer(lastPeer, usersIdx, chatsIdx, chanIdx); ip != nil {
			offsetPeer = ip
		}
		if offsetDate == 0 {
			break
		}
	}
	return out, nil
}

// titleForPeer renders the display title for a dialog peer: group/channel title,
// or for a DM the counterpart's "First Last" / @username. Returns "" if the peer
// isn't in the entity set — the daemon then derives a DM name from the non-self
// sender's display_name.
func titleForPeer(peer tg.PeerClass, users map[int64]*tg.User, chats map[int64]*tg.Chat, channels map[int64]*tg.Channel) string {
	switch v := peer.(type) {
	case *tg.PeerUser:
		if u, ok := users[v.UserID]; ok {
			return displayName(u)
		}
	case *tg.PeerChat:
		if ch, ok := chats[v.ChatID]; ok {
			return ch.Title
		}
	case *tg.PeerChannel:
		if ch, ok := channels[v.ChannelID]; ok {
			return ch.Title
		}
	}
	return ""
}

// isMuted reports whether a dialog is muted: an explicit mute-until in the
// future (0 = unmuted; a far-future value = muted indefinitely).
func isMuted(ns tg.PeerNotifySettings, nowUnix int) bool {
	if mu, ok := ns.GetMuteUntil(); ok && mu > nowUnix {
		return true
	}
	return false
}

// fetchDialogFilterLabels maps each canonical chat id to the names of the
// dialog filters (folders/labels) it belongs to. Best-effort: returns an empty
// map on any error so missing labels never break metadata.
func (c *Connector) fetchDialogFilterLabels(ctx context.Context, api *tg.Client) map[string][]string {
	out := map[string][]string{}
	res, err := api.MessagesGetDialogFilters(ctx)
	if err != nil || res == nil {
		return out
	}
	for _, fc := range res.Filters {
		var (
			title string
			peers []tg.InputPeerClass
		)
		switch f := fc.(type) {
		case *tg.DialogFilter:
			title = f.Title.Text
			peers = append(peers, f.PinnedPeers...)
			peers = append(peers, f.IncludePeers...)
		case *tg.DialogFilterChatlist:
			title = f.Title.Text
			peers = append(peers, f.PinnedPeers...)
			peers = append(peers, f.IncludePeers...)
		default:
			continue // DialogFilterDefault ("All chats") carries no peers
		}
		if title == "" {
			continue
		}
		for _, ip := range peers {
			if cid := inputPeerToChatID(ip); cid != "" {
				out[cid] = append(out[cid], title)
			}
		}
	}
	return out
}

// inputPeerToChatID converts an InputPeer (as found in dialog filters) to the
// canonical chat id. InputPeerSelf maps to the authenticated user's id.
func inputPeerToChatID(ip tg.InputPeerClass) string {
	switch v := ip.(type) {
	case *tg.InputPeerUser:
		return "telegram:user:" + strconv.FormatInt(v.UserID, 10)
	case *tg.InputPeerChat:
		return "telegram:chat:" + strconv.FormatInt(v.ChatID, 10)
	case *tg.InputPeerChannel:
		return "telegram:channel:" + strconv.FormatInt(v.ChannelID, 10)
	default:
		return ""
	}
}
