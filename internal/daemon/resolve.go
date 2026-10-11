package daemon

import (
	"context"

	"github.com/seahyc/switchboard-oss/internal/api"
	"github.com/seahyc/switchboard-oss/internal/connectors/lark"
	"github.com/seahyc/switchboard-oss/internal/event"
	"github.com/seahyc/switchboard-oss/internal/gbrainfeed"
	"github.com/seahyc/switchboard-oss/internal/store"
)

// ResolvePerson resolves a name query to person cards, backing GET /people/resolve
// + the resolve_person MCP tool (#24). Two layers:
//
//  1. PEOPLE GRAPH (people-layer's gbrainfeed.Projector.ResolvePerson): accent-
//     folded canonical lookup + merge-on-read composite cards. This is the primary
//     path and covers anyone the user has messaged.
//  2. LARK DIRECTORY FALLBACK (#24 P2): when the graph returns NO hit, search the
//     Lark org directory by name (the connector's SearchByName → search/v1/user,
//     hydrated to Profiles) and turn the results into Source:"lark" cards via
//     people-layer's LarkDirectoryCards. This resolves colleagues the user has
//     never messaged (the "find Alex Example" case) — they're in the org but not the
//     message graph.
//
// The fallback degrades gracefully: a missing/erroring Lark connector logs a warn
// and returns the (empty) graph result rather than failing the whole lookup.
func (d *Daemon) ResolvePerson(ctx context.Context, q string) ([]api.PersonCard, error) {
	cards := d.proj.ResolvePerson(q)
	if len(cards) == 0 {
		cards = d.larkDirectoryFallback(ctx, q)
	}
	// #35/#32: turn each resolved person's identity set into directly-openable chat
	// pointers (recent_chats, newest-first, DM-inclusive). RecentChats[0] is a
	// sendable target, closing the send→person link without guessing a chat_id.
	d.fillRecentChats(ctx, cards)
	return cards, nil
}

// CoreNetwork returns the user's salience-ranked people (#15), backing GET
// /people/core_network. Thin passthrough to people-layer's projector scorer; tier
// filters (inner|active|peripheral|dormant) and limit caps the list.
func (d *Daemon) CoreNetwork(_ context.Context, tier string, limit int) ([]api.PersonCard, error) {
	return d.proj.CoreNetwork(tier, limit), nil
}

// larkDirectoryFallback resolves a name the people-graph missed against the Lark org
// directory (#24 P2): SearchByName → search/v1/user → directory cards. Degrades to
// nil (not an error) on a missing connector / directory hiccup / no match, so the
// lookup never fails outright.
func (d *Daemon) larkDirectoryFallback(ctx context.Context, q string) []api.PersonCard {
	lc, ok := d.connectors[event.PlatformLark].(*lark.Connector)
	if !ok {
		return nil // no Lark connector wired — nothing more to try
	}
	profiles, err := lc.SearchByName(ctx, q, 50)
	if err != nil {
		d.log.Warn("resolve_person: lark directory fallback failed", "q", q, "err", err)
		return nil // degrade — don't fail the lookup on a directory hiccup
	}
	if len(profiles) == 0 {
		return nil
	}
	entries := make([]gbrainfeed.LarkDirectoryEntry, 0, len(profiles))
	for _, p := range profiles {
		entries = append(entries, gbrainfeed.LarkDirectoryEntry{
			Name:   p.DisplayName,
			OpenID: p.OpenID,
			Email:  p.Email,
			Mobile: p.Phone,
		})
	}
	return gbrainfeed.LarkDirectoryCards(q, entries)
}

// fillRecentChats populates each card's RecentChats from the store: the person's
// most-recent conversations (newest-first, DM-inclusive), keyed off that card's OWN
// identity set so a multi-person result attributes chats correctly. Best-effort —
// a directory-only person with no stored messages just gets an empty list, and a
// store hiccup logs + skips rather than failing the resolve.
func (d *Daemon) fillRecentChats(ctx context.Context, cards []api.PersonCard) {
	const maxRecent = 6
	for i := range cards {
		refs := gbrainfeed.SenderIDsFromCards(cards[i : i+1])
		if len(refs) == 0 {
			continue
		}
		srefs := make([]store.SenderRef, 0, len(refs))
		for _, r := range refs {
			srefs = append(srefs, store.SenderRef{Platform: r.Platform, SenderID: r.SenderID})
		}
		recents, err := d.store.RecentChatsForSenders(ctx, srefs, maxRecent)
		if err != nil {
			d.log.Warn("resolve_person: recent-chats fill failed", "err", err)
			continue
		}
		if len(recents) == 0 {
			continue
		}
		out := make([]api.ChatRef, 0, len(recents))
		for _, rc := range recents {
			title := rc.Title
			// For a 1:1 chat ON this person's card the counterparty IS the resolved
			// person, so a blank title resolves to the card's own name — no dependency
			// on #31 p2p-titling for this surface (per people-layer). IsDM is the
			// robust signal (cardinality-based), so it fires even for genuine 1:1s
			// mislabeled chat_type="group".
			if title == "" && rc.IsDM {
				title = cards[i].Name
			}
			cr := api.ChatRef{ChatID: rc.ChatID, Title: title, Snippet: rc.Snippet}
			if !rc.LastTS.IsZero() {
				cr.LastTS = rc.LastTS.Unix()
			}
			out = append(out, cr)
		}
		cards[i].RecentChats = out
	}
}
