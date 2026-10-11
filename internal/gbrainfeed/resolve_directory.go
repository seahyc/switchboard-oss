package gbrainfeed

import (
	"strings"

	"github.com/seahyc/switchboard-oss/internal/api"
	"github.com/seahyc/switchboard-oss/internal/gbrainfeed/idmatch"
)

// LarkDirectoryEntry is one hydrated Lark org-directory user — the NEUTRAL shape the
// daemon maps the connector's contact/v3 User into (name, open_id, email, mobile,
// handle). Keeps gbrainfeed free of any Lark-connector import: core2's handler does
// search→hydrate→[]LarkDirectoryEntry, this package turns those into cards.
type LarkDirectoryEntry struct {
	Name   string
	OpenID string
	Email  string
	Mobile string
	Handle string
}

// LarkDirectoryCards is the #24 phase-2 fallback: when the people graph returns no
// match, resolve_person queries the Lark org directory (colleagues never messaged,
// e.g. "Bình An Đỗ") and this builds the cards. The query is accent-FOLDED with the
// SAME idmatch.NormName as the graph path (đ→d etc.), and each directory entry is
// kept only if every folded query token appears in its folded name (the Lark search
// can return loose matches; this is order-independent so "Đỗ An Bình" still matches
// "Binh An Do"). Each result → a Source:"lark" card with name + open_id + email/
// phone. Returns an EMPTY slice (never nil).
func LarkDirectoryCards(query string, entries []LarkDirectoryEntry) []api.PersonCard {
	q := idmatch.NormName(query)
	qset := map[string]bool{}
	for _, t := range strings.Fields(q) {
		qset[t] = true
	}
	out := make([]api.PersonCard, 0, len(entries))
	for _, e := range entries {
		if e.Name == "" && e.OpenID == "" {
			continue
		}
		if q != "" && !tokenSubset(qset, idmatch.NormName(e.Name)) {
			continue
		}
		card := api.PersonCard{
			Name:       e.Name,
			Identified: e.Name != "",
			Platforms:  []string{"lark"},
			Identities: []api.CardIdentity{{
				Platform: "lark", UserID: e.OpenID, DisplayName: e.Name, Handle: e.Handle,
			}},
			MatchedOn: e.Name,
			Source:    "lark",
		}
		if e.OpenID != "" {
			card.OpenIDs = []string{e.OpenID}
		}
		if e.Email != "" {
			card.Emails = []string{e.Email}
		}
		if e.Mobile != "" {
			card.Phones = []string{e.Mobile}
		}
		if e.Handle != "" {
			card.Handles = []string{e.Handle}
		}
		out = append(out, card)
	}
	return out
}
