package meta

import (
	"regexp"
	"strings"
)

// This file holds the PURE, dependency-free parsing for the "start a new
// conversation" (cold-DM) flow. mautrix-meta has no public username→id resolver
// (its `resolve-identifier` wants a NUMERIC id), so we drive the bridge bot's
// management commands and parse their replies (documented reply formats):
//
//   search <query>  → "Search results:\n\n* `100000003` / Example Profile\n…"
//   start-chat <id> → "Created chat with `100000003` / Example Profile: Example
//                      Profile (https://matrix.to/#/!example:example.org)"
//
// Keeping the parsing pure makes it unit-testable without a live bridge.

// searchHit is one result from the bridge's `search` command.
type searchHit struct {
	ID   string // numeric remote user id
	Name string // display name (NOT the @username — the bridge returns the name)
}

var (
	// "* `<digits>` / <name>"  (one per search result line)
	reSearchHit = regexp.MustCompile("(?m)^\\s*\\*\\s*`(\\d+)`\\s*/\\s*(.+?)\\s*$")
	// the portal room id inside the matrix.to link of a "Created chat …" reply
	reCreatedRoom = regexp.MustCompile(`matrix\.to/#/(![^)\s]+)`)
)

// allDigits reports whether s is a non-empty run of ASCII digits (a numeric id).
func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// parseSearchHits extracts the (id, name) results from a `search` reply.
func parseSearchHits(body string) []searchHit {
	var out []searchHit
	for _, m := range reSearchHit.FindAllStringSubmatch(body, -1) {
		out = append(out, searchHit{ID: m[1], Name: strings.TrimSpace(m[2])})
	}
	return out
}

// parseCreatedRoomID pulls the portal room id out of a `start-chat` reply's
// matrix.to link. Returns "" if not found (e.g. a failure reply).
func parseCreatedRoomID(body string) string {
	if m := reCreatedRoom.FindStringSubmatch(body); m != nil {
		return m[1]
	}
	return ""
}

// isBridgeError reports whether a bot reply is a failure (so we don't mistake it
// for a result). The bridge prefixes failures with "Failed".
func isBridgeError(body string) bool {
	return strings.HasPrefix(strings.TrimSpace(body), "Failed")
}
