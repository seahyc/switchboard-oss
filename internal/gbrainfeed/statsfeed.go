package gbrainfeed

import (
	"context"
	"time"
)

// PersonStat is per-IDENTITY interaction aggregates from the message store — the
// raw input the daemon supplies (via PersonStatsProvider) for salience. It is
// keyed by (Platform, SenderID) where SenderID == event.Sender.PlatformUserID, so
// the projector can roll several identities up into one canonical person.
//
// This is the ONLY external dependency of the salience layer; core implements the
// provider over the SQLite store (GROUP BY platform, sender_id + join chats). The
// pure scoring + calibration code never touches it directly — it consumes the
// rolled-up PersonSignals.
type PersonStat struct {
	Platform string
	SenderID string

	MsgCount      int // their messages (sender_id=X, is_self=0)
	YourMsgCount  int // your messages (is_self=1) in chats where X participates
	DMMsgCount    int // their messages in DM chats
	GroupMsgCount int // their messages in group/channel chats
	MentionsYou   int // their messages with mentions_self=1
	YouInitiated  int // conversations you started with X

	LastTS time.Time // most recent message in any chat with X

	HasDM        bool
	RepliedToYou bool // X replied to one of your messages
	DMPinned     bool
	DMMuted      bool
	DMArchived   bool
}

// PersonStatsProvider is the daemon-wired capability that exposes per-identity
// interaction stats (mirrors ContactsProvider). The projector pulls it on a tick,
// rolls it up, scores, and updates the people graph. Mocked in tests until core
// ships the real store-backed implementation.
type PersonStatsProvider interface {
	PersonStats(ctx context.Context) ([]PersonStat, error)
}

// IdentityRef names one per-platform identity of a canonical person.
type IdentityRef struct {
	Platform string
	UserID   string
}

// CanonicalMembership is the projector's view of a canonical person for rollup:
// its identity + the member identities whose stats roll up into it. Kept as an
// explicit input (rather than reaching into the resolver) so RollUpSignals is a
// PURE, unit-testable function.
type CanonicalMembership struct {
	CanonicalID string
	Name        string
	Identified  bool
	IsSelf      bool // the account owner's own canonical → excluded from the network
	Members     []IdentityRef
}

// RollUpSignals folds per-identity PersonStats up into per-canonical-person
// PersonSignals, deriving the scorer's inputs (recency from the newest LastTS,
// breadth from distinct platforms, coarse reciprocity, direct-interaction flags).
// PURE + deterministic given `now` (recency is frozen at collection time so a
// cached signal set re-scores identically during calibration).
func RollUpSignals(memberships []CanonicalMembership, stats []PersonStat, now time.Time) []PersonSignals {
	byID := make(map[IdentityRef]PersonStat, len(stats))
	for _, st := range stats {
		byID[IdentityRef{st.Platform, st.SenderID}] = st
	}

	out := make([]PersonSignals, 0, len(memberships))
	for _, m := range memberships {
		sig := PersonSignals{
			CanonicalID: m.CanonicalID,
			Name:        m.Name,
			Identified:  m.Identified,
			IsSelf:      m.IsSelf,
		}
		platforms := map[string]bool{}
		var theirMsgs, yourMsgs int
		for _, ref := range m.Members {
			platforms[ref.Platform] = true
			st, ok := byID[ref]
			if !ok {
				continue
			}
			sig.MsgCount += st.MsgCount
			theirMsgs += st.DMMsgCount
			yourMsgs += st.YourMsgCount
			sig.Initiated += st.YouInitiated
			sig.MentionsExch += st.MentionsYou
			if st.MentionsYou > 0 || st.RepliedToYou {
				sig.AddressedYou = true
			}
			if st.YourMsgCount > 0 {
				sig.YouAddressedThem = true
			}
			if st.HasDM {
				sig.HasDM = true
			}
			sig.DMPinned = sig.DMPinned || st.DMPinned
			sig.DMMuted = sig.DMMuted || st.DMMuted
			sig.DMArchived = sig.DMArchived || st.DMArchived
			if st.LastTS.After(sig.LastSeen) {
				sig.LastSeen = st.LastTS
			}
		}
		sig.Platforms = sortedKeys(platforms)
		sig.ReciprocityRatio = ratio(yourMsgs, theirMsgs)
		sig.RecencyDays = recencyDays(sig.LastSeen, now)
		out = append(out, sig)
	}
	return out
}

func ratio(a, b int) float64 {
	lo, hi := a, b
	if lo > hi {
		lo, hi = hi, lo
	}
	if hi <= 0 {
		return 0
	}
	return float64(lo) / float64(hi)
}

func recencyDays(last, now time.Time) int {
	if last.IsZero() || now.IsZero() {
		return -1
	}
	d := int(now.Sub(last).Hours() / 24)
	if d < 0 {
		return 0
	}
	return d
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// small slice; simple insertion-free sort
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
