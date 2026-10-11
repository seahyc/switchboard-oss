package comprehend

import (
	"context"
	"sort"
	"strings"
	"time"
)

// ProjectionConfig tunes the deterministic reply-graph projections. Zero values
// fall back to sensible defaults via withDefaults.
type ProjectionConfig struct {
	// Now is the reference time (injected for testability/determinism).
	Now time.Time
	// WaitingMaxAge bounds how stale a waiting-on-me thread can be before it's
	// considered dead rather than pending (default 30d).
	WaitingMaxAge time.Duration
	// GhostingMinAge is how long after the user's last message a thread counts as
	// awaiting-their-reply / ghosting (default 48h).
	GhostingMinAge time.Duration
	// GhostingMaxAge bounds how old a ghosting thread can be before it's dead
	// rather than pending (default 30d).
	GhostingMaxAge time.Duration
	// SnippetLen clips last-message text in cards (default 140).
	SnippetLen int
}

func (c ProjectionConfig) withDefaults() ProjectionConfig {
	if c.Now.IsZero() {
		c.Now = time.Now()
	}
	if c.WaitingMaxAge == 0 {
		c.WaitingMaxAge = 30 * 24 * time.Hour
	}
	if c.GhostingMinAge == 0 {
		c.GhostingMinAge = 48 * time.Hour
	}
	if c.GhostingMaxAge == 0 {
		c.GhostingMaxAge = 30 * 24 * time.Hour
	}
	if c.SnippetLen == 0 {
		c.SnippetLen = 140
	}
	return c
}

// Projections holds the two deterministic reply-graphs.
type Projections struct {
	WaitingOnMe []WaitingCard
	Ghosting    []WaitingCard
}

// Project computes the waiting-on-me + ghosting reply-graphs from the event store
// with ZERO inference. The gate reuses the store's deterministic importance tier
// (low/muted/archived threads are excluded). Cards are sorted most-urgent-first
// (longest wait), important before normal.
func (r *Reader) Project(ctx context.Context, cfg ProjectionConfig) (*Projections, error) {
	cfg = cfg.withDefaults()

	last, err := r.lastMessagePerChat(ctx)
	if err != nil {
		return nil, err
	}
	selfTS, err := r.lastSelfTSPerChat(ctx)
	if err != nil {
		return nil, err
	}
	mentions, err := r.chatsMentioningSelf(ctx)
	if err != nil {
		return nil, err
	}
	inboundSender, err := r.latestInboundSenderPerChat(ctx)
	if err != nil {
		return nil, err
	}

	p := &Projections{}
	for _, lm := range last {
		// Gate: never surface muted/archived/low-importance threads, nor the
		// excluded chats (Mamba's own bot DM — it would otherwise top the deck
		// with its own status messages).
		if lm.muted || lm.archived || lm.importance == "low" || excludedChat(lm.chatID) {
			continue
		}
		platform := platformOf(lm.chatID, lm.chatType)

		if !lm.isSelf {
			// Counterpart spoke last → they're waiting on the user.
			if !eligibleInbound(lm.chatType, mentions[lm.chatID]) {
				continue
			}
			// Wait is measured from THEIR last message — "how long since they pinged
			// and got no reply". Bounded by WaitingMaxAge so dead threads drop out.
			age := cfg.Now.Sub(lm.ts)
			if age < 0 || age > cfg.WaitingMaxAge {
				continue
			}
			// Precision gate: surface a thread only if it's a genuine two-way
			// conversation (the user has replied here before) OR it scored the
			// top importance tier. This deterministically drops newsletters, cold
			// marketing blasts, and broadcast DMs the user never engaged with —
			// without any inference.
			_, replied := selfTS[lm.chatID]
			// Email-to-you sets mentions_self for every recipient, so a newsletter
			// looks "mentioned". Treat email as a real thread only if you've actually
			// replied in it — that cleanly drops marketing/list mail.
			if lm.chatType == "email" || platform == "gmail" {
				if !replied {
					continue
				}
			} else {
				// Elsewhere: surface genuine two-way threads, top-tier contacts, or
				// real @mentions (groups/channels). Drops cold broadcast DMs.
				if !replied && lm.importance != "important" && !mentions[lm.chatID] {
					continue
				}
			}
			since, err := r.inboundStreakStart(ctx, lm.chatID, selfTS[lm.chatID])
			if err != nil {
				return nil, err
			}
			if since.IsZero() {
				since = lm.ts
			}
			p.WaitingOnMe = append(p.WaitingOnMe, WaitingCard{
				ID:           lm.chatID,
				Person:       prettyName(firstNonEmpty(lm.sender, lm.title, lm.derived), lm.chatID),
				ChatID:       lm.chatID,
				Platform:     platform,
				ChatTitle:    chatTitle(lm),
				Since:        since,
				WaitingHours: int(age.Hours()),
				LastMsgID:    lm.id,
				LastMsgText:  clip(lm.text, cfg.SnippetLen),
				Importance:   firstNonEmpty(lm.importance, "normal"),
				Direction:    DirWaitingOnMe,
			})
			continue
		}

		// The user spoke last. It's ghosting only if the counterpart had ever
		// spoken (otherwise it's a broadcast, not a stalled reply) and the wait is
		// in the pending window.
		if _, theyEverSpoke := inboundSender[lm.chatID]; !theyEverSpoke {
			continue
		}
		age := cfg.Now.Sub(lm.ts)
		if age < cfg.GhostingMinAge || age > cfg.GhostingMaxAge {
			continue
		}
		p.Ghosting = append(p.Ghosting, WaitingCard{
			ID:           lm.chatID,
			Person:       prettyName(firstNonEmpty(inboundSender[lm.chatID], lm.title, lm.derived), lm.chatID),
			ChatID:       lm.chatID,
			Platform:     platform,
			ChatTitle:    chatTitle(lm),
			Since:        lm.ts, // when the user last reached out
			WaitingHours: int(age.Hours()),
			LastMsgID:    lm.id,
			LastMsgText:  clip(lm.text, cfg.SnippetLen),
			Importance:   firstNonEmpty(lm.importance, "normal"),
			Direction:    DirGhosting,
		})
	}

	sortCards(p.WaitingOnMe)
	sortCards(p.Ghosting)
	return p, nil
}

// eligibleInbound decides whether an inbound-last thread should surface as
// waiting-on-me. DMs and email threads always count; groups/channels only when
// the user was @-mentioned (otherwise group chatter would flood the deck).
func eligibleInbound(chatType string, mentioned bool) bool {
	switch chatType {
	case "dm", "email", "":
		// "" = type not yet classified; a DM-like thread that scored above low is
		// worth surfacing. (Channels are excluded explicitly below.)
		return true
	case "group":
		return mentioned
	case "channel":
		return mentioned
	default:
		return true
	}
}

func chatTitle(lm lastMsg) string {
	return prettyName(firstNonEmpty(lm.title, lm.derived, lm.sender), lm.chatID)
}

// prettyName resolves a human-legible label. When the people-layer has no name
// (empty, or a raw platform id leaked through), it formats a fallback from the
// chat_id — "unknown (+<phone>)" for a WhatsApp MSISDN, "unknown" otherwise —
// instead of dumping "whatsapp:13135550002@s.whatsapp.net" in the brief.
func prettyName(name, chatID string) string {
	name = strings.TrimSpace(name)
	if name != "" && !looksLikeRawID(name) {
		return name
	}
	// whatsapp:<msisdn>@s.whatsapp.net → "unknown (+<msisdn>)"
	if strings.HasPrefix(chatID, "whatsapp:") {
		local := chatID[len("whatsapp:"):]
		if at := strings.IndexByte(local, '@'); at >= 0 {
			id := local[:at]
			if strings.HasSuffix(local, "@s.whatsapp.net") && isAllDigits(id) {
				return "unknown (+" + id + ")"
			}
		}
		return "unknown"
	}
	return "someone"
}

// looksLikeRawID detects a platform id that slipped into the name slot
// ("whatsapp:...", "...@s.whatsapp.net", "...@lid").
func looksLikeRawID(s string) bool {
	return strings.Contains(s, "@s.whatsapp.net") || strings.HasSuffix(s, "@lid") ||
		strings.HasPrefix(s, "whatsapp:") || strings.HasPrefix(s, "telegram:")
}

func isAllDigits(s string) bool {
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

// platformOf derives the platform from the chat_id prefix ("<platform>:..."),
// falling back to the stored chat_type only if there's no prefix.
func platformOf(chatID, _ string) string {
	if i := strings.IndexByte(chatID, ':'); i > 0 {
		return chatID[:i]
	}
	return ""
}

// sortCards orders most-urgent-first: important tier first, then longest wait.
func sortCards(cards []WaitingCard) {
	sort.SliceStable(cards, func(i, j int) bool {
		ri, rj := impRank(cards[i].Importance), impRank(cards[j].Importance)
		if ri != rj {
			return ri > rj
		}
		return cards[i].WaitingHours > cards[j].WaitingHours
	})
}

func impRank(s string) int {
	switch s {
	case "important":
		return 2
	case "normal":
		return 1
	default:
		return 0
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func clip(s string, n int) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "\n", " "))
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return strings.TrimSpace(string(r[:n])) + "…"
}
