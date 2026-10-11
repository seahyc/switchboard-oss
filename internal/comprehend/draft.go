package comprehend

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
)

// reThink matches a reasoning block emitted by reasoning models before the reply.
var reThink = regexp.MustCompile(`(?s)<think>.*?</think>`)

// Drafter pre-writes suggest-only reply drafts for who's-waiting threads (T2.3),
// so the deck offers one-tap send instead of a blank compose box. It is
// importance-gated (only top-tier threads), drafts in the user's voice + the thread's
// own language, and STORES the result — it NEVER sends anything.
type Drafter struct {
	Reader *Reader
	Store  *Store
	Local  ChatModel // ollama 3b — bulk
	Smart  ChatModel // chatmock — top items (optional)

	LocalModel string
	SmartModel string

	// TopN top-priority threads (most recent first) get the higher-quality smart
	// model; the rest use the local model. Default 4.
	TopN int
	// MaxThreads caps how many threads get a draft per run (cost/latency). Default 8.
	MaxThreads int
	// ContextMsgs is how many prior messages to feed as tone/context. Default 8.
	ContextMsgs int

	Logger *slog.Logger
}

// DraftStats reports a run.
type DraftStats struct {
	Considered int
	Drafted    int
	Smart      int
	Skipped    int
}

func (d *Drafter) logger() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return slog.Default()
}

func (d *Drafter) withDefaults() {
	if d.LocalModel == "" {
		d.LocalModel = "qwen2.5:3b-instruct"
	}
	if d.SmartModel == "" {
		d.SmartModel = "gpt-5.4-mini"
	}
	if d.TopN == 0 {
		d.TopN = 4
	}
	if d.MaxThreads == 0 {
		d.MaxThreads = 8
	}
	if d.ContextMsgs == 0 {
		d.ContextMsgs = 8
	}
}

// GenerateForWaiting drafts replies for the importance-gated waiting-on-me
// threads and stores them. Only "important" threads are drafted (Tier 0–1). The
// most recent MaxThreads are taken; the first TopN of those use the smart model.
// Returns stats. Errors on a single thread are logged and skipped, not fatal.
func (d *Drafter) GenerateForWaiting(ctx context.Context, cards []WaitingCard) (DraftStats, error) {
	d.withDefaults()
	var st DraftStats

	// Gate: important threads only, most-recent ping first, capped.
	var todo []WaitingCard
	for _, c := range cards {
		if c.Direction != DirWaitingOnMe || c.Importance != "important" {
			continue
		}
		todo = append(todo, c)
	}
	st.Considered = len(todo)
	// most recent first
	for i := 0; i < len(todo); i++ {
		for j := i + 1; j < len(todo); j++ {
			if todo[j].WaitingHours < todo[i].WaitingHours {
				todo[i], todo[j] = todo[j], todo[i]
			}
		}
	}
	// Skip threads that already have a FRESH draft (answers the current last
	// message) — so a periodic regen only spends model calls on new/changed
	// threads, keeping the scarce subscription + CPU load near zero in steady state.
	existing, err := d.Store.draftsByChat(ctx)
	if err != nil {
		return st, err
	}
	var fresh []WaitingCard
	for _, c := range todo {
		if dr, ok := existing[c.ChatID]; ok && dr.lastMsgID == c.LastMsgID && dr.draft != "" {
			continue
		}
		fresh = append(fresh, c)
	}
	todo = fresh
	if len(todo) > d.MaxThreads {
		todo = todo[:d.MaxThreads]
	}

	for i, c := range todo {
		if ctx.Err() != nil {
			return st, ctx.Err()
		}
		thread, err := d.Reader.RecentThread(ctx, c.ChatID, d.ContextMsgs)
		if err != nil {
			d.logger().Warn("draft: thread context failed", "chat", c.ChatID, "err", err)
			st.Skipped++
			continue
		}
		if len(thread) == 0 {
			st.Skipped++
			continue
		}
		model, name, isSmart := d.Local, d.LocalModel, false
		if d.Smart != nil && i < d.TopN {
			model, name, isSmart = d.Smart, d.SmartModel, true
		}
		draft, err := d.draftOne(ctx, model, name, c, thread)
		if err != nil {
			d.logger().Warn("draft: generation failed", "chat", c.ChatID, "err", err)
			// Fall back to local if the smart pass failed.
			if isSmart && d.Local != nil {
				if draft, err = d.draftOne(ctx, d.Local, d.LocalModel, c, thread); err != nil {
					st.Skipped++
					continue
				}
				name, isSmart = d.LocalModel, false
			} else {
				st.Skipped++
				continue
			}
		}
		draft = cleanDraft(draft)
		if draft == "" {
			st.Skipped++
			continue
		}
		if err := d.Store.PutDraft(ctx, c.ChatID, c.LastMsgID, draft, detectLang(thread), name); err != nil {
			return st, err
		}
		st.Drafted++
		if isSmart {
			st.Smart++
		}
	}
	return st, nil
}

func (d *Drafter) draftOne(ctx context.Context, model ChatModel, name string, c WaitingCard, thread []ThreadMsg) (string, error) {
	return model.Chat(ctx, name, draftSystemPrompt, renderDraftUser(c, thread), false)
}

const draftSystemPrompt = `You write a SHORT reply that the user ("me") will personally send in a chat — you are NOT an assistant answering; you ghost-write the user's own reply, first person.

Rules:
- Match the user's voice and register from THEIR prior messages in the thread (how casual/formal, emoji use, length).
- Reply in the SAME LANGUAGE as the latest incoming message. If the conversation is in Chinese, reply in Chinese; if mixed, mirror the other person's last message.
- Keep it natural and concise — a real chat message, not an email. No greeting or sign-off unless the user normally uses one.
- Address what the other person actually said/asked. If a concrete answer is unknown, write a natural holding reply ("let me check and get back to you" in the right language/tone).
- Output ONLY the reply text. No quotes, no preamble, no explanation.`

func renderDraftUser(c WaitingCard, thread []ThreadMsg) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Chat with %s on %s. Recent conversation (oldest first):\n", c.Person, c.Platform)
	for _, m := range thread {
		who := c.Person
		if m.IsSelf {
			who = "me"
		}
		fmt.Fprintf(&sb, "%s: %s\n", who, clip(m.Text, 400))
	}
	fmt.Fprintf(&sb, "\nWrite my reply to %s's latest message.", c.Person)
	return sb.String()
}

// cleanDraft strips reasoning blocks, wrapping quotes/labels/fences a model
// sometimes adds, and clips to a sane chat length.
func cleanDraft(s string) string {
	// Reasoning models (chatmock gpt-5*) emit <think>...</think> before the reply.
	s = reThink.ReplaceAllString(s, "")
	// If an unterminated <think> remains (truncation), drop everything up to the
	// last </think>, else from the tag onward.
	if i := strings.LastIndex(s, "</think>"); i >= 0 {
		s = s[i+len("</think>"):]
	} else if i := strings.Index(s, "<think>"); i >= 0 {
		s = s[:i]
	}
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")
	s = strings.TrimSpace(s)
	// strip a leading "me:" / "Reply:" label
	for _, p := range []string{"me:", "Me:", "Reply:", "reply:", "Draft:", "draft:"} {
		if strings.HasPrefix(s, p) {
			s = strings.TrimSpace(s[len(p):])
		}
	}
	// strip surrounding quotes
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	return clip(s, 600)
}

// detectLang is a cheap heuristic for the stored draft's lang tag (UI hint only;
// generation language is enforced by the prompt). "zh" if the latest inbound
// message has a meaningful share of CJK characters, else "".
func detectLang(thread []ThreadMsg) string {
	var latest string
	for i := len(thread) - 1; i >= 0; i-- {
		if !thread[i].IsSelf {
			latest = thread[i].Text
			break
		}
	}
	if latest == "" && len(thread) > 0 {
		latest = thread[len(thread)-1].Text
	}
	cjk, total := 0, 0
	for _, r := range latest {
		if r > ' ' {
			total++
		}
		if (r >= 0x4E00 && r <= 0x9FFF) || (r >= 0x3400 && r <= 0x4DBF) {
			cjk++
		}
	}
	if total > 0 && cjk*100/total >= 20 {
		return "zh"
	}
	return ""
}
