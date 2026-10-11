// Command comprehend is switchboard's F1/F2 comprehension CLI. It reads the
// canonical event store READ-ONLY and emits the deterministic reply-graph
// projections, the morning brief, and (optionally) extracted commitments.
//
// Subcommands:
//
//	comprehend waiting [--json]     who's-waiting + ghosting reply-graphs (deterministic)
//	comprehend brief   [--json]     assembled morning brief (text by default)
//	comprehend commitments [--json] [--status open] extracted commitments
//	comprehend extract [--batch N] [--no-smart]     run the commitment extraction funnel
//	comprehend health               check store + ollama + chatmock reachability
//
// SUGGEST-ONLY: this binary never sends a message. Delivery of the brief is a
// separate concern (deploy/comprehend-brief.sh pipes `brief` into `hermes send`).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/seahyc/switchboard-oss/internal/comprehend"
	"github.com/seahyc/switchboard-oss/internal/llm"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	ctx := context.Background()

	var err error
	switch cmd {
	case "waiting":
		err = cmdWaiting(ctx, args)
	case "brief":
		err = cmdBrief(ctx, args)
	case "commitments":
		err = cmdCommitments(ctx, args)
	case "extract":
		err = cmdExtract(ctx, args)
	case "draft":
		err = cmdDraft(ctx, args)
	case "backfill-due":
		err = cmdBackfillDue(ctx, args)
	case "health":
		err = cmdHealth(ctx, args)
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `comprehend — switchboard comprehension projections + brief (suggest-only)

usage:
  comprehend waiting       [--db PATH] [--json]
  comprehend brief         [--db PATH] [--cdb PATH] [--json]
  comprehend commitments   [--cdb PATH] [--status open|resolved|slipped] [--json]
  comprehend draft         [--db PATH] [--cdb PATH] [--max N] [--top N] [--no-smart]
  comprehend backfill-due  [--db PATH] [--cdb PATH]
  comprehend extract       [--db PATH] [--cdb PATH] [--batch N] [--no-smart] [--ollama-url URL] [--chatmock-url URL]
  comprehend health        [--db PATH] [--ollama-url URL] [--chatmock-url URL]
`)
}

func defaultDB() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, "switchboard", "data", "switchboard.db")
	}
	return "switchboard.db"
}

func defaultCDB() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, "switchboard", "data", "comprehend.db")
	}
	return "comprehend.db"
}

func cmdWaiting(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("waiting", flag.ExitOnError)
	dbPath := fs.String("db", defaultDB(), "switchboard.db path (read-only)")
	cdbPath := fs.String("cdb", defaultCDB(), "comprehend.db path (for stored drafts)")
	asJSON := fs.Bool("json", false, "emit JSON")
	fs.Parse(args)

	r, err := comprehend.OpenReader(*dbPath)
	if err != nil {
		return err
	}
	defer r.Close()
	proj, err := r.Project(ctx, comprehend.ProjectionConfig{Now: time.Now()})
	if err != nil {
		return err
	}
	// Attach stored suggest-only drafts to the cards (fresh ones only). Best-effort:
	// if the draft store is unavailable, cards simply carry draft=null.
	if st, err := comprehend.OpenStore(*cdbPath); err == nil {
		_ = st.AttachDrafts(ctx, proj.WaitingOnMe)
		_ = st.AttachDrafts(ctx, proj.Ghosting)
		st.Close()
	}
	// Surface the high-value "swipe right to send" cards first: draft-bearing cards
	// lead the deck, preserving the urgency order within each group.
	proj.WaitingOnMe = draftFirst(proj.WaitingOnMe)
	if *asJSON {
		return emitJSON(map[string]any{
			"generated_at":  time.Now().Format(time.RFC3339),
			"waiting_on_me": nonNil(proj.WaitingOnMe),
			"ghosting":      nonNil(proj.Ghosting),
		})
	}
	fmt.Printf("⏳ Waiting on you (%d)\n", len(proj.WaitingOnMe))
	for _, c := range proj.WaitingOnMe {
		fmt.Printf("  • %-22s %-9s %3dh  %q\n", trunc(c.Person, 22), c.Platform, c.WaitingHours, clipText(c.LastMsgText, 60))
	}
	fmt.Printf("\n👻 No reply yet from them (%d)\n", len(proj.Ghosting))
	for _, c := range proj.Ghosting {
		fmt.Printf("  • %-22s %-9s %3dh\n", trunc(c.Person, 22), c.Platform, c.WaitingHours)
	}
	return nil
}

func cmdBrief(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("brief", flag.ExitOnError)
	dbPath := fs.String("db", defaultDB(), "switchboard.db path (read-only)")
	cdbPath := fs.String("cdb", defaultCDB(), "comprehend.db path (commitments)")
	asJSON := fs.Bool("json", false, "emit JSON")
	fs.Parse(args)

	r, err := comprehend.OpenReader(*dbPath)
	if err != nil {
		return err
	}
	defer r.Close()

	var commits comprehend.CommitmentSource
	if st, err := comprehend.OpenStore(*cdbPath); err == nil {
		defer st.Close()
		commits = st
	}

	cfg := comprehend.BriefConfig{Now: time.Now()}
	brief, err := r.AssembleBrief(ctx, cfg, commits)
	if err != nil {
		return err
	}
	// Attach suggest-only drafts to the brief's waiting preview so the brief screen
	// can render send-ready cards too (same field, never auto-sent).
	if st, ok := commits.(*comprehend.Store); ok && st != nil {
		_ = st.AttachDrafts(ctx, brief.WaitingOnMe)
	}
	if *asJSON {
		return emitJSON(brief)
	}
	fmt.Print(brief.RenderText(cfg))
	return nil
}

func cmdCommitments(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("commitments", flag.ExitOnError)
	cdbPath := fs.String("cdb", defaultCDB(), "comprehend.db path")
	status := fs.String("status", "", "filter: open|resolved|slipped")
	asJSON := fs.Bool("json", false, "emit JSON")
	fs.Parse(args)

	st, err := comprehend.OpenStore(*cdbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	cs, err := st.Commitments(ctx, *status)
	if err != nil {
		return err
	}
	if *asJSON {
		return emitJSON(nonNilC(cs))
	}
	fmt.Printf("commitments (%d)\n", len(cs))
	for _, c := range cs {
		due := c.Due
		if due == "" {
			due = "—"
		}
		fmt.Printf("  • [%s] %s → %s: %s (due %s, %s, conf %.2f)\n",
			c.Status, c.Who, c.ToWhom, c.What, due, c.Extractor, c.Confidence)
	}
	return nil
}

func cmdExtract(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("extract", flag.ExitOnError)
	dbPath := fs.String("db", defaultDB(), "switchboard.db path (read-only)")
	cdbPath := fs.String("cdb", defaultCDB(), "comprehend.db path")
	batch := fs.Int("batch", 50, "max candidates to process this run")
	since := fs.Duration("since", 0, "recent fast-scan: only process messages newer than now-DURATION (separate cursor; for freshness/goal-1)")
	noSmart := fs.Bool("no-smart", false, "disable the chatmock smart pass (local-only)")
	ollamaURL := fs.String("ollama-url", "", "ollama base URL (empty → OLLAMA_URL env / docker default)")
	ollamaModel := fs.String("ollama-model", "qwen2.5:3b-instruct", "local extraction model (3b is OOM-safe under the 6.5G ollama cap; 7b gets SIGKILL-ed)")
	chatmockURL := fs.String("chatmock-url", "http://127.0.0.1:8401", "chatmock proxy URL")
	smartModel := fs.String("smart-model", "gpt-5.4-mini", "chatmock model")
	fs.Parse(args)

	r, err := comprehend.OpenReader(*dbPath)
	if err != nil {
		return err
	}
	defer r.Close()
	st, err := comprehend.OpenStore(*cdbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	local := llm.New(llm.Config{BaseURL: *ollamaURL, AdjModel: *ollamaModel, Container: "honcho-ollama-1", MinInterval: 1 * time.Second})
	ex := &comprehend.Extractor{
		Reader:     r,
		Store:      st,
		Local:      local,
		LocalModel: *ollamaModel,
		SmartModel: *smartModel,
	}
	if *since > 0 {
		ex.Scope = "commitments-recent"
		ex.Floor = time.Now().Add(-*since)
	}
	if !*noSmart {
		ex.Smart = comprehend.NewChatMock(comprehend.ChatMockConfig{BaseURL: *chatmockURL})
	}
	stats, err := ex.Run(ctx, *batch)
	if err != nil {
		return err
	}
	fmt.Printf("extract: scanned=%d cache_hits=%d escalated=%d committed=%d watermark=%s\n",
		stats.Scanned, stats.CacheHits, stats.Escalated, stats.Committed, stats.Watermark.Format(time.RFC3339))
	return nil
}

func cmdDraft(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("draft", flag.ExitOnError)
	dbPath := fs.String("db", defaultDB(), "switchboard.db path (read-only)")
	cdbPath := fs.String("cdb", defaultCDB(), "comprehend.db path")
	maxThreads := fs.Int("max", 8, "max waiting threads to draft this run")
	topN := fs.Int("top", 4, "top N threads use the chatmock smart model")
	noSmart := fs.Bool("no-smart", false, "local-only (no chatmock)")
	ollamaURL := fs.String("ollama-url", "", "ollama base URL")
	ollamaModel := fs.String("ollama-model", "qwen2.5:3b-instruct", "local draft model")
	chatmockURL := fs.String("chatmock-url", "http://127.0.0.1:8401", "chatmock proxy URL")
	smartModel := fs.String("smart-model", "gpt-5.4-mini", "chatmock model")
	fs.Parse(args)

	r, err := comprehend.OpenReader(*dbPath)
	if err != nil {
		return err
	}
	defer r.Close()
	st, err := comprehend.OpenStore(*cdbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	proj, err := r.Project(ctx, comprehend.ProjectionConfig{Now: time.Now()})
	if err != nil {
		return err
	}
	local := llm.New(llm.Config{BaseURL: *ollamaURL, AdjModel: *ollamaModel, Container: "honcho-ollama-1", MinInterval: 1 * time.Second})
	dr := &comprehend.Drafter{
		Reader: r, Store: st, Local: local,
		LocalModel: *ollamaModel, SmartModel: *smartModel,
		TopN: *topN, MaxThreads: *maxThreads,
	}
	if !*noSmart {
		dr.Smart = comprehend.NewChatMock(comprehend.ChatMockConfig{BaseURL: *chatmockURL})
	}
	stats, err := dr.GenerateForWaiting(ctx, proj.WaitingOnMe)
	if err != nil {
		return err
	}
	fmt.Printf("draft: considered=%d drafted=%d (smart=%d) skipped=%d\n",
		stats.Considered, stats.Drafted, stats.Smart, stats.Skipped)
	return nil
}

func cmdBackfillDue(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("backfill-due", flag.ExitOnError)
	dbPath := fs.String("db", defaultDB(), "switchboard.db path (read-only)")
	cdbPath := fs.String("cdb", defaultCDB(), "comprehend.db path")
	fs.Parse(args)

	r, err := comprehend.OpenReader(*dbPath)
	if err != nil {
		return err
	}
	defer r.Close()
	st, err := comprehend.OpenStore(*cdbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	n, err := comprehend.BackfillDue(ctx, r, st, comprehend.SGT)
	if err != nil {
		return err
	}
	fmt.Printf("backfill-due: filled %d commitment due dates\n", n)
	return nil
}

func cmdHealth(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	dbPath := fs.String("db", defaultDB(), "switchboard.db path (read-only)")
	ollamaURL := fs.String("ollama-url", "", "ollama base URL")
	chatmockURL := fs.String("chatmock-url", "http://127.0.0.1:8401", "chatmock proxy URL")
	fs.Parse(args)

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	ok := true
	if r, err := comprehend.OpenReader(*dbPath); err != nil {
		fmt.Printf("store     ✗ %v\n", err)
		ok = false
	} else {
		r.Close()
		fmt.Printf("store     ✓ %s\n", *dbPath)
	}
	if *ollamaURL == "" {
		if u, err := llm.ResolveOllamaURL(ctx, "honcho-ollama-1"); err == nil {
			*ollamaURL = u
		}
	}
	local := llm.New(llm.Config{BaseURL: *ollamaURL, Container: "honcho-ollama-1"})
	if err := local.Health(ctx); err != nil {
		fmt.Printf("ollama    ✗ %v\n", err)
		ok = false
	} else {
		fmt.Printf("ollama    ✓ %s\n", local.BaseURL())
	}
	cm := comprehend.NewChatMock(comprehend.ChatMockConfig{BaseURL: *chatmockURL})
	if err := cm.Health(ctx); err != nil {
		fmt.Printf("chatmock  ✗ %v\n", err)
		ok = false
	} else {
		fmt.Printf("chatmock  ✓ %s\n", *chatmockURL)
	}
	if !ok {
		return fmt.Errorf("one or more health checks failed")
	}
	return nil
}

func emitJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// draftFirst stable-partitions cards so draft-bearing ones lead (the immediately
// actionable "swipe right to send" cards), preserving relative order within each
// group so the urgency ordering is otherwise intact.
func draftFirst(cards []comprehend.WaitingCard) []comprehend.WaitingCard {
	withDraft := make([]comprehend.WaitingCard, 0, len(cards))
	without := make([]comprehend.WaitingCard, 0, len(cards))
	for _, c := range cards {
		if c.Draft != nil && *c.Draft != "" {
			withDraft = append(withDraft, c)
		} else {
			without = append(without, c)
		}
	}
	return append(withDraft, without...)
}

func nonNil(c []comprehend.WaitingCard) []comprehend.WaitingCard {
	if c == nil {
		return []comprehend.WaitingCard{}
	}
	return c
}

func nonNilC(c []comprehend.Commitment) []comprehend.Commitment {
	if c == nil {
		return []comprehend.Commitment{}
	}
	return c
}

func trunc(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

func clipText(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}
