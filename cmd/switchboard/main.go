// Command switchboard is the agent-facing CLI for the switchboard daemon (#57). It is a
// thin client over the daemon's HTTP API (127.0.0.1:8717) with machine-readable --json
// output — the CLI+skill replacement for the retired MCP server. The HTTP API remains the
// real multi-consumer service interface (sendgate, gbrain projector); this is one client.
//
// Read/browse subcommands are safe. The SIX side-effecting verbs (send, send-media, react,
// start-chat, tap-button, vote-poll) are the outbound-to-real-people surface gated by
// sendgate: primarily by the harness PreToolUse hook (re-pointed to match these verbs),
// and with a fail-closed CLI backstop (see sendgateGuard) so a missed hook match FAILS the
// send rather than letting an unapproved one through.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/seahyc/switchboard-oss/internal/api"
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
	// --- read / browse (safe) ---
	case "list-chats":
		err = cmdListChats(ctx, args)
	case "read":
		err = cmdRead(ctx, args)
	case "message":
		err = cmdMessage(ctx, args)
	case "search":
		err = cmdSearch(ctx, args)
	case "grep":
		err = cmdGrep(ctx, args)
	case "query-sql":
		err = cmdQuerySQL(ctx, args)
	case "mentions":
		err = cmdMentions(ctx, args)
	case "social-activity":
		err = cmdSocialActivity(ctx, args)
	case "get-media":
		err = cmdGetMedia(ctx, args)
	case "ig-profile":
		err = cmdIGProfile(ctx, args)
	case "ig-stories":
		err = cmdIGStories(ctx, args)
	case "ig-feed":
		err = cmdIGFeed(ctx, args)
	case "ig-following-feed":
		err = cmdIGFollowingFeed(ctx, args)
	case "ig-following":
		err = cmdIGFollowing(ctx, args)
	case "x-following-feed":
		err = cmdXFollowingFeed(ctx, args)
	case "x-profile":
		err = cmdXProfile(ctx, args)
	case "x-ingest":
		err = cmdXIngest(ctx, args)
	case "resolve-person":
		err = cmdResolvePerson(ctx, args)
	case "core-network":
		err = cmdCoreNetwork(ctx, args)
	case "list-proposals":
		err = cmdListProposals(ctx, args)
	case "observability":
		err = cmdObservability(ctx, args)
	case "auth-status":
		err = cmdAuthStatus(ctx, args)
	case "group-invite":
		err = cmdGroupInvite(ctx, args)
	case "honcho-backfill":
		err = cmdHonchoBackfill(ctx, args)
	// --- mutations that are NOT outbound-to-people (ungated, as today) ---
	case "purge-chat":
		err = cmdPurgeChat(ctx, args)
	case "backfill":
		err = cmdBackfill(ctx, args)
	case "reproject":
		err = cmdReproject(ctx, args)
	case "resolve-proposal":
		err = cmdResolveProposal(ctx, args)
	case "merge-canonicals":
		err = cmdMergeCanonicals(ctx, args)
	case "rename-canonical":
		err = cmdRenameCanonical(ctx, args)
	case "auth-submit":
		err = cmdAuthSubmit(ctx, args)
	// --- side-effecting: outbound to real people (sendgate-GATED) ---
	case "send":
		err = cmdSend(ctx, args)
	case "send-media":
		err = cmdSendMedia(ctx, args)
	case "react":
		err = cmdReact(ctx, args)
	case "start-chat":
		err = cmdStartChat(ctx, args)
	case "tap-button":
		err = cmdTapButton(ctx, args)
	case "vote-poll":
		err = cmdVotePoll(ctx, args)
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
	fmt.Fprintln(os.Stderr, "  purge-chat --db PATH --chat-id ID --confirm ID [--media-root PATH] (offline local erasure)")
	fmt.Fprint(os.Stderr, `switchboard — agent CLI over the switchboard daemon (JSON output)

read/browse:
  list-chats        [--platform P] [--since T] [--limit N]
  read CHAT_ID      [--since T] [--before T] [--limit N] [--desc]
  message ID
  search QUERY      [--platform P] [--person NAME] [--since T] [--limit N]
  grep PATTERN      [--platform P] [--person NAME] [--since T] [--limit N]
  query-sql SQL
  mentions          [--since T] [--limit N]
  social-activity   [--platform x|instagram] [--sub post,repost,trend] [--since T] [--limit N]
  get-media ID
  ig-profile HANDLE | ig-stories | ig-feed [--max-id M] [--limit N]
  ig-following-feed [--limit N] | ig-following
  x-following-feed  [--limit N] | x-profile HANDLE [--limit N] | x-ingest [--limit N]
  resolve-person NAME | core-network [--tier T] [--limit N] | list-proposals [--limit N] [--offset N]
  observability | auth-status [PLATFORM]
  group-invite --chat CHAT_ID [--reset]   # group's shareable invite link
  honcho-backfill [--max-chats N] [--per-chat N]   # replay inner-tier WA-DM history → Honcho

mutations (not outbound to people):
  backfill --chat CHAT_ID [--since T] | reproject
  resolve-proposal ID --decision approve|reject
  merge-canonicals --a ID --b ID | rename-canonical ID --name NAME
  auth-submit --platform P --field phone|code|password --value V

send / act (sendgate-APPROVED — outbound to real people):
  send --chat CHAT_ID --text TEXT [--reply-to ID]
  send-media --chat CHAT_ID --media-id MID [--text T] [--reply-to ID] [--as-document] [--mark-forwarded]
  react --chat CHAT_ID --message-id ID --emoji E
  start-chat --platform P --identifier HANDLE
  tap-button --chat CHAT_ID --message-id ID --button BUTTON_ID
  vote-poll --chat CHAT_ID --message-id ID --option OPT

common: --addr HOST:PORT (default 127.0.0.1:8717)
`)
}

// --- helpers ---

func newFS(name string) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	addr := fs.String("addr", envAddr(), "daemon API address")
	return fs, addr
}

func client(addr string) *api.Client { return api.NewClient(addr) }

func emit(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// parseSince accepts an RFC3339 timestamp OR a Go duration meaning "ago" (e.g. "24h",
// "168h"). Empty → zero time (no lower bound).
func parseSince(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		return time.Now().Add(-d).UTC(), nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time %q (use RFC3339 or a duration-ago like 24h)", s)
	}
	return t.UTC(), nil
}

func firstPositional(fs *flag.FlagSet, name string) (string, error) {
	if fs.NArg() < 1 {
		return "", fmt.Errorf("%s required", name)
	}
	return fs.Arg(0), nil
}

// sendgateGuard is the fail-closed backstop for the 6 outbound-to-real-people verbs.
// The PRIMARY gate is the harness PreToolUse sendgate hook (re-pointed to match these
// verbs). This backstop guarantees that even if the hook's bash-command matcher is ever
// bypassed (piped/aliased/path invocation), a side-effecting verb FAILS rather than
// sending unapproved. It is DORMANT until ops sets SWITCHBOARD_REQUIRE_SENDGATE (Phase 2,
// once sendgate is wired to inject SWITCHBOARD_SENDGATE_APPROVED on approval) — so it
// can't break sends before the marker mechanism exists.
func sendgateGuard(verb string) error {
	if os.Getenv("SWITCHBOARD_REQUIRE_SENDGATE") == "" {
		return nil
	}
	if strings.TrimSpace(os.Getenv("SWITCHBOARD_SENDGATE_APPROVED")) == "" {
		return fmt.Errorf("blocked: %q is outbound-to-real-people and must be approved via sendgate "+
			"(no approval marker present)", verb)
	}
	return nil
}

// --- read / browse ---

func cmdListChats(ctx context.Context, args []string) error {
	fs, addr := newFS("list-chats")
	platform := fs.String("platform", "", "filter by platform")
	since := fs.String("since", "", "lower time bound (RFC3339 or duration-ago)")
	limit := fs.Int("limit", 100, "max chats")
	fs.Parse(args)
	t, err := parseSince(*since)
	if err != nil {
		return err
	}
	chats, err := client(*addr).ListChats(ctx, *platform, t, *limit)
	if err != nil {
		return err
	}
	return emit(chats)
}

func cmdRead(ctx context.Context, args []string) error {
	fs, addr := newFS("read")
	since := fs.String("since", "", "lower time bound (RFC3339 or duration-ago)")
	before := fs.String("before", "", "upper time bound (RFC3339 or duration-ago)")
	limit := fs.Int("limit", 50, "max messages")
	desc := fs.Bool("desc", false, "newest-first")
	fs.Parse(args)
	chatID, err := firstPositional(fs, "chat_id")
	if err != nil {
		return err
	}
	sinceT, err := parseSince(*since)
	if err != nil {
		return err
	}
	beforeT, err := parseSince(*before)
	if err != nil {
		return err
	}
	msgs, err := client(*addr).ReadWindow(ctx, chatID, sinceT, beforeT, *limit, *desc)
	if err != nil {
		return err
	}
	return emit(msgs)
}

func cmdMessage(ctx context.Context, args []string) error {
	fs, addr := newFS("message")
	fs.Parse(args)
	id, err := firstPositional(fs, "id")
	if err != nil {
		return err
	}
	m, err := client(*addr).Message(ctx, id)
	if err != nil {
		return err
	}
	return emit(m)
}

func cmdSearch(ctx context.Context, args []string) error {
	fs, addr := newFS("search")
	platform := fs.String("platform", "", "filter by platform")
	person := fs.String("person", "", "filter by person (name → identities)")
	since := fs.String("since", "", "lower time bound")
	limit := fs.Int("limit", 50, "max results")
	fs.Parse(args)
	query, err := firstPositional(fs, "query")
	if err != nil {
		return err
	}
	t, err := parseSince(*since)
	if err != nil {
		return err
	}
	msgs, err := client(*addr).Search(ctx, query, *platform, *person, t, *limit)
	if err != nil {
		return err
	}
	return emit(msgs)
}

func cmdGrep(ctx context.Context, args []string) error {
	fs, addr := newFS("grep")
	platform := fs.String("platform", "", "filter by platform")
	person := fs.String("person", "", "filter by person")
	since := fs.String("since", "", "lower time bound")
	limit := fs.Int("limit", 50, "max results")
	fs.Parse(args)
	pattern, err := firstPositional(fs, "pattern")
	if err != nil {
		return err
	}
	t, err := parseSince(*since)
	if err != nil {
		return err
	}
	res, err := client(*addr).Grep(ctx, pattern, *platform, *person, t, *limit)
	if err != nil {
		return err
	}
	return emit(res)
}

func cmdQuerySQL(ctx context.Context, args []string) error {
	fs, addr := newFS("query-sql")
	fs.Parse(args)
	sql, err := firstPositional(fs, "sql")
	if err != nil {
		return err
	}
	res, err := client(*addr).QuerySQL(ctx, sql)
	if err != nil {
		return err
	}
	return emit(res)
}

func cmdMentions(ctx context.Context, args []string) error {
	fs, addr := newFS("mentions")
	since := fs.String("since", "", "lower time bound")
	limit := fs.Int("limit", 50, "max results")
	fs.Parse(args)
	t, err := parseSince(*since)
	if err != nil {
		return err
	}
	msgs, err := client(*addr).Mentions(ctx, t, *limit)
	if err != nil {
		return err
	}
	return emit(msgs)
}

func cmdSocialActivity(ctx context.Context, args []string) error {
	fs, addr := newFS("social-activity")
	platform := fs.String("platform", "", "filter by platform (x|instagram)")
	sub := fs.String("sub", "", "comma-separated Social.Sub filter (e.g. post,repost,trend)")
	since := fs.String("since", "", "lower time bound (RFC3339 or duration-ago)")
	limit := fs.Int("limit", 100, "max events")
	fs.Parse(args)
	t, err := parseSince(*since)
	if err != nil {
		return err
	}
	var subs []string
	for _, s := range strings.Split(*sub, ",") {
		if s = strings.TrimSpace(s); s != "" {
			subs = append(subs, s)
		}
	}
	acts, err := client(*addr).SocialActivity(ctx, *platform, t, *limit, subs)
	if err != nil {
		return err
	}
	return emit(acts)
}

func cmdGetMedia(ctx context.Context, args []string) error {
	fs, addr := newFS("get-media")
	fs.Parse(args)
	id, err := firstPositional(fs, "media_id")
	if err != nil {
		return err
	}
	m, err := client(*addr).MediaInfo(ctx, id)
	if err != nil {
		return err
	}
	return emit(m)
}

func cmdIGProfile(ctx context.Context, args []string) error {
	fs, addr := newFS("ig-profile")
	fs.Parse(args)
	handle, err := firstPositional(fs, "handle")
	if err != nil {
		return err
	}
	p, err := client(*addr).IGProfile(ctx, handle)
	if err != nil {
		return err
	}
	return emit(p)
}

func cmdIGStories(ctx context.Context, args []string) error {
	fs, addr := newFS("ig-stories")
	fs.Parse(args)
	r, err := client(*addr).IGStories(ctx)
	if err != nil {
		return err
	}
	return emit(r)
}

func cmdIGFeed(ctx context.Context, args []string) error {
	fs, addr := newFS("ig-feed")
	maxID := fs.String("max-id", "", "pagination cursor")
	limit := fs.Int("limit", 0, "max posts")
	fs.Parse(args)
	r, err := client(*addr).IGFeed(ctx, *maxID, *limit)
	if err != nil {
		return err
	}
	return emit(r)
}

func cmdIGFollowingFeed(ctx context.Context, args []string) error {
	fs, addr := newFS("ig-following-feed")
	limit := fs.Int("limit", 0, "max posts")
	fs.Parse(args)
	r, err := client(*addr).IGFollowingFeed(ctx, *limit)
	if err != nil {
		return err
	}
	return emit(r)
}

func cmdIGFollowing(ctx context.Context, args []string) error {
	fs, addr := newFS("ig-following")
	fs.Parse(args)
	r, err := client(*addr).IGFollowing(ctx)
	if err != nil {
		return err
	}
	return emit(r)
}

func cmdXFollowingFeed(ctx context.Context, args []string) error {
	fs, addr := newFS("x-following-feed")
	limit := fs.Int("limit", 0, "max posts")
	fs.Parse(args)
	r, err := client(*addr).XFollowingFeed(ctx, *limit)
	if err != nil {
		return err
	}
	return emit(r)
}

func cmdXProfile(ctx context.Context, args []string) error {
	fs, addr := newFS("x-profile")
	limit := fs.Int("limit", 0, "max recent posts")
	fs.Parse(args)
	handle, err := firstPositional(fs, "handle")
	if err != nil {
		return err
	}
	r, err := client(*addr).XProfile(ctx, handle, *limit)
	if err != nil {
		return err
	}
	return emit(r)
}

func cmdXIngest(ctx context.Context, args []string) error {
	fs, addr := newFS("x-ingest")
	limit := fs.Int("limit", 50, "max posts to pull")
	fs.Parse(args)
	n, err := client(*addr).IngestXActivity(ctx, *limit)
	if err != nil {
		return err
	}
	return emit(map[string]int{"posts": n})
}

func cmdResolvePerson(ctx context.Context, args []string) error {
	fs, addr := newFS("resolve-person")
	fs.Parse(args)
	name, err := firstPositional(fs, "name")
	if err != nil {
		return err
	}
	r, err := client(*addr).ResolvePerson(ctx, name)
	if err != nil {
		return err
	}
	return emit(r)
}

func cmdCoreNetwork(ctx context.Context, args []string) error {
	fs, addr := newFS("core-network")
	tier := fs.String("tier", "", "salience tier (inner|active|normal)")
	limit := fs.Int("limit", 100, "max people")
	fs.Parse(args)
	r, err := client(*addr).CoreNetwork(ctx, *tier, *limit)
	if err != nil {
		return err
	}
	return emit(r)
}

func cmdListProposals(ctx context.Context, args []string) error {
	fs, addr := newFS("list-proposals")
	limit := fs.Int("limit", 0, "page size")
	offset := fs.Int("offset", 0, "page offset")
	fs.Parse(args)
	r, err := client(*addr).PendingProposals(ctx, *limit, *offset)
	if err != nil {
		return err
	}
	return emit(r)
}

func cmdObservability(ctx context.Context, args []string) error {
	fs, addr := newFS("observability")
	fs.Parse(args)
	r, err := client(*addr).Observability(ctx)
	if err != nil {
		return err
	}
	return emit(r)
}

func cmdHonchoBackfill(ctx context.Context, args []string) error {
	fs, addr := newFS("honcho-backfill")
	maxChats := fs.Int("max-chats", 200, "max WA DM chats to scan")
	perChat := fs.Int("per-chat", 500, "max messages per chat")
	fs.Parse(args)
	status, err := client(*addr).HonchoBackfill(ctx, *maxChats, *perChat)
	if err != nil {
		return err
	}
	return emit(map[string]string{"status": status})
}

func cmdGroupInvite(ctx context.Context, args []string) error {
	fs, addr := newFS("group-invite")
	chat := fs.String("chat", "", "group chat_id")
	reset := fs.Bool("reset", false, "rotate (invalidate the old) link")
	fs.Parse(args)
	if *chat == "" {
		return fmt.Errorf("--chat required")
	}
	link, err := client(*addr).GroupInvite(ctx, *chat, *reset)
	if err != nil {
		return err
	}
	return emit(map[string]string{"chat_id": *chat, "invite_link": link})
}

func cmdAuthStatus(ctx context.Context, args []string) error {
	fs, addr := newFS("auth-status")
	fs.Parse(args)
	platform := ""
	if fs.NArg() > 0 {
		platform = fs.Arg(0)
	}
	r, err := client(*addr).AuthStatus(ctx, platform)
	if err != nil {
		return err
	}
	return emit(r)
}

// --- ungated mutations ---

func cmdBackfill(ctx context.Context, args []string) error {
	fs, addr := newFS("backfill")
	chat := fs.String("chat", "", "chat_id to backfill")
	since := fs.String("since", "", "lower time bound")
	fs.Parse(args)
	if *chat == "" {
		return fmt.Errorf("--chat required")
	}
	t, err := parseSince(*since)
	if err != nil {
		return err
	}
	if err := client(*addr).Backfill(ctx, api.BackfillBody{ChatID: *chat, Since: t}); err != nil {
		return err
	}
	return emit(map[string]bool{"ok": true})
}

func cmdReproject(ctx context.Context, args []string) error {
	fs, addr := newFS("reproject")
	fs.Parse(args)
	n, err := client(*addr).Reproject(ctx)
	if err != nil {
		return err
	}
	return emit(map[string]int{"identities": n})
}

func cmdResolveProposal(ctx context.Context, args []string) error {
	fs, addr := newFS("resolve-proposal")
	decision := fs.String("decision", "", "approve|reject")
	fs.Parse(args)
	id, err := firstPositional(fs, "proposal_id")
	if err != nil {
		return err
	}
	if *decision != "approve" && *decision != "reject" {
		return fmt.Errorf("--decision must be approve or reject")
	}
	out, err := client(*addr).ResolveProposal(ctx, id, *decision)
	if err != nil {
		return err
	}
	return emit(out)
}

func cmdMergeCanonicals(ctx context.Context, args []string) error {
	fs, addr := newFS("merge-canonicals")
	a := fs.String("a", "", "canonical id")
	b := fs.String("b", "", "canonical id")
	fs.Parse(args)
	if *a == "" || *b == "" {
		return fmt.Errorf("--a and --b required")
	}
	out, err := client(*addr).MergeCanonicals(ctx, *a, *b)
	if err != nil {
		return err
	}
	return emit(out)
}

func cmdRenameCanonical(ctx context.Context, args []string) error {
	fs, addr := newFS("rename-canonical")
	name := fs.String("name", "", "confirmed display name")
	fs.Parse(args)
	id, err := firstPositional(fs, "canon_id")
	if err != nil {
		return err
	}
	if *name == "" {
		return fmt.Errorf("--name required")
	}
	if err := client(*addr).RenameCanonical(ctx, id, *name); err != nil {
		return err
	}
	return emit(map[string]bool{"ok": true})
}

func cmdAuthSubmit(ctx context.Context, args []string) error {
	fs, addr := newFS("auth-submit")
	platform := fs.String("platform", "", "connector")
	field := fs.String("field", "", "phone|code|password")
	value := fs.String("value", "", "value")
	fs.Parse(args)
	if *platform == "" || *field == "" {
		return fmt.Errorf("--platform and --field required")
	}
	if err := client(*addr).AuthSubmit(ctx, *platform, *field, *value); err != nil {
		return err
	}
	return emit(map[string]bool{"ok": true})
}

// --- sendgate-GATED: outbound to real people ---

func cmdSend(ctx context.Context, args []string) error {
	fs, addr := newFS("send")
	chat := fs.String("chat", "", "chat_id")
	text := fs.String("text", "", "message text")
	replyTo := fs.String("reply-to", "", "message id to reply to")
	fs.Parse(args)
	if err := sendgateGuard("send"); err != nil {
		return err
	}
	if *chat == "" || *text == "" {
		return fmt.Errorf("--chat and --text required")
	}
	id, err := client(*addr).Send(ctx, api.SendBody{ChatID: *chat, Text: *text, ReplyTo: *replyTo})
	if err != nil {
		return err
	}
	return emit(map[string]string{"message_id": id})
}

func cmdSendMedia(ctx context.Context, args []string) error {
	fs, addr := newFS("send-media")
	chat := fs.String("chat", "", "chat_id")
	mediaID := fs.String("media-id", "", "stored media id (from get-media/read/search)")
	text := fs.String("text", "", "caption")
	replyTo := fs.String("reply-to", "", "message id to reply to")
	asDoc := fs.Bool("as-document", false, "send as document")
	markFwd := fs.Bool("mark-forwarded", false, "set platform forwarded metadata")
	fs.Parse(args)
	if err := sendgateGuard("send-media"); err != nil {
		return err
	}
	if *chat == "" || *mediaID == "" {
		return fmt.Errorf("--chat and --media-id required")
	}
	id, err := client(*addr).SendMedia(ctx, api.SendMediaBody{
		ChatID: *chat, MediaID: *mediaID, Text: *text, ReplyTo: *replyTo,
		AsDocument: *asDoc, MarkForwarded: *markFwd,
	})
	if err != nil {
		return err
	}
	return emit(map[string]string{"message_id": id})
}

func cmdReact(ctx context.Context, args []string) error {
	fs, addr := newFS("react")
	chat := fs.String("chat", "", "chat_id")
	msgID := fs.String("message-id", "", "message id")
	emoji := fs.String("emoji", "", "reaction emoji")
	fs.Parse(args)
	if err := sendgateGuard("react"); err != nil {
		return err
	}
	if *chat == "" || *msgID == "" || *emoji == "" {
		return fmt.Errorf("--chat, --message-id, --emoji required")
	}
	if err := client(*addr).React(ctx, api.ReactBody{ChatID: *chat, MessageID: *msgID, Emoji: *emoji}); err != nil {
		return err
	}
	return emit(map[string]bool{"ok": true})
}

func cmdStartChat(ctx context.Context, args []string) error {
	fs, addr := newFS("start-chat")
	platform := fs.String("platform", "", "platform")
	identifier := fs.String("identifier", "", "handle / phone / id")
	fs.Parse(args)
	if err := sendgateGuard("start-chat"); err != nil {
		return err
	}
	if *platform == "" || *identifier == "" {
		return fmt.Errorf("--platform and --identifier required")
	}
	out, err := client(*addr).StartChat(ctx, api.StartChatBody{Platform: *platform, Identifier: *identifier})
	if err != nil {
		return err
	}
	return emit(out)
}

func cmdTapButton(ctx context.Context, args []string) error {
	fs, addr := newFS("tap-button")
	chat := fs.String("chat", "", "chat_id")
	msgID := fs.String("message-id", "", "message id")
	button := fs.String("button", "", "button id")
	fs.Parse(args)
	if err := sendgateGuard("tap-button"); err != nil {
		return err
	}
	if *chat == "" || *msgID == "" || *button == "" {
		return fmt.Errorf("--chat, --message-id, --button required")
	}
	res, err := client(*addr).TapButton(ctx, api.TapBody{ChatID: *chat, MessageID: *msgID, ButtonID: *button})
	if err != nil {
		return err
	}
	return emit(map[string]string{"result": res})
}

func cmdVotePoll(ctx context.Context, args []string) error {
	fs, addr := newFS("vote-poll")
	chat := fs.String("chat", "", "chat_id")
	msgID := fs.String("message-id", "", "message id")
	option := fs.String("option", "", "poll option")
	fs.Parse(args)
	if err := sendgateGuard("vote-poll"); err != nil {
		return err
	}
	if *chat == "" || *msgID == "" || *option == "" {
		return fmt.Errorf("--chat, --message-id, --option required")
	}
	if err := client(*addr).VotePoll(ctx, api.VoteBody{ChatID: *chat, MessageID: *msgID, Option: *option}); err != nil {
		return err
	}
	return emit(map[string]bool{"ok": true})
}

func envAddr() string {
	if addr := os.Getenv("SWITCHBOARD_ADDR"); addr != "" {
		return addr
	}
	return api.DefaultAddr
}
