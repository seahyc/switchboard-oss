---
name: switchboard-read
description: Read and browse the user's unified messaging history + social feeds via the `switchboard` CLI — list chats, read threads, search/grep the firehose, run read-only SQL, look up people, and browse Instagram/X. Use whenever you need to find, read, or reason over the user's messages (WhatsApp/Telegram/Lark/Gmail/Instagram/Messenger), their social feeds, or who someone is. Read-only and safe — never sends anything.
---

# switchboard-read

`switchboard` is a CLI over the local switchboard daemon (the user's unified inbox +
social ingestion across WhatsApp, Telegram, Lark, Gmail, Instagram/Messenger, and X).
It talks to the daemon's HTTP API on `127.0.0.1:8717` and prints **JSON** to stdout.

**This skill is read-only.** Sending, reacting, starting chats, tapping buttons, and
voting are a SEPARATE skill (`switchboard-act`) because they message real people and go
through an approval gate. Nothing here ever sends.

Run `switchboard help` for the full usage. All commands emit JSON; pipe to `jq`.

## When to use

- "What did X say?", "catch me up on the <group>" → `read` / `search`
- "Find the message about <topic>" → `search` (full-text) or `grep` (regex)
- "Who is <name> / how do I reach them?" → `resolve-person`
- "What's unread / who's waiting on me?" → `list-chats`, `mentions`
- "What are my friends posting?" / "what's on X?" → `ig-following-feed`, `x-following-feed`
- Anything analytical over the history → `query-sql` (read-only SQL over the firehose)

## Core commands

```
switchboard list-chats [--platform whatsapp|telegram|lark|gmail|instagram|x] [--since 24h] [--limit 100]
switchboard read <chat_id> [--since 7d] [--before <ts>] [--limit 50] [--desc]   # --desc = newest-first
switchboard message <id>                                  # one message by full id
switchboard search "<query>" [--platform P] [--person NAME] [--since 7d] [--limit 50]
switchboard grep "<regex>"   [--platform P] [--person NAME] [--since 7d] [--limit 50]
switchboard query-sql "SELECT ... FROM messages WHERE ..."   # READ-ONLY
switchboard mentions [--since 7d] [--limit 50]            # messages that @-mention the user
switchboard get-media <media_id>                          # resolve a stored attachment (path/url/meta)
```

`--since` / `--before` accept either an RFC3339 timestamp or a **duration-ago** like
`24h`, `7d` is not valid — use `168h`. (Go durations: `h`/`m`/`s`.)

## People

```
switchboard resolve-person "<name>"        # name → canonical person + their reachable identities/chats
switchboard core-network [--tier inner|active|normal] [--limit 100]
switchboard list-proposals [--limit N] [--offset N]   # pending identity-merge proposals to review
```

## Social browse (Instagram / X — read-only)

```
switchboard ig-following-feed [--limit N]   # recent posts from accounts the user follows
switchboard ig-following                    # the complete follow list (enriched)
switchboard ig-profile <handle> | switchboard ig-stories | switchboard ig-feed [--max-id M] [--limit N]
switchboard x-following-feed [--limit N]    # X following timeline
switchboard x-profile <handle> [--limit N]
switchboard x-ingest [--limit N]            # pull the X feed+trends into the log (then read via search/query-sql)
```

The unified social log (IG + X, newest-first) is also a first-class read:

```
switchboard social-activity [--platform x|instagram] [--sub post,repost,trend] [--since 24h] [--limit 100]
```

Social posts land in the canonical log as `social_activity` events (`sub` = `post` /
`repost` / `story` / `trend`) with raw engagement fields (`likes`, `retweets`, `replies`,
`quotes`, `views`; trend volume in `views`) — read + judge them yourself; switchboard does
not rank or filter them. Prefer `social-activity` over `query-sql` for the social log.

## Ops / health (read-only)

```
switchboard observability        # per-platform ingest coverage + connector health + lag
switchboard auth-status [platform]   # connector login/health
switchboard group-invite --chat <chat_id> [--reset]   # a group's shareable invite link (WhatsApp)
```

## Tips

- `--addr HOST:PORT` overrides the daemon address (default `127.0.0.1:8717`).
- Everything is JSON — chain with `jq` (e.g. `switchboard list-chats --json | jq '.[].chat_id'`).
- `chat_id` values come from `list-chats` / `search` results; pass them to `read`.
- For "messages from <person>" use `search --person "<name>"` (it resolves the name to
  the person's identities; the raw `sender_name` is often blank by design).
