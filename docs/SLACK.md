# Slack connector

The Slack connector reads only an explicit list of conversation IDs and uses a
user-provided Slack user token from a private file. `auth.test` verifies and
records the team and user identity during construction. Keep the token file
readable only by its owner (`chmod 600`). Configure an internal app in the
user's own Slack workspace and install it for that user; the connector does not
perform an OAuth installation flow.

For public-channel capture, create an internal app from
[`slack-app-manifest.json`](slack-app-manifest.json), select your workspace, and
install it for your user. The manifest requests only `channels:history` and
`channels:read`. On the app's **Install App** page, save the **User OAuth Token**
to a private token file; do not use an app-level or bot token. Find the channel ID
in Slack's channel details or in its web-client URL. Configure only the channels
you intend to capture. The Slack scopes can access other public channels your
user belongs to; Switchboard's allowlist limits what this process fetches.

Required read scopes are `channels:history`, `groups:history`, `im:history`, and
`mpim:history` for the kinds of conversations the app needs to read. Slack's
`conversations.history` documentation lists these scopes for both user and bot
tokens, but this connector expects a user token so it can read conversations
the user belongs to without requiring a bot to be invited. It does not enumerate
the workspace: every configured channel ID is fetched directly, and an empty
allowlist is an error.

`Connect` polls the configured channels and overlaps the previous five minutes
to detect recently edited messages. `Since`, when set, bounds its first poll.
`Backfill` uses Slack's `oldest` bound and cursor pagination. Threads are fetched
with paginated `conversations.replies`. Slack message timestamps stay as decimal
strings in canonical IDs; parsing uses integer seconds and fractional
nanoseconds, without float conversion. An observed text change emits a separate
`KindEdit` event whose `ReplyTo` points at the original message.

Polling cannot guarantee detection of every deletion or edits to messages older
than its recent overlap. Slack history and reply pagination also remain subject
to workspace/API retention and rate limits. HTTP 429 responses honor
`Retry-After` with a cancellable retry bounded to 30 seconds. Request failures
report status or Slack's error code, never response bodies or credentials.

Supported behavior is read-only: message capture, bounded backfill, thread
capture, and optional channel metadata. `Send` and `React` return an explicit unsupported
error. No write scopes are needed.

Official references:

- [conversations.history](https://docs.slack.dev/reference/methods/conversations.history/)
- [conversations.replies](https://docs.slack.dev/reference/methods/conversations.replies/)
- [Slack scopes](https://docs.slack.dev/reference/scopes/)

Install the standalone binary at `~/.local/bin/switchboardd`. That directory is
not usually on a service manager's `PATH`, so invoke the binary by its full path
in service configuration.

Create a private token file without putting the token in shell history. Run
these commands, then paste the token at the hidden prompt:

```sh
bash -c '
  umask 077
  mkdir -p "$HOME/.switchboard"
  read -r -s -p "Slack user OAuth token: " SLACK_TOKEN || exit 1
  printf "\n"
  test -n "$SLACK_TOKEN" || exit 1
  printf "%s" "$SLACK_TOKEN" > "$HOME/.switchboard/slack.token"
  unset SLACK_TOKEN
  chmod 600 "$HOME/.switchboard/slack.token"
'
```

Keep the token file out of source control.

Start the daemon with an explicit start bound, channel allowlist, and media
directory. These are the connector's supported daemon flags:

```sh
~/.local/bin/switchboardd \
  -db "$HOME/.switchboard/switchboard.db" \
  -api-token-file "$HOME/.switchboard/api.token" \
  -qr-dir "$HOME/.switchboard" \
  -media-root "$HOME/.switchboard/media" \
  -slack-token-file "$HOME/.switchboard/slack.token" \
  -slack-channels C12345678,C23456789 -slack-poll-interval 1m \
  -slack-since 2026-10-01T00:00:00Z
```

After startup, verify the Slack identity and that the allowlisted channels
appear. Set the CLI token-file path in the same shell. The daemon's generated
administrator token is in `~/.switchboard/api.token`:

```sh
export SWITCHBOARD_TOKEN_FILE="$HOME/.switchboard/api.token"
~/.local/bin/switchboard auth-status slack
~/.local/bin/switchboard list-chats --platform slack
```

To search Slack messages, use this command. Putting options first also works
with v0.1.0:

```sh
~/.local/bin/switchboard search --platform slack --limit 10 "invoice"
```

Optional channel titles use `conversations.info` and require the corresponding
`channels:read`, `groups:read`, `im:read`, or `mpim:read` scope. Without these,
message capture can proceed, but metadata refresh will fail. Late replies to
threads whose roots lie outside the poll window can be missed; use bounded
backfill to revisit older threads. Attachments are not downloaded by this
connector. Live account validation remains a release gate.
