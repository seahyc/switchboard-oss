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

Run with an explicit start bound and private token file:

```sh
switchboardd -slack-token-file ~/.switchboard/slack.token \
  -slack-channels C12345678,C23456789 -slack-poll-interval 1m \
  -slack-since 2026-10-01T00:00:00Z
```

Optional channel titles use `conversations.info` and require the corresponding
`channels:read`, `groups:read`, `im:read`, or `mpim:read` scope. Without these,
message capture can proceed, but metadata refresh will fail. Late replies to
threads whose roots lie outside the poll window can be missed; use bounded
backfill to revisit older threads. Attachments are not downloaded by this
connector. Live account validation remains a release gate.
