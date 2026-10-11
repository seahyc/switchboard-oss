# Connectors

The daemon starts with no connectors. Enable only accounts you intend to archive, and keep the corresponding session files, OAuth credentials, and API tokens in private local storage. Connector behavior and setup effort differ; only WhatsApp and Slack have a concise standalone path in this repository.

| Connector | Enable it with | Setup and limits |
| --- | --- | --- |
| WhatsApp | `-whatsapp-db "$HOME/.switchboard/whatsapp.db"` | Pair from the generated QR image. The session is local and persists across restarts. See [Self-hosting](SELF_HOSTING.md) and [WhatsApp Linked devices](https://faq.whatsapp.com/1317564962315842/). |
| Slack | `-slack-token-file PATH -slack-channels ID[,ID...]` | Experimental, read-only polling. Requires a private user OAuth token and explicit channel IDs. See [Slack setup and limits](SLACK.md) and [Slack API scopes](https://docs.slack.dev/reference/scopes/). |
| Telegram | `-telegram-app-id ID -telegram-app-hash HASH` | Requires an API ID and hash from [Telegram API development tools](https://my.telegram.org/apps). The login flow requests phone/code/password as needed. Set `-telegram-session` to a private absolute path; the default `telegram.session` is relative to the daemon's working directory. Telegram's [API terms](https://core.telegram.org/api/terms) apply. |
| Lark / Feishu | `-lark-config DIR` | Requires a custom app ID and secret, an approved user-access token with the needed messaging/chat permissions, and local `config.yaml` plus `tokens.json` files. See [Lark Open Platform](https://open.larksuite.com/) and the [official Lark CLI](https://github.com/larksuite/cli), whose token format the connector reads. This is an advanced setup; the project does not provide a turnkey app-approval flow. |
| Gmail | `-gmail -gmail-credentials-file PATH -gmail-accounts address@example.com` | Requires OAuth credentials with Gmail read access and an explicit mailbox allowlist. See [Gmail credential file setup](GMAIL.md), [Gmail API OAuth](https://developers.google.com/workspace/gmail/api/auth/web-server), and [Gmail scopes](https://developers.google.com/workspace/gmail/api/auth/scopes). |
| Instagram / Messenger | `-meta -meta-token-file PATH` | Requires a separately operated Matrix homeserver and mautrix-meta bridge. It is not bundled or configured as a standalone integration. |
| Instagram browser | `-ig-browser -ig-session-file PATH -ig-proxy HOST:PORT -ig-expected-egress IP` | Read-only, experimental session-based browsing. Requires exported session data and a proxy whose egress matches the configured address. No session or proxy is supplied. |
| X browser | `-x-browser -x-session-file PATH -x-proxy HOST:PORT -x-expected-egress IP` | Read-only, experimental session-based browsing. Requires exported session data and a proxy whose egress matches the configured address. No session or proxy is supplied. |

The documented standalone starter is [WhatsApp](SELF_HOSTING.md#connect-whatsapp). Slack configuration uses these exact daemon flags:

```sh
switchboardd \
  -slack-token-file "$HOME/.switchboard/slack.token" \
  -slack-channels C12345678 \
  -slack-poll-interval 1m \
  -slack-since 2026-10-01T00:00:00Z
```

Create the Slack user token in a private file with mode `600` before starting the daemon. The channel allowlist is mandatory. Polling can miss deletions and edits outside its recent overlap; history and rate limits depend on the workspace and Slack API. `Send` and `React` are unsupported by this connector. See [the detailed Slack guide](SLACK.md) before installing an app or granting scopes.

No connector enables outbound behavior by itself. The daemon's outbound flag is disabled by default. If a caller can send messages, it must enforce per-send approval. gbrain, Honcho, the optional identity matcher, and a model server are separate integrations; none is required for the default archive and CLI flow.
