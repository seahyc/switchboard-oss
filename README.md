# Switchboard

Switchboard is a self-hosted message archive. It captures messages from connectors you explicitly enable, stores them in a local SQLite database, and provides a localhost API and CLI for browsing that archive. The default install starts with no connected accounts and needs no gbrain, Honcho, model server, or paid inference API.

## Quick start from source

Requires Go 1.25.8 or newer on macOS or Linux. [Install Go](https://go.dev/dl/), then:

```sh
git clone https://github.com/seahyc/switchboard-oss.git
cd switchboard-oss
./scripts/install-standalone.sh
```

In a terminal, start the empty daemon:

```sh
~/.local/bin/switchboardd \
  -db "$HOME/.switchboard/switchboard.db" \
  -api-token-file "$HOME/.switchboard/api.token" \
  -qr-dir "$HOME/.switchboard"
```

The daemon creates a private API token and binds to `127.0.0.1:8717` by default. In another terminal, list chats:

```sh
export SWITCHBOARD_TOKEN_FILE="$HOME/.switchboard/api.token"
~/.local/bin/switchboard list-chats
```

An empty installation returns an empty list. To connect an account, follow the [self-hosting guide](docs/SELF_HOSTING.md) and [connector guide](docs/CONNECTORS.md). For published binary archives, use the [GitHub Releases page](https://github.com/seahyc/switchboard-oss/releases) and verify the adjacent SHA-256 file before extracting.

## Connectors and data

Connectors are disabled unless configured. WhatsApp is the shortest first setup: enable its local session database, scan the generated QR image from your phone's Linked Devices screen, then check connection status and list chats with the CLI. Slack capture is an experimental, read-only poller that requires a private user token and an explicit channel ID allowlist. Other connectors and their dependencies are described in [Connectors](docs/CONNECTORS.md).

Messages and credentials stay on the host running Switchboard. Local capture does not mirror platform deletions into every stored copy; read the [privacy and purge guide](docs/PRIVACY.md) before connecting an account. Keep the API on loopback and protect `~/.switchboard`.

## Development and licenses

Run `make test` and `make vet` before proposing code changes. The project source is MIT-licensed; release binaries also include a GPL-3.0 dependency. See [third-party notices](THIRD_PARTY_NOTICES.md) and [release requirements](docs/RELEASING.md).
