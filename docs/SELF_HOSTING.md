# Self-hosting

Switchboard runs on macOS or Linux. A source install requires Go 1.25.8 or newer. No database server, model service, gbrain, Honcho, or paid inference API is required for an empty archive or basic message capture.

## Install

From source:

```sh
git clone https://github.com/seahyc/switchboard-oss.git
cd switchboard-oss
./scripts/install-standalone.sh
```

The installer builds `switchboardd`, `switchboard`, and `switchboard-mcp` with CGO disabled. It installs binaries under `~/.local/bin` and creates the private state directory `~/.switchboard` with owner-only permissions. It does not start the daemon or connect an account.

Choose `darwin-arm64` for Apple Silicon, `darwin-amd64` for Intel Macs, `linux-amd64` for most Intel/AMD Linux hosts, or `linux-arm64` for ARM Linux. Download the matching release archive and its `.sha256` file from [GitHub Releases](https://github.com/seahyc/switchboard-oss/releases). Verify the checksum before extracting:

```sh
# macOS
shasum -a 256 -c switchboard-VERSION-OS-ARCH.tar.gz.sha256

# Linux
sha256sum -c switchboard-VERSION-OS-ARCH.tar.gz.sha256
```

Extract into a new directory, then install the prebuilt binaries without Go:

```sh
mkdir switchboard-release
tar -xzf switchboard-VERSION-OS-ARCH.tar.gz -C switchboard-release
cd switchboard-release
```

```sh
./scripts/install-standalone.sh --bin-dir .
```

Keep the bundled license files and third-party notices with the binaries. On Linux, you can also install a systemd user unit with `--systemd`; the installer does not enable or start it. It refuses to replace an existing unit unless you explicitly add `--replace-service`.

## Start an empty daemon

Run this in a terminal and leave it running:

```sh
~/.local/bin/switchboardd \
  -db "$HOME/.switchboard/switchboard.db" \
  -api-token-file "$HOME/.switchboard/api.token" \
  -qr-dir "$HOME/.switchboard"
```

The API defaults to `127.0.0.1:8717`; keep it on loopback. The daemon creates a random administrator token at `~/.switchboard/api.token` if the file is missing. Treat that token like a password. In another terminal, use the CLI:

```sh
export SWITCHBOARD_TOKEN_FILE="$HOME/.switchboard/api.token"
~/.local/bin/switchboard list-chats
```

The default daemon has no connectors registered, so `list-chats` returns an empty list. The CLI also reads the default token file automatically, but the explicit environment setting makes the credential source clear.

Outbound actions are disabled by default. `-allow-outbound` only enables daemon capability; callers still need to require per-action approval. Do not expose the API on a public interface.

## Connect WhatsApp

Stop the empty daemon with Ctrl-C. Restart it with a separate WhatsApp session database:

```sh
~/.local/bin/switchboardd \
  -db "$HOME/.switchboard/switchboard.db" \
  -api-token-file "$HOME/.switchboard/api.token" \
  -qr-dir "$HOME/.switchboard" \
  -whatsapp-db "$HOME/.switchboard/whatsapp.db"
```

The connector starts a QR login and writes the current code to `~/.switchboard/whatsapp-qr.png`; the image refreshes while login is pending. Open that local image in an image viewer, then on your phone open WhatsApp's **Linked devices** screen and scan it. Follow [WhatsApp's linked-device help](https://faq.whatsapp.com/1317564962315842/). The linked device appears as **Switchboard**. The pairing session is stored locally in `whatsapp.db` and reused after restart.

In the CLI terminal, check status and list chats:

```sh
~/.local/bin/switchboard auth-status whatsapp
~/.local/bin/switchboard list-chats --platform whatsapp
```

Messages become available as WhatsApp delivers live messages and any history-sync events. Do not assume every message or deletion is available in the linked-device history. You can disconnect Switchboard from the phone's Linked devices screen; local records remain until you use the documented [local purge process](PRIVACY.md).

## Other connectors

See [Connector setup and limitations](CONNECTORS.md) for Slack, Telegram, Lark, Gmail, Meta, Instagram, and X. Connectors require their own credentials or sessions and are disabled unless configured. Keep every token, session, and OAuth file out of shell history and source control. Read the [privacy guide](PRIVACY.md) before enabling capture.

## Run as a Linux user service

After installing with `--systemd`, inspect `~/.config/systemd/user/switchboardd.service`, then run:

```sh
systemctl --user daemon-reload
systemctl --user enable --now switchboardd.service
journalctl --user -u switchboardd.service -f
```

For WhatsApp or Slack, add the connector flags to the unit's `ExecStart` before starting it. Keep token values in private files.

## Update and back up

Stop the daemon before replacing binaries or copying its SQLite database and connector state. Protect backups as private message data. Install the next release, review its release notes, then restart. The installer preserves state.

If startup reports a port conflict, stop the existing process or choose another loopback port with `-addr 127.0.0.1:8718`. Point the CLI at it with `SWITCHBOARD_ADDR=127.0.0.1:8718`. Credential files must have mode `600`.
