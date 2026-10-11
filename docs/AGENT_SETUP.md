# Agent setup

This guide connects a local agent to a running Switchboard daemon. The CLI is a
local HTTP client. Keep the daemon on loopback and keep its administrator token
in a private file.

## Configure the CLI

The standalone installer puts `switchboard` under `~/.local/bin`. Add that
directory to the agent's environment `PATH`, or use the full path in commands.
Set the credential file and, if needed, the daemon address:

```sh
export PATH="$HOME/.local/bin:$PATH"
export SWITCHBOARD_TOKEN_FILE="$HOME/.switchboard/api.token"
# Optional. Use host:port with no URL scheme.
export SWITCHBOARD_ADDR="127.0.0.1:8717"
```

The token file is created by `switchboardd` when it starts if it does not
exist. Restrict it to the owner with mode `600`. Never put the token value in
an agent config, command line, or example. If the daemon listens on a different
address, `SWITCHBOARD_ADDR` must contain only `host:port`, such as
`127.0.0.1:8718`.

Use these read commands to discover and inspect messages:

```sh
switchboard help
switchboard list-chats
switchboard list-chats --platform whatsapp --limit 20
switchboard search --platform whatsapp --limit 10 "project name"
switchboard read --limit 50 --desc "whatsapp:CHAT_ID"
```

The updated CLI accepts options before or after positional arguments. With
v0.1.0, put options first as shown above. Use `--` before a search term that
starts with a dash.

## Retrieve an attachment or PDF

Start the daemon with a media directory, for example
`-media-root "$HOME/.switchboard/media"`. Without `-media-root`, attachment
downloads and local file retrieval are disabled. Automatic downloads are
importance-gated. Use `get-media` with the attachment ID shown in a message's
media metadata to ask the connector to retrieve it on demand:

```sh
switchboard get-media 'whatsapp:EVENT_ID#0'
```

The result includes metadata and a local path. This path is on the same machine
as the daemon. The agent must have access to that filesystem path and use a
local PDF reader or document extraction tool to inspect a PDF. Switchboard
searches message text and metadata; it does not index PDF contents. Cite the
source chat, message timestamp, and attachment name when reporting details from
a file.

Retrieval depends on the connector still being able to fetch the source file.
Platform history, file availability, the media size cap, and cleanup can limit
what remains available. A successful message search does not prove the
attachment bytes are still retrievable. Slack currently does not download file
attachments.

## Optional MCP connection

The release also includes `switchboard-mcp`, a local stdio MCP adapter. A
generic MCP client can launch it with this configuration shape, replacing the
absolute executable path with the installed path on that machine:

```json
{
  "mcpServers": {
    "switchboard": {
      "command": "/absolute/path/to/switchboard-mcp",
      "env": {
        "SWITCHBOARD_ADDR": "127.0.0.1:8717",
        "SWITCHBOARD_TOKEN_FILE": "/absolute/path/to/.switchboard/api.token"
      }
    }
  }
}
```

The MCP adapter reads `SWITCHBOARD_ADDR`, defaults to `127.0.0.1:8717`, and
uses the daemon API token from `SWITCHBOARD_TOKEN` or `SWITCHBOARD_TOKEN_FILE`.
Use the private token-file path in client configuration. Do not put token
values in a checked-in config. The adapter includes tools that can send
messages or perform other changes. Those tools are outbound-capable; use the
read-only CLI commands above when the agent only needs to search and retrieve
files. MCP `get_media` returns a local path, so the agent and daemon must share
access to that filesystem.
