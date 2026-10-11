# Privacy and data lifecycle

Switchboard stores raw platform payloads and derived records in local state. Raw payload storage is append-only in the current design. Platform-side deletion does not erase every copy already captured by Switchboard, backups, exports, or derived indexes.

Keep the state directory and backups access-controlled. Avoid exporting real message content into issues, logs, or test fixtures. Connector credentials and sessions are sensitive and should be revoked at their source when no longer needed.

Stop the daemon before running the offline local purge command. The database
writer lock prevents maintenance from opening a running daemon's database:

```sh
switchboard purge-chat --db ~/.switchboard/switchboard.db \
  --chat-id 'slack:T123:C123' --confirm 'slack:T123:C123' \
  --media-root ~/.switchboard/media
```

Use the actual database and media paths from your configuration. `--media-root`
is required if the chat has stored media. The command removes that chat's event
history, message/search rows, cursors, and media references. It deletes blobs
only when no other chat references them, compacts the database, and records a
capture block so subsequent history or live messages cannot reintroduce the
chat. Repeating the exact chat ID is required before any deletion.

This is logical erasure of Switchboard's local archive. Copies already exported
to gbrain, Honcho, another agent, a backup, or platform storage need separate
removal. It is not a guarantee of forensic erasure on the underlying filesystem.
Previously discarded edit/delete events cannot be recovered by this upgrade.
