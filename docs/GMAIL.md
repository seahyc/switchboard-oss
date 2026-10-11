# Gmail credential file

The Gmail connector can read OAuth credentials from either the existing
`keyrings.alt` keyring or a JSON file supplied with `-gmail-credentials-file`.
When the JSON file is selected, also pass `-gmail-accounts` with the exact
mailbox addresses to enable. The connector rejects an empty allow-list and
ignores records for accounts outside it. Without `-gmail-credentials-file`,
the existing keyring configuration remains in use.

Create the OAuth credentials through a Google OAuth flow you have approved for
the account. The file must contain one JSON array of records. Keep refresh
tokens and client secrets private; create the file with mode `0600` and store it
in a directory readable only by the service account.

```json
[
  {
    "email": "mailbox@example.com",
    "refresh_token": "REDACTED_REFRESH_TOKEN",
    "client_id": "REDACTED_OAUTH_CLIENT_ID",
    "client_secret": "REDACTED_OAUTH_CLIENT_SECRET",
    "scopes": [
      "https://www.googleapis.com/auth/gmail.readonly"
    ]
  }
]
```

Each record needs a non-empty `email`, `refresh_token`, `client_id`,
`client_secret`, and at least one Gmail scope. `token_uri` is optional and
defaults to `https://oauth2.googleapis.com/token`. If supplied, it must be
`https://oauth2.googleapis.com/token` or
`https://accounts.google.com/o/oauth2/token`; other hosts and URL variations
are rejected. `expiry` is accepted for compatibility with Google credential
exports. The connector refreshes access tokens in memory and does not log token
or secret values.

For mailbox reading, request only
`https://www.googleapis.com/auth/gmail.readonly`. Limit `-gmail-accounts` to the
specific accounts whose access the user approved. Creating or importing this
file does not perform OAuth consent or contact Google's token endpoint; the
connector contacts the approved Google token endpoint when it starts using an
account.
