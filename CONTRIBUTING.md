# Contributing

Keep changes focused and explain user-visible behavior, data handling, and connector limitations. Do not include real messages, access tokens, session files, personal paths, or production state in commits, tests, screenshots, or logs. The source build requires Go 1.25.8 or newer.

Before opening a change, run `make test` and `make vet`.

For connector changes, describe the permissions and account setup required, whether the connector reads or sends, its polling/sync limits, and how stored data is removed. Do not claim a live connector flow works without completing and recording an account-level end-to-end check. See [Privacy](docs/PRIVACY.md) and [Releasing](docs/RELEASING.md).
