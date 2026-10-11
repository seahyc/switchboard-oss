# Security

Switchboard handles private message content and account credentials. Keep the API bound to loopback, protect the state directory and token files, and do not expose the daemon directly to an untrusted network. Review connector permissions and platform terms before use.

Please report suspected vulnerabilities privately through the repository's Security tab when private vulnerability reporting is available. Otherwise contact the maintainers privately; do not file a public issue with exploit details. Include the affected version, impact, and a reproducible description. Do not include real message contents, credentials, or session exports in a report.

Security fixes and releases are subject to the publication gates in [RELEASING.md](docs/RELEASING.md). A passing CI run alone does not establish release readiness.
