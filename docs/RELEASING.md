# Releasing

Release artifacts are built from a version tag and published as a draft for maintainer review. Manual workflow runs build artifacts without publishing. No release workflow deploys a service.

Before making a release public:

1. Audit the exact source tree and every archive for credentials, personal information, local paths, generated files, and deployment material. Publish a clean source snapshot with no private development history.
2. Review the project's MIT license and all third-party notices. The `switchboardd` binary includes `go.mau.fi/libsignal` v0.2.2 under GPL-3.0 terms.
3. Publish corresponding buildable source alongside any binary that contains GPL-licensed code. The source archive must include vendored dependency source, build instructions, dependency notices, and license files.
4. Run CI, review the complete release diff, and verify the installer, checksums, credential behavior, localhost binding, and empty-daemon CLI flow.
5. Run live account-level end-to-end checks for each connector named in release claims, using test accounts and the permissions described in [Connectors](CONNECTORS.md).

If corresponding-source packaging or a live connector gate is incomplete, publish a source-only first release and omit affected binary claims. Do not treat a green CI run or successful build as release approval. A draft GitHub release is not a public general-availability announcement; maintainers must review its contents and the gates above first.

The release workflow builds Linux and macOS archives for amd64 and arm64, with matching SHA-256 files and a corresponding-source archive. Confirm every target archive contains its binaries, installer, notices, and required license material before publication.
