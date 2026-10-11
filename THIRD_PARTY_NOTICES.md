# Third-party software

Switchboard's own source is licensed under MIT. Dependencies retain their own
licenses; MIT does not replace those terms.

Release archives include `third-party-licenses/manifest.json` with the modules
used by the binaries and their pinned versions, upstream source links, and a
copy of each dependency's license and notice files. Generate this bundle with:

```sh
CGO_ENABLED=0 python3 scripts/package-licenses.py --out dist/third-party-licenses
```

The generator fails if a runtime module has no recognized license file. Review
the bundle before distributing binaries. `go.mod` and `go.sum` pin dependencies;
the bundle records the dependencies compiled for the selected target.

The optional mautrix-meta bridges and Matrix homeserver run separately and have
their own distributions and license terms. They are not bundled in Switchboard's
binary archives. Operators must review their upstream notices before packaging
or redistributing those services.

The WhatsApp connector links `go.mau.fi/libsignal`, licensed under GPLv3.
The combined binaries must be redistributed under GPLv3-compatible terms,
with corresponding source. Release archives include a separate source bundle
with vendored build dependencies and build instructions. Switchboard source
files retain their MIT license. MPL-licensed dependencies also retain their
source and notice requirements.
