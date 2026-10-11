#!/usr/bin/env python3
"""Collect runtime Go module license notices into a fresh release folder."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from urllib.parse import quote


COMMANDS = ("./cmd/switchboardd", "./cmd/switchboard", "./cmd/switchboard-mcp")
LICENSE_PREFIXES = ("license", "copying", "notice", "copyright")
ROOT = Path(__file__).resolve().parent.parent


class PackageError(Exception):
    pass


def parse_go_list(stream: str) -> list[dict]:
    """Parse go list's concatenated JSON objects and return unique dependencies."""
    decoder = json.JSONDecoder()
    pos = 0
    records = {}
    while pos < len(stream):
        while pos < len(stream) and stream[pos].isspace():
            pos += 1
        if pos == len(stream):
            break
        try:
            item, pos = decoder.raw_decode(stream, pos)
        except json.JSONDecodeError as exc:
            raise PackageError("could not parse go list dependency output") from exc
        module = item.get("Module")
        if not isinstance(module, dict) or module.get("Main"):
            continue
        path = module.get("Path", "")
        version = module.get("Version", "")
        if not path or not version:
            continue
        replacement = module.get("Replace") or None
        key = (path, version, (replacement or {}).get("Path", ""), (replacement or {}).get("Version", ""))
        records[key] = module
    return [records[key] for key in sorted(records)]


def _effective_module(module: dict) -> dict:
    return module.get("Replace") or module


def _module_source_url(module: dict) -> str | None:
    effective = _effective_module(module)
    path, version = effective.get("Path", ""), effective.get("Version", "")
    if not path or not version:
        return None
    escaped_path = "/".join(_escape_module_case(part) for part in path.split("/"))
    escaped_version = _escape_module_case(version)
    return f"https://proxy.golang.org/{quote(escaped_path, safe='/!')}/@v/{quote(escaped_version, safe='!')}.zip"


def _escape_module_case(value: str) -> str:
    # Go module proxies encode uppercase ASCII as ! followed by lowercase.
    return "".join("!" + char.lower() if char.isupper() else char for char in value)


def _effective_dir(module: dict) -> Path | None:
    directory = _effective_module(module).get("Dir") or module.get("Dir")
    if not directory:
        return None
    return Path(directory).resolve()


def _license_files(module_dir: Path) -> list[Path]:
    found = []
    if not module_dir.is_dir():
        return found
    for root, dirs, filenames in os.walk(module_dir, followlinks=False):
        dirs[:] = sorted(d for d in dirs if d not in {".git", "vendor"})
        for filename in sorted(filenames):
            lower = filename.lower()
            if any(lower == prefix or lower.startswith(prefix + ".") or lower.startswith(prefix + "-") or lower.startswith(prefix + "_") for prefix in LICENSE_PREFIXES):
                path = Path(root, filename)
                if path.is_file() and not path.is_symlink():
                    found.append(path)
    return sorted(found, key=lambda p: p.relative_to(module_dir).as_posix().casefold())


def _module_slug(module: dict) -> str:
    path = module["Path"]
    version = module["Version"]
    # URL quoting ensures a dependency name cannot escape its package directory.
    return quote(path, safe="") + "@" + quote(version, safe="")


def _copy_module_notices(module: dict, staging: Path) -> dict:
    module_dir = _effective_dir(module)
    if module_dir is None:
        raise PackageError(f"{module.get('Path', '<unknown>')}: resolved source directory is unavailable")
    notices = _license_files(module_dir)
    if not notices:
        raise PackageError(f"{module.get('Path', '<unknown>')}: no recognized license or notice file")
    module_output = staging / "licenses" / _module_slug(module)
    output_files = []
    for source in notices:
        relative = source.relative_to(module_dir)
        destination = module_output / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        digest = hashlib.sha256(destination.read_bytes()).hexdigest()
        output_files.append({"path": destination.relative_to(staging).as_posix(), "sha256": digest})
    replacement = module.get("Replace")
    return {
        "path": module["Path"],
        "version": module["Version"],
        "sum": module.get("Sum") or None,
        "go_mod_sum": module.get("GoModSum") or None,
        "source_url": _module_source_url(module),
        "requested_source_url": _module_source_url({"Path": module["Path"], "Version": module["Version"]}),
        "replacement": None if not replacement else {
            "path": replacement.get("Path"),
            "version": replacement.get("Version") or None,
            "source_url": _module_source_url({"Path": replacement.get("Path", ""), "Version": replacement.get("Version", "")}),
        },
        "license_files": output_files,
    }


def _readme() -> str:
    return """# Third-party Go module notices\n\nThis folder contains the upstream license, copying, notice, and copyright files\nfound in the runtime Go modules used by the packaged Switchboard commands. Each\nfile is preserved at its module-relative path under `licenses/`. `manifest.json`\nrecords module versions, available Go module checksums, source URLs, and SHA-256\nhashes for copied notice files.\n\nWhere a module publishes a versioned source archive, `source_url` points to the\nGo module proxy archive. A local replacement has no remote source URL; the\nmanifest retains its requested module URL when available and records the\nreplacement path.\n\nThese files are reproduced as supplied by upstream. This notice collection is\nnot legal advice and does not interpret license obligations.\n"""


def generate(out_dir: Path, modules: list[dict]) -> None:
    out_dir = out_dir.absolute()
    if os.path.lexists(out_dir):
        raise PackageError(f"output path already exists: {out_dir}")
    out_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{out_dir.name}.staging-", dir=out_dir.parent))
    try:
        unique = {}
        for module in modules:
            replacement = module.get("Replace") or {}
            key = (module.get("Path", ""), module.get("Version", ""), replacement.get("Path", ""), replacement.get("Version", ""))
            unique[key] = module
        modules = [unique[key] for key in sorted(unique)]
        manifest_modules = []
        missing = []
        for module in modules:
            try:
                manifest_modules.append(_copy_module_notices(module, staging))
            except PackageError as exc:
                missing.append((module.get("Path", "<unknown>"), str(exc)))
        if missing:
            # Keep diagnostics limited to module identity; never echo file contents.
            names = ", ".join(sorted({name for name, _ in missing}))
            raise PackageError(f"missing license/source for module(s): {names}")
        manifest_modules.sort(key=lambda m: (m["path"], m["version"]))
        (staging / "README.md").write_text(_readme(), encoding="utf-8")
        (staging / "manifest.json").write_text(
            json.dumps({"modules": manifest_modules}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.rename(staging, out_dir)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def run_go_list(root: Path = ROOT) -> list[dict]:
    env = os.environ.copy()
    env["CGO_ENABLED"] = "0"
    command = ["go", "list", "-deps", "-json", *COMMANDS]
    try:
        result = subprocess.run(command, cwd=root, env=env, text=True, capture_output=True, check=False)
    except OSError as exc:
        raise PackageError("could not execute go list; ensure Go is installed") from exc
    if result.returncode:
        raise PackageError("go list failed; ensure runtime dependencies are available")
    modules = parse_go_list(result.stdout)
    if not modules:
        raise PackageError("go list returned no external runtime modules")
    return modules


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path, help="new output directory for release notices")
    args = parser.parse_args(argv)
    try:
        modules = run_go_list()
        generate(args.out, modules)
    except PackageError as exc:
        print(f"package-licenses: {exc}", file=sys.stderr)
        return 1
    except OSError:
        print("package-licenses: could not write output folder", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
