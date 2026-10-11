#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source_root="$(cd "$script_dir/.." && pwd)"

usage() {
  cat <<'USAGE'
Build and install the standalone Switchboard binaries.

Usage: scripts/install-standalone.sh [--bin-dir DIR] [--prefix DIR] [--state-dir DIR] [--systemd] [--replace-service] [--help]

Options:
  --bin-dir DIR    Install the three prebuilt release binaries without Go
  --prefix DIR     Binary install prefix (default: ~/.local)
  --state-dir DIR  Private state directory (default: ~/.switchboard)
  --systemd        Install a Linux systemd user unit; does not enable or start it
  --replace-service Replace an existing user unit when used with --systemd
  --help           Show this help

Source builds require Go. This script does not use sudo, install dependencies, start the
daemon, or connect an account.
USAGE
}

prefix="${HOME}/.local"
state_dir="${HOME}/.switchboard"
install_systemd=0
replace_service=0
bin_dir=""

while (($#)); do
  case "$1" in
    --prefix|--state-dir|--bin-dir)
      if (($# < 2)) || [[ -z "$2" || "$2" == --* ]]; then
        echo "error: $1 requires a directory" >&2
        exit 2
      fi
      case "$1" in
        --prefix) prefix="$2" ;;
        --state-dir) state_dir="$2" ;;
        --bin-dir) bin_dir="$2" ;;
      esac
      shift 2
      ;;
    --systemd)
      install_systemd=1
      shift
      ;;
    --replace-service)
      replace_service=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$bin_dir" ]] && ! command -v go >/dev/null 2>&1; then
  echo "error: Go is required; install Go and rerun this script" >&2
  exit 1
fi

case "$OSTYPE" in
  darwin*|linux*) ;;
  *) echo "error: supported host systems are macOS and Linux" >&2; exit 1 ;;
esac

if ((install_systemd)) && [[ "$OSTYPE" != linux* ]]; then
  echo "error: --systemd is supported on Linux only" >&2
  exit 1
fi

if ((!install_systemd && replace_service)); then
  echo "error: --replace-service requires --systemd" >&2
  exit 2
fi

unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
unit_path="$unit_dir/switchboardd.service"
if ((install_systemd)) && [[ -L "$unit_path" ]]; then
  echo "error: refusing to replace a symlinked user unit: $unit_path" >&2
  exit 1
fi
if ((install_systemd)) && [[ -e "$unit_path" && $replace_service -eq 0 ]]; then
  echo "error: user unit already exists at $unit_path; use --replace-service to replace it" >&2
  exit 1
fi

if [[ -L "$state_dir" ]]; then
  echo "error: state directory must not be a symlink: $state_dir" >&2
  exit 1
fi
mkdir -p "$prefix/bin"
prefix="$(cd "$prefix" && pwd -P)"
state_was_created=0
if [[ ! -e "$state_dir" ]]; then
  mkdir -p "$state_dir"
  chmod 700 "$state_dir"
  state_was_created=1
elif [[ ! -d "$state_dir" ]]; then
  echo "error: state path exists and is not a directory: $state_dir" >&2
  exit 1
fi
state_dir="$(cd "$state_dir" && pwd -P)"
if [[ "$state_dir" == / || "$state_dir" == "$(cd "$HOME" && pwd -P)" ]]; then
  echo "error: state directory cannot be / or the home directory" >&2
  exit 1
fi
if ((state_was_created == 0)); then
  if stat -f '%Lp' "$state_dir" >/dev/null 2>&1; then
    state_mode="$(stat -f '%Lp' "$state_dir")"
  else
    state_mode="$(stat -c '%a' "$state_dir")"
  fi
  if (( (8#$state_mode & 077) != 0 )); then
    echo "error: existing state directory must have mode 700 or stricter: $state_dir" >&2
    exit 1
  fi
fi

if [[ -n "$bin_dir" ]]; then
  bin_dir="$(cd "$bin_dir" && pwd -P)"
  for name in switchboardd switchboard switchboard-mcp; do
    if [[ ! -f "$bin_dir/$name" || ! -x "$bin_dir/$name" ]]; then
      echo "error: missing executable release binary: $bin_dir/$name" >&2
      exit 1
    fi
  done
  "$bin_dir/switchboardd" -version >/dev/null
  build_dir="$bin_dir"
else
  build_dir="$(mktemp -d)"
  trap 'rm -rf "$build_dir"' EXIT
  build_version="${VERSION:-dev}"
  (
    cd "$source_root"
    for name in switchboardd switchboard switchboard-mcp; do
      echo "Building $name ($build_version)..."
      CGO_ENABLED=0 go build -ldflags "-X main.Version=$build_version" -o "$build_dir/$name" "./cmd/$name"
    done
  )
fi

for name in switchboardd switchboard switchboard-mcp; do
  install -m 755 "$build_dir/$name" "$prefix/bin/$name"
done

if ((install_systemd)); then
  mkdir -p "$unit_dir"
  systemd_quote() {
    local input="$1" output='"' char i
    for ((i=0; i<${#input}; i++)); do
      char="${input:i:1}"
      case "$char" in
        \\) output+='\\' ;;
        '"') output+='\"' ;;
        '%') output+='%%' ;;
        $'\n') output+='\x0a' ;;
        $'\r') output+='\x0d' ;;
        $'\t') output+='\x09' ;;
        *) output+="$char" ;;
      esac
    done
    printf '%s"' "$output"
  }
  sed_escape() { printf '%s' "$1" | sed 's/[\\&|]/\\&/g'; }
  exec_unit="$(systemd_quote "$prefix/bin/switchboardd")"
  db_unit="$(systemd_quote "$state_dir/switchboard.db")"
  state_unit="$(systemd_quote "$state_dir")"
  token_unit="$(systemd_quote "$state_dir/api.token")"
  sed \
    -e "s|@EXEC@|$(sed_escape "$exec_unit")|g" \
    -e "s|@DB@|$(sed_escape "$db_unit")|g" \
    -e "s|@STATE@|$(sed_escape "$state_unit")|g" \
    -e "s|@TOKEN@|$(sed_escape "$token_unit")|g" \
    "$script_dir/switchboardd-standalone.service" > "$unit_path"
  chmod 600 "$unit_path"
  echo "Installed user unit at $unit_path (not enabled or started)."
fi

echo "Installed binaries in $prefix/bin; private state directory: $state_dir"
echo "Review the daemon options before starting it."
