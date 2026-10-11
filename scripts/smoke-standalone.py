#!/usr/bin/env python3
"""Exercise release binaries against fresh, private local state."""

import argparse
import json
import os
from pathlib import Path
import signal
import socket
import stat
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", required=True, type=Path)
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    binaries = args.bin_dir.resolve()
    daemon = binaries / "switchboardd"
    assert subprocess.check_output([daemon, "-version"], text=True).strip() == args.version
    subprocess.run([binaries / "switchboard-mcp", "-help"], check=True, capture_output=True)

    with tempfile.TemporaryDirectory(prefix="switchboard smoke ") as scratch:
        state = Path(scratch)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        address = f"127.0.0.1:{port}"
        token_file = state / "api.token"
        reader_file = state / "reader.token"
        command = [daemon, "-db", state / "archive.db", "-api-token-file", token_file,
                   "-read-token-file", reader_file, "-qr-dir", state, "-addr", address]
        with (state / "daemon.log").open("w") as log:
            process = subprocess.Popen(command, stdout=log, stderr=log)
            try:
                def request(path, token=None, body=None, headers=None):
                    values = dict(headers or {})
                    if token:
                        values["Authorization"] = "Bearer " + token
                    data = None if body is None else json.dumps(body).encode()
                    values["Content-Type"] = "application/json"
                    req = urllib.request.Request(f"http://{address}{path}", data=data, headers=values)
                    try:
                        with urllib.request.urlopen(req, timeout=2) as response:
                            return response.status, response.read()
                    except urllib.error.HTTPError as error:
                        return error.code, error.read()

                deadline = time.monotonic() + 30
                while True:
                    assert process.poll() is None, "daemon exited before becoming healthy"
                    try:
                        status, health = request("/healthz")
                        assert status == 200 and json.loads(health)["version"] == args.version
                        break
                    except urllib.error.URLError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("daemon did not become healthy")
                        time.sleep(0.1)

                admin = token_file.read_text().strip()
                reader = reader_file.read_text().strip()
                assert request("/chats")[0] == 401
                assert request("/chats", "incorrect")[0] == 401
                assert request("/chats", admin)[0] == 200
                assert request("/chats", reader)[0] == 200
                assert request("/backfill", reader, {"chat_id": "all"})[0] == 403
                assert request("/send", admin, {"chat_id": "test", "text": "blocked"})[0] == 403
                assert request("/chats", admin, headers={"Origin": "https://example.com"})[0] == 403
                assert request("/healthz", headers={"Host": "example.com"})[0] == 403
                env = dict(os.environ, SWITCHBOARD_TOKEN_FILE=str(token_file))
                env.pop("SWITCHBOARD_TOKEN", None)
                result = subprocess.check_output([binaries / "switchboard", "list-chats", "--addr", address], env=env)
                assert json.loads(result) in (None, [])
                for path in (token_file, reader_file, state / "archive.db"):
                    assert stat.S_IMODE(path.stat().st_mode) == 0o600, f"unsafe mode: {path.name}"
                print("PASS: native binaries, health/version, admin/reader authentication, outbound and origin guards, CLI, private state")
            finally:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                assert process.returncode == 0, "daemon did not shut down cleanly"


if __name__ == "__main__":
    main()
