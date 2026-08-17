#!/usr/bin/env python3
"""Run h2spec against the extension's HTTP/2 server (RFC 0024, h2
acceptance criteria).

Starts ``hgraph_web_h2spec_server``, waits for its listening line, runs
``h2spec http2 -t -k``, and passes iff every failure is in the accepted
deviation list below.

Accepted deviations
-------------------

* ``5.1.1/2`` (stream identifier numerically smaller than previous):
  nghttp2 treats the lower-numbered HEADERS as a frame on an implicitly
  closed stream and ignores it instead of raising the connection error
  h2spec expects.  The reference ``nghttpd`` server fails this exact case
  the same way (verified 2026-08-17 against nghttp2 1.67 / h2spec 2.6.0),
  so this is upstream leniency, not an integration defect.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path

ACCEPTED_DEVIATIONS = ("stream identifier that is numerically smaller",)
PORT = 18443


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: run_h2spec.py <h2spec-server-binary> <h2spec-binary>")
        return 2
    # Both commands must be existing executables, resolved to absolute
    # paths, before anything is spawned.
    server_binary = str(Path(sys.argv[1]).resolve())
    h2spec_binary = str(Path(sys.argv[2]).resolve())
    for binary in (server_binary, h2spec_binary):
        if not (Path(binary).is_file() and os.access(binary, os.X_OK)):
            print(f"not an executable: {binary}")
            return 2

    server = subprocess.Popen(
        [server_binary, str(PORT), "600"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.monotonic() + 30
        listening = False
        assert server.stdout is not None
        while time.monotonic() < deadline:
            line = server.stdout.readline()
            if "listening" in line:
                listening = True
                break
            if server.poll() is not None:
                break
        if not listening:
            print("the h2spec server did not start")
            return 1

        result = subprocess.run(
            [h2spec_binary, "http2", "-t", "-k", "-h", "127.0.0.1", "-p",
             str(PORT)],
            capture_output=True,
            text=True,
            timeout=600,
        )
        print(result.stdout)
        summary = re.search(
            r"(\d+) tests, (\d+) passed, (\d+) skipped, (\d+) failed",
            result.stdout,
        )
        if summary is None:
            print("h2spec produced no summary")
            return 1
        failures = [
            line
            for line in result.stdout.splitlines()
            if line.strip().startswith("×")
        ]
        unexpected = [
            line
            for line in set(failures)
            if not any(accepted in line for accepted in ACCEPTED_DEVIATIONS)
        ]
        if unexpected:
            print("unexpected h2spec failures:")
            for line in unexpected:
                print(f"  {line.strip()}")
            return 1
        print(
            f"h2spec conformance OK: {summary.group(2)}/{summary.group(1)} "
            f"passed, {summary.group(4)} failure(s) all in the accepted "
            "deviation list"
        )
        return 0
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()


if __name__ == "__main__":
    raise SystemExit(main())
