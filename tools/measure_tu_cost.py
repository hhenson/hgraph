#!/usr/bin/env python3
"""Measure peak compiler memory and wall time per translation unit.

Replays the compile commands of a configured *and built* tree (the private
precompiled headers must exist) with the object written to a scratch file, and
reports each translation unit's peak resident set size as the child process
saw it. This is the measurement behind the registration translation-unit
budget in ``docs/source/developer_guide/operators.rst`` ("Registration
translation units") and the recipe in ``build_system.rst``.

Example::

    cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \\
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
    cmake --build build
    python tools/measure_tu_cost.py build --filter lib/std/operators \\
        --budget 1024 --exempt data_frame_impl.cpp

Exit status is 1 when ``--budget`` is given and any measured unit exceeds it,
except units matched by ``--exempt`` (the documented exceptions), which are
reported but do not fail the run.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import shlex
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def _measure(entry: dict, scratch: Path) -> tuple[str, float, float, int]:
    """Compile one entry; return (file, peak MiB, seconds, return code)."""
    if "arguments" in entry:
        argv = list(entry["arguments"])
    else:
        argv = shlex.split(entry["command"])
    obj = scratch / (Path(entry["file"]).name + f".{os.getpid()}.{id(entry)}.o")
    if "-o" in argv:
        argv[argv.index("-o") + 1] = str(obj)
    else:
        argv += ["-o", str(obj)]
    helper = (
        "import resource, subprocess, sys\n"
        "r = subprocess.run(sys.argv[1:])\n"
        "print(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)\n"
        "sys.exit(r.returncode)\n"
    )
    started = time.monotonic()
    completed = subprocess.run(
        [sys.executable, "-c", helper, *argv],
        cwd=entry.get("directory"),
        capture_output=True,
        text=True,
    )
    seconds = time.monotonic() - started
    obj.unlink(missing_ok=True)
    if completed.returncode != 0:
        sys.stderr.write(completed.stderr[-4000:])
        return entry["file"], 0.0, seconds, completed.returncode
    kib = int(completed.stdout.strip().splitlines()[-1])
    # ru_maxrss is KiB on Linux and bytes on macOS.
    mib = kib / 1024 if sys.platform != "darwin" else kib / (1024 * 1024)
    return entry["file"], mib, seconds, 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("build_dir", type=Path, help="configured build tree")
    parser.add_argument(
        "--filter",
        default="",
        help="only measure sources whose path contains this substring",
    )
    parser.add_argument(
        "--budget",
        type=float,
        default=None,
        help="fail when any unit's peak memory exceeds this many MiB",
    )
    parser.add_argument(
        "--exempt",
        action="append",
        default=[],
        metavar="SUBSTRING",
        help="report but do not fail units whose path contains this substring",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="parallel compilations (peak figures are per process either way)",
    )
    args = parser.parse_args()

    commands_path = args.build_dir / "compile_commands.json"
    if not commands_path.exists():
        sys.stderr.write(
            f"{commands_path} not found; configure with "
            "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON\n"
        )
        return 2
    entries = [
        entry
        for entry in json.loads(commands_path.read_text())
        if args.filter in entry["file"]
    ]
    if not entries:
        sys.stderr.write("no translation units matched\n")
        return 2

    results = []
    with tempfile.TemporaryDirectory(prefix="hgraph-tu-") as scratch_dir:
        scratch = Path(scratch_dir)
        with ThreadPoolExecutor(max_workers=args.jobs) as pool:
            for file, mib, seconds, code in pool.map(
                lambda entry: _measure(entry, scratch), entries
            ):
                name = os.path.relpath(file)
                if code != 0:
                    print(f"FAILED ({code}) {name}", flush=True)
                    continue
                print(f"{mib:8.0f} MiB {seconds:7.1f} s  {name}", flush=True)
                results.append((mib, seconds, name))

    print("---- sorted by peak memory")
    over_budget = []
    for mib, seconds, name in sorted(results, reverse=True):
        marker = ""
        if args.budget is not None and mib > args.budget:
            if any(exempt in name for exempt in args.exempt):
                marker = "  OVER BUDGET (exempt)"
            else:
                marker = "  OVER BUDGET"
                over_budget.append(name)
        print(f"{mib:8.0f} MiB {seconds:7.1f} s  {name}{marker}")
    if len(results) != len(entries):
        return 1
    return 1 if over_budget else 0


if __name__ == "__main__":
    sys.exit(main())
