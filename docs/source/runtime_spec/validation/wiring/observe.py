"""Run every wiring case on each runtime and record the observations.

    python observe.py --reference <python> --candidate <python> [--repeats 3]

Each case runs in a fresh process per repeat. The repeats must agree; a case
whose repeats differ is recorded as unstable rather than resolved. Writes
``observed.json`` beside this file. It never reads ``reasoned.json``.
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from cases import CASES  # noqa: E402  (names only; the cases run in subprocesses)


def _identity(python: str) -> dict:
    probe = (
        "import json, platform, sys\n"
        "from importlib import metadata\n"
        "import hgraph\n"
        "try:\n    version = metadata.version('hgraph')\nexcept Exception:\n    version = 'unknown'\n"
        "print(json.dumps({'python': platform.python_version(), 'hgraph': version,"
        " 'native': hasattr(__import__('importlib').import_module('hgraph'), '_hgraph')"
        " or 'hgraph._hgraph' in sys.modules or '_hgraph' in sys.modules}))\n"
    )
    completed = subprocess.run([python, "-c", probe], capture_output=True, text=True, check=True)
    return json.loads(completed.stdout.strip().splitlines()[-1])


def _run(python: str, case: str) -> dict:
    completed = subprocess.run(
        [python, str(HERE / "cases.py"), case], capture_output=True, text=True, cwd=HERE
    )
    lines = [line for line in completed.stdout.splitlines() if line.startswith("{")]
    if completed.returncode != 0 or not lines:
        return {"harness_error": f"exit {completed.returncode}: {completed.stderr.strip()[-400:]}"}
    return json.loads(lines[-1])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()

    runtimes = {"python": args.reference, "cpp": args.candidate}
    result = {
        "platform": f"{platform.system()} {platform.machine()}",
        "runtimes": {name: _identity(python) for name, python in runtimes.items()},
        "candidate_revision": args.candidate_revision,
        "repeats": args.repeats,
        "cases": {},
    }
    for case in CASES:
        observed = {}
        for name, python in runtimes.items():
            runs = [_run(python, case) for _ in range(args.repeats)]
            stable = all(run == runs[0] for run in runs)
            observed[name] = runs[0] if stable else {"unstable": runs}
        result["cases"][case] = observed
        print(case, json.dumps(observed, sort_keys=True))
    (HERE / "observed.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
