from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from hgraph_unit_tests.ts_tests._runtime_trace_harness import SCENARIOS

TRACE_PREFIX = "TRACE_JSON::"
KNOWN_PARITY_MISMATCHES = {
    "map_input_goes_away": "Trailing teardown tick mismatch in `modified` trace (Python includes final False tick).",
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _run_trace_subprocess(scenario: str, use_cpp: bool) -> dict:
    env = os.environ.copy()
    env["HGRAPH_USE_CPP"] = "1" if use_cpp else "0"
    env["HGRAPH_TRACE_STATES"] = "0"

    script = (
        "import json, sys\n"
        "from hgraph_unit_tests.ts_tests._runtime_trace_harness import run_trace_scenario\n"
        "trace = run_trace_scenario(sys.argv[1])\n"
        f"print('{TRACE_PREFIX}' + json.dumps(trace, sort_keys=True))\n"
    )

    proc = subprocess.run(
        [sys.executable, "-c", script, scenario],
        cwd=_repo_root(),
        env=env,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        pytest.fail(
            f"Scenario '{scenario}' failed in mode HGRAPH_USE_CPP={env['HGRAPH_USE_CPP']}.\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )

    payload = None
    for line in proc.stdout.splitlines():
        if line.startswith(TRACE_PREFIX):
            payload = line[len(TRACE_PREFIX):]
    if payload is None:
        pytest.fail(
            f"Scenario '{scenario}' produced no trace payload in mode HGRAPH_USE_CPP={env['HGRAPH_USE_CPP']}.\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return json.loads(payload)


def _first_diff(lhs: Any, rhs: Any, path: str = "root") -> str | None:
    if type(lhs) is not type(rhs):
        return f"{path}: type {type(lhs).__name__} != {type(rhs).__name__}"
    if isinstance(lhs, dict):
        lhs_keys = set(lhs.keys())
        rhs_keys = set(rhs.keys())
        if lhs_keys != rhs_keys:
            return f"{path}: keys {sorted(lhs_keys)} != {sorted(rhs_keys)}"
        for key in sorted(lhs.keys()):
            diff = _first_diff(lhs[key], rhs[key], f"{path}.{key}")
            if diff:
                return diff
        return None
    if isinstance(lhs, list):
        if len(lhs) != len(rhs):
            return f"{path}: len {len(lhs)} != {len(rhs)}"
        for i, (lv, rv) in enumerate(zip(lhs, rhs)):
            diff = _first_diff(lv, rv, f"{path}[{i}]")
            if diff:
                return diff
        return None
    if lhs != rhs:
        return f"{path}: {lhs!r} != {rhs!r}"
    return None


def test_known_mismatch_scenarios_are_registered():
    unknown = set(KNOWN_PARITY_MISMATCHES).difference(SCENARIOS)
    assert not unknown, f"Unknown known-mismatch scenarios: {sorted(unknown)}"


@pytest.mark.parametrize("scenario", sorted(SCENARIOS.keys()))
def test_trace_parity_between_python_and_cpp_modes(scenario: str):
    cpp_trace = _run_trace_subprocess(scenario, use_cpp=True)
    py_trace = _run_trace_subprocess(scenario, use_cpp=False)
    if scenario in KNOWN_PARITY_MISMATCHES:
        if cpp_trace == py_trace:
            pytest.fail(
                f"Known mismatch scenario '{scenario}' now matches across runtimes; "
                "remove it from KNOWN_PARITY_MISMATCHES."
            )
        diff = _first_diff(cpp_trace, py_trace) or "traces differ (no structured diff found)"
        pytest.xfail(f"{KNOWN_PARITY_MISMATCHES[scenario]} First diff: {diff}")
    assert cpp_trace == py_trace
