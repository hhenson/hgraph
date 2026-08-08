"""Minimal pytest result recorder for the upstream conformance runner.

The plugin is loaded into isolated reference and candidate environments.  It
records stable pytest node ids and phase outcomes without requiring an extra
reporting dependency in either environment.
"""

from __future__ import annotations

import json
import platform
from pathlib import Path
from typing import Any

import pytest

_STATE: dict[str, Any] = {}


def pytest_addoption(parser) -> None:
    group = parser.getgroup("hgraph-conformance")
    group.addoption(
        "--hgraph-conformance-report",
        action="store",
        help="write the upstream conformance result as JSON",
    )


def pytest_configure(config) -> None:
    global _STATE
    _STATE = {
        "aliases": {},
        "collected": [],
        "collection_errors": [],
        "phases": {},
    }


def _diagnostic(report) -> str:
    longrepr = getattr(report, "longrepr", None)
    return "" if longrepr is None else str(longrepr)


def pytest_collection_finish(session) -> None:
    case_indexes: dict[str, int] = {}
    collected: list[str] = []
    for item in session.items:
        original_name = getattr(item, "originalname", None)
        if original_name:
            definition = f"{item.parent.nodeid}::{original_name}"
        else:
            definition = item.nodeid.split("[", 1)[0]
        if hasattr(item, "callspec"):
            case_index = case_indexes.get(definition, 0)
            case_indexes[definition] = case_index + 1
            stable_nodeid = f"{definition}[case-{case_index}]"
        else:
            stable_nodeid = definition
        _STATE["aliases"][item.nodeid] = stable_nodeid
        collected.append(stable_nodeid)
    _STATE["collected"] = collected


def pytest_collectreport(report) -> None:
    if not report.failed:
        return
    _STATE["collection_errors"].append(
        {
            "nodeid": report.nodeid,
            "outcome": "collection-error",
            "diagnostic": _diagnostic(report),
        }
    )


def pytest_runtest_logreport(report) -> None:
    nodeid = _STATE["aliases"].get(report.nodeid, report.nodeid)
    _STATE["phases"].setdefault(nodeid, {})[report.when] = {
        "outcome": report.outcome,
        "diagnostic": _diagnostic(report),
        "duration": getattr(report, "duration", 0.0),
        "pytest_nodeid": report.nodeid,
        "wasxfail": str(getattr(report, "wasxfail", "")),
    }


def _test_outcome(phases: dict[str, dict[str, Any]]) -> dict[str, Any]:
    for phase in ("setup", "call", "teardown"):
        result = phases.get(phase)
        if result and result["outcome"] == "failed":
            return {
                "outcome": "failed" if phase == "call" else "error",
                "phase": phase,
                "diagnostic": result["diagnostic"],
                "duration": sum(item["duration"] for item in phases.values()),
                "pytest_nodeid": result["pytest_nodeid"],
            }
    for phase in ("setup", "call", "teardown"):
        result = phases.get(phase)
        if result and result["outcome"] == "skipped":
            return {
                "outcome": "xfailed" if result["wasxfail"] else "skipped",
                "phase": phase,
                "diagnostic": result["diagnostic"],
                "duration": sum(item["duration"] for item in phases.values()),
                "pytest_nodeid": result["pytest_nodeid"],
            }
    call = phases.get("call")
    if call and call["outcome"] == "passed":
        return {
            "outcome": "xpassed" if call["wasxfail"] else "passed",
            "phase": "call",
            "diagnostic": "",
            "duration": sum(item["duration"] for item in phases.values()),
            "pytest_nodeid": call["pytest_nodeid"],
        }
    return {
        "outcome": "not-run",
        "phase": "unknown",
        "diagnostic": "",
        "duration": sum(item["duration"] for item in phases.values()),
        "pytest_nodeid": next((item["pytest_nodeid"] for item in phases.values()), ""),
    }


def pytest_sessionfinish(session, exitstatus) -> None:
    output = session.config.getoption("--hgraph-conformance-report")
    if not output:
        return
    tests = {
        nodeid: _test_outcome(phases)
        for nodeid, phases in sorted(_STATE["phases"].items())
    }
    payload = {
        "schema_version": 1,
        "pytest_version": pytest.__version__,
        "python_version": platform.python_version(),
        "exit_status": int(exitstatus),
        "collected": _STATE["collected"],
        "tests": tests,
        "collection_errors": _STATE["collection_errors"],
    }
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
