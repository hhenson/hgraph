"""Diagnostic content-parity verification (issue #221).

The observer hook surface is C++-only by ruling; the remaining obligation is
that trace / profiler / wiring-trace produce the SAME INFORMATIONAL CONTENT
as upstream's (format may differ; information may not). This module runs one
canonical graph set under both engines with all three diagnostics enabled,
normalizes each engine's raw output into information sets, and diffs them.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

_PROBE = r"""
import sys

SENTINEL = "@@SECTION@@"

from hgraph import TS, TSD, compute_node, graph, map_
from hgraph.test import eval_node

try:
    from hgraph.test import EvaluationTrace
    if hasattr(EvaluationTrace, "set_use_logger"):
        EvaluationTrace.set_use_logger(False)
except Exception:
    pass


@compute_node
def add_one(v: TS[int]) -> TS[int]:
    return v.value + 1


@compute_node
def scale(v: TS[int], factor: int = 10) -> TS[int]:
    return v.value * factor


@graph
def chain(t: TS[int]) -> TS[int]:
    return scale(add_one(t))


@graph
def keyed(tsd: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
    return map_(add_one, tsd)


CHAIN_INPUT = [1, None, 3]
KEYED_INPUT = [{"a": 1}, {"b": 5}]

print(f"{SENTINEL} trace-chain", flush=True)
eval_node(chain, CHAIN_INPUT, __trace__=True)
print(f"{SENTINEL} trace-keyed", flush=True)
eval_node(keyed, KEYED_INPUT, __trace__=True)
print(f"{SENTINEL} wiring-chain", flush=True)
eval_node(chain, CHAIN_INPUT, __trace_wiring__=True)
print(f"{SENTINEL} profile-chain", flush=True)
try:
    from hgraph.test import EvaluationProfiler
    profiler = EvaluationProfiler()
    eval_node(chain, CHAIN_INPUT, __observers__=[profiler])
    if hasattr(profiler, "snapshot"):
        # Structured-snapshot engine: print entries so the normalizer sees
        # the same information the printing engine emits as lines.
        snap = profiler.snapshot()
        for entry in snap.entries:
            phase = entry.evaluation
            print(f"PROFILE-ENTRY {entry.label} count={phase.count} total={phase.total_time} ns", flush=True)
        print(f"PROFILE-SUMMARY cycles={snap.graph_cycles} wall={snap.wall_time} ns", flush=True)
except Exception as error:  # noqa: BLE001 - a missing profiler IS a finding
    print(f"PROFILER-UNAVAILABLE: {type(error).__name__}: {error}", flush=True)
print(f"{SENTINEL} end", flush=True)
"""


def run_probe(python: Path | str) -> dict[str, str]:
    result = subprocess.run(
        [str(python), "-c", _PROBE],
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        raise RuntimeError(f"diagnostics probe failed:\n{result.stderr[-2000:]}")
    sections: dict[str, str] = {}
    current = "preamble"
    for line in result.stdout.splitlines():
        if line.startswith("@@SECTION@@"):
            current = line.split(" ", 1)[1].strip()
            sections[current] = ""
        else:
            sections[current] = sections.get(current, "") + line + "\n"
    return sections


# ---- normalizers: engine-specific text -> engine-neutral information sets --

_TIME = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?"


def _is_harness(name: str) -> bool:
    return "__harness" in name or "replay" in name or "record" in name


def normalize_trace(text: str) -> dict[str, Any]:
    """Information set per the issue: events, node identities, times, values."""
    events: list[tuple[str, str]] = []
    out_values: list[str] = []
    in_events = out_events = 0
    node_starts: set[str] = set()
    node_stops: set[str] = set()
    graph_markers: set[str] = set()
    times: set[str] = set()

    for line in text.splitlines():
        for m in re.finditer(rf"\[({_TIME})\]", line):
            times.add(m.group(1))
        # node identity: last path-ish token before '(' on node lines
        def node_name() -> str | None:
            m = re.search(r"([A-Za-z_][\w.:]*)(?:<[-\d, ]+>)?\(", line)
            if not m:
                return None
            name = m.group(1).split(".")[-1]
            # candidate labels render as implementation:label (issue #247);
            # the LABEL is the identity information.
            return name.split(":")[-1]

        if "Starting Graph" in line:
            graph_markers.add("graph-start")
        elif "Started Graph" in line:
            graph_markers.add("graph-started")
        elif "Eval Start" in line:
            graph_markers.add("eval-start")
        elif "Eval Done" in line:
            graph_markers.add("eval-done")
        elif "stopping" in line or "Graph stopped" in line.replace("stopping", ""):
            graph_markers.add("graph-stop")
        elif "Started node" in line:
            n = node_name()
            if n and not _is_harness(n):
                node_starts.add(n)
        elif "Stopped node" in line:
            n = node_name()
            if n and not _is_harness(n):
                node_stops.add(n)
        elif "[IN]" in line:
            in_events += 1
        elif "[OUT]" in line:
            out_events += 1
            n = node_name()
            m = re.search(r"->\*?\s*(.+?)\s*\[OUT\]", line)
            if m and n and not _is_harness(n):
                value = m.group(1).strip()
                # representation-free: strip reference tokens, memory
                # addresses, and node-coordinate tuples before payload
                # extraction (upstream renders REF plumbing; we render
                # resolved values — the PAYLOAD is the information).
                value = re.sub(r"REF\[[^]]*\]", "", value)
                value = re.sub(r"<0x[0-9a-fA-F]+>", "", value)
                value = re.sub(r"<[-\d, ]+>", "", value)
                out_values.append(value)
    return {
        "graph_markers": sorted(graph_markers),
        "node_starts": sorted(node_starts),
        "node_stops": sorted(node_stops),
        "in_events": in_events,
        "out_events": out_events,
        "user_out_values": sorted(out_values),
        "numeric_out_payloads": sorted({n for v in out_values for n in re.findall(r"-?\d+(?:\.\d+)?", v)}),
        "distinct_times": len(times),
    }


def normalize_wiring(text: str) -> dict[str, Any]:
    kinds: set[str] = set()
    wired_nodes: set[str] = set()
    overloads = 0
    for line in text.splitlines():
        low = line.lower()
        if "wiring graph" in low:
            kinds.add("graph")
        if "nested graph" in low:
            kinds.add("nested-graph")
        if "wiring node" in low:
            kinds.add("node")
            m = re.search(r"[Ww]iring node ([A-Za-z_][\w]*)", line)
            if m and not _is_harness(m.group(1)):
                wired_nodes.add(m.group(1))
        if "overload" in low:
            overloads += 1
    return {
        "kinds": sorted(kinds),
        "user_wired_nodes": sorted(wired_nodes),
        "overload_resolutions_reported": overloads > 0,
    }


def normalize_profile(text: str) -> dict[str, Any]:
    if "PROFILER-UNAVAILABLE" in text:
        return {"available": False}
    timed_nodes: set[str] = set()
    has_timings = False
    for line in text.splitlines():
        if re.search(r"\d+(?:\.\d+)?\s*(?:us|µs|ms|s\b|seconds|nanos|ns)", line) or re.search(r"in\s+\d", line):
            has_timings = True
            m = re.search(r"([A-Za-z_][\w]*)(?:<\d+>)?\(", line)
            if m and not _is_harness(m.group(1)):
                timed_nodes.add(m.group(1))
    return {
        "available": True,
        "has_timings": has_timings,
        "timed_user_nodes": sorted(timed_nodes),
        "line_count": len([l for l in text.splitlines() if l.strip()]),
    }


def compare_diagnostics(reference: dict[str, str], candidate: dict[str, str]) -> dict[str, Any]:
    report: dict[str, Any] = {"schema_version": 1, "sections": {}, "gaps": []}

    def diff(section: str, norm, keys: list[str]):
        ref = norm(reference.get(section, ""))
        cand = norm(candidate.get(section, ""))
        entry = {"reference": ref, "candidate": cand, "gaps": []}
        for key in keys:
            if ref.get(key) != cand.get(key):
                entry["gaps"].append({"key": key, "reference": ref.get(key), "candidate": cand.get(key)})
        report["sections"][section] = entry
        for g in entry["gaps"]:
            report["gaps"].append({"section": section, **g})

    diff("trace-chain", normalize_trace,
         ["graph_markers", "node_starts", "node_stops", "numeric_out_payloads"])
    diff("trace-keyed", normalize_trace,
         ["graph_markers", "numeric_out_payloads"])
    diff("wiring-chain", normalize_wiring,
         ["kinds", "user_wired_nodes", "overload_resolutions_reported"])
    diff("profile-chain", normalize_profile, ["available", "has_timings"])
    return report


def render_diagnostics_markdown(report: dict[str, Any]) -> str:
    lines = ["# Diagnostic content parity (issue #221)", ""]
    lines.append(f"Gaps: {len(report['gaps'])}")
    lines.append("")
    for section, entry in report["sections"].items():
        lines.append(f"## {section}")
        if not entry["gaps"]:
            lines.append("- information content matches")
        for g in entry["gaps"]:
            lines.append(f"- **{g['key']}**: reference={g['reference']!r} candidate={g['candidate']!r}")
        lines.append("")
    return "\n".join(lines) + "\n"
