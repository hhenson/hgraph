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


from hgraph import operator


@operator
def pick(v: TS[int]) -> TS[int]:
    'overloaded operator: exercises overload-resolution reporting' 


@compute_node(overloads=pick)
def pick_int(v: TS[int]) -> TS[int]:
    return v.value + 100


@graph
def overloaded(t: TS[int]) -> TS[int]:
    return pick(t)


CHAIN_INPUT = [1, None, 3]
KEYED_INPUT = [{"a": 1}, {"b": 5}]

print(f"{SENTINEL} trace-chain", flush=True)
eval_node(chain, CHAIN_INPUT, __trace__=True)
print(f"{SENTINEL} trace-keyed", flush=True)
eval_node(keyed, KEYED_INPUT, __trace__=True)
print(f"{SENTINEL} wiring-chain", flush=True)
eval_node(chain, CHAIN_INPUT, __trace_wiring__=True)
print(f"{SENTINEL} wiring-overload", flush=True)
eval_node(overloaded, [1], __trace_wiring__=True)
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
    key_value_pairs = set()
    nested_out_rows = 0
    for line in text.splitlines():
        if "[OUT]" in line or "[IN]" in line:
            for m in re.finditer(r"'?([A-Za-z_]\w*)'?\s*:\s*(-?\d+(?:\.\d+)?)\b", line):
                if m.group(1) not in ("removed", "modified", "removed_strict", "added"):
                    key_value_pairs.add((m.group(1), m.group(2)))
        if "[OUT]" in line:
            scope = re.search(rf"^\[(?:{_TIME})\](?:\[(?:{_TIME})\])?\s*\[([^\]]*)\]", line)
            if scope and scope.group(1).strip():
                nested_out_rows += 1
    return {
        "key_value_pairs": sorted(key_value_pairs),
        "nested_out_rows_present": nested_out_rows > 0,
        "graph_markers": sorted(graph_markers),
        "node_starts": sorted(node_starts),
        "node_stops": sorted(node_stops),
        "in_events": in_events,
        "out_events": out_events,
        "user_out_values": sorted(out_values),
        "numeric_out_payloads": sorted({n for v in out_values for n in re.findall(r"-?\d+(?:\.\d+)?", v)}),
        "distinct_times": len(times),
    }


def _user_operator_name(raw: str) -> str | None:
    """Map an engine-side operator identity to the user-facing name.

    The candidate registers python operators as ``__pyop__<qualname>_<addr>``;
    the reference reports the plain function name. Engine-internal operators
    (``__py_compute``/harness) carry no user identity."""
    name = raw.strip()
    if name.startswith("__pyop__"):
        name = name[len("__pyop__"):]
        name = re.sub(r"_[0-9a-f]+$", "", name)
        return name.split(".")[-1]
    if name.startswith("__") or _is_harness(name):
        return None
    return name.split(".")[-1]


def normalize_wiring(text: str) -> dict[str, Any]:
    kinds: set[str] = set()
    wired_nodes: set[str] = set()
    resolved_operators: set[str] = set()
    resolution_records: list[str] = []
    for line in text.splitlines():
        low = line.lower()
        if "wiring graph" in low:
            kinds.add("graph")
        if "nested graph" in low:
            kinds.add("nested-graph")
        if "wiring node" in low:
            kinds.add("node")
            m = re.search(r"[Ww]iring node ([^\s(]+)", line)
            if m:
                # node identity = the last path segment, label part when the
                # engine renders path/label [implementation] (issue #247).
                name = m.group(1).split("/")[-1].split(":")[-1]
                if not _is_harness(name):
                    wired_nodes.add(name)
        # candidate: "Resolved operator NAME at PATH to TARGET [rank N]"
        m = re.search(r"Resolved operator (\S+) at .* to (.+)$", line)
        if m:
            resolution_records.append(line.strip())
            user = _user_operator_name(m.group(1))
            if user:
                resolved_operators.add(user)
        # reference: "Overload resolution for NAME successful"
        m = re.search(r"Overload resolution for (\S+)", line)
        if m and "successful" in line:
            resolution_records.append(line.strip())
            user = _user_operator_name(m.group(1))
            if user:
                resolved_operators.add(user)
    return {
        "kinds": sorted(kinds),
        "user_wired_nodes": sorted(wired_nodes),
        "resolved_user_operators": sorted(resolved_operators),
        "resolution_records": resolution_records,
    }


def normalize_profile(text: str) -> dict[str, Any]:
    if "PROFILER-UNAVAILABLE" in text:
        return {"available": False}
    profiled_nodes: set[str] = set()
    has_timings = False
    for line in text.splitlines():
        # candidate: structured snapshot rows PROFILE-ENTRY label count=N total=T
        m = re.match(r"PROFILE-ENTRY (\S+) count=\d+ total=", line)
        if m:
            has_timings = True
            name = m.group(1).split("/")[-1].split(":")[-1]
            if not _is_harness(name) and name != "graph":
                profiled_nodes.add(name)
            continue
        if re.match(r"PROFILE-SUMMARY ", line):
            has_timings = True
            continue
        # reference: wall-timestamped lifecycle rows carry the timing
        # information per node (durations derive from stamp pairs).
        if re.match(rf"\[{_TIME}\]\[{_TIME}\]", line):
            has_timings = True
            m = re.search(r"([A-Za-z_][\w]*)(?:<[-\d, ]+>)?\(", line)
            if m and not _is_harness(m.group(1)):
                profiled_nodes.add(m.group(1).split(".")[-1])
    return {
        "available": True,
        "has_timings": has_timings,
        "profiled_user_nodes": sorted(profiled_nodes),
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
         ["graph_markers", "numeric_out_payloads", "nested_out_rows_present"])
    # key->value associations: the candidate must carry every association the
    # reference records (richer is allowed — we render resolved values where
    # upstream renders REF plumbing).
    ref_keyed = normalize_trace(reference.get("trace-keyed", ""))
    cand_keyed = normalize_trace(candidate.get("trace-keyed", ""))
    missing_pairs = [p for p in ref_keyed["key_value_pairs"]
                     if p not in cand_keyed["key_value_pairs"]]
    if missing_pairs:
        gap = {"section": "trace-keyed", "key": "key_value_pairs_missing",
               "reference": missing_pairs, "candidate": cand_keyed["key_value_pairs"]}
        report["sections"]["trace-keyed"]["gaps"].append(
            {k: v for k, v in gap.items() if k != "section"})
        report["gaps"].append(gap)
    diff("wiring-chain", normalize_wiring, ["kinds", "user_wired_nodes"])
    diff("wiring-overload", normalize_wiring, ["resolved_user_operators"])
    diff("profile-chain", normalize_profile,
         ["available", "has_timings", "profiled_user_nodes"])
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
