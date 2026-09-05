"""Architecture ratchets: layering leaks that must only ever shrink.

Design record: ``docs/source/developer_guide/testing.rst`` ("Architecture
ratchets"). Each entry below pins the number of occurrences of a pattern that
the 2026-09-04 fix-series retrospective identified as a rule applied at the
wrong layer (a consumer dereferencing a REF that binding should have
dereferenced, Python wiring matching type carriers by operator name, the
runtime probing a schema kind per tick, Python-object handling inside the
type layer, ...).

The test fails when a count moves in *either* direction:

* an increase means a new copy of a rule that already has an owner - fix it
  at the owning layer instead, or record the deliberate exception in the
  developer guide and raise the baseline in the same change;
* a decrease is the intended outcome of a consolidation - lower the baseline
  in the same change so the ratchet stays tight.

Run with ``HGRAPH_RATCHET_REPORT=1`` to print the current table instead of
asserting.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

_EXCLUDED_PARTS = {"tests", "_deps", "build", ".venv", "__pycache__"}


@dataclass(frozen=True)
class Ratchet:
    """One pinned occurrence count.

    ``mode`` is ``"matches"`` (total regex matches) or ``"files"`` (number of
    files containing at least one match).
    """

    id: str
    baseline: int
    roots: tuple[str, ...]
    suffixes: tuple[str, ...]
    pattern: str
    owner: str
    mode: str = "matches"


RATCHETS: tuple[Ratchet, ...] = (
    # --- REF transparency belongs to the binding (retrospective family 1) ---
    Ratchet(
        id="stdlib-ref-dereference",
        baseline=64,
        roots=("include/hgraph/lib/std/operators/impl",),
        suffixes=(".h",),
        pattern=r"\bdereference\(",
        owner="type_pattern input matcher binds the recursively dereferenced "
        "schema; an operator never dereferences its own input",
    ),
    Ratchet(
        id="wiring-ref-handling",
        baseline=22,
        roots=("python/hgraph/_wiring",),
        suffixes=(".py",),
        pattern=r"\b(is_ref|dereferenced|ref_target)\b",
        owner="binding inserts the from-REF adaptation "
        "(python_bridge.rst, 'Value and reference crossings')",
    ),
    Ratchet(
        id="value-consumer-source-callers",
        baseline=3,
        roots=("src/hgraph", "include/hgraph", "python"),
        suffixes=(".cpp", ".h"),
        pattern=r"\bvalue_consumer_source\(",
        owner="value_argument (variadic tails) and adapt_source_for_input "
        "(ordinary inputs) are the two rule sites; declaration + those two "
        "is the floor",
    ),
    # --- REF ownership at nested boundaries is a build-time property (family 2) ---
    Ratchet(
        id="runtime-ref-kind-probes",
        baseline=7,
        roots=("src/hgraph/runtime", "include/hgraph/runtime"),
        suffixes=(".cpp", ".h"),
        pattern=r"TSTypeKind::REF",
        owner="a node's REF handling mode is decided when the node is built "
        "(nested_graphs.rst), not by probing the schema per tick",
    ),
    # --- Type carriers are resolved by the resolver (family 3) ---
    Ratchet(
        id="wiring-operator-name-branches",
        baseline=8,
        roots=("python/hgraph/_wiring",),
        suffixes=(".py",),
        pattern=r"self\.__name__\s*(==|in)\s",
        owner="which argument is a type carrier is a property of the "
        "signature (operators.rst), never of the operator name",
    ),
    Ratchet(
        id="types-shadow-schema-dicts",
        baseline=0,
        roots=("python/hgraph/_types.py",),
        suffixes=(".py",),
        pattern=r"_(TS|VALUE)_SCALAR_TYPES\b",
        owner="the registry is the single schema-to-Python-type authority",
    ),
    # --- Type carriers are matched by the resolver (family 3) ---
    Ratchet(
        id="wiring-type-carrier-sites",
        baseline=0,
        roots=("python/hgraph/_wiring", "python/hgraph/_types.py"),
        suffixes=(".py",),
        pattern=r"\b(_binding_for_type_value|_match_type_argument|apply_type_carriers|"
        r"_resolved_placeholder_value|_bind_native_resolution)\b",
        owner="type arguments are matched and materialised by the registry "
        "(RFC 0033); the Python side only lowers patterns and pins",
    ),
    # --- One ancestry walker (family 4) ---
    Ratchet(
        id="bundle-only-ancestry",
        baseline=0,
        roots=("src/hgraph", "include/hgraph", "python"),
        suffixes=(".cpp", ".h"),
        pattern=r"\bbundle_is_a\(",
        owner="TypeRegistry::value_is_a is the one nominal ancestry walk; the "
        "bundle-only API was removed on 2026-09-05",
    ),
    Ratchet(
        id="stdlib-ancestry-walker",
        baseline=0,
        roots=("include/hgraph/lib/std", "src/hgraph/lib/std"),
        suffixes=(".cpp", ".h"),
        pattern=r"\bdispatch_bundle_is_a\b",
        owner="the registry owns ancestry; value_is_a is lock-free over the "
        "immutable parent links, so no operator needs its own walker",
    ),
    # --- Operators read bindings from views (family 5) ---
    Ratchet(
        id="stdlib-active-realization",
        baseline=34,
        roots=("include/hgraph/lib/std/operators/impl",),
        suffixes=(".h",),
        pattern=r"\bvalue_type_for_active_realization\b",
        owner="realized bindings travel with bound views; operators do not "
        "recompute realization",
    ),
    # --- Exception boundaries are named (AGENTS.md: prefer the scope.h guards) ---
    Ratchet(
        id="catch-all-sites",
        baseline=9,
        roots=("src/hgraph", "include/hgraph", "python"),
        suffixes=(".cpp", ".h"),
        pattern=r"catch\s*\(\s*\.\.\.\s*\)",
        owner="util/scope.h owns the catch-all forms (fallback_on_exception, "
        "annotate_on_exception, scope_exit<HideExceptions>, UnwindCleanupGuard, "
        "FirstExceptionRecorder); the only others are the documented "
        "translation boundaries (py_error_on_exception, retained_error_message, "
        "the node-phase error prefix). A new bare catch(...) is a boundary "
        "without a name: use a helper or document the boundary",
    ),
    # --- Python-object handling stays behind an ops table (family 6) ---
    Ratchet(
        id="python-object-hash-units",
        baseline=1,
        roots=("src/hgraph", "include/hgraph", "python"),
        suffixes=(".cpp", ".h"),
        pattern=r"\bPyObject_Hash\b",
        owner="python_bridge::object_hash/equals/compare/str "
        "(include/hgraph/python/object_semantics.h, implemented in "
        "src/hgraph/python/impl/object_semantics.cpp) are the one set of "
        "Python-object primitives; every ops table delegates to them",
        mode="files",
    ),
    Ratchet(
        id="type-layer-python-conditionals",
        baseline=165,
        roots=("src/hgraph/types", "include/hgraph/types"),
        suffixes=(".cpp", ".h"),
        pattern=r"HGRAPH_ENABLE_PYTHON_USER_NODES",
        owner="the type layer sees Python only through registered ops tables "
        "(python_bridge.rst, 'No kind-switches in conversion')",
    ),
    # --- One lifecycle path per Python node kind (family 7) ---
    Ratchet(
        id="wiring-layout-scalar-appends",
        baseline=1,
        roots=("python/hgraph/_wiring/_node.py",),
        suffixes=(".py",),
        pattern=r'\.append\("s"\)',
        owner="_lifecycle_layout is the one walk from a signature to the "
        "native layout string; the node eval builder is the only other "
        "site that appends a scalar code",
    ),
    # --- Rulings (family 8) ---
    Ratchet(
        id="runtime-thread-locals",
        baseline=2,
        roots=("src/hgraph/runtime", "include/hgraph/runtime"),
        suffixes=(".cpp", ".h"),
        pattern=r"\bthread_local\b",
        owner="per-graph state binds to the running graph, never the thread "
        "(CLAUDE.md conventions)",
    ),
)


def _files(ratchet: Ratchet):
    for root in ratchet.roots:
        path = REPO_ROOT / root
        if path.is_file():
            if path.suffix in ratchet.suffixes:
                yield path
            continue
        if not path.is_dir():
            continue
        for candidate in sorted(path.rglob("*")):
            if not candidate.is_file() or candidate.suffix not in ratchet.suffixes:
                continue
            relative = candidate.relative_to(REPO_ROOT)
            if _EXCLUDED_PARTS.intersection(relative.parts[:-1]):
                continue
            yield candidate


def measure(ratchet: Ratchet) -> tuple[int, dict[str, int]]:
    """Return the current count and a per-file breakdown."""
    expression = re.compile(ratchet.pattern)
    per_file: dict[str, int] = {}
    for path in _files(ratchet):
        hits = len(expression.findall(path.read_text(encoding="utf-8", errors="replace")))
        if hits:
            per_file[path.relative_to(REPO_ROOT).as_posix()] = hits
    if ratchet.mode == "files":
        return len(per_file), per_file
    return sum(per_file.values()), per_file


def _breakdown(per_file: dict[str, int]) -> str:
    return "\n".join(f"    {count:4d}  {path}" for path, count in sorted(per_file.items()))


_SOURCE_PRESENT = (REPO_ROOT / "src" / "hgraph").is_dir() and (
    REPO_ROOT / "python" / "hgraph"
).is_dir()


@pytest.mark.skipif(not _SOURCE_PRESENT, reason="ratchets read the source tree")
@pytest.mark.parametrize("ratchet", RATCHETS, ids=[r.id for r in RATCHETS])
def test_architecture_ratchet(ratchet: Ratchet, pytestconfig: pytest.Config):
    count, per_file = measure(ratchet)
    if os.environ.get("HGRAPH_RATCHET_REPORT"):
        reporter = pytestconfig.pluginmanager.get_plugin("terminalreporter")
        reporter.write_line(
            f"{ratchet.id}: {count} (baseline {ratchet.baseline})\n{_breakdown(per_file)}"
        )
        return
    if count > ratchet.baseline:
        pytest.fail(
            f"{ratchet.id}: {count} occurrences, baseline {ratchet.baseline}.\n"
            f"  Owner of this rule: {ratchet.owner}.\n"
            "  A new copy of a rule that already has an owner is the layering leak "
            "this ratchet exists to catch. Fix it at the owning layer, or record the "
            "exception in the developer guide and raise the baseline in the same "
            "change.\n"
            f"{_breakdown(per_file)}"
        )
    if count < ratchet.baseline:
        pytest.fail(
            f"{ratchet.id}: {count} occurrences, baseline {ratchet.baseline}.\n"
            "  The count dropped - good. Lower the baseline in "
            "python/tests/test_architecture_ratchets.py in this change so the "
            "ratchet stays tight.\n"
            f"{_breakdown(per_file)}"
        )


def test_ratchet_ids_are_unique():
    ids = [ratchet.id for ratchet in RATCHETS]
    assert len(ids) == len(set(ids))
