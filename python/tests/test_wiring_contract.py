"""The runtime specification's wiring cases hold for this runtime.

Each case in ``docs/source/runtime_spec/validation/wiring/cases.py`` runs in
a fresh interpreter (its operators must not leak into this session) and every
reasoned expectation in ``reasoned.json`` is asserted, naming the rules it
comes from (``runtime_spec/wiring.md``). The HGL front end's cases are the
compiler's to hold (WIR-14); they are not run here.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

VALIDATION = Path(__file__).resolve().parents[2] / "docs" / "source" / "runtime_spec" / "validation" / "wiring"
REASONED = json.loads((VALIDATION / "reasoned.json").read_text())
RUNTIME_CASES = sorted(case for case in REASONED if not case.startswith("_") and case != "hgl_front_end")

# Accepted expectations this runtime does not meet yet, by variation ID
# (validation/wiring/README.md), with the observation recorded for it. Each
# must still vary exactly as recorded: the correction that fixes one removes
# it from here.
KNOWN_VARIATIONS = {
    ("bundle_identity", "named_input_from_other_name"): ("WV-4", "wires"),
    ("bundle_identity", "two_names_repeat"): ("WV-4", "wires"),
    ("operator_contract", "widening_registers"): ("WV-6", "registered"),
    ("operator_contract", "widening_call_for_float"): ("WV-6", "wires"),
    ("failure_report", "names_the_graph_path"): ("WV-8", False),
    ("failure_report", "names_the_operator"): ("WV-9", False),
    ("caught_failure", "outcome"): ("WV-10", "wires"),
}


def _observe(case: str) -> dict:
    completed = subprocess.run(
        [sys.executable, str(VALIDATION / "cases.py"), case], capture_output=True, text=True, cwd=VALIDATION
    )
    lines = [line for line in completed.stdout.splitlines() if line.startswith("{")]
    assert completed.returncode == 0 and lines, completed.stderr[-2000:]
    return json.loads(lines[-1])


@pytest.mark.parametrize("case", RUNTIME_CASES)
def test_the_wiring_case_holds(case):
    observed = _observe(case)
    assert "harness_error" not in observed, observed["harness_error"]
    for field, spec in REASONED[case].items():
        if field == "recorded":
            continue
        if (case, field) in KNOWN_VARIATIONS:
            variation, recorded = KNOWN_VARIATIONS[(case, field)]
            assert observed[field] == recorded, (
                f"{case}.{field} ({variation}) no longer varies as recorded: observed {observed[field]!r}. "
                f"If it now meets the expectation {spec['expected']!r}, remove it from KNOWN_VARIATIONS."
            )
            continue
        assert observed[field] == spec["expected"], f"{case}.{field} ({', '.join(spec['rules'])}): {spec['derivation']}"


def test_every_cited_rule_is_defined():
    sys.path.insert(0, str(VALIDATION))
    try:
        import check
    finally:
        sys.path.remove(str(VALIDATION))
    defined = check._defined_rules()
    cited = {
        rule
        for case, fields in REASONED.items()
        if not case.startswith("_")
        for field, spec in fields.items()
        if field != "recorded" and not field.startswith("_")
        for rule in spec["rules"]
    }
    assert cited <= defined, sorted(cited - defined)
