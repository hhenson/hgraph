"""Score the wiring observations against the reasoned expectations.

    python check.py

Reads ``reasoned.json``, ``observed.json`` (the runtimes) and
``observed_hgl.json`` (the HGL front end) and writes ``assessment.json``.
Each asserted observation gets a verdict per the conformance procedure:

- ``both``: reasoning matches Python 0.5.41 and C++;
- ``python`` / ``cpp``: reasoning matches one runtime; the other varies;
- ``neither``: no runtime matches; the question goes to the owner.

The HGL front end is scored separately (WIR-14): ``match`` or ``varies``.
An expectation is never read from an observation.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent


SPEC = HERE.parent.parent


def _defined_rules() -> set[str]:
    """Every rule the chapters define, e.g. ``**WIR-7**`` or ``**TS-17**``."""
    rules: set[str] = set()
    for chapter in ("wiring.md", "time_series.md"):
        rules.update(re.findall(r"\*\*([A-Z]{2,4}-\d+)\*\*", (SPEC / chapter).read_text()))
    return rules


def main() -> None:
    reasoned = json.loads((HERE / "reasoned.json").read_text())
    defined = _defined_rules()
    cited = {rule for case, fields in reasoned.items() if not case.startswith("_")
             for field, spec in fields.items() if field != "recorded" and not field.startswith("_")
             for rule in spec["rules"]}
    if undefined := sorted(cited - defined):
        raise SystemExit(f"expectations cite rules no chapter defines: {undefined}")
    observed = json.loads((HERE / "observed.json").read_text())["cases"]
    hgl = json.loads((HERE / "observed_hgl.json").read_text())
    assessment: dict = {"runtimes": {}, "hgl": {}, "totals": {}}
    totals: dict[str, int] = {}
    for case, fields in reasoned.items():
        if case.startswith("_"):
            continue
        if case == "hgl_front_end":
            for field in fields.get("_recorded", []):
                assessment["hgl"][field] = {"observed": hgl.get(field), "verdict": "recorded"}
            for field, spec in fields.items():
                if field.startswith("_"):
                    continue
                verdict = "match" if hgl.get(field) == spec["expected"] else "varies"
                assessment["hgl"][field] = {
                    "expected": spec["expected"], "observed": hgl.get(field), "verdict": verdict, "rules": spec["rules"]}
                totals[f"hgl:{verdict}"] = totals.get(f"hgl:{verdict}", 0) + 1
            continue
        runs = observed[case]
        rows = {}
        for field, spec in fields.items():
            if field == "recorded":
                continue
            python = runs["python"].get(field, runs["python"].get("harness_error", "<missing>"))
            cpp = runs["cpp"].get(field, runs["cpp"].get("harness_error", "<missing>"))
            match_python = python == spec["expected"]
            match_cpp = cpp == spec["expected"]
            verdict = {(True, True): "both", (True, False): "python", (False, True): "cpp"}.get(
                (match_python, match_cpp), "neither")
            rows[field] = {"expected": spec["expected"], "python": python, "cpp": cpp,
                           "verdict": verdict, "rules": spec["rules"]}
            totals[verdict] = totals.get(verdict, 0) + 1
        for field in fields.get("recorded", []):
            rows[field] = {"python": runs["python"].get(field), "cpp": runs["cpp"].get(field), "verdict": "recorded"}
        assessment["runtimes"][case] = rows
    assessment["totals"] = dict(sorted(totals.items()))
    (HERE / "assessment.json").write_text(json.dumps(assessment, indent=2, sort_keys=True) + "\n")
    print(json.dumps(assessment["totals"], sort_keys=True))
    for case, rows in assessment["runtimes"].items():
        for field, row in rows.items():
            if row["verdict"] not in ("both", "recorded"):
                print(f"  {case}.{field}: {row['verdict']} (python={row['python']!r}, cpp={row['cpp']!r})")
    for field, row in assessment["hgl"].items():
        if row["verdict"] not in ("match", "recorded"):
            print(f"  hgl.{field}: expected {row['expected']!r}, observed {row['observed']!r}")


if __name__ == "__main__":
    main()
