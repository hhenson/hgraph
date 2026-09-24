"""Score reasoned expectations against the observed Python and C++ traces.

``python check.py`` rewrites ``assessment.json``. It never reads an
expectation from an observation: ``reasoned.json`` is compared, not produced.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _same(expected, actual) -> bool:
    # Canonical floats are hex strings, so NaN compares equal to itself here;
    # a raw float NaN (from a hand-written expectation) is handled the same way.
    if isinstance(expected, float) and isinstance(actual, float):
        return (math.isnan(expected) and math.isnan(actual)) or expected == actual
    return expected == actual


def _fields_absent(result: dict, spec: dict) -> bool:
    trace = result.get("trace")
    if result.get("status") != "ok" or not isinstance(trace, list):
        return False
    if spec["tick"] >= len(trace):
        return True
    tick = trace[spec["tick"]]
    if tick is None:
        return True
    names = {entry[0] for entry in tick.get("$map", [])}
    return not names & set(spec["fields"])


def satisfies(result: dict, expectation: dict) -> bool:
    mode = expectation["mode"]
    if mode == "trace":
        return result.get("status") == "ok" and _same(
            expectation["expected"], result.get("trace")
        )
    if mode == "map-fields-absent":
        return _fields_absent(result, expectation["expected"])
    raise ValueError(f"unknown mode {mode!r}")


def assess(reasoned: dict, observed: dict) -> dict:
    cases = {}
    for number, expectation in sorted(reasoned["cases"].items(), key=lambda kv: int(kv[0])):
        observation = observed["cases"][number]
        if expectation["mode"] == "open":
            verdict = "open"
        else:
            matches = [
                side
                for side in ("reference", "candidate")
                if satisfies(observation[side], expectation)
            ]
            verdict = {
                ("reference", "candidate"): "both",
                ("reference",): "reference",
                ("candidate",): "candidate",
                (): "neither",
            }[tuple(matches)]
        cases[number] = {
            "family": expectation["family"],
            "verdict": verdict,
            **(
                {"open_point": expectation["open_point"]}
                if "open_point" in expectation
                else {}
            ),
        }
    summary = Counter(
        (case["family"], case["verdict"]) for case in cases.values()
    )
    return {
        "summary": [
            {"family": family, "verdict": verdict, "count": count}
            for (family, verdict), count in sorted(summary.items())
        ],
        "cases": cases,
    }


def main() -> int:
    reasoned = json.loads((HERE / "reasoned.json").read_text())
    observed = json.loads((HERE / "observed.json").read_text())
    assessment = assess(reasoned, observed)
    (HERE / "assessment.json").write_text(
        json.dumps(assessment, indent=1, sort_keys=True) + "\n"
    )
    for row in assessment["summary"]:
        print(f"{row['count']:3d}  {row['family']:28s} {row['verdict']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
