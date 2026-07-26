"""Semantic comparison of canonical fresh-process outcomes."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Difference:
    classification: str
    path: str
    reference: Any
    candidate: Any

    def to_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "path": self.path,
            "reference": self.reference,
            "candidate": self.candidate,
        }


def _float_value(value: Any) -> float | None:
    if not isinstance(value, dict) or set(value) != {"$float"}:
        return None
    encoded = value["$float"]
    if encoded == "nan":
        return math.nan
    if encoded == "inf":
        return math.inf
    if encoded == "-inf":
        return -math.inf
    try:
        return float.fromhex(encoded)
    except (TypeError, ValueError):
        return None


def _first_difference(
    reference: Any,
    candidate: Any,
    *,
    path: str,
    float_abs_tolerance: float,
) -> Difference | None:
    reference_float = _float_value(reference)
    candidate_float = _float_value(candidate)
    if reference_float is not None and candidate_float is not None:
        equal = (
            math.isnan(reference_float) and math.isnan(candidate_float)
        ) or math.isclose(
            reference_float,
            candidate_float,
            rel_tol=0.0,
            abs_tol=float_abs_tolerance,
        )
        if equal:
            return None
    elif type(reference) is not type(candidate):
        return Difference("value", path, reference, candidate)

    if isinstance(reference, dict) and isinstance(candidate, dict):
        if set(reference) != set(candidate):
            return Difference(
                "shape", path, sorted(reference), sorted(candidate)
            )
        for key in sorted(reference):
            difference = _first_difference(
                reference[key],
                candidate[key],
                path=f"{path}.{key}",
                float_abs_tolerance=float_abs_tolerance,
            )
            if difference is not None:
                return difference
        return None
    if isinstance(reference, list) and isinstance(candidate, list):
        if len(reference) != len(candidate):
            return Difference(
                "length", path, len(reference), len(candidate)
            )
        for index, (reference_item, candidate_item) in enumerate(
            zip(reference, candidate)
        ):
            difference = _first_difference(
                reference_item,
                candidate_item,
                path=f"{path}[{index}]",
                float_abs_tolerance=float_abs_tolerance,
            )
            if difference is not None:
                return difference
        return None
    if reference != candidate:
        return Difference("value", path, reference, candidate)
    return None


def compare_outcomes(
    reference: dict[str, Any],
    candidate: dict[str, Any],
    *,
    float_abs_tolerance: float = 0.0,
) -> Difference | None:
    reference_status = reference.get("status")
    candidate_status = candidate.get("status")
    if reference_status != candidate_status:
        return Difference("status", "$.status", reference_status, candidate_status)
    if reference_status != "ok":
        reference_failure = (
            reference.get("phase"),
            reference.get("exception", {}).get("category"),
        )
        candidate_failure = (
            candidate.get("phase"),
            candidate.get("exception", {}).get("category"),
        )
        if reference_failure != candidate_failure:
            return Difference(
                "failure",
                "$.exception",
                reference_failure,
                candidate_failure,
            )
        return None
    return _first_difference(
        reference.get("trace"),
        candidate.get("trace"),
        path="$.trace",
        float_abs_tolerance=float_abs_tolerance,
    )


def semantic_signature(result: dict[str, Any]) -> str:
    if result.get("status") == "ok":
        value = {"status": "ok", "trace": result.get("trace")}
    else:
        value = {
            "status": result.get("status"),
            "phase": result.get("phase"),
            "category": result.get("exception", {}).get("category"),
            "returncode": result.get("process_returncode"),
        }
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
