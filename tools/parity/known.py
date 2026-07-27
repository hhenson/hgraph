"""Known-divergence records shared by the campaign classifier and publisher.

``known_divergences.json`` carries two suppression shapes:

- ``divergences``: exact failure fingerprints.
- ``families``: a template plus a ``parameters_not_equal`` map matching every
  recipe of that template whose named parameters all differ from the stated
  identity. An empty map matches every recipe of the template (used when a
  documented deviation applies to every recipe of a template).

Each family names a bounded ``relation`` which proves the observed traces are
the documented deviation.  Parameter membership alone is insufficient: a
payload regression inside an affected template must continue through the
normal verification and publishing pipeline.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .compare import compare_outcomes


DEFAULT_PATH = Path(__file__).with_name("known_divergences.json")
TRACE_VALUE = "trace-value"
SERVICE_ADAPTOR_ONE_CYCLE = "service-adaptor-one-cycle"
KEY_SET_SIZE_NO_RETICK = "key-set-size-no-retick"
SUBSCRIPTION_RESAMPLE_ONE_CYCLE = "subscription-resample-one-cycle"
NO_CHANGE_ELISION = "no-change-elision"


def load_known_divergences(
    path: Path | None = None,
) -> tuple[set[str], list[dict[str, Any]]]:
    try:
        raw = json.loads((path or DEFAULT_PATH).read_text())
    except (OSError, json.JSONDecodeError):
        return set(), []
    fingerprints = {
        item["fingerprint"]
        for item in raw.get("divergences", ())
        if isinstance(item, dict) and isinstance(item.get("fingerprint"), str)
    }
    families = [
        item
        for item in raw.get("families", ())
        if isinstance(item, dict)
        and isinstance(item.get("template"), str)
        and isinstance(item.get("parameters_not_equal"), dict)
    ]
    return fingerprints, families


def _matches_family_parameters(
    recipe: dict[str, Any], family: dict[str, Any]
) -> bool:
    parameters = recipe.get("parameters") or {}
    # A missing parameter resolves to the template default, which is the
    # stated identity, so an omitted parameter never places a recipe inside a
    # deviation family. An empty parameters_not_equal map matches the whole
    # template.
    return recipe.get("template") == family["template"] and all(
        parameters.get(name, identity) != identity
        for name, identity in family["parameters_not_equal"].items()
    )


def matches_known_family(
    recipe: dict[str, Any], families: list[dict[str, Any]]
) -> bool:
    return any(
        _matches_family_parameters(recipe, family)
        for family in families
    )


def _trace_value_relation(
    _recipe: dict[str, Any],
    difference: dict[str, Any],
    _reference: dict[str, Any],
    _candidate: dict[str, Any],
    _family: dict[str, Any],
) -> bool:
    return difference.get("classification") == "value"


def _service_adaptor_one_cycle_relation(
    _recipe: dict[str, Any],
    _difference: dict[str, Any],
    reference: dict[str, Any],
    candidate: dict[str, Any],
    _family: dict[str, Any],
) -> bool:
    reference_trace = reference.get("trace")
    candidate_trace = candidate.get("trace")
    return (
        isinstance(reference_trace, list)
        and isinstance(candidate_trace, list)
        and candidate_trace == [None, *reference_trace]
    )


def _without_map_field(trace: Any, field: str) -> Any:
    if not isinstance(trace, list):
        return trace

    normalized = []
    for tick in trace:
        if not (
            isinstance(tick, dict)
            and set(tick) == {"$map"}
            and isinstance(tick["$map"], list)
        ):
            normalized.append(tick)
            continue
        entries = [
            entry
            for entry in tick["$map"]
            if not (
                isinstance(entry, list)
                and len(entry) == 2
                and entry[0] == field
            )
        ]
        normalized.append({"$map": entries} if entries else None)
    return normalized


def _key_set_size_no_retick_relation(
    _recipe: dict[str, Any],
    difference: dict[str, Any],
    reference: dict[str, Any],
    candidate: dict[str, Any],
    family: dict[str, Any],
) -> bool:
    if difference.get("classification") not in ("value", "length"):
        return False
    reference_trace = reference.get("trace")
    candidate_trace = candidate.get("trace")
    if reference_trace == candidate_trace:
        return False
    normalized_reference = {
        "status": "ok",
        "trace": _without_map_field(reference_trace, "size"),
    }
    normalized_candidate = {
        "status": "ok",
        "trace": _without_map_field(candidate_trace, "size"),
    }
    return (
        compare_outcomes(
            normalized_reference,
            normalized_candidate,
            float_abs_tolerance=family.get("float_abs_tolerance", 0.0),
        )
        is None
    )


def _repeated_non_null_positions(values: Any) -> list[int]:
    if not isinstance(values, list):
        return []
    seen = set()
    repeated = []
    for index, value in enumerate(values):
        if value is None:
            continue
        if value in seen:
            repeated.append(index)
        else:
            seen.add(value)
    return repeated


def _subscription_resample_one_cycle_relation(
    recipe: dict[str, Any],
    _difference: dict[str, Any],
    reference: dict[str, Any],
    candidate: dict[str, Any],
    _family: dict[str, Any],
) -> bool:
    """Issue #66/#71: only the repeats that actually sample delay.

    The documented deviation inserts one no-tick cycle immediately before
    the payload of an EMITTING repeated subscription — a repeat that emits
    nothing (e.g. its value never arrived) shifts nothing. Align the traces
    with a two-pointer walk that may consume one reference ``None`` before
    the candidate payload at any emitting repeat position; the walk is
    deterministic because the delay branch only opens where the direct
    comparison fails. At least one repeat must account for the mismatch and
    the complete payload traces must be equal after alignment, so corrupt
    values, unrelated missing ticks, and first-subscription differences
    remain reportable.
    """
    repeated = set(
        _repeated_non_null_positions((recipe.get("inputs") or {}).get("symbol"))
    )
    reference_trace = reference.get("trace")
    candidate_trace = candidate.get("trace")
    if (
        not repeated
        or not isinstance(reference_trace, list)
        or not isinstance(candidate_trace, list)
    ):
        return False

    inserted = 0
    j = 0
    for i, item in enumerate(candidate_trace):
        if j < len(reference_trace) and reference_trace[j] == item:
            j += 1
            continue
        if (
            i in repeated
            and item is not None
            and j + 1 < len(reference_trace)
            and reference_trace[j] is None
            and reference_trace[j + 1] == item
        ):
            j += 2
            inserted += 1
            continue
        return False
    return j == len(reference_trace) and inserted >= 1


def _no_change_elision_relation(
    _recipe: dict[str, Any],
    difference: dict[str, Any],
    reference: dict[str, Any],
    candidate: dict[str, Any],
    _family: dict[str, Any],
) -> bool:
    """No-change-means-no-tick (ruling 2026-07-17): the candidate trace is
    the reference trace with re-ticks of an UNCHANGED value elided. Every
    position must either match exactly, or be a candidate ``None`` where the
    reference re-emitted the value it had already emitted; at least one
    elision must account for the difference. Extra candidate ticks, changed
    values, and length differences remain reportable."""
    if difference.get("classification") != "value":
        return False
    reference_trace = reference.get("trace")
    candidate_trace = candidate.get("trace")
    if (
        not isinstance(reference_trace, list)
        or not isinstance(candidate_trace, list)
        or len(reference_trace) != len(candidate_trace)
    ):
        return False
    last: Any = object()   # nothing emitted yet — never equal to a value
    elided = 0
    for ref, cand in zip(reference_trace, candidate_trace):
        unchanged = ref is not None and ref == last
        if ref is not None:
            last = ref
        if cand == ref:
            continue
        if cand is None and unchanged:
            elided += 1
            continue
        return False
    return elided >= 1


RELATIONS = {
    TRACE_VALUE: _trace_value_relation,
    NO_CHANGE_ELISION: _no_change_elision_relation,
    SERVICE_ADAPTOR_ONE_CYCLE: _service_adaptor_one_cycle_relation,
    KEY_SET_SIZE_NO_RETICK: _key_set_size_no_retick_relation,
    SUBSCRIPTION_RESAMPLE_ONE_CYCLE: (
        _subscription_resample_one_cycle_relation
    ),
}


def is_known_family_failure(
    recipe: dict[str, Any],
    difference: dict[str, Any],
    reference: dict[str, Any],
    candidate: dict[str, Any],
    families: list[dict[str, Any]],
) -> bool:
    """True when a mismatch is a documented deviation itself."""
    if (
        reference.get("status") != "ok"
        or candidate.get("status") != "ok"
        or not str(difference.get("path", "")).startswith("$.trace")
    ):
        return False

    for family in families:
        if not _matches_family_parameters(recipe, family):
            continue
        relation = RELATIONS.get(family.get("relation"))
        if relation is not None and relation(
            recipe,
            difference,
            reference,
            candidate,
            family,
        ):
            return True
    return False
