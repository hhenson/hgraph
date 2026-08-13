"""Known-divergence records shared by the campaign classifier and publisher.

``known_divergences.json`` carries two suppression shapes:

- ``divergences``: exact failure fingerprints.
- ``families``: a template plus optional ``parameters_equal`` and required
  ``parameters_not_equal`` maps. Named parameters must respectively equal the
  stated value or differ from the stated identity. Empty maps match every
  recipe of the template (used when a documented deviation applies to every
  recipe of a template).

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
KEY_SET_SIZE_NO_RETICK = "key-set-size-no-retick"
NO_CHANGE_ELISION = "no-change-elision"
VALID_SUBSET_REDUCE = "valid-subset-reduce"
SWITCH_FLIP_MAP_REMOVAL = "switch-flip-map-removal"
REQUEST_REPLY_ONE_CYCLE_EARLIER = "request-reply-one-cycle-earlier"
NESTED_REQUEST_REPLY_ONE_CYCLE_EARLIER = "nested-request-reply-one-cycle-earlier"
POLYMORPHIC_JSON_PRESERVES_LEAF = "polymorphic-json-preserves-leaf"


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
        parameters.get(name, object()) == expected
        for name, expected in family.get("parameters_equal", {}).items()
    ) and all(
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


def _polymorphic_json_preserves_leaf_relation(
    _recipe: dict[str, Any],
    difference: dict[str, Any],
    reference: dict[str, Any],
    candidate: dict[str, Any],
    _family: dict[str, Any],
) -> bool:
    """Released hgraph reconstructs a JSON-decoded Event as the declared
    base, while the candidate retains the concrete Heartbeat/Create/Cancel
    leaf. Admit only that exact information-preserving difference: project
    candidate event leaves back to Event and require the complete traces to
    become identical. Any changed base field, timing, container shape, or
    unrelated payload remains reportable."""
    if difference.get("classification") not in {"value", "length"}:
        return False

    changed = False

    def project(value):
        nonlocal changed
        if isinstance(value, list):
            return [project(item) for item in value]
        if not isinstance(value, dict):
            return value
        if set(value) == {"$dataclass"}:
            payload = value["$dataclass"]
            if isinstance(payload, dict):
                type_name = payload.get("type")
                leaf = type_name.rsplit(".", 1)[-1] if isinstance(type_name, str) else ""
                if leaf in {"HeartbeatEvent", "CreateEvent", "CancelEvent"}:
                    fields = payload.get("fields")
                    if not isinstance(fields, list):
                        return value
                    event_fields = [
                        field for field in fields
                        if isinstance(field, list)
                        and len(field) == 2
                        and field[0] == "event_id"
                    ]
                    if len(event_fields) != 1:
                        return value
                    changed = True
                    prefix = type_name.rsplit(".", 1)[0]
                    return {
                        "$dataclass": {
                            "type": f"{prefix}.Event",
                            "fields": [["event_id", project(event_fields[0][1])]],
                        }
                    }
        return {key: project(item) for key, item in value.items()}

    normalized_candidate = {
        "status": "ok",
        "trace": project(candidate.get("trace")),
    }
    normalized_reference = {
        "status": "ok",
        "trace": reference.get("trace"),
    }
    return changed and compare_outcomes(
        normalized_reference, normalized_candidate
    ) is None


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


def _valid_subset_reduce_relation(
    _recipe: dict[str, Any],
    difference: dict[str, Any],
    reference: dict[str, Any],
    candidate: dict[str, Any],
    _family: dict[str, Any],
) -> bool:
    """Issue #95: hg_cpp may publish an aggregate of the currently-valid
    mapped values while released hgraph remains invalid because another live
    keyed slot is still a phantom.

    Only additional candidate emissions are admitted, plus the CATCH-UP
    elision they compose with: when released hgraph later emits exactly the
    value the candidate already published early, the candidate's silent
    position is the ruled no-change elision of an equal re-tick (the two
    Accepted Deviations composing, e.g. a phantom map key whose removal
    empties upstream's dict one cycle after hg_cpp already reduced without
    it). Any other reference emission must match exactly, so payload
    corruption, missing aggregates, changed eventual values, and
    trace-length differences remain reportable.
    """
    if difference.get("classification") != "value":
        return False
    reference_trace = reference.get("trace")
    candidate_trace = candidate.get("trace")
    if reference_trace is None and isinstance(candidate_trace, list):
        # Released hgraph emitted NOTHING for the whole run — the limiting
        # case of catching up later (issues #174/#176): every candidate
        # position must then qualify as an admissible extra emission.
        reference_trace = [None] * len(candidate_trace)
    if (
        not isinstance(reference_trace, list)
        or not isinstance(candidate_trace, list)
        or len(reference_trace) != len(candidate_trace)
    ):
        return False

    extra = 0
    candidate_last: Any = object()   # nothing published yet
    for ref, cand in zip(reference_trace, candidate_trace):
        if cand is not None:
            candidate_last = cand
        if ref == cand:
            continue
        if ref is None and cand is not None:
            extra += 1
            continue
        if cand is None and ref == candidate_last:
            # Upstream catching up to the candidate's earlier emission;
            # the candidate elides the equal re-tick (no-change ruling).
            continue
        return False
    return extra >= 1


def _switch_flip_valid_subset_relation(
    recipe: dict[str, Any],
    difference: dict[str, Any],
    reference: dict[str, Any],
    candidate: dict[str, Any],
    _family: dict[str, Any],
) -> bool:
    """The switch-flip route of the issue #95 deviation, WINDOWED (PR #165
    review): an extra candidate emission is admitted only inside a
    disturbance window — from a driving-input tick (``selector`` /
    ``outer_selector`` / ``values``, the events that can flip a branch or
    put a request round trip in flight) until the next agreeing reference
    emission. Outside a window every position must match exactly, so a
    spurious candidate tick on a settled request-reply pipeline stays
    reportable, as do payload mismatches and missing emissions anywhere."""
    if difference.get("classification") != "value":
        return False
    reference_trace = reference.get("trace")
    candidate_trace = candidate.get("trace")
    if reference_trace is None and isinstance(candidate_trace, list):
        # Released hgraph emitted NOTHING for the whole run — the limiting
        # case of catching up later (issues #174/#176): every candidate
        # position must then qualify as an admissible extra emission.
        reference_trace = [None] * len(candidate_trace)
    if (
        not isinstance(reference_trace, list)
        or not isinstance(candidate_trace, list)
        or len(reference_trace) != len(candidate_trace)
    ):
        return False

    inputs = recipe.get("inputs") or {}
    disturbed = set()
    for name in ("selector", "outer_selector", "values"):
        ticks = inputs.get(name) or ()
        for index, tick in enumerate(ticks):
            if tick is not None:
                disturbed.add(index)

    extra = 0
    window = False
    candidate_last: Any = object()   # nothing published yet
    for index, (ref, cand) in enumerate(
        zip(reference_trace, candidate_trace)
    ):
        if index in disturbed:
            window = True
        if cand is not None:
            candidate_last = cand
        if ref is not None:
            if ref == cand:
                window = False
                continue
            if cand is None and ref == candidate_last:
                # Upstream catching up to the candidate's earlier in-window
                # emission; the candidate elides the equal re-tick (the
                # no-change ruling composing with the valid-subset one).
                window = False
                continue
            return False
        if cand is None:
            continue
        if not window:
            return False
        extra += 1
    return extra >= 1


def _canonical_map_entries(tick: Any) -> list[Any] | None:
    if (
        isinstance(tick, dict)
        and set(tick) == {"$map"}
        and isinstance(tick["$map"], list)
    ):
        return tick["$map"]
    return None


def _request_reply_one_cycle_earlier_relation(
    _recipe: dict[str, Any],
    difference: dict[str, Any],
    reference: dict[str, Any],
    candidate: dict[str, Any],
    _family: dict[str, Any],
) -> bool:
    """RFC 0014 self-coupled request/reply transport removes exactly the
    released implementation's leading response-feedback cycle.

    Preserve every payload and silent cycle after that boundary.  This is a
    sequence relation, not a general tolerance for shifted or missing ticks.
    """
    if difference.get("classification") != "length":
        return False
    reference_trace = reference.get("trace")
    candidate_trace = candidate.get("trace")
    return (
        isinstance(reference_trace, list)
        and isinstance(candidate_trace, list)
        and len(reference_trace) == len(candidate_trace) + 1
        and reference_trace[0] is None
        and any(tick is not None for tick in candidate_trace)
        and reference_trace[1:] == candidate_trace
    )


def _nested_request_reply_one_cycle_earlier_relation(
    recipe: dict[str, Any],
    difference: dict[str, Any],
    reference: dict[str, Any],
    candidate: dict[str, Any],
    _family: dict[str, Any],
) -> bool:
    """RFC 0014 response timing under an unreduced nested map.

    A stable structural-map prefix may precede the removed feedback cycle, but
    the request/reply-backed alpha branch must be active at that cycle and the
    advanced candidate tick must contain a response value. Everything after
    that single removal remains exact.
    """
    if difference.get("classification") != "length":
        return False
    reference_trace = reference.get("trace")
    candidate_trace = candidate.get("trace")
    if (
        not isinstance(reference_trace, list)
        or not isinstance(candidate_trace, list)
        or len(reference_trace) != len(candidate_trace) + 1
    ):
        return False

    removed_index = None
    for index, (ref, cand) in enumerate(
        zip(reference_trace, candidate_trace)
    ):
        if ref == cand:
            continue
        if (
            ref is None
            and cand is not None
            and reference_trace[index + 1 :] == candidate_trace[index:]
        ):
            removed_index = index
        break
    if removed_index is None:
        # Removing a trailing silence exposes no earlier response.
        return False
    active_selector = None
    selector_ticks = (recipe.get("inputs") or {}).get("selector") or ()
    for index in range(removed_index + 1):
        selector = selector_ticks[index] if index < len(selector_ticks) else None
        if selector is not None:
            active_selector = selector
        if index >= removed_index or active_selector != "alpha":
            continue
        entries = _canonical_map_entries(candidate_trace[index])
        if entries:
            # A beta branch may establish a non-empty structural prefix before
            # the flip. Once alpha is active, however, an agreeing response
            # before the removed cycle proves this is not its first advance.
            return False
    if active_selector != "alpha":
        return False

    response_entries = _canonical_map_entries(
        candidate_trace[removed_index]
    )
    return bool(response_entries) and any(
        isinstance(entry, list)
        and len(entry) == 2
        and entry[1] != {"$remove": True}
        for entry in response_entries
    )


def _switch_flip_map_removal_relation(
    recipe: dict[str, Any],
    difference: dict[str, Any],
    reference: dict[str, Any],
    candidate: dict[str, Any],
    _family: dict[str, Any],
) -> bool:
    """Issues #105/#117/#119/#133/#145: a ``beta`` -> request-reply
    ``alpha`` flip makes the mapped child transiently invalid. Released
    hgraph publishes an empty TSD delta while retaining its previous element;
    hg_cpp publishes removals so the TSD contains exactly the currently-valid
    child outputs.

    Admit only removal-only candidate deltas for every currently-live output,
    exactly on those flips, opposite a canonical empty reference delta.
    RFC 0014 may additionally remove the single silent response-feedback tick
    immediately after one such flip. Everything else must match, including
    the response payload now delivered one cycle earlier.
    """
    if difference.get("classification") not in ("value", "length"):
        return False
    reference_trace = reference.get("trace")
    candidate_trace = candidate.get("trace")
    if not isinstance(reference_trace, list) or not isinstance(
        candidate_trace, list
    ):
        return False

    flip_to_alpha = set()
    active_selector = None
    for index, selector in enumerate(
        (recipe.get("inputs") or {}).get("selector") or ()
    ):
        if selector is None:
            continue
        if selector == "alpha" and active_selector == "beta":
            flip_to_alpha.add(index)
        active_selector = selector

    if len(reference_trace) != len(candidate_trace):
        if (
            len(reference_trace) != len(candidate_trace) + 1
            or len(flip_to_alpha) != 1
        ):
            return False
        flip = next(iter(flip_to_alpha))
        if (
            flip + 2 >= len(reference_trace)
            or reference_trace[flip + 1] is not None
            or reference_trace[flip + 2] is None
        ):
            return False
        # Align the traces only by removing the old response-feedback cycle.
        # The loop below still proves the flip removal and every payload.
        reference_trace = [
            *reference_trace[: flip + 1],
            *reference_trace[flip + 2 :],
        ]

    live_keys: set[str] = set()
    admitted = 0
    for index, (ref, cand) in enumerate(
        zip(reference_trace, candidate_trace)
    ):
        candidate_entries = _canonical_map_entries(cand)
        if ref != cand:
            reference_entries = _canonical_map_entries(ref)
            if (
                index not in flip_to_alpha
                or reference_entries != []
                or not candidate_entries
            ):
                return False
            removed_keys: set[str] = set()
            for entry in candidate_entries:
                if (
                    not isinstance(entry, list)
                    or len(entry) != 2
                    or not isinstance(entry[0], str)
                    or entry[1] != {"$remove": True}
                    or entry[0] in removed_keys
                ):
                    return False
                removed_keys.add(entry[0])
            if removed_keys != live_keys:
                return False
            admitted += 1

        if candidate_entries is None:
            continue
        for entry in candidate_entries:
            if (
                not isinstance(entry, list)
                or len(entry) != 2
                or not isinstance(entry[0], str)
            ):
                return False
            if entry[1] == {"$remove": True}:
                live_keys.discard(entry[0])
            else:
                live_keys.add(entry[0])
    return admitted >= 1


SWITCH_FLIP_VALID_SUBSET = "switch-flip-valid-subset-reduce"

RELATIONS = {
    TRACE_VALUE: _trace_value_relation,
    NO_CHANGE_ELISION: _no_change_elision_relation,
    VALID_SUBSET_REDUCE: _valid_subset_reduce_relation,
    SWITCH_FLIP_VALID_SUBSET: _switch_flip_valid_subset_relation,
    SWITCH_FLIP_MAP_REMOVAL: _switch_flip_map_removal_relation,
    REQUEST_REPLY_ONE_CYCLE_EARLIER: (
        _request_reply_one_cycle_earlier_relation
    ),
    NESTED_REQUEST_REPLY_ONE_CYCLE_EARLIER: (
        _nested_request_reply_one_cycle_earlier_relation
    ),
    KEY_SET_SIZE_NO_RETICK: _key_set_size_no_retick_relation,
    POLYMORPHIC_JSON_PRESERVES_LEAF: (
        _polymorphic_json_preserves_leaf_relation
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
