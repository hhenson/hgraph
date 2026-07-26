"""Differential campaign orchestration and failure promotion."""

from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path
from typing import Any, Iterable

from .catalog import CATALOG
from .compare import compare_outcomes, semantic_signature
from .coverage import coverage_report
from .environments import PARITY_ROOT, ParityEnvironments
from .issues import failure_fingerprint
from .model import Recipe
from .process import ReferenceTraceCache, run_recipe
from .reduce import reduce_recipe


def _load_known_divergences(path: Path) -> tuple[set[str], list[dict[str, Any]]]:
    """Exact fingerprints plus family rules from known_divergences.json.

    A family rule classifies every failure inside a documented deviation's
    parameter space as known, so new minimized variants (which mint new
    fingerprints) do not publish as issues: ``{"template": ...,
    "parameters_not_equal": {name: identity, ...}}`` matches a recipe of that
    template whose named parameters all differ from the stated identity.
    """
    try:
        raw = json.loads(path.read_text())
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


def _matches_known_family(
    recipe: dict[str, Any], families: list[dict[str, Any]]
) -> bool:
    parameters = recipe.get("parameters") or {}
    return any(
        recipe.get("template") == family["template"]
        and all(
            parameters.get(name) != identity
            for name, identity in family["parameters_not_equal"].items()
        )
        for family in families
    )


def _stable(results: list[dict[str, Any]]) -> bool:
    return len({semantic_signature(result) for result in results}) == 1


def _verify_pair(
    environments: ParityEnvironments,
    recipe: Recipe,
    *,
    timeout_seconds: float,
    attempts: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    reference = [
        run_recipe(
            environments.reference_python,
            recipe,
            timeout=timeout_seconds,
        )
        for _ in range(attempts)
    ]
    candidate = [
        run_recipe(
            environments.candidate_python,
            recipe,
            timeout=timeout_seconds,
        )
        for _ in range(attempts)
    ]
    return reference, candidate


def run_campaign(
    recipes: Iterable[Recipe],
    environments: ParityEnvironments,
    *,
    operator_inventory: Iterable[str] = (),
    timeout_seconds: float = 30.0,
    time_budget_seconds: float = 600.0,
    verify_replays: int = 3,
    reduce_failures: bool = True,
    reduction_budget_seconds: float = 120.0,
    known_divergences_path: Path | None = None,
    cache_path: Path | None = None,
) -> dict[str, Any]:
    recipes = list(recipes)
    started = time.monotonic()
    cache = ReferenceTraceCache(
        cache_path or PARITY_ROOT / "cache" / "reference-traces",
        environments.reference_identity,
    )
    known, known_families = _load_known_divergences(
        known_divergences_path
        or Path(__file__).with_name("known_divergences.json")
    )
    matches: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    known_failures: list[dict[str, Any]] = []
    attempted: list[Recipe] = []
    cache_hits = 0

    for recipe in recipes:
        if time.monotonic() - started >= time_budget_seconds:
            break
        attempted.append(recipe)
        spec = CATALOG[recipe.template]
        reference, cached = cache.run(
            environments.reference_python,
            recipe,
            timeout=timeout_seconds,
        )
        cache_hits += int(cached)
        candidate = run_recipe(
            environments.candidate_python,
            recipe,
            timeout=timeout_seconds,
        )

        if reference.get("status") != "ok":
            reference_replays, candidate_replays = _verify_pair(
                environments,
                recipe,
                timeout_seconds=timeout_seconds,
                attempts=verify_replays,
            )
            quarantined.append(
                {
                    "classification": "reference-failure",
                    "recipe": recipe.to_dict(),
                    "reference_stable": _stable(reference_replays),
                    "reference_replays": reference_replays,
                    "candidate_replays": candidate_replays,
                }
            )
            continue

        difference = compare_outcomes(
            reference,
            candidate,
            float_abs_tolerance=spec.float_abs_tolerance,
        )
        if difference is None:
            matches.append(
                {
                    "recipe_id": recipe.id,
                    "recipe_fingerprint": recipe.fingerprint,
                    "reference_cache_hit": cached,
                }
            )
            continue

        if _matches_known_family(recipe.to_dict(), known_families):
            # First-pass sanity check: a mismatch inside a documented
            # deviation's parameter space is a known failure; do not spend
            # verification replays or reduction budget minting a new
            # fingerprint for it.
            failure = {
                "original_recipe": recipe.to_dict(),
                "minimized_recipe": recipe.to_dict(),
                "difference": difference.to_dict(),
                "reference": reference,
                "candidate": candidate,
                "reduction": {
                    "attempts": 0,
                    "accepted": 0,
                    "timed_out": False,
                    "steps": [],
                },
            }
            failure["failure_fingerprint"] = failure_fingerprint(failure)
            known_failures.append(failure)
            continue

        reference_replays, candidate_replays = _verify_pair(
            environments,
            recipe,
            timeout_seconds=timeout_seconds,
            attempts=verify_replays,
        )
        if (
            not _stable(reference_replays)
            or any(result.get("status") != "ok" for result in reference_replays)
        ):
            quarantined.append(
                {
                    "classification": "reference-unstable",
                    "recipe": recipe.to_dict(),
                    "reference_stable": _stable(reference_replays),
                    "reference_replays": reference_replays,
                    "candidate_replays": candidate_replays,
                }
            )
            continue
        if not _stable(candidate_replays):
            quarantined.append(
                {
                    "classification": "candidate-unstable",
                    "recipe": recipe.to_dict(),
                    "reference_replays": reference_replays,
                    "candidate_replays": candidate_replays,
                }
            )
            continue
        verified_difference = compare_outcomes(
            reference_replays[0],
            candidate_replays[0],
            float_abs_tolerance=spec.float_abs_tolerance,
        )
        if verified_difference is None:
            quarantined.append(
                {
                    "classification": "transient-mismatch",
                    "recipe": recipe.to_dict(),
                    "reference_replays": reference_replays,
                    "candidate_replays": candidate_replays,
                }
            )
            continue

        minimized = recipe
        reduction_payload = {
            "attempts": 0,
            "accepted": 0,
            "timed_out": False,
            "steps": [],
        }
        if reduce_failures:

            def still_fails(candidate_recipe: Recipe) -> bool:
                candidate_spec = CATALOG[candidate_recipe.template]
                reference_result, _ = cache.run(
                    environments.reference_python,
                    candidate_recipe,
                    timeout=timeout_seconds,
                )
                if reference_result.get("status") != "ok":
                    return False
                candidate_result = run_recipe(
                    environments.candidate_python,
                    candidate_recipe,
                    timeout=timeout_seconds,
                )
                return (
                    compare_outcomes(
                        reference_result,
                        candidate_result,
                        float_abs_tolerance=candidate_spec.float_abs_tolerance,
                    )
                    is not None
                )

            reduction = reduce_recipe(
                recipe,
                still_fails,
                time_budget_seconds=min(
                    reduction_budget_seconds,
                    max(
                        1.0,
                        time_budget_seconds - (time.monotonic() - started),
                    ),
                ),
            )
            minimized = reduction.recipe
            reduction_payload = {
                key: value
                for key, value in reduction.to_dict().items()
                if key != "recipe"
            }

        final_reference, final_candidate = _verify_pair(
            environments,
            minimized,
            timeout_seconds=timeout_seconds,
            attempts=verify_replays,
        )
        if (
            not _stable(final_reference)
            or not _stable(final_candidate)
            or any(result.get("status") != "ok" for result in final_reference)
        ):
            quarantined.append(
                {
                    "classification": "reduced-case-unstable",
                    "recipe": minimized.to_dict(),
                    "reference_replays": final_reference,
                    "candidate_replays": final_candidate,
                    "reduction": reduction_payload,
                }
            )
            continue
        final_difference = compare_outcomes(
            final_reference[0],
            final_candidate[0],
            float_abs_tolerance=CATALOG[
                minimized.template
            ].float_abs_tolerance,
        )
        if final_difference is None:
            quarantined.append(
                {
                    "classification": "reduction-lost-mismatch",
                    "recipe": minimized.to_dict(),
                    "reduction": reduction_payload,
                }
            )
            continue
        failure = {
            "original_recipe": recipe.to_dict(),
            "minimized_recipe": minimized.to_dict(),
            "difference": final_difference.to_dict(),
            "reference": final_reference[0],
            "candidate": final_candidate[0],
            "reduction": reduction_payload,
        }
        failure["failure_fingerprint"] = failure_fingerprint(failure)
        if failure["failure_fingerprint"] in known or _matches_known_family(
            failure["minimized_recipe"], known_families
        ):
            known_failures.append(failure)
        else:
            failures.append(failure)

    elapsed = time.monotonic() - started
    coverage = coverage_report(
        attempted,
        operator_inventory=operator_inventory,
    )
    return {
        "schema_version": 1,
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(
            timespec="seconds"
        ),
        "reference_identity": environments.reference_identity,
        "candidate_identity": environments.candidate_identity,
        "candidate_fingerprint": environments.candidate_fingerprint,
        "summary": {
            "selected": len(recipes),
            "attempted": len(attempted),
            "matched": len(matches),
            "verified_failures": len(failures),
            "known_failures": len(known_failures),
            "quarantined": len(quarantined),
            "reference_cache_hits": cache_hits,
            "elapsed_seconds": round(elapsed, 3),
            "budget_exhausted": len(attempted) < len(recipes),
        },
        "matches": matches,
        "verified_failures": failures,
        "known_failures": known_failures,
        "quarantined": quarantined,
        "coverage": coverage,
    }


def render_campaign_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# hgraph differential parity campaign",
        "",
        f"- reference: `{report['reference_identity']}`",
        f"- candidate: `{report['candidate_identity']}`",
        f"- selected/attempted: {summary['selected']}/{summary['attempted']}",
        f"- matched: {summary['matched']}",
        f"- verified mismatches: {summary['verified_failures']}",
        f"- known mismatches: {summary['known_failures']}",
        f"- quarantined: {summary['quarantined']}",
        f"- reference cache hits: {summary['reference_cache_hits']}",
        f"- elapsed: {summary['elapsed_seconds']}s",
        "",
    ]
    if report["verified_failures"]:
        lines.extend(["## Verified mismatches", ""])
        for failure in report["verified_failures"]:
            difference = failure["difference"]
            lines.append(
                f"- `{failure['minimized_recipe']['id']}`: "
                f"{difference['classification']} at `{difference['path']}` "
                f"(`{failure['failure_fingerprint']}`)"
            )
        lines.append("")
    if report["quarantined"]:
        lines.extend(["## Quarantine", ""])
        for failure in report["quarantined"]:
            lines.append(
                f"- `{failure['recipe']['id']}`: {failure['classification']}"
            )
        lines.append("")
    if not report["verified_failures"] and not report["quarantined"]:
        lines.append("No new behavioral differences were found.")
        lines.append("")
    return "\n".join(lines)
