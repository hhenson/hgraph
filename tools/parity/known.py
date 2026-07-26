"""Known-divergence records shared by the campaign classifier and publisher.

``known_divergences.json`` carries two suppression shapes:

- ``divergences``: exact failure fingerprints.
- ``families``: a template plus a ``parameters_not_equal`` map matching every
  recipe of that template whose named parameters all differ from the stated
  identity. An empty map matches every recipe of the template (used when a
  documented deviation shifts every trace of a template, e.g. a designed
  transport-timing difference).

Family suppression covers only the documented deviation's shape: both
implementations completed and disagreed on trace content — a differing value
or a differing tick/field count (``value`` or ``length``), since ruled
no-tick/timing deviations surface as missing ticks or missing map fields.
Anything else — a candidate crash or a status difference — is not the
documented deviation and must continue through the normal pipeline.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_PATH = Path(__file__).with_name("known_divergences.json")


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


def matches_known_family(
    recipe: dict[str, Any], families: list[dict[str, Any]]
) -> bool:
    parameters = recipe.get("parameters") or {}
    # A missing parameter resolves to the template default, which is the
    # stated identity, so an omitted parameter never places a recipe inside a
    # deviation family. An empty parameters_not_equal map matches the whole
    # template.
    return any(
        recipe.get("template") == family["template"]
        and all(
            parameters.get(name, identity) != identity
            for name, identity in family["parameters_not_equal"].items()
        )
        for family in families
    )


def is_known_family_failure(
    recipe: dict[str, Any],
    difference: dict[str, Any],
    reference: dict[str, Any],
    candidate: dict[str, Any],
    families: list[dict[str, Any]],
) -> bool:
    """True when a mismatch is a documented deviation itself."""
    return (
        reference.get("status") == "ok"
        and candidate.get("status") == "ok"
        and difference.get("classification") in ("value", "length")
        and str(difference.get("path", "")).startswith("$.trace")
        and matches_known_family(recipe, families)
    )


