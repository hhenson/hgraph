"""Semantic coverage accounting for recipes and the operator catalogue."""

from __future__ import annotations

import itertools
import json
from collections import Counter
from typing import Any, Iterable

from .catalog import CATALOG
from .model import Recipe


def recipe_features(recipe: Recipe) -> tuple[str, ...]:
    spec = CATALOG[recipe.template]
    values = set(recipe.features) | set(spec.features)
    values.update(f"operator:{name}" for name in spec.operators)
    values.add(f"template:{recipe.template}")
    values.add(
        "ticks:short"
        if recipe.tick_count < 8
        else "ticks:medium"
        if recipe.tick_count <= 32
        else "ticks:long"
    )
    return tuple(sorted(values))


def coverage_report(
    recipes: Iterable[Recipe],
    *,
    operator_inventory: Iterable[str] = (),
) -> dict[str, Any]:
    recipes = list(recipes)
    feature_counts: Counter[str] = Counter()
    pair_counts: Counter[tuple[str, str]] = Counter()
    for recipe in recipes:
        features = recipe_features(recipe)
        feature_counts.update(features)
        pair_counts.update(itertools.combinations(features, 2))

    catalogued_operators = sorted(
        {
            operator
            for spec in CATALOG.values()
            for operator in spec.operators
        }
    )
    inventory = sorted(set(operator_inventory))
    return {
        "schema_version": 1,
        "recipe_count": len(recipes),
        "template_counts": dict(
            sorted(Counter(recipe.template for recipe in recipes).items())
        ),
        "feature_counts": dict(sorted(feature_counts.items())),
        "pair_counts": {
            " | ".join(pair): count
            for pair, count in sorted(pair_counts.items())
        },
        "catalogued_operators": catalogued_operators,
        "operator_inventory": inventory,
        "missing_operators": sorted(set(inventory) - set(catalogued_operators)),
    }


def render_coverage_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# hgraph parity coverage",
        "",
        f"- recipes: {report['recipe_count']}",
        f"- templates: {len(report['template_counts'])}",
        f"- semantic features: {len(report['feature_counts'])}",
        f"- observed feature pairs: {len(report['pair_counts'])}",
        f"- catalogued operators: {len(report['catalogued_operators'])}",
        f"- inventory operators not catalogued: {len(report['missing_operators'])}",
        "",
        "## Template counts",
        "",
    ]
    lines.extend(
        f"- `{name}`: {count}"
        for name, count in report["template_counts"].items()
    )
    lines.extend(["", "## Coverage frontier", ""])
    if report["missing_operators"]:
        lines.extend(
            f"- `{name}`" for name in report["missing_operators"][:100]
        )
        if len(report["missing_operators"]) > 100:
            lines.append(
                f"- … {len(report['missing_operators']) - 100} more"
            )
    else:
        lines.append("- Every inventoried operator is represented in the catalogue.")
    return "\n".join(lines) + "\n"


def coverage_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True) + "\n"
