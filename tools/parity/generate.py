"""Typed property-based recipe generation.

Hypothesis is an optional controller dependency and is imported lazily so the
installed hg_cpp runtime and its compatibility suite do not depend on it.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .catalog import CATALOG, validate_recipe
from .model import Recipe, SCHEMA_VERSION


def _recipe_from_payload(payload: dict[str, Any], seed: int) -> Recipe:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    suffix = hashlib.sha256(encoded.encode()).hexdigest()[:12]
    raw = {
        "schema_version": SCHEMA_VERSION,
        "id": f"generated-{payload['template'].replace('_', '-')}-{suffix}",
        "description": "Deterministically generated parity exploration case.",
        "template": payload["template"],
        "inputs": payload["inputs"],
        "parameters": payload.get("parameters", {}),
        "features": sorted(set(payload.get("features", ()))),
        "seed": seed,
    }
    recipe = Recipe.from_dict(raw)
    validate_recipe(recipe)
    return recipe


def recipe_payload_strategy(*, min_ticks: int = 8, max_ticks: int = 32):
    from hypothesis import strategies as st

    if not 1 <= min_ticks <= max_ticks <= 256:
        raise ValueError("tick bounds must satisfy 1 <= min <= max <= 256")

    @st.composite
    def scalar_expression(draw):
        type_name = draw(st.sampled_from(("int", "float")))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        if type_name == "int":
            scalar = st.integers(min_value=-20, max_value=20)
        else:
            scalar = st.floats(
                min_value=-20,
                max_value=20,
                allow_nan=False,
                allow_infinity=False,
                width=32,
            )
        lhs = [draw(scalar)] + [
            draw(st.one_of(st.none(), scalar)) for _ in range(count - 1)
        ]
        rhs = [draw(scalar)] + [
            draw(st.one_of(st.none(), scalar)) for _ in range(count - 1)
        ]
        leaves = st.one_of(
            st.just({"input": "lhs"}),
            st.just({"input": "rhs"}),
            scalar.map(lambda value: {"const": value}),
        )

        def extend(children):
            binary = st.builds(
                lambda operation, args: {
                    "op": operation,
                    "args": list(args),
                },
                st.sampled_from(("add", "sub", "mul")),
                st.tuples(children, children),
            )
            unary = st.builds(
                lambda operation, item: {"op": operation, "args": [item]},
                st.sampled_from(("neg", "pos", "abs", "dedup")),
                children,
            )
            return st.one_of(binary, unary)

        expression = draw(st.recursive(leaves, extend, max_leaves=8))
        operations: set[str] = set()

        def visit(item):
            if "op" in item:
                operations.add(item["op"])
                for argument in item["args"]:
                    visit(argument)

        visit(expression)
        return {
            "template": "scalar_expression",
            "inputs": {"lhs": lhs, "rhs": rhs},
            "parameters": {
                "input_types": {"lhs": type_name, "rhs": type_name},
                "output_type": type_name,
                "expression": expression,
            },
            "features": [
                *CATALOG["scalar_expression"].features,
                f"type:{type_name}",
                f"ticks:{'long' if count > 16 else 'medium'}",
                *(f"operator:{operation}" for operation in sorted(operations)),
            ],
        }

    @st.composite
    def feedback_accumulate(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        scalar = st.integers(min_value=-20, max_value=20)
        values = [draw(scalar)] + [
            draw(st.one_of(st.none(), scalar)) for _ in range(count - 1)
        ]
        return {
            "template": "feedback_accumulate",
            "inputs": {"value": values},
            "parameters": {"initial": draw(st.integers(-5, 5))},
            "features": [
                *CATALOG["feedback_accumulate"].features,
                "type:int",
                "operator:feedback",
            ],
        }

    @st.composite
    def switch_arithmetic(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        scalar = st.integers(min_value=-20, max_value=20)
        selector = [draw(st.sampled_from(("plus", "minus")))] + [
            draw(st.one_of(st.none(), st.sampled_from(("plus", "minus"))))
            for _ in range(count - 1)
        ]
        lhs = [draw(scalar)] + [
            draw(st.one_of(st.none(), scalar)) for _ in range(count - 1)
        ]
        rhs = [draw(scalar)] + [
            draw(st.one_of(st.none(), scalar)) for _ in range(count - 1)
        ]
        return {
            "template": "switch_arithmetic",
            "inputs": {"selector": selector, "lhs": lhs, "rhs": rhs},
            "features": [
                *CATALOG["switch_arithmetic"].features,
                "type:int",
                "operator:switch_",
            ],
        }

    @st.composite
    def tsd_map_reduce(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        active = {"a"}
        ticks: list[Any] = [{"a": draw(st.integers(-10, 10))}]
        keys = ("a", "b", "c")
        for _ in range(count - 1):
            action = draw(st.sampled_from(("none", "update", "add", "remove")))
            if action == "none":
                ticks.append(None)
                continue
            if action == "remove" and active:
                key = draw(st.sampled_from(sorted(active)))
                ticks.append({key: {"$remove": True}})
                active.remove(key)
                continue
            available = [key for key in keys if key not in active]
            if action == "add" and available:
                key = draw(st.sampled_from(available))
                active.add(key)
            elif active:
                key = draw(st.sampled_from(sorted(active)))
            else:
                key = draw(st.sampled_from(keys))
                active.add(key)
            ticks.append({key: draw(st.integers(-10, 10))})
        return {
            "template": "tsd_map_reduce",
            "inputs": {"values": ticks},
            "parameters": {
                "increment": draw(st.integers(-3, 3)),
                "zero": draw(st.integers(-3, 3)),
            },
            "features": [
                *CATALOG["tsd_map_reduce"].features,
                "type:int",
                "operator:map_",
                "operator:reduce",
            ],
        }

    return st.one_of(
        scalar_expression(),
        feedback_accumulate(),
        switch_arithmetic(),
        tsd_map_reduce(),
    )


def generate_recipes(
    count: int,
    *,
    seed: int,
    min_ticks: int = 8,
    max_ticks: int = 32,
    templates: tuple[str, ...] | None = None,
) -> list[Recipe]:
    from hypothesis import HealthCheck, Phase, given, seed as hypothesis_seed, settings

    if count < 1:
        return []
    payloads: list[dict[str, Any]] = []

    def collect(payload):
        payloads.append(payload)

    strategy = recipe_payload_strategy(
        min_ticks=min_ticks, max_ticks=max_ticks
    )
    if templates is not None:
        allowed = frozenset(templates)
        unknown = allowed - frozenset(CATALOG)
        if unknown:
            raise ValueError(
                f"unknown generated template(s): {', '.join(sorted(unknown))}"
            )
        strategy = strategy.filter(
            lambda payload: payload["template"] in allowed
        )
    generated = given(strategy)(collect)
    generated = settings(
        max_examples=count,
        database=None,
        deadline=None,
        phases=(Phase.generate,),
        suppress_health_check=(HealthCheck.too_slow,),
    )(generated)
    hypothesis_seed(seed)(generated)()

    recipes: list[Recipe] = []
    seen: set[str] = set()
    for payload in payloads:
        recipe = _recipe_from_payload(payload, seed)
        if recipe.fingerprint not in seen:
            seen.add(recipe.fingerprint)
            recipes.append(recipe)
    return recipes
