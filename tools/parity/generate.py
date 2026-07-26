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
                # Identity zero only: with a non-identity zero the reference
                # result is capacity-history-dependent (documented reduce
                # deviation, parity_matrix.rst), so differential exploration
                # there measures the deviation, not candidate defects. The
                # corpus tracks the deviation cases explicitly.
                "zero": 0,
            },
            "features": [
                *CATALOG["tsd_map_reduce"].features,
                "type:int",
                "operator:map_",
                "operator:reduce",
            ],
        }

    def sparse_ticks(draw, count, values):
        return [draw(values)] + [
            draw(st.one_of(st.none(), values)) for _ in range(count - 1)
        ]

    @st.composite
    def service_reference(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        return {
            "template": "service_reference",
            "inputs": {
                "value": sparse_ticks(
                    draw, count, st.integers(min_value=-20, max_value=20)
                )
            },
            "parameters": {
                "base": draw(st.integers(min_value=-20, max_value=20)),
                "path": draw(st.sampled_from(("desk", "rates", "reference"))),
            },
            "features": [*CATALOG["service_reference"].features],
        }

    @st.composite
    def service_request_reply(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        return {
            "template": "service_request_reply",
            "inputs": {
                "value": sparse_ticks(
                    draw, count, st.integers(min_value=-20, max_value=20)
                )
            },
            "parameters": {
                "increment": draw(st.integers(min_value=-5, max_value=5)),
                "path": draw(st.sampled_from(("requests", "adjust", "reply"))),
            },
            "features": [*CATALOG["service_request_reply"].features],
        }

    @st.composite
    def service_subscription(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        # Each symbol subscribes at most once: re-subscribing a previously
        # computed symbol is the designed same-cycle sampling deviation
        # (services.rst scheduling matrix; issue #66), so differential
        # exploration there measures the deviation, not candidate defects.
        # The corpus tracks the re-subscription case explicitly.
        pool = list(
            draw(
                st.permutations(("a", "fx", "rates", "EURUSD", "long_symbol"))
            )
        )
        ticks = [pool.pop(0)]
        for _ in range(count - 1):
            if pool and draw(st.booleans()):
                ticks.append(pool.pop(0))
            else:
                ticks.append(None)
        return {
            "template": "service_subscription",
            "inputs": {"symbol": ticks},
            "parameters": {
                "multiplier": draw(st.integers(min_value=-3, max_value=10)),
                "path": draw(st.sampled_from(("quotes", "prices", "live"))),
            },
            "features": [*CATALOG["service_subscription"].features],
        }

    @st.composite
    def adaptor_loopback(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        return {
            "template": "adaptor_loopback",
            "inputs": {
                "value": sparse_ticks(
                    draw, count, st.integers(min_value=-20, max_value=20)
                )
            },
            "parameters": {
                "factor": draw(st.integers(min_value=-5, max_value=5)),
                "path": draw(st.sampled_from(("loopback", "io", "duplex"))),
            },
            "features": [*CATALOG["adaptor_loopback"].features],
        }

    @st.composite
    def service_adaptor_roundtrip(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        return {
            "template": "service_adaptor_roundtrip",
            "inputs": {
                "value": sparse_ticks(
                    draw, count, st.integers(min_value=-20, max_value=20)
                )
            },
            "parameters": {
                "increment": draw(st.integers(min_value=-5, max_value=5)),
            },
            "features": [*CATALOG["service_adaptor_roundtrip"].features],
        }

    @st.composite
    def context_switch(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        selector = sparse_ticks(
            draw, count, st.sampled_from(("add", "subtract"))
        )
        value = sparse_ticks(
            draw, count, st.integers(min_value=-20, max_value=20)
        )
        offset = sparse_ticks(
            draw, count, st.integers(min_value=-20, max_value=20)
        )
        return {
            "template": "context_switch",
            "inputs": {
                "selector": selector,
                "value": value,
                "offset": offset,
            },
            "features": [*CATALOG["context_switch"].features],
        }

    @st.composite
    def operator_pipeline(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        nonzero = st.one_of(
            st.integers(min_value=-9, max_value=-1),
            st.integers(min_value=1, max_value=9),
        )
        return {
            "template": "operator_pipeline",
            "inputs": {
                "lhs": sparse_ticks(
                    draw, count, st.integers(min_value=-50, max_value=50)
                ),
                "rhs": sparse_ticks(draw, count, nonzero),
                "choose_minimum": sparse_ticks(draw, count, st.booleans()),
            },
            "parameters": {"format_ref": draw(st.booleans())},
            "features": [*CATALOG["operator_pipeline"].features],
        }

    @st.composite
    def tsd_key_set_pipeline(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        universe = tuple(range(-4, 5))
        # A non-empty initial map: an empty first delta is the ruled
        # no-change-means-no-tick space (released hgraph ticks an empty map,
        # hg_cpp emits no tick — mesh_key_set inherits these inputs). The
        # corpus tracks the empty-initial case explicitly.
        initial = set(
            draw(st.sets(st.sampled_from(universe), min_size=1, max_size=4))
        )
        active = set(initial)
        ticks: list[Any] = [
            {
                "$map": [
                    [key, draw(st.integers(min_value=-20, max_value=20))]
                    for key in sorted(initial)
                ]
            }
        ]
        for _ in range(count - 1):
            action = draw(
                st.sampled_from(("none", "update", "add", "remove", "replace"))
            )
            if action == "none":
                ticks.append(None)
                continue
            available = [item for item in universe if item not in active]
            removable = sorted(active)
            entries: list[list[Any]] = []
            if action == "update" and removable:
                key = draw(st.sampled_from(removable))
                entries.append(
                    [key, draw(st.integers(min_value=-20, max_value=20))]
                )
            if action in {"add", "replace"} and available:
                key = draw(st.sampled_from(available))
                active.add(key)
                entries.append(
                    [key, draw(st.integers(min_value=-20, max_value=20))]
                )
            if action in {"remove", "replace"} and removable:
                key = draw(st.sampled_from(removable))
                active.remove(key)
                entries.append([key, {"$remove": True}])
            # An action that nets to no entries is the ruled empty-delta
            # no-tick space; emit a quiet tick instead.
            ticks.append({"$map": entries} if entries else None)
        return {
            "template": "tsd_key_set_pipeline",
            "inputs": {
                "values": ticks,
                "probe": sparse_ticks(
                    draw, count, st.integers(min_value=-4, max_value=4)
                ),
            },
            # dedup_size stays true: the undeduped size re-tick is the ruled
            # no-change-means-no-tick deviation (issue #65); differential
            # exploration there measures the deviation, not candidate
            # defects. The corpus tracks the dedup_size=false case.
            "parameters": {"dedup_size": True},
            "features": [*CATALOG["tsd_key_set_pipeline"].features],
        }

    @st.composite
    def mesh_key_set(draw):
        source = draw(tsd_key_set_pipeline())
        return {
            "template": "mesh_key_set",
            "inputs": {"values": source["inputs"]["values"]},
            "parameters": {
                "factor": draw(st.integers(min_value=-5, max_value=5))
            },
            "features": [*CATALOG["mesh_key_set"].features],
        }

    return st.one_of(
        scalar_expression(),
        feedback_accumulate(),
        switch_arithmetic(),
        tsd_map_reduce(),
        service_reference(),
        service_request_reply(),
        service_subscription(),
        adaptor_loopback(),
        service_adaptor_roundtrip(),
        context_switch(),
        operator_pipeline(),
        tsd_key_set_pipeline(),
        mesh_key_set(),
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
