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


def recipe_payload_strategy(*, min_ticks: int = 8, max_ticks: int = 32,
                            templates: tuple[str, ...] | None = None):
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
        # mode:postponed-annotations (issue #83 class): the same expression
        # occasionally runs from a PEP 563 module, so string annotations
        # exercise the signature-resolution path on both distributions.
        postponed = draw(st.sampled_from((False, False, False, True)))
        return {
            "template": "scalar_expression",
            "inputs": {"lhs": lhs, "rhs": rhs},
            "parameters": {
                "input_types": {"lhs": type_name, "rhs": type_name},
                "output_type": type_name,
                "expression": expression,
                "postponed_annotations": postponed,
            },
            "features": [
                *CATALOG["scalar_expression"].features,
                f"type:{type_name}",
                f"ticks:{'long' if count > 16 else 'medium'}",
                *(f"operator:{operation}" for operation in sorted(operations)),
                *(("mode:postponed-annotations",) if postponed else ()),
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

    # ---- templates targeted at the 2026-07 compatibility-issue classes ----

    _TEMPORAL_PROPERTIES = {
        "date": ("year", "month", "day"),
        "datetime": ("year", "month", "day", "hour", "minute", "second",
                     "microsecond"),
        "timedelta": ("days", "seconds", "microseconds"),
    }
    _TEMPORAL_METHODS = {
        "date": ("weekday", "isoweekday"),
        "datetime": ("weekday", "isoweekday"),
        "timedelta": ("total_seconds",),
    }
    _TEMPORAL_OUTPUT = {"total_seconds": "float"}

    @st.composite
    def temporal_expression(draw):
        import datetime as dt

        input_type = draw(st.sampled_from(("date", "datetime")))
        target = draw(st.sampled_from(("difference", "shifted", "input")))
        kind = "timedelta" if target == "difference" else input_type
        accessor = draw(st.sampled_from(
            _TEMPORAL_PROPERTIES[kind] + _TEMPORAL_METHODS[kind]))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        day = st.integers(min_value=0, max_value=36_500)
        micro = st.integers(min_value=0, max_value=86_399_999_999)

        def sample(with_none):
            base = dt.datetime(1990, 1, 1)
            offset = base + dt.timedelta(days=draw(day),
                                         microseconds=draw(micro))
            if input_type == "date":
                encoded = {"$date": offset.date().isoformat()}
            else:
                encoded = {"$datetime": offset.isoformat()}
            if with_none and draw(st.booleans()):
                return None
            return encoded

        lhs = [sample(False)] + [sample(True) for _ in range(count - 1)]
        inputs = {"lhs": lhs}
        parameters = {
            "input_type": input_type,
            "target": target,
            "accessor": accessor,
            "output_type": _TEMPORAL_OUTPUT.get(accessor, "int"),
            "postponed_annotations": draw(
                st.sampled_from((False, False, False, True))),
        }
        if target == "difference":
            inputs["rhs"] = [sample(False)] + [
                sample(True) for _ in range(count - 1)]
        elif target == "shifted":
            parameters["delta"] = {
                "days": draw(st.integers(min_value=-3_650, max_value=3_650)),
                "seconds": draw(st.integers(min_value=-10_000, max_value=10_000)),
                "microseconds": draw(
                    st.integers(min_value=-10_000, max_value=10_000)),
            }
        return {
            "template": "temporal_expression",
            "inputs": inputs,
            "parameters": parameters,
            "features": [
                *CATALOG["temporal_expression"].features,
                f"type:{input_type}",
                f"operator:{accessor}",
                f"temporal:{target}",
                *(("mode:postponed-annotations",)
                  if parameters["postponed_annotations"] else ()),
            ],
        }

    @st.composite
    def collection_size(draw):
        shape = draw(st.sampled_from(("str", "tss", "tsd", "tsl")))
        operation = ("len" if shape == "tsl"
                     else draw(st.sampled_from(("len", "is_empty", "contains"))))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        parameters = {"shape": shape, "operation": operation}
        keys = st.sampled_from(("a", "b", "c", "d"))
        small = st.integers(min_value=-5, max_value=5)
        if shape == "str":
            text = st.text(
                alphabet="abcdef", min_size=0, max_size=6)
            series = [draw(text)] + [
                draw(st.one_of(st.none(), text)) for _ in range(count - 1)]
            inputs = {"ts": series}
            if operation == "contains":
                parameters["probe"] = draw(st.sampled_from(("a", "cd", "")))
        elif shape == "tss":
            def delta():
                added = draw(st.lists(small, max_size=3))
                removed = draw(st.lists(small, max_size=2))
                return {"$set_delta": {"added": added, "removed": removed}}
            inputs = {"ts": [delta()] + [
                None if draw(st.booleans()) else delta()
                for _ in range(count - 1)]}
            if operation == "contains":
                parameters["probe"] = draw(small)
        elif shape == "tsd":
            def tick():
                entries = draw(st.lists(
                    st.tuples(keys, st.one_of(st.none(), small)),
                    min_size=1, max_size=3, unique_by=lambda kv: kv[0]))
                return {key: value for key, value in entries}
            inputs = {"ts": [tick() for _ in range(count)]}
            if operation == "contains":
                parameters["probe"] = draw(keys)
        else:
            series = st.one_of(st.none(), small)
            inputs = {
                "a": [draw(small)] + [draw(series) for _ in range(count - 1)],
                "b": [draw(small)] + [draw(series) for _ in range(count - 1)],
            }
        return {
            "template": "collection_size",
            "inputs": inputs,
            "parameters": parameters,
            "features": [
                *CATALOG["collection_size"].features,
                f"shape:{shape.upper() if shape != 'str' else 'TS'}",
                f"operator:{operation}",
            ],
        }

    @st.composite
    def lifecycle_state(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        small = st.integers(min_value=-10, max_value=10)
        values = [draw(small)] + [
            draw(st.one_of(st.none(), small)) for _ in range(count - 1)]
        spellings = ("default", "bare", "unannotated")
        parameters = {
            "start_spelling": draw(st.sampled_from(spellings)),
            "stop_spelling": draw(st.sampled_from((None,) + spellings)),
            "seed": draw(st.integers(min_value=-50, max_value=50)),
        }
        return {
            "template": "lifecycle_state",
            "inputs": {"value": values},
            "parameters": parameters,
            "features": [
                *CATALOG["lifecycle_state"].features,
                f"lifecycle:start-{parameters['start_spelling']}",
                f"lifecycle:stop-{parameters['stop_spelling'] or 'absent'}",
            ],
        }

    @st.composite
    def nested_higher_order(draw):
        # The composition breeding ground: churning key sets under map_/mesh_,
        # per-key switch_ branches flipping (nested graphs start/stop),
        # services/adaptors INSIDE the branches, optionally the whole
        # pipeline under an outer switch_ tearing it down and rebuilding it.
        inner = draw(st.sampled_from(
            ("arithmetic", "request_reply", "request_reply",
             "subscription", "adaptor")))
        outer = draw(st.sampled_from(("map", "map", "mesh")))
        subscription = inner == "subscription"
        wrap = False if subscription else draw(st.booleans())
        reduce_output = (True if wrap or inner == "adaptor"
                         else draw(st.sampled_from((True, True, False))))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        keys = ("k1", "k2", "k3")
        active: set = set()
        retired: set = set()
        first_key = draw(st.sampled_from(keys))
        active.add(first_key)
        values: list = [{first_key: draw(st.integers(-10, 10))}]
        for _ in range(count - 1):
            action = draw(st.sampled_from(("none", "update", "add", "remove")))
            if action == "none" or (action != "add" and not active):
                values.append(None)
                continue
            if action == "remove":
                key = draw(st.sampled_from(sorted(active)))
                values.append({key: {"$remove": True}})
                active.remove(key)
                retired.add(key)
                continue
            if action == "add":
                # The re-subscription timing deviation is ruled: with a
                # subscription leaf a removed key is never re-added.
                pool = [key for key in keys if key not in active
                        and not (subscription and key in retired)]
                if not pool:
                    values.append(None)
                    continue
                key = draw(st.sampled_from(pool))
                active.add(key)
            else:
                key = draw(st.sampled_from(sorted(active)))
            values.append({key: draw(st.integers(-10, 10))})
        selector = ["alpha"] + [
            draw(st.sampled_from((None, "alpha", "beta", "beta")))
            for _ in range(count - 1)]
        inputs = {"values": values, "selector": selector}
        parameters = {
            "inner": inner,
            "outer": outer,
            "wrap_switch": wrap,
            "reduce_output": reduce_output,
            "increment": draw(st.integers(min_value=-5, max_value=5)),
        }
        if wrap:
            inputs["outer_selector"] = ["on"] + [
                draw(st.sampled_from((None, "on", "off")))
                for _ in range(count - 1)]
        return {
            "template": "nested_higher_order",
            "inputs": inputs,
            "parameters": parameters,
            "features": [
                *CATALOG["nested_higher_order"].features,
                f"nested:{outer}",
                f"nested-leaf:{inner}",
                *(("nested:outer-switch",) if wrap else ()),
                *(("topology:reduce",) if reduce_output else ()),
            ],
        }

    @st.composite
    def data_frame_recording(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        small = st.integers(min_value=-20, max_value=20)
        values = [draw(small)] + [
            draw(st.one_of(st.none(), small)) for _ in range(count - 1)]
        return {
            "template": "data_frame_recording",
            "inputs": {"ts": values},
            "parameters": {
                "as_of_offset": draw(st.integers(min_value=1, max_value=100)),
            },
            "features": [*CATALOG["data_frame_recording"].features],
        }

    # (name, factory) pairs; a repeated name WEIGHTS that template in the
    # unrestricted draw (nested_higher_order is the composition breeding
    # ground and draws double).
    weighted = (
        ("scalar_expression", scalar_expression),
        ("feedback_accumulate", feedback_accumulate),
        ("switch_arithmetic", switch_arithmetic),
        ("tsd_map_reduce", tsd_map_reduce),
        ("service_reference", service_reference),
        ("service_request_reply", service_request_reply),
        ("service_subscription", service_subscription),
        ("adaptor_loopback", adaptor_loopback),
        ("service_adaptor_roundtrip", service_adaptor_roundtrip),
        ("context_switch", context_switch),
        ("operator_pipeline", operator_pipeline),
        ("tsd_key_set_pipeline", tsd_key_set_pipeline),
        ("mesh_key_set", mesh_key_set),
        ("temporal_expression", temporal_expression),
        ("collection_size", collection_size),
        ("lifecycle_state", lifecycle_state),
        ("data_frame_recording", data_frame_recording),
        ("nested_higher_order", nested_higher_order),
        ("nested_higher_order", nested_higher_order),
    )
    if templates is None:
        return st.one_of(*(factory() for _, factory in weighted))
    # A restricted profile draws ONLY the allowed strategies — selecting at
    # the source, never filtering the union (a post-hoc filter discards most
    # draws and trips hypothesis's filter_too_much health check).
    allowed = frozenset(templates)
    unknown = allowed - {name for name, _ in weighted}
    if unknown:
        raise ValueError(
            f"unknown generated template(s): {', '.join(sorted(unknown))}")
    return st.one_of(*(factory() for name, factory in weighted
                       if name in allowed))


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
        min_ticks=min_ticks, max_ticks=max_ticks, templates=templates
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
