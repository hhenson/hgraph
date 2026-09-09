"""Typed property-based recipe generation.

Hypothesis is an optional controller dependency and is imported lazily so the
installed C++-first hgraph runtime and its compatibility suite do not depend on it.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .catalog import (CATALOG, _POLYMORPHIC_KEY_OPERATIONS,
                      REFERENCE_SOURCE_FEATURES, REFERENCE_SOURCE_TEMPLATES,
                      REFERENCE_SOURCES, validate_recipe)
from .model import Recipe, SCHEMA_VERSION


_DIVIDE_POLICIES = ("ERROR", "NAN", "INF", "NONE", "ZERO", "ONE")


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
    def scalar_operator_arguments(draw):
        operation = draw(st.sampled_from((
            "add", "sub", "mul", "div", "floordiv", "mod", "pow",
            "eq", "ne", "lt", "le", "gt", "ge",
        )))
        scalar_side = draw(st.sampled_from(("lhs", "rhs")))
        input_type = draw(st.sampled_from(("int", "float")))
        scalar_type = (
            input_type
            if operation in {"eq", "ne", "lt", "le", "gt", "ge"}
            else draw(st.sampled_from(("int", "float")))
        )
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))

        def numeric(type_name, minimum=-8, maximum=8):
            values = st.integers(min_value=minimum, max_value=maximum)
            return values if type_name == "int" else values.map(float)

        def nonzero(type_name):
            values = st.one_of(
                st.integers(min_value=-8, max_value=-1),
                st.integers(min_value=1, max_value=8),
            )
            return values if type_name == "int" else values.map(float)

        def sparse_series(values, first=None):
            initial = draw(values if first is None else st.just(first))
            return [initial] + [
                draw(st.one_of(st.none(), values)) for _ in range(count - 1)
            ]

        divide_by_zero = None
        output_type = (
            "bool"
            if operation in {"eq", "ne", "lt", "le", "gt", "ge"}
            else "float"
            if operation == "div" or "float" in (input_type, scalar_type)
            else "int"
        )

        if operation in {"div", "floordiv", "mod"}:
            if operation == "div":
                zero_policies = ("NAN", "INF", "NONE", "ZERO", "ONE")
            elif operation == "floordiv" and output_type == "int":
                zero_policies = ("NONE", "ZERO")
            elif operation == "floordiv":
                zero_policies = ("NAN", "INF", "NONE", "ZERO", "ONE")
            elif output_type == "int":
                zero_policies = ("NONE",)
            else:
                zero_policies = ("NAN", "INF", "NONE")
            exercise_zero = draw(st.booleans())
            divide_by_zero = draw(st.sampled_from(
                zero_policies if exercise_zero else _DIVIDE_POLICIES
            ))
            if scalar_side == "rhs":
                scalar_value = (
                    0 if scalar_type == "int" else 0.0
                ) if exercise_zero else draw(nonzero(scalar_type))
                ticks = sparse_series(numeric(input_type))
            else:
                scalar_value = draw(numeric(scalar_type))
                denominator = numeric(input_type)
                if exercise_zero:
                    ticks = sparse_series(
                        st.one_of(denominator, st.just(
                            0 if input_type == "int" else 0.0
                        )),
                        first=0 if input_type == "int" else 0.0,
                    )
                else:
                    ticks = sparse_series(nonzero(input_type))
        elif operation == "pow":
            divide_by_zero = draw(st.sampled_from(_DIVIDE_POLICIES))
            integer_output = output_type == "int"
            exercise_zero = not integer_output and draw(st.booleans())
            if scalar_side == "rhs":
                if exercise_zero:
                    scalar_value = -1 if scalar_type == "int" else -1.0
                    zero = 0 if input_type == "int" else 0.0
                    ticks = sparse_series(
                        numeric(input_type, minimum=0, maximum=5), first=zero
                    )
                    divide_by_zero = draw(st.sampled_from(
                        ("NAN", "INF", "NONE", "ZERO", "ONE")
                    ))
                else:
                    scalar_value = draw(numeric(
                        scalar_type, minimum=0, maximum=4
                    ))
                    ticks = sparse_series(numeric(
                        input_type,
                        minimum=-5 if integer_output else 0,
                        maximum=5,
                    ))
            else:
                if exercise_zero:
                    scalar_value = 0 if scalar_type == "int" else 0.0
                    negative_one = -1 if input_type == "int" else -1.0
                    ticks = sparse_series(
                        numeric(input_type, minimum=-1, maximum=4),
                        first=negative_one,
                    )
                    divide_by_zero = draw(st.sampled_from(
                        ("NAN", "INF", "NONE", "ZERO", "ONE")
                    ))
                else:
                    scalar_value = draw(numeric(
                        scalar_type, minimum=0, maximum=5
                    ))
                    ticks = sparse_series(numeric(
                        input_type, minimum=0, maximum=4
                    ))
        else:
            scalar_value = draw(numeric(scalar_type))
            ticks = sparse_series(numeric(input_type))

        parameters = {
            "operation": operation,
            "input_type": input_type,
            "scalar_type": scalar_type,
            "scalar_side": scalar_side,
            "scalar_value": scalar_value,
        }
        if divide_by_zero is not None:
            parameters["divide_by_zero"] = divide_by_zero
        return {
            "template": "scalar_operator_arguments",
            "inputs": {"value": ticks},
            "parameters": parameters,
            "features": [
                *CATALOG["scalar_operator_arguments"].features,
                f"operator:{operation}",
                f"argument:scalar-{scalar_side}",
                f"type:input-{input_type}",
                f"type:scalar-{scalar_type}",
                f"type:output-{output_type}",
                *(
                    (f"policy:divide-by-zero-{divide_by_zero.lower()}",)
                    if divide_by_zero is not None else ()
                ),
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
        symbols = st.sampled_from(("a", "fx", "rates", "EURUSD", "long_symbol"))
        ticks = [draw(symbols)]
        for _ in range(count - 1):
            ticks.append(draw(st.one_of(st.none(), symbols)))
        return {
            "template": "service_subscription",
            "inputs": {"symbol": ticks},
            "parameters": {
                # A zero multiplier turns distinct subscriptions into equal
                # outputs, measuring the ruled no-change re-tick behavior
                # instead of service correctness.
                "multiplier": draw(st.one_of(
                    st.integers(min_value=-3, max_value=-1),
                    st.integers(min_value=1, max_value=10),
                )),
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
    @st.composite
    def temporal_expression(draw):
        import datetime as dt

        input_type = draw(st.sampled_from(("date", "datetime")))
        target = draw(st.sampled_from(("difference", "shifted", "input")))
        kind = "timedelta" if target == "difference" else input_type
        # Released hgraph exposes method-call spellings as WiringPort values
        # rather than callable ports in this generated form. Keep those
        # candidate-only extensions in ordinary compatibility tests; the
        # differential generator uses the common property surface.
        accessor = draw(st.sampled_from(_TEMPORAL_PROPERTIES[kind]))
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
            "output_type": "int",
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
        operation = (
            "len"
            if shape == "tsl"
            else draw(st.sampled_from(
                ("len", "contains")
                if shape == "str"
                else ("len", "is_empty", "contains")
            ))
        )
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        parameters = {
            "shape": shape,
            "operation": operation,
            # Normalize released hgraph's equal-value re-ticks at the graph
            # boundary. Fixed corpus cases retain the unnormalized output to
            # pin the accepted no-change deviation.
            "normalize_output": True,
        }
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
                # added/removed MUST be disjoint (ruling 2026-07-28): an
                # element in both is incorrect data, rejected at set_delta
                # construction — the generator must not produce it.
                added = draw(st.lists(small, max_size=3))
                removed = [
                    value for value in draw(st.lists(small, max_size=2))
                    if value not in added
                ]
                return {"$set_delta": {"added": added, "removed": removed}}
            inputs = {"ts": [delta()] + [
                None if draw(st.booleans()) else delta()
                for _ in range(count - 1)]}
            if operation == "contains":
                parameters["probe"] = draw(small)
        elif shape == "tsd":
            # Full delta space: values, explicit lenient removals, and None
            # per-key no-ticks (ruling 2026-07-28: None means nothing
            # happened for that key — both runtimes agree).
            entry_value = st.one_of(
                small,
                st.just({"$remove_if_exists": True}),
                st.none(),
            )
            def tick():
                entries = draw(st.lists(
                    st.tuples(keys, entry_value),
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
            "state_access": draw(st.sampled_from(("attribute", "mapping"))),
        }
        return {
            "template": "lifecycle_state",
            "inputs": {"value": values},
            "parameters": parameters,
            "features": [
                *CATALOG["lifecycle_state"].features,
                f"lifecycle:start-{parameters['start_spelling']}",
                f"lifecycle:stop-{parameters['stop_spelling'] or 'absent'}",
                f"state-access:{parameters['state_access']}",
            ],
        }

    @st.composite
    def nested_higher_order(draw):
        # The composition breeding ground: churning key sets under map_/mesh_,
        # per-key switch_ branches flipping (nested graphs start/stop),
        # an adaptor around the reduced pipeline, and optionally the whole
        # pipeline under an outer switch_ tearing it down and rebuilding it.
        # Service-backed children have intentional invalid startup/round-trip
        # windows under nested map/reduce. Standalone service generators cover
        # their agreed behavior; fixed nested corpus cases pin the deviations.
        inner = draw(st.sampled_from(("arithmetic", "arithmetic", "adaptor")))
        outer = draw(st.sampled_from(("map", "map", "mesh")))
        # Feeding an adaptor-wrapped result through the outer switch creates
        # a reference-side wiring cycle. The inner switch and keyed churn
        # remain covered without that unsupported composition.
        wrap = False if inner == "adaptor" else draw(st.booleans())
        # Generated reductions always provide the identity zero in the
        # catalogue. Keeping one scalar output also lets dedup normalize the
        # separately ruled no-change re-tick behavior. Map-valued and
        # non-identity-zero deviations remain fixed corpus cases.
        reduce_output = True
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        keys = ("k1", "k2", "k3")
        active: set = set()
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
                continue
            if action == "add":
                pool = [key for key in keys if key not in active]
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
            for _ in range(count - 1)
        ]
        inputs = {"values": values, "selector": selector}
        parameters = {
            "inner": inner,
            "outer": outer,
            "wrap_switch": wrap,
            "reduce_output": reduce_output,
            "normalize_output": True,
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
                "topology:reduce",
                "reduction:explicit-identity-zero",
                "normalization:dedup",
            ],
        }

    @st.composite
    def data_frame_recording(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        small = st.integers(min_value=-20, max_value=20)
        values = [draw(small)] + [
            draw(st.one_of(st.none(), small)) for _ in range(count - 1)]
        as_of_offset = draw(st.integers(min_value=1, max_value=100))
        column_names = "configured" if as_of_offset % 2 else "default"
        return {
            "template": "data_frame_recording",
            "inputs": {"ts": values},
            "parameters": {
                "as_of_offset": as_of_offset,
                "column_names": column_names,
            },
            "features": [
                *CATALOG["data_frame_recording"].features,
                *(
                    ("configuration:custom-table-column-names",)
                    if column_names == "configured"
                    else ()
                ),
            ],
        }

    def event_payload(draw, index, *, force_kind=None):
        kind = force_kind or draw(st.sampled_from((
            "heartbeat", "create", "cancel"
        )))
        value = {"kind": kind, "event_id": f"event-{index}"}
        if kind == "create":
            value.update(
                order_id=f"order-{draw(st.integers(min_value=0, max_value=5))}",
                quantity=draw(st.integers(min_value=-20, max_value=20)),
            )
        elif kind == "cancel":
            value.update(
                order_id=f"order-{draw(st.integers(min_value=0, max_value=5))}",
                reason=draw(st.sampled_from(("user", "risk", "expired"))),
            )
        return value

    @st.composite
    def polymorphic_event_flow(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        operation = draw(st.sampled_from((
            "compute",
            "emit",
            "feedback",
            "feedback_default",
            "tuple",
            "fixed_tuple_frame",
            "set",
            "mapping",
            "collect_values",
            "batch",
            "window",
            "json_round_trip",
            "record_replay",
        )))
        # Normal campaign profiles use at least three ticks, so every ordinary
        # recipe crosses the abstract base with all three concrete layouts.
        # Keeping short, reducer-style generation within its requested bounds
        # makes the strategy usable by focused tests too.
        seed_kinds = ("heartbeat", "create", "cancel")
        events = [
            event_payload(draw, index, force_kind=seed_kinds[index])
            if index < len(seed_kinds)
            else (
                None
                if draw(st.booleans())
                else event_payload(draw, index)
            )
            for index in range(count)
        ]
        seed_triggers = (False, True, False)
        trigger = [
            seed_triggers[index]
            if index < len(seed_triggers)
            else draw(st.one_of(st.none(), st.booleans()))
            for index in range(count)
        ]
        if operation in {"mapping", "collect_values"}:
            # Released hgraph samples only on a same-cycle key/value update,
            # while the candidate intentionally also accepts a later key for
            # the current value. That settled sampling difference is not the
            # polymorphic-storage behavior this family targets.
            key = [
                None
                if event is None
                else draw(st.sampled_from(("order", "risk", "heartbeat")))
                for event in events
            ]
        else:
            seed_keys = ("order", "order", "heartbeat")
            key = [
                seed_keys[index]
                if index < len(seed_keys)
                else draw(st.one_of(
                    st.none(), st.sampled_from(("order", "risk", "heartbeat"))
                ))
                for index in range(count)
            ]
        return {
            "template": "polymorphic_event_flow",
            "inputs": {"events": events, "trigger": trigger, "key": key},
            "parameters": {"operation": operation},
            "features": [
                *CATALOG["polymorphic_event_flow"].features,
                f"polymorphic-operation:{operation}",
                "type:transitive-subclass",
                "lifecycle:leaf-change",
            ],
        }

    @st.composite
    def polymorphic_tsd_key(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        operation = draw(st.sampled_from(_POLYMORPHIC_KEY_OPERATIONS))
        names = ("front", "back", "spot")
        tenors = (None, "M1", "M2")

        def entry(index):
            return {
                "name": draw(st.sampled_from(names)),
                "tenor": draw(st.sampled_from(tenors)),
                "value": draw(st.integers(min_value=-8, max_value=8)),
            }

        # Seed with a base key and a descendant key so every recipe mixes the
        # two representations in one dictionary from the first cycle.
        ticks = [[
            {"name": "front", "tenor": None, "value": draw(
                st.integers(min_value=-8, max_value=8))},
            {"name": "front", "tenor": "M1", "value": draw(
                st.integers(min_value=-8, max_value=8))},
        ]]
        for index in range(1, count):
            if draw(st.booleans()):
                ticks.append(None)
                continue
            size = draw(st.integers(min_value=1, max_value=3))
            ticks.append([entry(index) for _ in range(size)])
        features = [
            *CATALOG["polymorphic_tsd_key"].features,
            f"key-operation:{operation}",
            "type:transitive-subclass",
        ]
        if operation == "non_peered_map":
            # Only this operation reaches _via_non_peered_ref.  Tagging the
            # template unconditionally would let passthrough and feedback
            # recipes count as non-peered coverage and mask the absence of the
            # one operation that exercises the target-link path.
            features += ["reference:REF", "binding:non-peered"]
        return {
            "template": "polymorphic_tsd_key",
            "inputs": {"entries": ticks},
            "parameters": {"operation": operation},
            "features": features,
        }

    @st.composite
    def polymorphic_field_projection(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        position = draw(st.sampled_from((
            "nested_field",
            "nested_field",
            "top_level_cast",
            "tsd_value",
            "tsb_field",
            "plain_field",
        )))
        symbols = ("BOM", "SPOT", "FRONT", "BACK")
        ticks = []
        for index in range(count):
            if index and draw(st.booleans()):
                ticks.append(None)
                continue
            ticks.append({
                "symbol": draw(st.sampled_from(symbols)),
                "underlying": draw(st.sampled_from(symbols)),
            })
        if all(tick is None for tick in ticks):
            ticks[0] = {"symbol": symbols[0], "underlying": symbols[1]}
        return {
            "template": "polymorphic_field_projection",
            "inputs": {"series": ticks},
            "parameters": {"position": position},
            "features": [
                *CATALOG["polymorphic_field_projection"].features,
                f"field-position:{position}",
                "type:transitive-subclass",
            ],
        }

    @st.composite
    def polymorphic_event_map(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        operation = draw(st.sampled_from((
            "map_compute",
            "map_emit",
            "map_feedback",
            "map_emit_feedback_outer",
        )))
        keys = ("order-a", "order-b", "heartbeat")
        if operation == "map_emit_feedback_outer":
            # emit(TSD) serializes keyed changes. Multiple same-cycle initial
            # keys have hash-order-dependent ordering in released hgraph, so
            # introduce keys on separate cycles and retain deterministic
            # payload ordering as the differential contract.
            active = {"order-a"}
            ticks = [{
                "order-a": event_payload(draw, 0, force_kind="create"),
            }]
        else:
            active = {"order-a", "heartbeat"}
            ticks = [{
                "order-a": event_payload(draw, 0, force_kind="create"),
                "heartbeat": event_payload(draw, 1, force_kind="heartbeat"),
            }]
        for index in range(1, count):
            action = draw(st.sampled_from((
                "none", "update", "update", "add", "remove"
            )))
            if action == "none":
                ticks.append(None)
                continue
            if action == "remove" and active:
                key = draw(st.sampled_from(sorted(active)))
                active.remove(key)
                ticks.append({key: {"$remove": True}})
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
            ticks.append({key: event_payload(draw, index + 1)})
        return {
            "template": "polymorphic_event_map",
            "inputs": {"events": ticks},
            "parameters": {"operation": operation},
            "features": [
                *CATALOG["polymorphic_event_map"].features,
                f"polymorphic-operation:{operation}",
                "type:transitive-subclass",
                "lifecycle:leaf-change",
                "lifecycle:key-removal",
            ],
        }

    @st.composite
    def structural_map_projection(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        projection = draw(st.sampled_from(
            (
                "lookup",
                "combine",
                "dispatch_combine",
                "dereference",
                "captured_combine",
            )
        ))
        row_keys = (
            ("left", "right")
            if projection == "captured_combine"
            else ("a", "b")
        )
        rows = [{
            row_keys[0]: {"value": 1, "quantity": 10, "label": "alpha"},
            row_keys[1]: {"value": 2, "quantity": 20, "label": "beta"},
        }]
        lookups = [{"left": "a", "right": "b"}]
        clients = {"left", "right"}
        for index in range(1, count):
            # Released hgraph replays captured TSD history into a late-starting
            # key-only child. Re-adding a client after an earlier removal can
            # therefore apply that removal to an empty replay dictionary.
            actions = (
                ("none", "row", "row", "lookup", "remove")
                if projection == "captured_combine"
                else ("none", "row", "row", "lookup", "add", "remove")
            )
            action = draw(st.sampled_from(actions))
            row_tick = None
            lookup_tick = None
            if action == "row":
                row = draw(st.sampled_from(row_keys))
                row_tick = {row: {
                    "value": draw(st.integers(min_value=-20, max_value=20)),
                    "quantity": draw(st.integers(min_value=-20, max_value=20)),
                    "label": f"{row}-{index}",
                }}
            elif action == "lookup" and clients:
                client = draw(st.sampled_from(sorted(clients)))
                lookup_tick = {client: draw(st.sampled_from(("a", "b")))}
            elif action == "add":
                available = [
                    key for key in ("left", "right", "extra") if key not in clients
                ]
                if available:
                    client = draw(st.sampled_from(available))
                    clients.add(client)
                    lookup_tick = {client: draw(st.sampled_from(("a", "b")))}
            elif action == "remove" and clients:
                client = draw(st.sampled_from(sorted(clients)))
                clients.remove(client)
                lookup_tick = {client: {"$remove": True}}
            rows.append(row_tick)
            lookups.append(lookup_tick)
        return {
            "template": "structural_map_projection",
            "inputs": {"lookups": lookups, "rows": rows},
            "parameters": {"projection": projection},
            "features": [
                *CATALOG["structural_map_projection"].features,
                f"projection:{projection}",
                "topology:typed-child-graph",
                "lifecycle:key-removal",
            ],
        }

    @st.composite
    def arrow_typed_projection(draw):
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        sides = [
            ("BUY", "SELL")[index]
            if index < 2
            else draw(st.one_of(
                st.none(), st.sampled_from(("BUY", "SELL"))
            ))
            for index in range(count)
        ]
        events = [
            event_payload(
                draw,
                index,
                force_kind=("heartbeat", "create")[index],
            )
            if index < 2
            else (
                None
                if draw(st.booleans())
                else event_payload(draw, index)
            )
            for index in range(count)
        ]
        projection = draw(st.sampled_from(("pair", "first", "second")))
        debug = draw(st.sampled_from(("none", "direct", "configured")))
        execution = draw(st.sampled_from(("graph", "eval")))
        return {
            "template": "arrow_typed_projection",
            "inputs": {"side": sides, "events": events},
            "parameters": {
                "projection": projection,
                "debug": debug,
                "execution": execution,
            },
            "features": [
                *CATALOG["arrow_typed_projection"].features,
                f"projection:{projection}",
                f"arrow-debug:{debug}",
                f"execution:{execution}",
                "type:transitive-subclass",
            ],
        }

    # ------------------------------------------------------------------
    # Operator-family templates (the 2026-09 coverage frontier).
    #
    # A family template wires exactly ONE operator, named by the recipe's
    # ``operation`` parameter, so one strategy per template varies the
    # operator together with the input types, options and tick history that
    # operator reads. Where the PR #808 divergence report already recorded a
    # difference reachable from the template language, the draw excludes
    # exactly that space and names the divergence: discovery must test the
    # agreed contract rather than spend examples rediscovering a recorded
    # difference. Each excluded case stays expressible, and the fixed corpus
    # keeps a recipe per family, so a resolved divergence only needs the
    # exclusion lifted here.

    def family_scalar(type_name):
        return {
            "bool": st.booleans(),
            "int": st.integers(min_value=-20, max_value=20),
            "float": st.floats(
                min_value=-20,
                max_value=20,
                allow_nan=False,
                allow_infinity=False,
                width=32,
            ),
            "str": st.text(alphabet="abcde", min_size=0, max_size=6),
        }[type_name]

    def stepped_int(draw, current, *, low=-20, high=20):
        """An integer in ``[low, high]`` that is never ``current``.

        Restating a value the collection already holds lands in the ruled
        no-change space (issue #65): one runtime re-emits the equal
        recompute and the other elides it -- upstream elides for some
        operators and hg_cpp for others, so a recipe that restates a value
        measures the elision policy rather than the operator.
        """
        span = high - low + 1
        delta = draw(st.integers(min_value=1, max_value=span - 1))
        base = low if current is None else current
        return low + (base - low + delta) % span

    def stepped_choice(draw, current, pool):
        """A member of ``pool`` that is never ``current`` (same reason)."""
        options = [item for item in pool if item != current]
        return draw(st.sampled_from(options))

    def family_ticks(draw, count, type_name):
        import datetime as _dt

        if type_name == "date":
            values = st.dates(
                min_value=_dt.date(1970, 1, 1),
                max_value=_dt.date(2099, 12, 31),
            ).map(lambda value: {"$date": value.isoformat()})
        elif type_name == "datetime":
            values = st.datetimes(
                min_value=_dt.datetime(1970, 1, 1),
                max_value=_dt.datetime(2099, 12, 31),
            ).map(lambda value: {"$datetime": value.isoformat()})
        else:
            values = family_scalar(type_name)
        return sparse_ticks(draw, count, values)

    def set_delta_ticks(draw, count, elements):
        """A live-set-consistent ``$set_delta`` history.

        added/removed stay disjoint (ruling 2026-07-28), a removal names an
        element the set holds, and a tick that would change nothing is a
        no-tick rather than an empty delta -- released hgraph publishes the
        empty delta where hg_cpp elides it, which is the separately ruled
        no-change deviation, not this family's subject.
        """
        live: set = set()
        ticks: list[Any] = []
        for index in range(count):
            added = sorted(
                {
                    value
                    for value in draw(st.lists(elements, max_size=3))
                    if value not in live
                }
            )
            removed = (
                sorted(draw(st.lists(
                    st.sampled_from(sorted(live)), max_size=2, unique=True,
                )))
                if live
                else []
            )
            removed = [value for value in removed if value not in added]
            if index == 0 and not added:
                added = [draw(elements)]
            if not added and not removed:
                ticks.append(None)
                continue
            live.update(added)
            live.difference_update(removed)
            ticks.append({"$set_delta": {"added": added, "removed": removed}})
        return ticks

    def tsd_int_ticks(draw, count, keys=("a", "b", "c"), bands=None):
        """A live-key-consistent ``TSD[str, TS[int]]`` history.

        ``bands`` maps a key to its own ``(low, high)`` value range, which
        ``flip`` needs: with disjoint ranges the map stays injective, so a
        key moving away cannot silently take another key's image with it.
        """
        live: dict[str, Any] = {}
        ticks: list[Any] = []
        for index in range(count):
            entries: dict[str, Any] = {}
            # One operation per key per tick: a remove and a re-add of the
            # same key collapse into a single entry, and the re-add would
            # then restate the value the key already had.
            for key in draw(st.lists(
                st.sampled_from(keys), min_size=1, max_size=2, unique=True,
            )):
                if index and key in live and draw(st.integers(0, 3)) == 0:
                    entries[key] = {"$remove": True}
                    live.pop(key, None)
                else:
                    low, high = (bands or {}).get(key, (-20, 20))
                    value = stepped_int(draw, live.get(key), low=low, high=high)
                    entries[key] = value
                    live[key] = value
            ticks.append(entries)
        return ticks

    @st.composite
    def unary_operator(draw):
        #: operation -> the input types drawn for it. Narrower than the
        #: catalogue's accepted set wherever a recorded divergence lives:
        #: ``str_`` of a bool (D1), a TSD (D2) or an emptied TSS (D3), and
        #: ``cast_`` parsing a string (D4), are drawn by no example.
        drawable = {
            "abs_": ("int", "float"),
            "cast_": ("int", "float"),
            "invert_": ("int", "bool"),
            "ln": ("float",),
            "neg_": ("int", "float"),
            "not_": ("bool", "int", "str"),
            "pos_": ("int", "float"),
            "sign": ("int", "float"),
            "str_": ("int", "date", "datetime"),
            "type_": ("bool", "int", "float", "str"),
        }
        operation = draw(st.sampled_from(sorted(drawable)))
        input_type = draw(st.sampled_from(drawable[operation]))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        parameters: dict[str, Any] = {
            "operation": operation,
            "input_type": input_type,
        }
        if operation == "ln":
            # D5: the released operator contracts a positive domain and
            # raises on 0 or a negative value where the candidate returns
            # the IEEE result, so the domain is drawn strictly positive.
            ticks = sparse_ticks(draw, count, st.floats(
                min_value=0.001,
                max_value=1000.0,
                allow_nan=False,
                allow_infinity=False,
            ))
        else:
            ticks = family_ticks(draw, count, input_type)
        if operation == "cast_":
            parameters["target_type"] = draw(
                st.sampled_from(("int", "float", "str"))
            )
        if operation == "type_":
            # A TS[type] value is a runtime-owned repr; the recipe records
            # that the operator wires and evaluates, as the corpus case does.
            parameters["sink_result"] = True
        return {
            "template": "unary_operator",
            "inputs": {"ts": ticks},
            "parameters": parameters,
            "features": [
                *CATALOG["unary_operator"].features,
                f"operator:{operation}",
                f"type:{input_type}",
            ],
        }

    @st.composite
    def binary_operator(draw):
        drawable = {
            "bit_and": ("bool", "int", "tss_int"),
            "bit_or": ("bool", "int", "tss_int"),
            "bit_xor": ("bool", "int", "tss_int"),
            "cmp_": ("int", "float", "str"),
            "divmod_": ("int", "float"),
            "if_cmp": ("int", "float", "str"),
            "lshift_": ("int",),
            "max_": ("int", "float", "str"),
            "min_": ("int", "float", "str"),
            "rshift_": ("int",),
        }
        operation = draw(st.sampled_from(sorted(drawable)))
        input_type = draw(st.sampled_from(drawable[operation]))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        if input_type == "tss_int":
            elements = st.integers(min_value=-6, max_value=6)
            inputs = {
                "lhs": set_delta_ticks(draw, count, elements),
                "rhs": set_delta_ticks(draw, count, elements),
            }
        elif operation in ("lshift_", "rshift_"):
            # D6/D7: a shift count of 64 or more is arbitrary precision
            # upstream and "shift count is too large" on the candidate.
            # Under 64 the two agree exactly, including on negative
            # operands; the drawn magnitudes also keep the result inside
            # 64 bits, which is where the recorded difference lives.
            inputs = {
                "lhs": sparse_ticks(
                    draw, count, st.integers(min_value=-2048, max_value=2048)
                ),
                "rhs": sparse_ticks(
                    draw, count, st.integers(min_value=0, max_value=16)
                ),
            }
        elif operation == "divmod_":
            # A zero divisor fails identically in both runtimes (recorded);
            # drawing it would trade a value comparison for a quarantined
            # reference failure.
            numerator = family_scalar(input_type)
            denominator = st.one_of(
                st.integers(min_value=-9, max_value=-1),
                st.integers(min_value=1, max_value=9),
            )
            if input_type == "float":
                denominator = denominator.map(float)
            inputs = {
                "lhs": sparse_ticks(draw, count, numerator),
                "rhs": sparse_ticks(draw, count, denominator),
            }
        else:
            inputs = {
                "lhs": family_ticks(draw, count, input_type),
                "rhs": family_ticks(draw, count, input_type),
            }
        return {
            "template": "binary_operator",
            "inputs": inputs,
            "parameters": {"operation": operation, "input_type": input_type},
            "features": [
                *CATALOG["binary_operator"].features,
                f"operator:{operation}",
                f"type:{input_type}",
                *(("shape:TSS",) if input_type == "tss_int" else ()),
            ],
        }

    @st.composite
    def string_operator(draw):
        operation = draw(st.sampled_from(
            ("join", "match_", "replace", "split", "substr")
        ))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        text = st.text(alphabet="abc012", min_size=0, max_size=6)
        parameters: dict[str, Any] = {"operation": operation}
        if operation == "join":
            parameters["separator"] = draw(st.sampled_from(("-", ", ", "")))
            inputs = {
                "s": sparse_ticks(draw, count, text),
                "t": sparse_ticks(draw, count, text),
            }
        elif operation == "substr":
            start = draw(st.integers(min_value=-4, max_value=4))
            parameters["start"] = start
            parameters["end"] = draw(
                st.integers(min_value=start, max_value=start + 5)
            )
            inputs = {"s": sparse_ticks(draw, count, text)}
        elif operation == "replace":
            parameters["pattern"] = draw(
                st.sampled_from(("a", "[0-9]+", "a+b", "[abc]", "b.c"))
            )
            # D8: the candidate treats a replacement's group references as
            # literal text, so the drawn replacements carry none.
            parameters["replacement"] = draw(
                st.sampled_from(("", "#", "zz", "-"))
            )
            inputs = {"s": sparse_ticks(draw, count, text)}
        elif operation == "match_":
            projection = draw(st.sampled_from(("is_match", "groups")))
            parameters["projection"] = projection
            parameters["pattern"] = draw(st.sampled_from(
                ("a(b+)", "([abc])([0-9])", "(a+)")
                if projection == "groups"
                else ("a", "[0-9]+", "a(b+)", "^ab")
            ))
            inputs = {"s": sparse_ticks(draw, count, text)}
        else:  # split
            separator = draw(st.sampled_from((",", "-", "::")))
            parameters["separator"] = separator
            target = draw(st.sampled_from(("tuple", "tsl")))
            parameters["to"] = target
            part = st.text(alphabet="abc012", min_size=0, max_size=3)
            if target == "tsl":
                # D9: a TSL target whose size does not match the number of
                # parts raises upstream and silently leaves the tail
                # invalid on the candidate, so every drawn tick splits into
                # exactly ``size`` parts. Each part also differs from the
                # part before it at the same index: a TSL element whose
                # value is unchanged is re-emitted upstream and elided by
                # the candidate (the ruled no-change deviation), which
                # would measure the elision rather than ``split``.
                size = draw(st.integers(min_value=1, max_value=4))
                parameters["size"] = size
                pool = ("a", "b", "c", "d0", "e1")
                previous: list[Any] = [None] * size
                ticks: list[Any] = []
                for index in range(count):
                    if index and draw(st.booleans()):
                        ticks.append(None)
                        continue
                    previous = [
                        stepped_choice(draw, previous[position], pool)
                        for position in range(size)
                    ]
                    ticks.append(separator.join(previous))
                inputs = {"s": ticks}
            else:
                values = st.lists(part, min_size=1, max_size=4).map(
                    separator.join
                )
                inputs = {"s": sparse_ticks(draw, count, values)}
        return {
            "template": "string_operator",
            "inputs": inputs,
            "parameters": parameters,
            "features": [
                *CATALOG["string_operator"].features,
                f"operator:{operation}",
            ],
        }

    @st.composite
    def stream_shape(draw):
        drawable = {
            "drop": ("int", "float", "str"),
            "drop_dups": ("int", "float", "str"),
            "freeze": ("int", "float"),
            "lag": ("int", "float", "str"),
            "schedule": ("int",),
            "slice_": ("int", "float", "str"),
            "step": ("int", "float", "str"),
            "take": ("int", "float", "str"),
            "throttle": ("int", "float", "str"),
            "to_window": ("int", "float"),
            "until_true": ("int", "float"),
        }
        operation = draw(st.sampled_from(sorted(drawable)))
        input_type = draw(st.sampled_from(drawable[operation]))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        parameters: dict[str, Any] = {
            "operation": operation,
            "input_type": input_type,
        }
        if operation == "lag":
            if draw(st.booleans()):
                parameters["period_micros"] = draw(
                    st.integers(min_value=1, max_value=8)
                )
            else:
                parameters["count"] = draw(
                    st.integers(min_value=0, max_value=8)
                )
        elif operation == "drop":
            # N4 (new, this change): with a timedelta the released ``drop``
            # emits the buffered value at the cycle the window expires even
            # when nothing ticked there; the candidate emits only values
            # that tick after it. A count is the agreed spelling.
            parameters["count"] = draw(st.integers(min_value=0, max_value=8))
        elif operation == "take":
            # D10: the released ``take`` accepts INT_OR_TIME_DELTA and the
            # candidate lost the timedelta overload, so a count is drawn.
            parameters["count"] = draw(st.integers(min_value=0, max_value=8))
        elif operation == "schedule":
            parameters["period_micros"] = draw(
                st.integers(min_value=1, max_value=8)
            )
            parameters["max_ticks"] = draw(
                st.integers(min_value=1, max_value=8)
            )
            parameters["initial_delay"] = draw(st.booleans())
        elif operation == "slice_":
            start = draw(st.integers(min_value=0, max_value=4))
            parameters["start"] = start
            parameters["stop"] = draw(
                st.integers(min_value=start, max_value=start + 6)
            )
            parameters["step_size"] = draw(
                st.integers(min_value=1, max_value=4)
            )
        elif operation == "step":
            parameters["step_size"] = draw(st.integers(min_value=1, max_value=4))
        elif operation == "throttle":
            parameters["period_micros"] = draw(
                st.integers(min_value=1, max_value=8)
            )
            parameters["delay_first_tick"] = draw(st.booleans())
        elif operation == "to_window":
            parameters["count"] = draw(st.integers(min_value=1, max_value=6))
            # D11: with min_count above one the reference emits from the
            # first tick while the candidate withholds until the window is
            # full. min_count == 1 is the space the two agree on.
            parameters["min_count"] = 1
            parameters["reduction"] = (
                draw(st.sampled_from(("sum_", "mean")))
                if input_type == "float"
                else "sum_"
            )
        elif operation in ("freeze", "until_true"):
            parameters["threshold"] = draw(
                st.integers(min_value=-20, max_value=20)
            )
        return {
            "template": "stream_shape",
            "inputs": {"ts": family_ticks(draw, count, input_type)},
            "parameters": parameters,
            "features": [
                *CATALOG["stream_shape"].features,
                f"operator:{operation}",
                f"type:{input_type}",
            ],
        }

    @st.composite
    def flow_control(draw):
        drawable = {
            # D12/D13: over a TSD the reference republishes the whole
            # collection when ``filter_`` re-opens and clears the arm
            # ``route_by_index`` leaves; both draw scalar inputs only.
            "filter_": ("bool", "int", "float", "str"),
            "gate": ("bool", "int", "float", "str", "tsd"),
            "if_": ("bool", "int", "float", "str", "tsd"),
            "if_true": ("bool", "int", "float", "str", "tsd"),
            "route_by_index": ("bool", "int", "float", "str"),
            "sample": ("bool", "int", "float", "str"),
        }
        operation = draw(st.sampled_from(sorted(drawable)))
        input_type = draw(st.sampled_from(drawable[operation]))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        parameters: dict[str, Any] = {
            "operation": operation,
            "input_type": input_type,
        }
        if operation == "gate":
            # A buffer that can hold every tick: an overflow raises in both
            # runtimes, trading the value comparison for a quarantined
            # reference failure.
            parameters["buffer_length"] = draw(st.integers(
                min_value=min(count, 64), max_value=64,
            ))
        elif operation == "if_":
            parameters["branch"] = draw(st.sampled_from(("true", "false")))
        elif operation == "if_true":
            parameters["tick_once_only"] = draw(st.booleans())
        elif operation == "route_by_index":
            parameters["size"] = draw(st.integers(min_value=1, max_value=4))
        if operation == "route_by_index":
            size = parameters["size"]
            condition = sparse_ticks(
                draw, count, st.integers(min_value=0, max_value=size - 1)
            )
        else:
            condition = sparse_ticks(draw, count, st.booleans())
        value = (
            tsd_int_ticks(draw, count)
            if input_type == "tsd"
            else family_ticks(draw, count, input_type)
        )
        return {
            "template": "flow_control",
            "inputs": {"condition": condition, "ts": value},
            "parameters": parameters,
            "features": [
                *CATALOG["flow_control"].features,
                f"operator:{operation}",
                f"type:{input_type}",
                *(("shape:TSD",) if input_type == "tsd" else ()),
            ],
        }

    @st.composite
    def set_operator(draw):
        operation = draw(st.sampled_from((
            "bit_and", "bit_or", "bit_xor", "difference", "intersection",
            "symmetric_difference", "union",
        )))
        element_type = draw(st.sampled_from(("int", "str")))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        elements = (
            st.integers(min_value=-6, max_value=6)
            if element_type == "int"
            else st.sampled_from(("a", "b", "c", "d", "e"))
        )
        names = ["a", "b"]
        # N1 (new, this change): a THREE-input ``intersection`` or
        # ``symmetric_difference`` folds with a zero the released package
        # cannot resolve for a TSS (only zero_int/zero_float/zero_str
        # exist), so it fails at wiring upstream and evaluates on the
        # candidate. ``union`` is the variadic spelling both accept.
        if operation == "union":
            names += ["c"] if draw(st.booleans()) else []
        return {
            "template": "set_operator",
            "inputs": {
                name: set_delta_ticks(draw, count, elements) for name in names
            },
            "parameters": {
                "operation": operation,
                "element_type": element_type,
            },
            "features": [
                *CATALOG["set_operator"].features,
                f"operator:{operation}",
                f"type:{element_type}",
                f"arity:{len(names)}",
            ],
        }

    @st.composite
    def tsd_operator(draw):
        operation = draw(st.sampled_from((
            "collapse_keys", "flip", "flip_keys", "merge", "partition",
            "rekey", "uncollapse_keys", "unpartition",
        )))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        outer = ("a", "b", "c")
        letters = ("x", "y", "z")

        def nested_ticks():
            """``TSD[str, TSD[int, TS[str]]]`` -- $map carries the int keys."""
            live: dict[tuple, Any] = {}
            ticks: list[Any] = []
            for index in range(count):
                key = draw(st.sampled_from(outer))
                inner = draw(st.integers(min_value=1, max_value=3))
                slot = (key, inner)
                if index and slot in live and draw(st.integers(0, 3)) == 0:
                    live.pop(slot, None)
                    entry = [inner, {"$remove": True}]
                else:
                    value = stepped_choice(draw, live.get(slot), letters)
                    live[slot] = value
                    entry = [inner, value]
                ticks.append({key: {"$map": [entry]}})
            return ticks

        if operation == "flip":
            # D15: after a key moves, a value another key still holds must
            # survive; upstream drops it. Disjoint per-key value bands keep
            # the map injective, which is the agreed space.
            bands = {
                key: ((position + 1) * 100, (position + 1) * 100 + 9)
                for position, key in enumerate(outer)
            }
            inputs = {"ts": tsd_int_ticks(draw, count, outer, bands)}
        elif operation in ("collapse_keys", "flip_keys"):
            inputs = {"ts": nested_ticks()}
        elif operation == "uncollapse_keys":
            live_pairs: dict[tuple, Any] = {}
            ticks = []
            for index in range(count):
                pair = (
                    draw(st.sampled_from(outer)),
                    draw(st.integers(min_value=1, max_value=3)),
                )
                key = {"$tuple": [pair[0], pair[1]]}
                if index and pair in live_pairs and draw(st.integers(0, 3)) == 0:
                    live_pairs.pop(pair, None)
                    ticks.append({"$map": [[key, {"$remove": True}]]})
                else:
                    value = stepped_choice(draw, live_pairs.get(pair), letters)
                    live_pairs[pair] = value
                    ticks.append({"$map": [[key, value]]})
            inputs = {"ts": ticks}
        elif operation == "unpartition":
            # Disjoint inner key spaces: the same inner key under two
            # partitions makes the unpartitioned result order-dependent.
            partitions = {"x": ("a", "b"), "y": ("c", "d")}
            live_pairs = {}
            ticks = []
            for index in range(count):
                label = draw(st.sampled_from(sorted(partitions)))
                key = draw(st.sampled_from(partitions[label]))
                slot = (label, key)
                if index and slot in live_pairs and draw(st.integers(0, 3)) == 0:
                    live_pairs.pop(slot, None)
                    ticks.append({label: {key: {"$remove": True}}})
                else:
                    value = stepped_int(draw, live_pairs.get(slot))
                    live_pairs[slot] = value
                    ticks.append({label: {key: value}})
            inputs = {"ts": ticks}
        elif operation == "merge":
            # N5 (new, this change): removing a key from one input while the
            # other still holds it AT THE SAME VALUE leaves the merged value
            # unchanged; upstream emits nothing and the candidate re-emits
            # it. Disjoint value bands per input make an equal fallback
            # impossible while keeping the key spaces overlapping, so
            # removals stay covered.
            inputs = {
                "ts": tsd_int_ticks(
                    draw, count, outer, {key: (-20, -1) for key in outer}
                ),
                "other": tsd_int_ticks(
                    draw, count, outer, {key: (1, 20) for key in outer}
                ),
            }
        else:  # partition / rekey
            targets = (
                {"a": "x", "b": "y", "c": "z"}
                if operation == "rekey"
                # A partition label is a grouping, so collisions are the
                # subject; a rekey target must stay injective or the result
                # depends on which source wins.
                else {"a": "x", "b": "x", "c": "y"}
            )
            keys: list[Any] = [dict(targets)] + [
                None if draw(st.booleans()) else dict(targets)
                for _ in range(count - 1)
            ]
            inputs = {"ts": tsd_int_ticks(draw, count), "keys": keys}
        parameters: dict[str, Any] = {"operation": operation}
        if operation == "uncollapse_keys":
            parameters["remove_empty"] = draw(st.booleans())
        return {
            "template": "tsd_operator",
            "inputs": inputs,
            "parameters": parameters,
            "features": [
                *CATALOG["tsd_operator"].features,
                f"operator:{operation}",
            ],
        }

    @st.composite
    def tsl_operator(draw):
        drawable = {
            "all_": ("bool",),
            "any_": ("bool",),
            # ``index_of`` is corpus-only: its index recomputes to the same
            # value on most tick histories, and an equal recompute is the
            # ruled no-change space (issue #65) rather than this family's
            # subject. The fixed corpus recipe keeps the operator covered.
            "merge": ("bool", "int", "float", "str"),
            "race": ("bool", "int", "float", "str"),
        }
        operation = draw(st.sampled_from(sorted(drawable)))
        input_type = draw(st.sampled_from(drawable[operation]))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        if draw(st.booleans()):
            names = ("a", "b", "c")
        else:
            names = ("a", "b")
        return {
            "template": "tsl_operator",
            "inputs": {
                name: family_ticks(draw, count, input_type) for name in names
            },
            "parameters": {"operation": operation, "input_type": input_type},
            "features": [
                *CATALOG["tsl_operator"].features,
                f"operator:{operation}",
                f"type:{input_type}",
                f"arity:{len(names)}",
            ],
        }

    @st.composite
    def temporal_component(draw):
        operation = draw(st.sampled_from((
            "day_of_month", "evaluation_time_in_range", "explode",
            "last_modified_date", "last_modified_time", "month_of_year",
            "year",
        )))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        parameters: dict[str, Any] = {"operation": operation}
        if operation == "evaluation_time_in_range":
            start = draw(st.integers(min_value=1, max_value=32))
            parameters["start_micros"] = start
            parameters["end_micros"] = draw(
                st.integers(min_value=start, max_value=start + 32)
            )
        input_type = (
            "date"
            if operation in
            ("day_of_month", "explode", "month_of_year", "year")
            else "int"
        )
        components = {"day_of_month": "day", "month_of_year": "month",
                      "year": "year"}
        if operation in components:
            # N3 (new, this change): the released date-component accessors
            # dedup -- an unchanged component does not re-tick -- where the
            # candidate emits on every input tick. Consecutive draws step the
            # component the operation reads, which is the agreed space.
            # ``explode`` needs no such step: its per-element deltas agree.
            component = components[operation]
            previous: Any = None
            ticks: list[Any] = []
            for index in range(count):
                if index and draw(st.booleans()):
                    ticks.append(None)
                    continue
                parts = {
                    "year": draw(st.integers(min_value=1970, max_value=2099)),
                    "month": draw(st.integers(min_value=1, max_value=12)),
                    # 28 keeps every (year, month, day) triple a real date.
                    "day": draw(st.integers(min_value=1, max_value=28)),
                }
                bounds = {"year": (1970, 2099), "month": (1, 12), "day": (1, 28)}
                low, high = bounds[component]
                previous = stepped_int(draw, previous, low=low, high=high)
                parts[component] = previous
                ticks.append({"$date": "{year:04d}-{month:02d}-{day:02d}".format(
                    **parts
                )})
        else:
            ticks = family_ticks(draw, count, input_type)
        return {
            "template": "temporal_component",
            "inputs": {"ts": ticks},
            "parameters": parameters,
            "features": [
                *CATALOG["temporal_component"].features,
                f"operator:{operation}",
                f"type:{input_type}",
            ],
        }

    @st.composite
    def table_round_trip(draw):
        operation = draw(st.sampled_from(("round_trip", "schema")))
        shape = draw(st.sampled_from(
            ("ts_int", "ts_str")
            if operation == "schema"
            else ("ts_int", "ts_str", "tsd", "tsb")
        ))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        if shape == "ts_int":
            ticks = family_ticks(draw, count, "int")
        elif shape == "ts_str":
            ticks = family_ticks(draw, count, "str")
        elif shape == "tsd":
            ticks = tsd_int_ticks(draw, count)
        else:
            ticks = [
                {
                    "x": draw(st.integers(min_value=-20, max_value=20)),
                    "y": draw(st.text(alphabet="abc", min_size=0, max_size=4)),
                }
                for _ in range(count)
            ]
        return {
            "template": "table_round_trip",
            "inputs": {"ts": ticks},
            "parameters": {"operation": operation, "shape": shape},
            "features": [
                *CATALOG["table_round_trip"].features,
                f"operator:{operation}",
                f"shape:{shape}",
            ],
        }

    @st.composite
    def json_round_trip(draw):
        operation = draw(st.sampled_from(("decode", "round_trip")))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        # Floats are left out of the drawn documents: the JSON text is
        # compared verbatim, so a float would compare the two runtimes'
        # shortest-repr rules rather than the codec.
        leaves = st.one_of(
            st.none(),
            st.booleans(),
            st.integers(min_value=-1000, max_value=1000),
            st.text(alphabet="abc ", min_size=0, max_size=4),
        )
        documents = st.recursive(
            leaves,
            lambda children: st.one_of(
                st.lists(children, max_size=3),
                st.dictionaries(
                    st.sampled_from(("a", "b", "c")), children, max_size=3
                ),
            ),
            max_leaves=6,
        ).map(lambda value: json.dumps(value))
        return {
            "template": "json_round_trip",
            "inputs": {"ts": sparse_ticks(draw, count, documents)},
            "parameters": {"operation": operation},
            "features": [
                *CATALOG["json_round_trip"].features,
                f"operator:{operation}",
            ],
        }

    @st.composite
    def data_frame_conversion(draw):
        operation = draw(st.sampled_from(("to_data_frame", "from_data_frame")))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        parameters: dict[str, Any] = {"operation": operation}
        if operation == "from_data_frame":
            offsets = sorted(draw(st.sets(
                st.integers(min_value=1, max_value=64),
                min_size=1,
                max_size=8,
            )))
            parameters["rows"] = [
                [offset, draw(st.integers(min_value=-20, max_value=20))]
                for offset in offsets
            ]
        return {
            "template": "data_frame_conversion",
            "inputs": {"ts": family_ticks(draw, count, "int")},
            "parameters": parameters,
            "features": [
                *CATALOG["data_frame_conversion"].features,
                f"operator:{operation}",
            ],
        }

    @st.composite
    def compound_scalar_field(draw):
        operation = draw(
            st.sampled_from(("downcast_ref", "getattr_", "setattr_"))
        )
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        # D23 is avoided by the executor's model (not frozen) and by naming
        # a declared attribute; the draw only chooses whether the value is
        # the Derived leaf, which downcast_ref requires.
        derived = operation == "downcast_ref"

        def event():
            tick = {"a": draw(st.integers(min_value=-20, max_value=20))}
            if derived:
                tick["b"] = draw(
                    st.text(alphabet="abc", min_size=0, max_size=4)
                )
            return tick

        events: list[Any] = [event()] + [
            None if draw(st.booleans()) else event() for _ in range(count - 1)
        ]
        inputs: dict[str, Any] = {"event": events}
        if operation == "setattr_":
            # ``setattr_(event, "a", value)`` over a one-field model yields
            # Base(a=value), so a repeated (or absent) value tick is an
            # equal recompute: the ruled no-change space, not the operator.
            assigned: list[Any] = []
            current = None
            for _ in range(count):
                current = stepped_int(draw, current)
                assigned.append(current)
            inputs["value"] = assigned
        return {
            "template": "compound_scalar_field",
            "inputs": inputs,
            "parameters": {"operation": operation},
            "features": [
                *CATALOG["compound_scalar_field"].features,
                f"operator:{operation}",
            ],
        }

    @st.composite
    def sink_operator(draw):
        operation = draw(st.sampled_from(
            ("assert_", "debug_print", "log_", "null_sink", "print_")
        ))
        count = draw(st.integers(min_value=min_ticks, max_value=max_ticks))
        # N2 (new, this change): the template compares the series with an
        # integer threshold, and released hgraph has no
        # ``gt_(TS[float], int)`` overload -- it fails at wiring where the
        # candidate evaluates. ``assert_`` therefore draws integer series.
        input_type = (
            "int"
            if operation == "assert_"
            else draw(st.sampled_from(("bool", "int", "float", "str")))
        )
        parameters: dict[str, Any] = {
            "operation": operation,
            "input_type": input_type,
        }
        # D14: released hgraph installs a stdout logging handler where the
        # candidate installs none, so ``log_`` writes nothing observable and
        # ``debug_print`` prefixes each line with a wall-clock stamp. The
        # standard output of both is also unstable across the boolean
        # ("True"/"true") and integral-float ("3.0"/"3") renderings, so a
        # captured recipe draws ``print_``/``null_sink``/``assert_`` over the
        # types whose rendering the two runtimes agree on.
        capture = (
            draw(st.sampled_from(("none", "stdout")))
            if operation in ("assert_", "null_sink", "print_")
            and input_type in ("int", "str")
            else "none"
        )
        parameters["capture"] = capture
        if operation == "assert_":
            # The assertion must hold: a failing assert compares two error
            # paths, and the family's subject is the sink's effect.
            parameters["threshold"] = -21
            parameters["message"] = draw(
                st.sampled_from(("parity assertion", "must be positive"))
            )
            ticks = sparse_ticks(
                draw, count, st.integers(min_value=-20, max_value=20)
            )
        else:
            if operation in ("debug_print", "log_", "print_"):
                # A label is a literal, never a logging preamble: the sink
                # capture filter normalizes a real one.
                parameters["label"] = draw(
                    st.sampled_from(("value", "v", "observed", "tick"))
                )
            ticks = family_ticks(draw, count, input_type)
        return {
            "template": "sink_operator",
            "inputs": {"ts": ticks},
            "parameters": parameters,
            "features": [
                *CATALOG["sink_operator"].features,
                f"operator:{operation}",
                f"type:{input_type}",
                f"capture:{capture}",
            ],
        }

    # (name, factory) pairs for discovery. The service strategies exercise
    # the Python parity contract directly; only still-accepted divergences are
    # constrained within their individual generators.
    discovery_weighted = (
        ("scalar_expression", scalar_expression),
        ("scalar_operator_arguments", scalar_operator_arguments),
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
        ("polymorphic_event_flow", polymorphic_event_flow),
        ("polymorphic_event_flow", polymorphic_event_flow),
        ("polymorphic_event_map", polymorphic_event_map),
        ("polymorphic_event_map", polymorphic_event_map),
        ("polymorphic_field_projection", polymorphic_field_projection),
        ("polymorphic_field_projection", polymorphic_field_projection),
        ("polymorphic_tsd_key", polymorphic_tsd_key),
        ("polymorphic_tsd_key", polymorphic_tsd_key),
        ("structural_map_projection", structural_map_projection),
        ("structural_map_projection", structural_map_projection),
        ("arrow_typed_projection", arrow_typed_projection),
        ("arrow_typed_projection", arrow_typed_projection),
        ("nested_higher_order", nested_higher_order),
        ("nested_higher_order", nested_higher_order),
        # The operator-family templates: without a draw strategy each is
        # exercised only by its fixed corpus recipes, so the fuzzer could
        # never vary its operator, types, options or tick history.
        ("unary_operator", unary_operator),
        ("binary_operator", binary_operator),
        ("string_operator", string_operator),
        ("stream_shape", stream_shape),
        ("flow_control", flow_control),
        ("set_operator", set_operator),
        ("tsd_operator", tsd_operator),
        ("tsl_operator", tsl_operator),
        ("temporal_component", temporal_component),
        ("table_round_trip", table_round_trip),
        ("json_round_trip", json_round_trip),
        ("data_frame_conversion", data_frame_conversion),
        ("compound_scalar_field", compound_scalar_field),
        ("sink_operator", sink_operator),
    )
    # Every projecting template draws its REF-producing source too: the
    # reference_source parameter (catalog.REFERENCE_SOURCES) routes each input
    # through a TSL projection, a TSD item, a map_ element, a switch_ branch
    # or an if_ arm, so the differential oracle sees every producer of the
    # REF consumer sweep under random ticks.
    reference_sources = st.sampled_from(REFERENCE_SOURCES)

    def with_reference_source(name, factory):
        if name not in REFERENCE_SOURCE_TEMPLATES:
            return factory

        @st.composite
        def wrapped(draw):
            payload = draw(factory())
            source = draw(reference_sources)
            payload["parameters"] = {**payload.get("parameters", {}),
                                     "reference_source": source}
            # The route's own tags come from the source, never from the
            # template's static features.
            payload["features"] = [*payload.get("features", ()),
                                   f"reference-source:{source}",
                                   *REFERENCE_SOURCE_FEATURES[source]]
            return payload

        return wrapped

    selectable = tuple((name, with_reference_source(name, factory))
                       for name, factory in discovery_weighted)
    if templates is None:
        return st.one_of(*(factory() for _, factory in selectable))
    # A restricted profile draws ONLY the allowed strategies — selecting at
    # the source, never filtering the union (a post-hoc filter discards most
    # draws and trips hypothesis's filter_too_much health check).
    allowed = frozenset(templates)
    unknown = allowed - {name for name, _ in selectable}
    if unknown:
        raise ValueError(
            f"unknown generated template(s): {', '.join(sorted(unknown))}")
    return st.one_of(*(factory() for name, factory in selectable
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
