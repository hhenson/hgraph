"""Typed property-based recipe generation.

Hypothesis is an optional controller dependency and is imported lazily so the
installed C++-first hgraph runtime and its compatibility suite do not depend on it.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .catalog import CATALOG, validate_recipe
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
        rows = [{
            "a": {"value": 1, "quantity": 10, "label": "alpha"},
            "b": {"value": 2, "quantity": 20, "label": "beta"},
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
                row = draw(st.sampled_from(("a", "b")))
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
        ("structural_map_projection", structural_map_projection),
        ("structural_map_projection", structural_map_projection),
        ("arrow_typed_projection", arrow_typed_projection),
        ("arrow_typed_projection", arrow_typed_projection),
        ("nested_higher_order", nested_higher_order),
        ("nested_higher_order", nested_higher_order),
    )
    selectable = discovery_weighted
    if templates is None:
        return st.one_of(*(factory() for _, factory in discovery_weighted))
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
