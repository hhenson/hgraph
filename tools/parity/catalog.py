"""Bounded graph-template catalogue used by both runtime environments.

This module intentionally does not import hgraph at module import time. The
subprocess runner supplies whichever implementation is installed in its
isolated environment.
"""

import json
import re
from dataclasses import dataclass

from .model import Recipe, RecipeError


_SCALAR_TYPES = {"bool": bool, "float": float, "int": int, "str": str}
_NUMERIC_TYPES = {"float", "int"}
_BINARY_OPS = {
    "add": ("+", "same"),
    "sub": ("-", "numeric"),
    "mul": ("*", "numeric"),
    "eq": ("==", "bool"),
    "ne": ("!=", "bool"),
    "lt": ("<", "bool"),
    "le": ("<=", "bool"),
    "gt": (">", "bool"),
    "ge": (">=", "bool"),
}
_UNARY_OPS = {"neg", "pos", "abs", "dedup"}

_SCALAR_ARGUMENT_OPERATIONS = {
    "add": "add_",
    "sub": "sub_",
    "mul": "mul_",
    "div": "div_",
    "floordiv": "floordiv_",
    "mod": "mod_",
    "pow": "pow_",
    "eq": "eq_",
    "ne": "ne_",
    "lt": "lt_",
    "le": "le_",
    "gt": "gt_",
    "ge": "ge_",
}
_COMPARISON_OPERATIONS = {"eq", "ne", "lt", "le", "gt", "ge"}
_DIVIDE_POLICY_OPERATIONS = {"div", "floordiv", "mod", "pow"}
_DIVIDE_BY_ZERO_POLICIES = {"ERROR", "NAN", "INF", "NONE", "ZERO", "ONE"}
_SET_DELTA = "$set_delta"


@dataclass(frozen=True)
class TemplateSpec:
    name: str
    required_inputs: tuple[str, ...] | None
    features: tuple[str, ...]
    operators: tuple[str, ...]
    execute: object
    float_abs_tolerance: float = 0.0


def _decode_value(hg, value):
    if isinstance(value, list):
        return [_decode_value(hg, item) for item in value]
    if not isinstance(value, dict):
        return value
    if set(value) == {"$date"}:
        import datetime as _dt

        return _dt.date.fromisoformat(value["$date"])
    if set(value) == {"$datetime"}:
        import datetime as _dt

        return _dt.datetime.fromisoformat(value["$datetime"])
    if set(value) == {"$remove"} and value["$remove"] is True:
        return hg.REMOVE
    if set(value) == {"$remove_if_exists"} and value["$remove_if_exists"] is True:
        return hg.REMOVE_IF_EXISTS
    if set(value) == {_SET_DELTA}:
        delta = value[_SET_DELTA]
        if not isinstance(delta, dict) or set(delta) != {"added", "removed"}:
            raise RecipeError(f"{_SET_DELTA} requires added and removed lists")
        added = [_decode_value(hg, item) for item in delta["added"]]
        removed = [_decode_value(hg, item) for item in delta["removed"]]
        overlap = set(added) & set(removed)
        if overlap:
            # Ruling 2026-07-28: added/removed must be disjoint — an element
            # in both is incorrect data, not a recipe to explore.
            raise RecipeError(
                f"{_SET_DELTA} added/removed overlap: {sorted(overlap)!r}"
            )
        return hg.set_delta(added=added, removed=removed)
    if set(value) == {"$tuple"}:
        # The mirror of the canonicalizer's ``$tuple``: the only way a recipe
        # can express a tuple-keyed TSD (``uncollapse_keys``) or a tuple
        # scalar tick, both of which must stay hashable through decoding.
        items = value["$tuple"]
        if not isinstance(items, list):
            raise RecipeError("$tuple requires a JSON list")
        return tuple(_decode_value(hg, item) for item in items)
    if set(value) == {"$frozendict"}:
        from frozendict import frozendict

        return frozendict(
            {
                key: _decode_value(hg, item)
                for key, item in value["$frozendict"].items()
            }
        )
    if set(value) == {"$map"}:
        entries = value["$map"]
        if (
            not isinstance(entries, list)
            or not all(
                isinstance(entry, list) and len(entry) == 2
                for entry in entries
            )
        ):
            raise RecipeError("$map requires a list of [key, value] pairs")
        result = {}
        for key, item in entries:
            decoded_key = _decode_value(hg, key)
            try:
                if decoded_key in result:
                    raise RecipeError("$map keys must be unique")
                result[decoded_key] = _decode_value(hg, item)
            except TypeError as error:
                raise RecipeError("$map keys must be hashable") from error
        return result
    return {key: _decode_value(hg, item) for key, item in value.items()}


def decoded_inputs(hg, recipe):
    return {
        name: [_decode_value(hg, value) for value in ticks]
        for name, ticks in recipe.inputs.items()
    }


#: The REF-producing sources a recipe may route its inputs through: the
#: ``reference_source`` parameter of every projecting template. The default
#: is the fixed structural ``TSL`` projection the catalogue always used (so
#: the committed corpus keeps its fingerprints); the others are the generic
#: sources of the REF consumer sweep (``python/tests/test_ref_consumer_sweep.py``),
#: so the differential campaign exercises the same producers under random ticks.
REFERENCE_SOURCES = (
    "tsl_projection",
    "tsd_getitem",
    "map_element",
    "switch_branch",
    "if_true",
)
DEFAULT_REFERENCE_SOURCE = "tsl_projection"

#: The templates whose validators accept the ``reference_source`` parameter
#: beside their own (every projecting template except the two whose parameter
#: set is closed: ``polymorphic_tsd_key`` and ``value_consumer_reference``).
REFERENCE_SOURCE_TEMPLATES = frozenset({
    "adaptor_loopback",
    "collection_size",
    "context_switch",
    "feedback_accumulate",
    "mesh_key_set",
    "nested_higher_order",
    "operator_pipeline",
    "service_adaptor_roundtrip",
    "service_reference",
    "service_request_reply",
    "service_subscription",
    "switch_arithmetic",
    "tsd_key_set_pipeline",
    "tsd_map_reduce",
})

#: The templates that route an input through ``_via_reference``: the recipe's
#: source, not the template, owns the shape / binding / operator tags of that
#: route (``reference_source_features``). Kept in step with the catalogue by
#: ``test_projecting_templates_are_the_ones_that_route_through_a_reference``.
PROJECTING_TEMPLATES = frozenset({
    *REFERENCE_SOURCE_TEMPLATES,
    "polymorphic_tsd_key",
    "value_consumer_reference",
})

#: What each REF-producing source contributes to a recipe's coverage
#: features: the shape it goes through, how the consumer binds to it, the
#: operators it spells. Every source publishes a reference.
REFERENCE_SOURCE_FEATURES = {
    "tsl_projection": (
        "reference:REF", "shape:TSL", "binding:non-peered", "operator:getitem_",
    ),
    "tsd_getitem": (
        "reference:REF", "shape:TSD", "binding:peered", "operator:getitem_",
    ),
    "map_element": (
        "reference:REF", "shape:TSD", "binding:peered", "topology:map",
        "operator:map_", "operator:getitem_",
    ),
    "switch_branch": (
        "reference:REF", "binding:peered", "topology:switch", "operator:switch_",
    ),
    "if_true": (
        "reference:REF", "shape:TSB", "binding:peered", "operator:if_",
    ),
}
assert set(REFERENCE_SOURCE_FEATURES) == set(REFERENCE_SOURCES)


def reference_source_features(recipe) -> tuple[str, ...]:
    """The coverage features the recipe's REF-producing source contributes."""
    if recipe.template not in PROJECTING_TEMPLATES:
        return ()
    source = recipe.parameters.get("reference_source", DEFAULT_REFERENCE_SOURCE)
    return REFERENCE_SOURCE_FEATURES.get(source, ())


_KEYED_NODES: dict[int, object] = {}


def _keyed_node(hg):
    """A compute node publishing its REF input under one TSD key (per runtime module)."""
    node = _KEYED_NODES.get(id(hg))
    if node is None:

        @hg.compute_node
        def _parity_keyed(ts: hg.REF[hg.TIME_SERIES_TYPE]) -> hg.TSD[str, hg.REF[hg.TIME_SERIES_TYPE]]:
            return {"k": ts.value}

        node = _KEYED_NODES[id(hg)] = _parity_keyed
    return node


def _via_reference(hg, value, recipe):
    """Route ``value`` through the recipe's REF-producing source.

    Every source publishes a reference the consumer must see through: a
    structural ``TSL`` child projection, a ``TSD`` item lookup, a ``map_``
    element, a ``switch_`` branch, or the ``true`` arm of ``if_``.
    """
    source = recipe.parameters.get("reference_source", DEFAULT_REFERENCE_SOURCE)
    if source == "tsl_projection":
        return hg.getitem_(hg.TSL.from_ts(value, value), 0)
    if source == "tsd_getitem":
        return _keyed_node(hg)(value)[hg.const("k")]
    if source == "map_element":
        return hg.map_(lambda v: v, _keyed_node(hg)(value))[hg.const("k")]
    if source == "switch_branch":
        return hg.switch_(hg.const("a"), {"a": lambda t: t}, value)
    if source == "if_true":
        return hg.if_(hg.const(True), value).true
    raise RecipeError(f"unknown reference_source {source!r}")


def _validate_reference_source(recipe):
    if "reference_source" not in recipe.parameters:
        return
    source = recipe.parameters["reference_source"]
    if recipe.template not in REFERENCE_SOURCE_TEMPLATES:
        raise RecipeError(f"{recipe.template} does not take a reference_source")
    # An explicit null is not an omission: the executor would read it back.
    if not isinstance(source, str) or source not in REFERENCE_SOURCES:
        raise RecipeError(
            f"reference_source must be one of {REFERENCE_SOURCES}, got {source!r}")


def _expression_type(expression, input_types):
    if not isinstance(expression, dict):
        raise RecipeError("expression must be an object")
    if set(expression) == {"input"}:
        name = expression["input"]
        if name not in input_types:
            raise RecipeError(f"expression references unknown input {name!r}")
        return input_types[name]
    if set(expression) == {"const"}:
        value = expression["const"]
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        if isinstance(value, str):
            return "str"
        raise RecipeError("expression constants must be scalar JSON values")
    operation = expression.get("op")
    args = expression.get("args")
    if operation in _BINARY_OPS:
        if not isinstance(args, list) or len(args) != 2:
            raise RecipeError(f"{operation} requires exactly two arguments")
        lhs = _expression_type(args[0], input_types)
        rhs = _expression_type(args[1], input_types)
        if lhs != rhs:
            raise RecipeError(f"{operation} arguments must have the same type")
        rule = _BINARY_OPS[operation][1]
        if rule == "numeric" and lhs not in _NUMERIC_TYPES:
            raise RecipeError(f"{operation} requires numeric arguments")
        if operation == "add" and lhs not in _NUMERIC_TYPES | {"str"}:
            raise RecipeError("add requires numeric or string arguments")
        return "bool" if rule == "bool" else lhs
    if operation in _UNARY_OPS:
        if not isinstance(args, list) or len(args) != 1:
            raise RecipeError(f"{operation} requires exactly one argument")
        item_type = _expression_type(args[0], input_types)
        if operation in {"neg", "pos", "abs"} and item_type not in _NUMERIC_TYPES:
            raise RecipeError(f"{operation} requires a numeric argument")
        return item_type
    raise RecipeError(f"unsupported expression operation {operation!r}")


def _expression_source(expression):
    if "input" in expression:
        return expression["input"]
    if "const" in expression:
        return repr(expression["const"])
    operation = expression["op"]
    args = expression["args"]
    if operation in _BINARY_OPS:
        return (
            f"({_expression_source(args[0])} "
            f"{_BINARY_OPS[operation][0]} {_expression_source(args[1])})"
        )
    if operation == "neg":
        return f"(-{_expression_source(args[0])})"
    if operation == "pos":
        return f"(+{_expression_source(args[0])})"
    if operation == "abs":
        return f"hg.abs_({_expression_source(args[0])})"
    if operation == "dedup":
        return f"hg.dedup({_expression_source(args[0])})"
    raise AssertionError(operation)


def _validate_scalar_expression(recipe):
    input_types = recipe.parameters.get("input_types")
    expression = recipe.parameters.get("expression")
    if not isinstance(input_types, dict) or set(input_types) != set(recipe.inputs):
        raise RecipeError(
            "scalar_expression input_types must name every recipe input exactly once"
        )
    for name, type_name in input_types.items():
        if type_name not in _SCALAR_TYPES:
            raise RecipeError(f"unsupported scalar input type {type_name!r} for {name}")
    output_type = _expression_type(expression, input_types)
    declared_output = recipe.parameters.get("output_type", output_type)
    if declared_output != output_type:
        raise RecipeError(
            f"declared output_type {declared_output!r} does not match {output_type!r}"
        )
    postponed = recipe.parameters.get("postponed_annotations", False)
    if not isinstance(postponed, bool):
        raise RecipeError("postponed_annotations must be a boolean")


def _scalar_expression(hg, recipe):
    from hgraph.test import eval_node

    input_types = recipe.parameters["input_types"]
    output_type = _expression_type(recipe.parameters["expression"], input_types)
    arguments = ", ".join(
        f"{name}: hg.TS[{type_name}]" for name, type_name in input_types.items()
    )
    # mode:postponed-annotations (issue #83 class): the generated module opts
    # into PEP 563, so every annotation reaches the wiring layer as a STRING.
    prefix = ("from __future__ import annotations\n"
              if recipe.parameters.get("postponed_annotations", False) else "")
    source = (
        f"{prefix}"
        "@hg.graph\n"
        f"def parity_graph({arguments}) -> hg.TS[{output_type}]:\n"
        f"    return {_expression_source(recipe.parameters['expression'])}\n"
    )
    namespace = {"hg": hg}
    exec(compile(source, f"<parity:{recipe.id}>", "exec"), namespace)
    inputs = decoded_inputs(hg, recipe)
    return eval_node(
        namespace["parity_graph"],
        *(inputs[name] for name in input_types),
    )


def _scalar_argument_output_type(operation, input_type, scalar_type):
    if operation in _COMPARISON_OPERATIONS:
        return "bool"
    if operation == "div":
        return "float"
    return "float" if "float" in (input_type, scalar_type) else "int"


def _validate_scalar_operator_arguments(recipe):
    parameters = recipe.parameters
    operation = parameters.get("operation")
    if operation not in _SCALAR_ARGUMENT_OPERATIONS:
        raise RecipeError(
            "scalar_operator_arguments operation must name a supported "
            "numeric binary operator"
        )
    input_type = parameters.get("input_type")
    scalar_type = parameters.get("scalar_type")
    if input_type not in _NUMERIC_TYPES or scalar_type not in _NUMERIC_TYPES:
        raise RecipeError(
            "scalar_operator_arguments input_type and scalar_type must be int or float"
        )
    if operation in _COMPARISON_OPERATIONS and input_type != scalar_type:
        raise RecipeError(
            "scalar_operator_arguments comparison operands must have matching types"
        )
    scalar_side = parameters.get("scalar_side")
    if scalar_side not in {"lhs", "rhs"}:
        raise RecipeError(
            "scalar_operator_arguments scalar_side must be lhs or rhs"
        )

    expected_input = _SCALAR_TYPES[input_type]
    for tick in recipe.inputs["value"]:
        if tick is not None and type(tick) is not expected_input:
            raise RecipeError(
                f"scalar_operator_arguments {input_type} input contains "
                f"{type(tick).__name__}"
            )
    if not any(tick is not None for tick in recipe.inputs["value"]):
        raise RecipeError("scalar_operator_arguments requires a valid input tick")

    scalar_value = parameters.get("scalar_value")
    if type(scalar_value) is not _SCALAR_TYPES[scalar_type]:
        raise RecipeError(
            f"scalar_operator_arguments scalar_value must be {scalar_type}"
        )

    has_policy = "divide_by_zero" in parameters
    policy = parameters.get("divide_by_zero")
    if operation in _DIVIDE_POLICY_OPERATIONS:
        if has_policy and policy not in _DIVIDE_BY_ZERO_POLICIES:
            raise RecipeError(
                "scalar_operator_arguments divide_by_zero must name a "
                "DivideByZero value"
            )
    elif has_policy:
        raise RecipeError(
            f"scalar_operator_arguments {operation} does not accept divide_by_zero"
        )

    if operation in {"div", "floordiv", "mod"}:
        denominators = (
            [scalar_value]
            if scalar_side == "rhs"
            else [tick for tick in recipe.inputs["value"] if tick is not None]
        )
        if any(value == 0 for value in denominators):
            output_type = _scalar_argument_output_type(
                operation, input_type, scalar_type
            )
            allowed = (
                {"NAN", "INF", "NONE", "ZERO", "ONE"}
                if operation == "div"
                else {"NONE", "ZERO"}
                if operation == "floordiv" and output_type == "int"
                else {"NAN", "INF", "NONE", "ZERO", "ONE"}
                if operation == "floordiv"
                else {"NONE"}
                if output_type == "int"
                else {"NAN", "INF", "NONE"}
            )
            if policy not in allowed:
                raise RecipeError(
                    f"scalar_operator_arguments {operation} zero divisor "
                    f"requires one of {sorted(allowed)}"
                )

    if operation == "pow":
        bases = (
            [scalar_value]
            if scalar_side == "lhs"
            else [tick for tick in recipe.inputs["value"] if tick is not None]
        )
        exponents = (
            [scalar_value]
            if scalar_side == "rhs"
            else [tick for tick in recipe.inputs["value"] if tick is not None]
        )
        output_type = _scalar_argument_output_type(
            operation, input_type, scalar_type
        )
        if output_type == "int" and any(value < 0 for value in exponents):
            raise RecipeError(
                "scalar_operator_arguments integer pow requires non-negative exponents"
            )
        if any(base == 0 for base in bases) and any(
            exponent < 0 for exponent in exponents
        ):
            allowed = (
                {"NONE"}
                if output_type == "int"
                else {"NAN", "INF", "NONE", "ZERO", "ONE"}
            )
            if policy not in allowed:
                raise RecipeError(
                    "scalar_operator_arguments zero to a negative power "
                    f"requires one of {sorted(allowed)}"
                )
        if any(base < 0 for base in bases) and any(
            isinstance(exponent, float) and not exponent.is_integer()
            for exponent in exponents
        ):
            raise RecipeError(
                "scalar_operator_arguments fractional powers require non-negative bases"
            )


def _scalar_operator_arguments(hg, recipe):
    from hgraph.test import eval_node

    parameters = recipe.parameters
    operation = parameters["operation"]
    input_type = parameters["input_type"]
    scalar_type = parameters["scalar_type"]
    scalar_side = parameters["scalar_side"]
    scalar_value = parameters["scalar_value"]
    output_type = _scalar_argument_output_type(
        operation, input_type, scalar_type
    )
    value = "value"
    scalar = repr(scalar_value)
    lhs, rhs = (scalar, value) if scalar_side == "lhs" else (value, scalar)
    policy = parameters.get("divide_by_zero")
    policy_argument = (
        f", divide_by_zero=hg.DivideByZero.{policy}" if policy else ""
    )
    source = (
        "@hg.graph\n"
        f"def parity_graph(value: hg.TS[{input_type}]) -> hg.TS[{output_type}]:\n"
        f"    return hg.{_SCALAR_ARGUMENT_OPERATIONS[operation]}("
        f"{lhs}, {rhs}{policy_argument})\n"
    )
    namespace = {"hg": hg}
    exec(compile(source, f"<parity:{recipe.id}>", "exec"), namespace)
    inputs = decoded_inputs(hg, recipe)
    return eval_node(namespace["parity_graph"], inputs["value"])


# --------------------------------------------------------------------------
# Temporal accessor expressions (issue #82 class): date/datetime arithmetic
# feeding the upstream getattr_ property/method tables.

_TEMPORAL_PROPERTIES = {
    "date": {"year": "int", "month": "int", "day": "int"},
    "datetime": {
        "year": "int", "month": "int", "day": "int",
        "hour": "int", "minute": "int", "second": "int", "microsecond": "int",
    },
    "timedelta": {"days": "int", "seconds": "int", "microseconds": "int"},
}

_TEMPORAL_METHODS = {
    "date": {"weekday": "int", "isoweekday": "int"},
    "datetime": {"weekday": "int", "isoweekday": "int"},
    "timedelta": {"total_seconds": "float"},
}


def _temporal_accessor_kind(recipe):
    target = recipe.parameters.get("target")
    if target == "difference":
        return "timedelta"
    return recipe.parameters.get("input_type")


def _validate_temporal_ticks(recipe, name):
    """Reject malformed temporal tick encodings at the trusted boundary.

    A curated or model-proposed recipe must not defer ``{"$date": 123}`` to
    a runtime decode failure: every non-null tick carries exactly the tag
    matching ``input_type`` and a valid ISO string (datetimes NAIVE — the
    UTC convention)."""
    import datetime as dt_module

    input_type = recipe.parameters.get("input_type")
    tag = "$date" if input_type == "date" else "$datetime"
    decoder = (dt_module.date.fromisoformat if input_type == "date"
               else dt_module.datetime.fromisoformat)
    for tick in recipe.inputs.get(name, ()):
        if tick is None:
            continue
        if (not isinstance(tick, dict) or set(tick) != {tag}
                or not isinstance(tick[tag], str)):
            raise RecipeError(
                f"temporal_expression {name} ticks must be "
                f'{{"{tag}": "<iso-string>"}} or null')
        try:
            decoded = decoder(tick[tag])
        except ValueError as error:
            raise RecipeError(
                f"temporal_expression {name} tick {tick[tag]!r} is not a "
                f"valid ISO {input_type}") from error
        if input_type == "datetime" and decoded.tzinfo is not None:
            raise RecipeError(
                f"temporal_expression {name} datetime ticks must be naive "
                "(the UTC convention)")


def _validate_temporal_expression(recipe):
    parameters = recipe.parameters
    input_type = parameters.get("input_type")
    if input_type not in ("date", "datetime"):
        raise RecipeError("temporal_expression input_type must be date or datetime")
    target = parameters.get("target")
    if target not in ("difference", "shifted", "input"):
        raise RecipeError("temporal_expression target must be difference/shifted/input")
    if set(recipe.inputs) != ({"lhs", "rhs"} if target == "difference" else {"lhs"}):
        raise RecipeError("temporal_expression inputs do not match its target")
    for name in recipe.inputs:
        _validate_temporal_ticks(recipe, name)
    delta = parameters.get("delta")
    if target == "shifted":
        if (not isinstance(delta, dict)
                or set(delta) - {"days", "seconds", "microseconds"}
                or not all(isinstance(v, int) and not isinstance(v, bool)
                           and -10_000 <= v <= 10_000 for v in delta.values())):
            raise RecipeError("temporal_expression shifted target needs a bounded delta")
    elif delta is not None:
        raise RecipeError("temporal_expression delta applies to the shifted target only")
    kind = _temporal_accessor_kind(recipe)
    accessor = parameters.get("accessor")
    table = {**_TEMPORAL_PROPERTIES[kind], **_TEMPORAL_METHODS[kind]}
    if accessor not in table:
        raise RecipeError(f"temporal_expression accessor {accessor!r} not valid for {kind}")
    if parameters.get("output_type", table[accessor]) != table[accessor]:
        raise RecipeError("temporal_expression output_type does not match the accessor")
    postponed = parameters.get("postponed_annotations", False)
    if not isinstance(postponed, bool):
        raise RecipeError("postponed_annotations must be a boolean")


def _temporal_expression(hg, recipe):
    import datetime as dt_module

    from hgraph.test import eval_node

    parameters = recipe.parameters
    input_type = parameters["input_type"]
    target = parameters["target"]
    accessor = parameters["accessor"]
    kind = _temporal_accessor_kind(recipe)
    output_type = {**_TEMPORAL_PROPERTIES[kind], **_TEMPORAL_METHODS[kind]}[accessor]
    call = "()" if accessor in _TEMPORAL_METHODS[kind] else ""
    if target == "difference":
        arguments = f"lhs: hg.TS[{input_type}], rhs: hg.TS[{input_type}]"
        base = "(lhs - rhs)"
    elif target == "shifted":
        arguments = f"lhs: hg.TS[{input_type}]"
        base = f"(lhs + timedelta(**{parameters['delta']!r}))"
    else:
        arguments = f"lhs: hg.TS[{input_type}]"
        base = "lhs"
    prefix = ("from __future__ import annotations\n"
              if parameters.get("postponed_annotations", False) else "")
    source = (
        f"{prefix}"
        "@hg.graph\n"
        f"def parity_graph({arguments}) -> hg.TS[{output_type}]:\n"
        "    lhs = hg.getitem_(hg.TSL.from_ts(lhs, lhs), 0)\n"
        f"    return {base}.{accessor}{call}\n"
    )
    namespace = {
        "hg": hg,
        "date": dt_module.date,
        "datetime": dt_module.datetime,
        "timedelta": dt_module.timedelta,
    }
    exec(compile(source, f"<parity:{recipe.id}>", "exec"), namespace)
    inputs = decoded_inputs(hg, recipe)
    ordered = ("lhs", "rhs") if target == "difference" else ("lhs",)
    return eval_node(namespace["parity_graph"], *(inputs[name] for name in ordered))


# --------------------------------------------------------------------------
# Collection sizes (issue #81 class): len_/is_empty/contains_ over every
# upstream-supported sized shape.

_COLLECTION_SHAPES = ("str", "tss", "tsd", "tsl")
_COLLECTION_OPERATIONS = ("len", "is_empty", "contains")


def _validate_collection_size(recipe):
    parameters = recipe.parameters
    shape = parameters.get("shape")
    if shape not in _COLLECTION_SHAPES:
        raise RecipeError(f"collection_size shape must be one of {_COLLECTION_SHAPES}")
    operation = parameters.get("operation")
    if operation not in _COLLECTION_OPERATIONS:
        raise RecipeError(
            f"collection_size operation must be one of {_COLLECTION_OPERATIONS}")
    if shape == "str" and operation == "is_empty":
        raise RecipeError(
            "collection_size is_empty is not supported for TS[str] by released hgraph"
        )
    if shape == "tsl":
        if operation != "len":
            raise RecipeError("collection_size tsl covers len only")
        if set(recipe.inputs) != {"a", "b"}:
            raise RecipeError("collection_size tsl requires inputs a and b")
    elif set(recipe.inputs) != {"ts"}:
        raise RecipeError("collection_size requires the ts input")
    probe = parameters.get("probe")
    if operation == "contains":
        expected = str if shape in ("str", "tsd") else int
        if not isinstance(probe, expected) or isinstance(probe, bool):
            raise RecipeError("collection_size contains needs a matching probe scalar")
    elif probe is not None:
        raise RecipeError("collection_size probe applies to contains only")
    if not isinstance(parameters.get("normalize_output", False), bool):
        raise RecipeError("collection_size normalize_output must be a boolean")


def _collection_size(hg, recipe):
    from hgraph.test import eval_node

    parameters = recipe.parameters
    shape = parameters["shape"]
    operation = parameters["operation"]
    probe = parameters.get("probe")
    normalize_output = parameters.get("normalize_output", False)
    inputs = decoded_inputs(hg, recipe)
    if shape == "tsl":
        @hg.graph
        def parity_graph(a: hg.TS[int], b: hg.TS[int]) -> hg.TS[int]:
            result = hg.len_(hg.TSL.from_ts(_via_reference(hg, a, recipe), b))
            return hg.dedup(result) if normalize_output else result

        return eval_node(parity_graph, inputs["a"], inputs["b"])

    annotation = {
        "str": hg.TS[str],
        "tss": hg.TSS[int],
        "tsd": hg.TSD[str, hg.TS[int]],
    }[shape]

    if operation == "len":
        @hg.graph
        def parity_graph(ts: annotation) -> hg.TS[int]:
            result = hg.len_(_via_reference(hg, ts, recipe))
            return hg.dedup(result) if normalize_output else result
    elif operation == "is_empty":
        @hg.graph
        def parity_graph(ts: annotation) -> hg.TS[bool]:
            result = hg.is_empty(_via_reference(hg, ts, recipe))
            return hg.dedup(result) if normalize_output else result
    else:
        @hg.graph
        def parity_graph(ts: annotation) -> hg.TS[bool]:
            result = hg.contains_(_via_reference(hg, ts, recipe), probe)
            return hg.dedup(result) if normalize_output else result

    return eval_node(parity_graph, inputs["ts"])


# --------------------------------------------------------------------------
# Lifecycle signature spellings (issue #79 class): start/stop parameters
# match the eval signature by name; every accepted spelling behaves alike.

_LIFECYCLE_SPELLINGS = {
    "default": "_state: hg.STATE = None",
    "bare": "_state: hg.STATE",
    "unannotated": "_state",
}


def _validate_lifecycle_state(recipe):
    parameters = recipe.parameters
    for phase in ("start_spelling", "stop_spelling"):
        spelling = parameters.get(phase)
        if spelling is not None and spelling not in _LIFECYCLE_SPELLINGS:
            raise RecipeError(
                f"lifecycle_state {phase} must be one of {tuple(_LIFECYCLE_SPELLINGS)}")
    if parameters.get("start_spelling") is None:
        raise RecipeError("lifecycle_state requires a start_spelling (state seeding)")
    seed = parameters.get("seed", 0)
    if not isinstance(seed, int) or isinstance(seed, bool) or not -100 <= seed <= 100:
        raise RecipeError("lifecycle_state seed must be a bounded integer")
    if parameters.get("state_access", "attribute") not in ("attribute", "mapping"):
        raise RecipeError("lifecycle_state state_access must be 'attribute' or 'mapping'")
    if set(recipe.inputs) != {"value"}:
        raise RecipeError("lifecycle_state requires the value input")


def _lifecycle_state(hg, recipe):
    from hgraph.test import eval_node

    parameters = recipe.parameters
    seed = parameters.get("seed", 0)
    state_access = parameters.get("state_access", "attribute")
    start_signature = _LIFECYCLE_SPELLINGS[parameters["start_spelling"]]
    stop_spelling = parameters.get("stop_spelling")
    if state_access == "mapping":
        eval_body = (
            "    if _state.is_updated():\n"
            "        raise AssertionError('state unexpectedly dirty before evaluation')\n"
            "    _state.total = _state['total'] + value.value\n"
            "    expected = [('total', _state['total'])]\n"
            "    if list(_state.keys()) != ['total'] or list(_state.items()) != expected:\n"
            "        raise AssertionError('state mapping views disagree')\n"
            "    if list(_state.values()) != [_state.total]:\n"
            "        raise AssertionError('state values view disagrees')\n"
            "    result = _state['total']\n"
            "    _state.reset_updated()\n"
            "    return result\n"
        )
        start_body = (
            f"    _state.total = {seed}\n"
            "    if not isinstance(_state, hg.STATE) or _state['total'] != _state.total:\n"
            "        raise AssertionError('naked STATE mapping surface unavailable')\n"
            "    _state.reset_updated()\n"
        )
        stop_body = (
            "    if _state['total'] != _state.total:\n"
            "        raise AssertionError('naked STATE did not persist through stop')\n"
        )
    else:
        eval_body = (
            "    _state.total = _state.total + value.value\n"
            "    return _state.total\n"
        )
        start_body = f"    _state.total = {seed}\n"
        stop_body = "    pass\n"
    stop_block = ""
    if stop_spelling is not None:
        stop_block = (
            "@lifecycle_node.stop\n"
            f"def lifecycle_stop({_LIFECYCLE_SPELLINGS[stop_spelling]}):\n"
            f"{stop_body}"
        )
    source = (
        "@hg.compute_node\n"
        "def lifecycle_node(value: hg.TS[int], _state: hg.STATE = None) -> hg.TS[int]:\n"
        f"{eval_body}"
        "@lifecycle_node.start\n"
        f"def lifecycle_start({start_signature}):\n"
        f"{start_body}"
        f"{stop_block}"
        "@hg.graph\n"
        "def parity_graph(value: hg.TS[int]) -> hg.TS[int]:\n"
        "    value = hg.getitem_(hg.TSL.from_ts(value, value), 0)\n"
        "    return lifecycle_node(value)\n"
    )
    namespace = {"hg": hg}
    exec(compile(source, f"<parity:{recipe.id}>", "exec"), namespace)
    inputs = decoded_inputs(hg, recipe)
    return eval_node(namespace["parity_graph"], inputs["value"])


# --------------------------------------------------------------------------
# Real-time configuration: an omitted start captures wall-clock UTC rather
# than inheriting the simulation-only MIN_ST sentinel.

def _validate_realtime_default_start(recipe):
    if recipe.parameters:
        raise RecipeError("realtime_default_start takes no parameters")
    probe = recipe.inputs["probe"]
    if len(probe) != 1 or not isinstance(probe[0], bool):
        raise RecipeError("realtime_default_start requires one boolean probe")


def _realtime_default_start(hg, recipe):
    from datetime import timedelta

    probe = decoded_inputs(hg, recipe)["probe"][0]

    @hg.compute_node
    def started_after_simulation_sentinel(
        value: hg.TS[bool], _api: hg.EvaluationEngineApi = None,
    ) -> hg.TS[bool]:
        return value.value and _api.start_time > hg.MIN_ST

    @hg.graph
    def parity_graph() -> hg.TS[bool]:
        return started_after_simulation_sentinel(hg.const(probe))

    result = hg.evaluate_graph(
        parity_graph,
        config=hg.GraphConfiguration(
            run_mode=hg.EvaluationMode.REAL_TIME,
            end_time=timedelta(milliseconds=50),
        ),
    )
    return [value for _, value in result]


# --------------------------------------------------------------------------
# Deeply nested higher-order structures: map_/mesh_ over a CHURNING key set,
# a per-key switch_ FLIPPING branches (nested graphs start/stop), services
# (request-reply / subscription) and adaptors living INSIDE those branches,
# optionally the whole pipeline under an outer switch_ that tears it down and
# rebuilds it. This composition space is where production issues breed.

_NESTED_INNER = ("arithmetic", "request_reply", "subscription", "adaptor")
_NESTED_OUTER = ("map", "mesh")


def _keys_re_added(ticks):
    removed = set()
    for tick in ticks:
        if not isinstance(tick, dict):
            continue
        for key, value in tick.items():
            if isinstance(value, dict) and value.get("$remove") is True:
                removed.add(key)
            elif key in removed:
                return True
    return False


def _validate_nested_higher_order(recipe):
    parameters = recipe.parameters
    inner = parameters.get("inner")
    if inner not in _NESTED_INNER:
        raise RecipeError(f"nested_higher_order inner must be one of {_NESTED_INNER}")
    outer = parameters.get("outer")
    if outer not in _NESTED_OUTER:
        raise RecipeError(f"nested_higher_order outer must be one of {_NESTED_OUTER}")
    wrap_switch = parameters.get("wrap_switch", False)
    reduce_output = parameters.get("reduce_output", True)
    normalize_output = parameters.get("normalize_output", False)
    if (not isinstance(wrap_switch, bool)
            or not isinstance(reduce_output, bool)
            or not isinstance(normalize_output, bool)):
        raise RecipeError(
            "nested_higher_order wrap_switch/reduce_output/normalize_output "
            "must be booleans")
    if wrap_switch and not reduce_output:
        raise RecipeError(
            "nested_higher_order wrap_switch requires reduce_output (one output shape)")
    if normalize_output and not reduce_output:
        raise RecipeError(
            "nested_higher_order normalize_output requires reduce_output")
    increment = parameters.get("increment", 1)
    if (not isinstance(increment, int) or isinstance(increment, bool)
            or not -20 <= increment <= 20):
        raise RecipeError("nested_higher_order increment must be a bounded integer")
    expected = {"values", "selector"}
    if wrap_switch:
        expected.add("outer_selector")
    if set(recipe.inputs) != expected:
        raise RecipeError(f"nested_higher_order requires inputs {sorted(expected)}")
    for name, allowed in (("selector", ("alpha", "beta")),
                          ("outer_selector", ("on", "off"))):
        for tick in recipe.inputs.get(name, ()):
            if tick is not None and tick not in allowed:
                raise RecipeError(f"nested_higher_order {name} ticks must be in {allowed}")
    if inner == "adaptor" and not reduce_output:
        raise RecipeError(
            "nested_higher_order adaptor inner requires reduce_output "
            "(the adaptor consumes the reduced pipeline output)")
    if inner == "subscription":
        # The service re-subscription timing deviation is RULED (roadmap.rst):
        # generated recipes stay out of that space — a removed key is never
        # re-added, and the outer switch (which would re-subscribe every key
        # on re-entry) is excluded.
        if wrap_switch:
            raise RecipeError(
                "nested_higher_order subscription inner excludes wrap_switch")
        if _keys_re_added(recipe.inputs["values"]):
            raise RecipeError(
                "nested_higher_order subscription inner must not re-add removed keys")


def _nested_higher_order(hg, recipe):
    from hgraph.test import eval_node

    parameters = recipe.parameters
    inner = parameters["inner"]
    outer = parameters["outer"]
    wrap_switch = parameters.get("wrap_switch", False)
    reduce_output = parameters.get("reduce_output", True)
    normalize_output = parameters.get("normalize_output", False)
    increment = parameters.get("increment", 1)
    path = f"nested_{inner}"

    # ---- the service/adaptor leaf living inside the alpha branch ----
    if inner == "request_reply":
        @hg.request_reply_service
        def adjust(path: str, request: hg.TS[int]) -> hg.TS[int]: ...

        @hg.service_impl(interfaces=adjust)
        def adjust_impl(request: hg.TSD[int, hg.TS[int]]) -> hg.TSD[int, hg.TS[int]]:
            return hg.map_(lambda value: value + increment, request)

        def register(): hg.register_service(path, adjust_impl)
        def alpha_leaf(value): return adjust(path, value)
    elif inner == "subscription":
        @hg.subscription_service
        def quote(path: str, symbol: hg.TS[str]) -> hg.TS[int]: ...

        @hg.graph
        def quote_value(symbol: hg.TS[str]) -> hg.TS[int]:
            return hg.len_(symbol) * increment

        @hg.service_impl(interfaces=quote)
        def quote_impl(symbol: hg.TSS[str]) -> hg.TSD[str, hg.TS[int]]:
            return hg.map_(quote_value, __keys__=symbol, __key_arg__="symbol")

        def register(): hg.register_service(path, quote_impl)
        def alpha_leaf(value, key): return quote(path, key) + value
    elif inner == "adaptor":
        @hg.adaptor
        def loopback(path: str, value: hg.TS[int]) -> hg.TS[int]: ...

        @hg.adaptor_impl(interfaces=loopback)
        def loopback_impl(path: str, value: hg.TS[int]) -> hg.TS[int]:
            return value + increment

        def register(): hg.register_adaptor(path, loopback_impl)
        def alpha_leaf(value): return loopback(path, value)
    else:
        def register(): pass
        def alpha_leaf(value): return value + increment

    # ---- the per-key graph: a switch_ flipping between the service-backed
    #      alpha branch and plain arithmetic (branch flips start/stop the
    #      nested graphs and their service/adaptor clients) ----
    # Upstream-supported composition space only (the generator does not
    # explore upstream-broken shapes): a per-key ADAPTOR client cycles
    # released hgraph's toposort, so the adaptor consumes the reduced
    # pipeline output instead; the SUBSCRIPTION subscribes per key OUTSIDE
    # the switch (key churn still subscribes/unsubscribes) while the switch
    # flips the arithmetic around it.
    if inner == "subscription":
        @hg.graph
        def alpha_branch(value: hg.TS[int]) -> hg.TS[int]:
            return value + increment

        @hg.graph
        def beta_branch(value: hg.TS[int]) -> hg.TS[int]:
            return value * 2 - increment

        @hg.graph
        def per_key_graph(key: hg.TS[str], value: hg.TS[int],
                          selector: hg.TS[str]) -> hg.TS[int]:
            quoted = alpha_leaf(value, key)
            return hg.switch_(
                selector,
                {"alpha": alpha_branch, "beta": beta_branch},
                quoted,
            )
    else:
        if inner == "adaptor":
            @hg.graph
            def alpha_branch(value: hg.TS[int]) -> hg.TS[int]:
                return value + increment
        else:
            @hg.graph
            def alpha_branch(value: hg.TS[int]) -> hg.TS[int]:
                return alpha_leaf(value)

        @hg.graph
        def beta_branch(value: hg.TS[int]) -> hg.TS[int]:
            return value * 2 - increment

        @hg.graph
        def per_key_graph(key: hg.TS[str], value: hg.TS[int],
                          selector: hg.TS[str]) -> hg.TS[int]:
            del key
            return hg.switch_(
                selector,
                {"alpha": alpha_branch, "beta": beta_branch},
                value,
            )

    @hg.graph
    def mesh_keyed(key: hg.TS[str], selector: hg.TS[str]) -> hg.TS[int]:
        return per_key_graph(key, hg.len_(key), selector)

    def pipeline(values, selector):
        if outer == "mesh":
            mapped = hg.mesh_(
                mesh_keyed, selector,
                __keys__=hg.keys_(values), __key_arg__="key",
            )
        else:
            mapped = hg.map_(per_key_graph, values, selector)
        if reduce_output:
            reduced = hg.reduce(lambda lhs, rhs: lhs + rhs, mapped, 0)
            if inner == "adaptor":
                # The adaptor consumes the churning nested pipeline's output.
                return alpha_leaf(reduced)
            return reduced
        return mapped

    if wrap_switch:
        @hg.graph
        def parity_graph(values: hg.TSD[str, hg.TS[int]], selector: hg.TS[str],
                         outer_selector: hg.TS[str]) -> hg.TS[int]:
            register()
            values = _via_reference(hg, values, recipe)
            result = hg.switch_(
                outer_selector,
                {
                    "on": lambda values, selector: pipeline(values, selector),
                    "off": lambda values, selector: hg.len_(values) * 0,
                },
                values,
                selector,
            )
            return hg.dedup(result) if normalize_output else result
    elif reduce_output:
        @hg.graph
        def parity_graph(values: hg.TSD[str, hg.TS[int]],
                         selector: hg.TS[str]) -> hg.TS[int]:
            register()
            values = _via_reference(hg, values, recipe)
            result = pipeline(values, selector)
            return hg.dedup(result) if normalize_output else result
    else:
        @hg.graph
        def parity_graph(values: hg.TSD[str, hg.TS[int]],
                         selector: hg.TS[str]) -> hg.TSD[str, hg.TS[int]]:
            register()
            values = _via_reference(hg, values, recipe)
            return pipeline(values, selector)

    inputs = decoded_inputs(hg, recipe)
    ordered = ["values", "selector"] + (["outer_selector"] if wrap_switch else [])
    return eval_node(
        parity_graph,
        *(inputs[name] for name in ordered),
        __end_time__=hg.MIN_ST + (recipe.tick_count + 6) * hg.MIN_TD,
    )


# --------------------------------------------------------------------------
# Data-frame recording surface (issues #92/#417): the frames the recorder
# frameworks hand back to user code — configured names and timezone included.


def _validate_data_frame_recording(recipe):
    if set(recipe.inputs) != {"ts"}:
        raise RecipeError("data_frame_recording requires the ts input")
    as_of_offset = recipe.parameters.get("as_of_offset", 30)
    if (not isinstance(as_of_offset, int) or isinstance(as_of_offset, bool)
            or not 1 <= as_of_offset <= 10_000):
        raise RecipeError("data_frame_recording as_of_offset must be a bounded integer")
    column_names = recipe.parameters.get("column_names", "default")
    if not isinstance(column_names, str) or column_names not in {
        "default", "configured"
    }:
        raise RecipeError(
            "data_frame_recording column_names must be 'default' or 'configured'"
        )


def _canonical_frame_surface(frame):
    """A frame in a distribution-independent canonical shape.

    Works for both boundary forms (upstream polars DataFrame, hg_cpp
    pyarrow Table): column names with their timezone presentation, plus the
    row values (datetimes canonicalize downstream via isoformat — a
    tz-aware value renders with its offset, so an aware/naive divergence is
    a trace difference)."""
    if hasattr(frame, "to_pylist"):   # pyarrow.Table
        columns = []
        for field in frame.schema:
            tz = getattr(field.type, "tz", None)
            columns.append({"name": field.name, "tz": tz})
        rows = [
            [row[column["name"]] for column in columns]
            for row in frame.to_pylist()
        ]
        return {"columns": columns, "rows": rows}
    if hasattr(frame, "to_dicts"):   # polars.DataFrame
        columns = []
        for name, dtype in frame.schema.items():
            tz = getattr(dtype, "time_zone", None)
            columns.append({"name": name, "tz": tz})
        rows = [
            [row[column["name"]] for column in columns]
            for row in frame.to_dicts()
        ]
        return {"columns": columns, "rows": rows}
    raise RecipeError(f"unsupported frame surface {type(frame)!r}")


def _data_frame_recording(hg, recipe):
    from hgraph.test import eval_node
    from hgraph.adaptors.data_frame import (
        DATA_FRAME_RECORD_REPLAY,
        MemoryDataFrameStorage,
    )

    as_of_offset = recipe.parameters.get("as_of_offset", 30)
    column_names = recipe.parameters.get("column_names", "default")
    inputs = decoded_inputs(hg, recipe)
    with hg.GlobalState(), MemoryDataFrameStorage() as storage:
        if column_names == "configured":
            hg.set_table_schema_date_key("event_time")
            hg.set_table_schema_as_of_key("observed_at")
        hg.set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        hg.set_as_of(hg.MIN_ST + hg.MIN_TD * as_of_offset)
        eval_node(hg.record[hg.TS[int]], ts=inputs["ts"], key="ts",
                  recordable_id="parity")
        frame = storage.read_frame("parity.ts")
        replayed = eval_node(hg.replay[hg.TS[int]], key="ts",
                             recordable_id="parity")
    return {"frame": _canonical_frame_surface(frame), "replayed": replayed}


def _feedback_accumulate(hg, recipe):
    from hgraph.test import eval_node

    initial = recipe.parameters.get("initial", 0)

    @hg.graph
    def parity_graph(value: hg.TS[int]) -> hg.TS[int]:
        value = _via_reference(hg, value, recipe)
        state = hg.feedback(hg.TS[int], initial)
        total = value + hg.passive(state())
        state(total)
        return total

    inputs = decoded_inputs(hg, recipe)
    return eval_node(parity_graph, inputs["value"])


def _switch_arithmetic(hg, recipe):
    from hgraph.test import eval_node

    @hg.graph
    def parity_graph(
        selector: hg.TS[str], lhs: hg.TS[int], rhs: hg.TS[int]
    ) -> hg.TS[int]:
        selector = _via_reference(hg, selector, recipe)
        lhs = _via_reference(hg, lhs, recipe)
        rhs = _via_reference(hg, rhs, recipe)
        return hg.switch_(
            selector,
            {
                "plus": lambda lhs, rhs: lhs + rhs,
                "minus": lambda lhs, rhs: lhs - rhs,
            },
            lhs,
            rhs,
        )

    inputs = decoded_inputs(hg, recipe)
    return eval_node(
        parity_graph,
        inputs["selector"],
        inputs["lhs"],
        inputs["rhs"],
    )


def _tsd_map_reduce(hg, recipe):
    from hgraph.test import eval_node

    increment = recipe.parameters.get("increment", 1)
    zero = recipe.parameters.get("zero", 0)

    @hg.graph
    def parity_graph(values: hg.TSD[str, hg.TS[int]]) -> hg.TS[int]:
        values = _via_reference(hg, values, recipe)
        mapped = hg.map_(lambda value: value + increment, values)
        return hg.reduce(lambda lhs, rhs: lhs + rhs, mapped, zero)

    inputs = decoded_inputs(hg, recipe)
    return eval_node(parity_graph, inputs["values"])


def _service_reference(hg, recipe):
    from hgraph.test import eval_node

    base = recipe.parameters.get("base", 40)
    path = recipe.parameters.get("path", "desk")

    @hg.reference_service
    def configured_value(path: str) -> hg.TS[int]: ...

    @hg.service_impl(interfaces=configured_value)
    def configured_value_impl(path: str) -> hg.TS[int]:
        return hg.const(base + len(path), tp=hg.TS[int])

    @hg.graph
    def parity_graph(value: hg.TS[int]) -> hg.TS[int]:
        hg.register_service(path, configured_value_impl)
        value = _via_reference(hg, value, recipe)
        return value + hg.passive(configured_value(path=path))

    inputs = decoded_inputs(hg, recipe)
    return eval_node(parity_graph, inputs["value"])


def _service_request_reply(hg, recipe):
    from hgraph.test import eval_node

    increment = recipe.parameters.get("increment", 3)
    path = recipe.parameters.get("path", "requests")

    @hg.request_reply_service
    def adjust(path: str, request: hg.TS[int]) -> hg.TS[int]: ...

    @hg.service_impl(interfaces=adjust)
    def adjust_impl(
        request: hg.TSD[int, hg.TS[int]]
    ) -> hg.TSD[int, hg.TS[int]]:
        return hg.map_(lambda value: value + increment, request)

    @hg.graph
    def parity_graph(value: hg.TS[int]) -> hg.TS[int]:
        hg.register_service(path, adjust_impl)
        value = _via_reference(hg, value, recipe)
        return adjust(path, value)

    inputs = decoded_inputs(hg, recipe)
    return eval_node(
        parity_graph,
        inputs["value"],
        __end_time__=hg.MIN_ST + (recipe.tick_count + 4) * hg.MIN_TD,
    )


def _service_subscription(hg, recipe):
    from hgraph.test import eval_node

    multiplier = recipe.parameters.get("multiplier", 10)
    path = recipe.parameters.get("path", "quotes")
    dependency = recipe.parameters.get("dependency", False)

    @hg.subscription_service
    def quote(path: str, symbol: hg.TS[str]) -> hg.TS[int]: ...

    if dependency:
        dependency_path = f"{path}-offset"

        @hg.reference_service
        def offset(path: str = dependency_path) -> hg.TS[int]: ...

        @hg.service_impl(interfaces=offset)
        def offset_impl(path: str = dependency_path) -> hg.TS[int]:
            return hg.const(1)

        @hg.graph
        def quote_value(
            symbol: hg.TS[str], amount: hg.TS[int]
        ) -> hg.TS[int]:
            return hg.len_(symbol) * multiplier + amount

        @hg.service_impl(interfaces=quote)
        def quote_values(
            symbol: hg.TSS[str],
        ) -> hg.TSD[str, hg.TS[int]]:
            return hg.map_(
                quote_value,
                __keys__=symbol,
                __key_arg__="symbol",
                amount=offset(path=dependency_path),
            )
    else:
        @hg.graph
        def quote_value(symbol: hg.TS[str]) -> hg.TS[int]:
            return hg.len_(symbol) * multiplier

        @hg.service_impl(interfaces=quote)
        def quote_values(
            symbol: hg.TSS[str],
        ) -> hg.TSD[str, hg.TS[int]]:
            return hg.map_(
                quote_value,
                __keys__=symbol,
                __key_arg__="symbol",
            )

    @hg.graph
    def parity_graph(symbol: hg.TS[str]) -> hg.TS[int]:
        if dependency:
            hg.register_service(dependency_path, offset_impl)
        hg.register_service(path, quote_values)
        symbol = _via_reference(hg, symbol, recipe)
        return quote(path, symbol)

    inputs = decoded_inputs(hg, recipe)
    return eval_node(
        parity_graph,
        inputs["symbol"],
        __end_time__=hg.MIN_ST + (recipe.tick_count + 4) * hg.MIN_TD,
    )


def _adaptor_loopback(hg, recipe):
    from hgraph.test import eval_node

    factor = recipe.parameters.get("factor", 2)
    path = recipe.parameters.get("path", "loopback")

    @hg.adaptor
    def loopback(path: str, value: hg.TS[int]) -> hg.TS[int]: ...

    @hg.adaptor_impl(interfaces=loopback)
    def loopback_impl(path: str, value: hg.TS[int]) -> hg.TS[int]:
        return value * factor

    @hg.graph
    def parity_graph(value: hg.TS[int]) -> hg.TS[int]:
        hg.register_adaptor(path, loopback_impl)
        value = _via_reference(hg, value, recipe)
        return loopback(path, value)

    inputs = decoded_inputs(hg, recipe)
    return eval_node(parity_graph, inputs["value"])


def _service_adaptor_roundtrip(hg, recipe):
    from hgraph.test import eval_node

    increment = recipe.parameters.get("increment", 1)

    @hg.service_adaptor
    def echo(request: hg.TS[int]) -> hg.TS[int]: ...

    @hg.service_adaptor_impl(interfaces=echo)
    def echo_impl(
        path: str, request: hg.TSD[int, hg.TS[int]]
    ) -> hg.TSD[int, hg.TS[int]]:
        return hg.map_(lambda value: value + increment, request)

    @hg.graph
    def parity_graph(value: hg.TS[int]) -> hg.TS[int]:
        hg.register_adaptor(None, echo_impl)
        value = _via_reference(hg, value, recipe)
        return echo(value)

    inputs = decoded_inputs(hg, recipe)
    return eval_node(
        parity_graph,
        inputs["value"],
        __end_time__=hg.MIN_ST + (recipe.tick_count + 2) * hg.MIN_TD,
    )


def _service_adaptor_parameterized_clients(hg, recipe):
    from hgraph.test import eval_node

    implementations = []

    @hg.service_adaptor
    def routed(
        path: str, passthrough: bool, value: hg.TS[int],
    ) -> hg.TS[int]: ...

    @hg.service_adaptor_impl(interfaces=routed)
    def routed_impl(
        path: str,
        passthrough: bool,
        value: hg.TSD[int, hg.TS[int]],
    ) -> hg.TSD[int, hg.TS[int]]:
        implementations.append((path, passthrough))
        delayed = hg.feedback(hg.TSD[int, hg.TS[int]])
        delayed(value)
        return hg.map_(
            lambda item: item if passthrough else item + 1,
            delayed(),
        )

    @hg.graph
    def parity_graph(
        values: hg.TSD[int, hg.TS[int]],
        direct: hg.TS[int],
        trigger: hg.TS[int],
    ) -> hg.TSL[hg.TS[int], hg.Size[3]]:
        hg.register_adaptor("shared", routed_impl)
        mapped = hg.map_(
            lambda item: routed("shared", False, item),
            values,
        )
        separate = routed("shared", True, direct)
        return hg.sample(
            trigger, hg.combine(mapped[0], mapped[1], separate))

    inputs = decoded_inputs(hg, recipe)
    trace = eval_node(
        parity_graph,
        inputs["values"],
        inputs["direct"],
        inputs["trigger"],
        __end_time__=hg.MIN_ST + (recipe.tick_count + 4) * hg.MIN_TD,
    )
    return {
        "trace": trace,
        "implementations": [
            {"path": path, "passthrough": passthrough}
            for path, passthrough in sorted(implementations)
        ],
    }


def _context_switch(hg, recipe):
    from hgraph.test import eval_node

    @hg.graph
    def add_offset(value: hg.TS[int]) -> hg.TS[int]:
        return value + hg.get_context("offset", hg.TS[int])

    @hg.graph
    def subtract_offset(value: hg.TS[int]) -> hg.TS[int]:
        return value - hg.get_context("offset", hg.TS[int])

    @hg.graph
    def parity_graph(
        selector: hg.TS[str],
        value: hg.TS[int],
        offset: hg.TS[int],
    ) -> hg.TS[int]:
        selector = _via_reference(hg, selector, recipe)
        value = _via_reference(hg, value, recipe)
        offset = _via_reference(hg, offset, recipe)
        with offset:
            return hg.switch_(
                selector,
                {"add": add_offset, "subtract": subtract_offset},
                value,
            )

    inputs = decoded_inputs(hg, recipe)
    return eval_node(
        parity_graph,
        inputs["selector"],
        inputs["value"],
        inputs["offset"],
    )


def _operator_pipeline(hg, recipe):
    from hgraph.test import eval_node

    format_ref = recipe.parameters.get("format_ref", False)

    class OperatorResult(hg.TimeSeriesSchema):
        quotient: hg.TS[int]
        remainder: hg.TS[int]
        minimum: hg.TS[int]
        maximum: hg.TS[int]
        selected: hg.TS[int]
        formatted: hg.TS[str]
        length: hg.TS[int]
        both: hg.TS[bool]
        either: hg.TS[bool]
        inverse: hg.TS[bool]
        valid: hg.TS[bool]
        modified: hg.TS[bool]

    @hg.graph
    def parity_graph(
        lhs: hg.TS[int],
        rhs: hg.TS[int],
        choose_minimum: hg.TS[bool],
    ) -> hg.TSB[OperatorResult]:
        lhs = _via_reference(hg, lhs, recipe)
        rhs = _via_reference(hg, rhs, recipe)
        choose_minimum = _via_reference(hg, choose_minimum, recipe)
        quotient = lhs // rhs
        remainder = lhs % rhs
        minimum = hg.min_(lhs, rhs)
        maximum = hg.max_(lhs, rhs)
        selected = hg.if_then_else(choose_minimum, minimum, maximum)
        formatted_value = selected if format_ref else selected + 0
        formatted = hg.format_("{}:{}", formatted_value, remainder)
        comparison = lhs > rhs
        return hg.combine[hg.TSB[OperatorResult]](
            quotient=quotient,
            remainder=remainder,
            minimum=minimum,
            maximum=maximum,
            selected=selected,
            formatted=formatted,
            length=hg.len_(formatted),
            both=hg.and_(choose_minimum, comparison),
            either=hg.or_(choose_minimum, comparison),
            inverse=hg.not_(choose_minimum),
            valid=hg.valid(selected),
            modified=hg.modified(selected),
        )

    inputs = decoded_inputs(hg, recipe)
    return eval_node(
        parity_graph,
        inputs["lhs"],
        inputs["rhs"],
        inputs["choose_minimum"],
    )


#: The declaration shapes a ``declaration_shape`` recipe may take.
#:
#: Every other template fixes its declarations and varies the values flowing
#: through them. This one varies the *declarations* instead: each shape crosses
#: a declared type with a differently-shaped resolved one, which is the
#: boundary that produced most of the fixes in PR #795 (a derived value through
#: a declared base, a partial mapping where a complete bundle is declared, a
#: mapped parameter that could bind either a collection or its element, and a
#: reference-carrying branch beside a directly built one).
DECLARATION_SHAPES = (
    "derived_through_base",
    "partial_bundle_return",
    "element_or_whole",
    "branch_shape_equivalence",
)


#: The exact inputs each shape wires. ``required_inputs`` cannot express this
#: because the set varies per shape, so the validator is the only boundary that
#: can reject a malformed recipe. It has to: a missing input raises the same
#: ``KeyError`` in BOTH runners, and two matching failures compare equal, so an
#: unvalidated recipe would be reported as a parity match.
DECLARATION_SHAPE_INPUTS = {
    "derived_through_base": ("value",),
    "partial_bundle_return": ("value",),
    "element_or_whole": ("key", "value"),
    "branch_shape_equivalence": ("key", "selector", "value"),
}


def _validate_declaration_shape(recipe):
    shape = recipe.parameters.get("declaration_shape")
    if shape is None:
        raise RecipeError(
            "declaration_shape requires a 'declaration_shape' parameter")
    if not isinstance(shape, str) or shape not in DECLARATION_SHAPES:
        raise RecipeError(
            f"declaration_shape must be one of {DECLARATION_SHAPES}, "
            f"got {shape!r}")
    unexpected = set(recipe.parameters) - {"declaration_shape"}
    if unexpected:
        raise RecipeError(
            f"declaration_shape takes no parameters besides "
            f"'declaration_shape', got {sorted(unexpected)}")
    expected = DECLARATION_SHAPE_INPUTS[shape]
    if tuple(sorted(recipe.inputs)) != expected:
        raise RecipeError(
            f"declaration_shape {shape!r} requires inputs {expected}, "
            f"got {tuple(sorted(recipe.inputs))}")


def _declaration_shape(hg, recipe):
    """Wire one declared-versus-resolved boundary, chosen by the recipe.

    The graphs are deliberately small: the subject is the shape of the
    declarations around a value, not the arithmetic inside them.
    """
    from dataclasses import dataclass

    from hgraph.test import eval_node

    shape = recipe.parameters["declaration_shape"]
    inputs = decoded_inputs(hg, recipe)

    @dataclass(frozen=True)
    class Base(hg.CompoundScalar):
        label: str

    @dataclass(frozen=True)
    class Derived(Base):
        amount: int

    class Pair(hg.TimeSeriesSchema):
        first: hg.TS[int]
        second: hg.TS[int]

    if shape == "derived_through_base":
        # The derived value must keep its own identity while flowing through a
        # graph that declares only the base.
        @hg.compute_node
        def make(value: hg.TS[int]) -> hg.TS[Base]:
            return Derived(label="d", amount=value.value)

        @hg.graph
        def through_base(value: hg.TS[Base]) -> hg.TS[Base]:
            return value

        @hg.graph
        def parity_graph(value: hg.TS[int]) -> hg.TS[Base]:
            return through_base(make(value))

        return eval_node(parity_graph, inputs["value"])

    if shape == "partial_bundle_return":
        # A graph declared to return a bundle returns a subset of its fields;
        # the omitted field must behave as a source that never ticks.
        @hg.graph
        def partial(value: hg.TS[int]) -> hg.TSB[Pair]:
            return {"first": value}

        @hg.graph
        def parity_graph(value: hg.TS[int]) -> hg.TS[int]:
            return partial(value).first

        return eval_node(parity_graph, inputs["value"])

    if shape == "element_or_whole":
        # ``nested`` is declared as a structured generic that BOTH the whole
        # collection and its element satisfy, which is the ambiguity the map
        # classifier has to resolve. A concrete ``TS[int]`` parameter would
        # only ever accept the element and would not reach that decision.
        @hg.graph
        def wrap(v: hg.TS[int], k: hg.TS[str]) -> hg.TSD[str, hg.TS[int]]:
            return hg.convert[hg.TSD[str, hg.TS[int]]](k, v)

        @hg.graph
        def child(
            value: hg.TS[int], nested: hg.TSD[str, hg.TIME_SERIES_TYPE]
        ) -> hg.TS[int]:
            return value + hg.len_(nested)

        @hg.graph
        def parity_graph(
            value: hg.TS[int], key: hg.TS[str]
        ) -> hg.TSD[str, hg.TS[int]]:
            inner = hg.convert[hg.TSD[str, hg.TS[int]]](key, value)
            nested = hg.map_(wrap, inner, key)
            return hg.map_(child, inner, nested)

        return eval_node(parity_graph, inputs["value"], inputs["key"])

    if shape == "branch_shape_equivalence":
        # Two branches produce the same declared type by different routes: one
        # builds the dictionary directly, the other reaches it through a
        # per-key child graph. Parameter names avoid ``key``, which a branch's
        # signature would otherwise offer to the switch key.
        @hg.graph
        def identity(a: hg.TS[int]) -> hg.TS[int]:
            return a

        @hg.graph
        def direct(a: hg.TS[int], b: hg.TS[str]) -> hg.TSD[str, hg.TS[int]]:
            return hg.convert[hg.TSD[str, hg.TS[int]]](b, a)

        @hg.graph
        def projected(a: hg.TS[int], b: hg.TS[str]) -> hg.TSD[str, hg.TS[int]]:
            return hg.map_(identity, hg.convert[hg.TSD[str, hg.TS[int]]](b, a))

        @hg.graph
        def parity_graph(
            selector: hg.TS[str], value: hg.TS[int], key: hg.TS[str]
        ) -> hg.TSD[str, hg.TS[int]]:
            return hg.switch_(
                selector, {"direct": direct, "projected": projected}, value, key
            )

        return eval_node(
            parity_graph, inputs["selector"], inputs["value"], inputs["key"]
        )

    raise RecipeError(f"unknown declaration_shape {shape!r}")


def _value_consumer_reference(hg, recipe):
    from hgraph.test import eval_node

    def double(value: int) -> int:
        return value * 2

    @hg.graph
    def parity_graph(
        lhs: hg.TS[int],
        rhs: hg.TS[int],
        choose_rhs: hg.TS[bool],
    ) -> hg.TS[int]:
        lhs = _via_reference(hg, lhs, recipe)
        rhs = _via_reference(hg, rhs, recipe)
        choose_rhs = _via_reference(hg, choose_rhs, recipe)
        selected = hg.if_then_else(choose_rhs, rhs, lhs)
        return hg.apply(double, selected)

    inputs = decoded_inputs(hg, recipe)
    return eval_node(
        parity_graph,
        inputs["lhs"],
        inputs["rhs"],
        inputs["choose_rhs"],
    )


def _tsd_key_set_pipeline(hg, recipe):
    from hgraph.test import eval_node

    dedup_size = recipe.parameters.get("dedup_size", True)

    class SetOperatorResult(hg.TimeSeriesSchema):
        size: hg.TS[int]
        empty: hg.TS[bool]
        contains: hg.TS[bool]
        minimum: hg.TS[int]
        maximum: hg.TS[int]
        total: hg.TS[int]
        average: hg.TS[float]

    @hg.graph
    def parity_graph(
        values: hg.TSD[int, hg.TS[int]], probe: hg.TS[int]
    ) -> hg.TSB[SetOperatorResult]:
        values = _via_reference(hg, values, recipe)
        probe = _via_reference(hg, probe, recipe)
        keys = hg.keys_(values)
        size = hg.len_(keys)
        if dedup_size:
            size = hg.dedup(size)
        return hg.combine[hg.TSB[SetOperatorResult]](
            size=size,
            empty=hg.is_empty(keys),
            contains=hg.contains_(keys, probe),
            minimum=hg.min_(keys, default_value=0),
            maximum=hg.max_(keys, default_value=0),
            total=hg.sum_(keys),
            average=hg.mean(keys),
        )

    inputs = decoded_inputs(hg, recipe)
    return eval_node(parity_graph, inputs["values"], inputs["probe"])


def _mesh_key_set(hg, recipe):
    from hgraph.test import eval_node

    factor = recipe.parameters.get("factor", 2)

    @hg.graph
    def keyed_value(key: hg.TS[int]) -> hg.TS[int]:
        return key * factor

    @hg.graph
    def parity_graph(
        values: hg.TSD[int, hg.TS[int]],
    ) -> hg.TSD[int, hg.TS[int]]:
        values = _via_reference(hg, values, recipe)
        return hg.mesh_(
            keyed_value,
            __keys__=hg.keys_(values),
            __key_arg__="key",
        )

    inputs = decoded_inputs(hg, recipe)
    return eval_node(parity_graph, inputs["values"])


def _issue_38_nested_tsd_feedback(hg, recipe):
    from frozendict import frozendict
    from hgraph.test import eval_node

    class FeedbackPosition(hg.TimeSeriesSchema):
        units: hg.TSD[str, hg.TS[float]]
        unit_values: hg.TSD[str, hg.TS[float]]

    @hg.graph
    def update_position(
        current: hg.TSB[FeedbackPosition],
        prices: hg.TSD[str, hg.TS[float]],
    ) -> hg.TSB[FeedbackPosition]:
        units = hg.const(
            frozendict({"next": 1.0}),
            hg.TSD[str, hg.TS[float]],
        )
        return hg.combine[hg.TSB[FeedbackPosition]](
            units=units,
            unit_values=hg.map_(lambda unit, price: price, units, prices),
        )

    @hg.graph
    def parity_graph(
        roll: hg.TS[bool],
        prices: hg.TSD[str, hg.TS[float]],
        trigger: hg.TS[int],
    ) -> hg.TS[float]:
        position_feedback = hg.feedback(hg.TSB[FeedbackPosition])
        initial_position = hg.combine[hg.TSB[FeedbackPosition]](
            units=hg.const(
                frozendict({"old": 0.5, "next": 0.5}),
                hg.TSD[str, hg.TS[float]],
            ),
            unit_values=hg.const(
                frozendict({"old": 10.0, "next": 10.0}),
                hg.TSD[str, hg.TS[float]],
            ),
        )
        position = hg.dedup(
            hg.default(hg.lag(position_feedback(), 1, trigger), initial_position)
        )
        output = hg.switch_(
            roll,
            {
                True: update_position,
                False: lambda current, prices: hg.dedup(current),
            },
            position,
            prices,
        )
        position_feedback(hg.dedup(output))
        return hg.sample(trigger, position.unit_values["next"])

    inputs = decoded_inputs(hg, recipe)
    return eval_node(
        parity_graph,
        inputs["roll"],
        inputs["prices"],
        inputs["trigger"],
    )


def _issue_40_no_key_rebind(hg, recipe):
    from frozendict import frozendict
    from hgraph.test import eval_node

    class RebasedFeedbackState(hg.TimeSeriesSchema):
        unit_values: hg.TSD[str, hg.TS[float]]
        target_units: hg.TSD[str, hg.TS[float]]

    @hg.graph
    def parity_graph(
        target: hg.TSD[str, hg.TS[float]],
        rebase: hg.TS[bool],
        prices: hg.TSD[str, hg.TS[float]],
        trigger: hg.TS[int],
    ) -> hg.TS[float]:
        state_feedback = hg.feedback(hg.TSB[RebasedFeedbackState])
        initial_state = hg.combine[hg.TSB[RebasedFeedbackState]](
            unit_values=hg.const(
                frozendict(), hg.TSD[str, hg.TS[float]]
            ),
            target_units=hg.const(
                frozendict(), hg.TSD[str, hg.TS[float]]
            ),
        )
        state = hg.default(
            hg.lag(state_feedback(), 1, trigger),
            initial_state,
        )
        target_units = hg.if_then_else(
            rebase,
            target,
            state.target_units,
        )
        unit_values = hg.map_(
            lambda unit, price: price,
            target_units,
            hg.no_key(prices),
        )
        state_feedback(
            hg.combine[hg.TSB[RebasedFeedbackState]](
                unit_values=unit_values,
                target_units=target_units,
            )
        )
        return hg.sample(
            trigger,
            hg.default(state.unit_values["next"], -1.0),
        )

    inputs = decoded_inputs(hg, recipe)
    return eval_node(
        parity_graph,
        inputs["target"],
        inputs["rebase"],
        inputs["prices"],
        inputs["trigger"],
    )


# --------------------------------------------------------------------------
# Realistic Python user surfaces which combine Python-owned nominal values,
# structural time-series, higher-order wiring, and helper frameworks.  These
# are deliberately public-API constructions: the campaign must find failures
# through the same paths an application uses, not by manipulating internals.

_POLYMORPHIC_EVENT_OPERATIONS = (
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
)
_POLYMORPHIC_EVENT_MAP_OPERATIONS = (
    "map_compute",
    "map_emit",
    "map_feedback",
    "map_emit_feedback_outer",
)
_STRUCTURAL_MAP_PROJECTIONS = (
    "lookup",
    "combine",
    "dispatch_combine",
    "dereference",
    "captured_combine",
)
_ARROW_PROJECTIONS = ("pair", "first", "second")
_ARROW_DEBUG_MODES = ("none", "direct", "configured")
_ARROW_EXECUTION_MODES = ("graph", "eval")


def _validate_event_spec(value, *, context):
    if value is None:
        return
    if not isinstance(value, dict):
        raise RecipeError(f"{context} must be an event object or null")
    kind = value.get("kind")
    fields = {
        "heartbeat": {"kind", "event_id"},
        "create": {"kind", "event_id", "order_id", "quantity"},
        "cancel": {"kind", "event_id", "order_id", "reason"},
    }
    if kind not in fields or set(value) != fields[kind]:
        raise RecipeError(
            f"{context} must be a heartbeat, create, or cancel event"
        )
    for name, item in value.items():
        if name == "quantity":
            if (
                not isinstance(item, int)
                or isinstance(item, bool)
                or not -1_000 <= item <= 1_000
            ):
                raise RecipeError(f"{context} quantity must be a bounded integer")
        elif not isinstance(item, str) or not 1 <= len(item) <= 64:
            raise RecipeError(f"{context} {name} must be a bounded string")


def _validate_polymorphic_event_flow(recipe):
    if set(recipe.inputs) != {"events", "trigger", "key"}:
        raise RecipeError(
            "polymorphic_event_flow requires events, trigger, and key inputs"
        )
    operation = recipe.parameters.get("operation")
    if operation not in _POLYMORPHIC_EVENT_OPERATIONS:
        raise RecipeError(
            "polymorphic_event_flow operation must be one of "
            f"{_POLYMORPHIC_EVENT_OPERATIONS}"
        )
    lengths = {len(values) for values in recipe.inputs.values()}
    if len(lengths) != 1:
        raise RecipeError("polymorphic_event_flow inputs must have equal tick counts")
    for index, value in enumerate(recipe.inputs["events"]):
        _validate_event_spec(value, context=f"events[{index}]")
    for value in recipe.inputs["trigger"]:
        if value is not None and not isinstance(value, bool):
            raise RecipeError("polymorphic_event_flow trigger ticks must be booleans")
    for value in recipe.inputs["key"]:
        if value is not None and (
            not isinstance(value, str) or not 1 <= len(value) <= 32
        ):
            raise RecipeError("polymorphic_event_flow key ticks must be bounded strings")


def _validate_polymorphic_event_map(recipe):
    if set(recipe.inputs) != {"events"}:
        raise RecipeError("polymorphic_event_map requires the events input")
    operation = recipe.parameters.get("operation")
    if operation not in _POLYMORPHIC_EVENT_MAP_OPERATIONS:
        raise RecipeError(
            "polymorphic_event_map operation must be one of "
            f"{_POLYMORPHIC_EVENT_MAP_OPERATIONS}"
        )
    for tick_index, tick in enumerate(recipe.inputs["events"]):
        if tick is None:
            continue
        if not isinstance(tick, dict):
            raise RecipeError("polymorphic_event_map ticks must be mappings or null")
        for key, value in tick.items():
            if not isinstance(key, str) or not 1 <= len(key) <= 32:
                raise RecipeError("polymorphic_event_map keys must be bounded strings")
            if isinstance(value, dict) and value == {"$remove": True}:
                continue
            _validate_event_spec(
                value, context=f"events[{tick_index}][{key!r}]"
            )


def _validate_structural_map_projection(recipe):
    if set(recipe.inputs) != {"lookups", "rows"}:
        raise RecipeError(
            "structural_map_projection requires lookups and rows inputs"
        )
    projection = recipe.parameters.get("projection")
    if projection not in _STRUCTURAL_MAP_PROJECTIONS:
        raise RecipeError(
            "structural_map_projection projection must be one of "
            f"{_STRUCTURAL_MAP_PROJECTIONS}"
        )
    if len(recipe.inputs["lookups"]) != len(recipe.inputs["rows"]):
        raise RecipeError(
            "structural_map_projection inputs must have equal tick counts"
        )
    for tick in recipe.inputs["lookups"]:
        if tick is None:
            continue
        if not isinstance(tick, dict):
            raise RecipeError("structural_map_projection lookups must be mappings")
        for key, value in tick.items():
            if not isinstance(key, str) or not 1 <= len(key) <= 32:
                raise RecipeError("structural_map_projection lookup keys are invalid")
            if isinstance(value, dict) and value == {"$remove": True}:
                continue
            if not isinstance(value, str) or not 1 <= len(value) <= 32:
                raise RecipeError(
                    "structural_map_projection lookup values must be bounded strings"
                )
    row_fields = {"value", "quantity", "label"}
    for tick in recipe.inputs["rows"]:
        if tick is None:
            continue
        if not isinstance(tick, dict):
            raise RecipeError("structural_map_projection rows must be mappings")
        for key, value in tick.items():
            if not isinstance(key, str) or not 1 <= len(key) <= 32:
                raise RecipeError("structural_map_projection row keys are invalid")
            if isinstance(value, dict) and value == {"$remove": True}:
                continue
            if not isinstance(value, dict) or not value or not set(value) <= row_fields:
                raise RecipeError(
                    "structural_map_projection row values must contain Row fields"
                )
            for field_name, field_value in value.items():
                if field_name == "label":
                    if not isinstance(field_value, str) or len(field_value) > 64:
                        raise RecipeError("structural row labels must be bounded strings")
                elif (
                    not isinstance(field_value, int)
                    or isinstance(field_value, bool)
                    or not -1_000 <= field_value <= 1_000
                ):
                    raise RecipeError(
                        "structural row numeric fields must be bounded integers"
                    )


def _validate_arrow_typed_projection(recipe):
    if set(recipe.inputs) != {"side", "events"}:
        raise RecipeError("arrow_typed_projection requires side and events inputs")
    if len(recipe.inputs["side"]) != len(recipe.inputs["events"]):
        raise RecipeError("arrow_typed_projection inputs must have equal tick counts")
    if recipe.parameters.get("projection") not in _ARROW_PROJECTIONS:
        raise RecipeError(
            f"arrow_typed_projection projection must be one of {_ARROW_PROJECTIONS}"
        )
    if recipe.parameters.get("debug") not in _ARROW_DEBUG_MODES:
        raise RecipeError(
            f"arrow_typed_projection debug must be one of {_ARROW_DEBUG_MODES}"
        )
    if recipe.parameters.get("execution") not in _ARROW_EXECUTION_MODES:
        raise RecipeError(
            "arrow_typed_projection execution must be one of "
            f"{_ARROW_EXECUTION_MODES}"
        )
    for value in recipe.inputs["side"]:
        if value is not None and value not in {"BUY", "SELL"}:
            raise RecipeError("arrow_typed_projection sides must be BUY or SELL")
    for index, value in enumerate(recipe.inputs["events"]):
        _validate_event_spec(value, context=f"events[{index}]")


def _polymorphic_event_model(hg):
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class Event(hg.CompoundScalar):
        event_id: str

    # Defining a public node against the base before defining extension leaves
    # models the normal package import order without reaching into metadata
    # internals.
    @hg.compute_node
    def upcast(value: hg.TS[Event]) -> hg.TS[Event]:
        return value.value

    @dataclass(frozen=True)
    class HeartbeatEvent(Event):
        pass

    @dataclass(frozen=True)
    class OrderEvent(Event):
        order_id: str

    @dataclass(frozen=True)
    class CreateEvent(OrderEvent):
        quantity: int

    @dataclass(frozen=True)
    class CancelEvent(OrderEvent):
        reason: str

    def decode(value):
        if value is None:
            return None
        kind = value["kind"]
        if kind == "heartbeat":
            return HeartbeatEvent(event_id=value["event_id"])
        if kind == "create":
            return CreateEvent(
                event_id=value["event_id"],
                order_id=value["order_id"],
                quantity=value["quantity"],
            )
        return CancelEvent(
            event_id=value["event_id"],
            order_id=value["order_id"],
            reason=value["reason"],
        )

    return Event, HeartbeatEvent, CreateEvent, CancelEvent, upcast, decode


def _polymorphic_event_flow(hg, recipe):
    from dataclasses import dataclass
    from typing import Mapping, Set

    from hgraph.test import eval_node

    Event, _, CreateEvent, _, upcast, decode = _polymorphic_event_model(hg)
    operation = recipe.parameters["operation"]
    events = [decode(value) for value in recipe.inputs["events"]]
    triggers = list(recipe.inputs["trigger"])
    keys = list(recipe.inputs["key"])
    end_time = hg.MIN_ST + (recipe.tick_count + 4) * hg.MIN_TD

    @dataclass(frozen=True)
    class FrameRow(hg.CompoundScalar):
        event_id: str

    @hg.compute_node
    def singleton(value: hg.TS[Event]) -> hg.TS[tuple[Event, ...]]:
        return (value.value,)

    @hg.graph
    def emitted(value: hg.TS[Event]) -> hg.TS[Event]:
        return hg.emit(singleton(value))

    @hg.graph
    def delayed(value: hg.TS[Event]) -> hg.TS[Event]:
        state = hg.feedback(hg.TS[Event])
        state(value)
        return state()

    default_event = CreateEvent(
        event_id="default-event", order_id="default-order", quantity=0
    )

    @hg.graph
    def delayed_from_default(value: hg.TS[Event]) -> hg.TS[Event]:
        state = hg.feedback(hg.TS[Event], default=default_event)
        state(value)
        return state()

    @hg.graph
    def singleton_tuple(value: hg.TS[Event]) -> hg.TS[tuple[Event, ...]]:
        return hg.convert[hg.TS[tuple[Event, ...]]](value)

    @hg.compute_node
    def fixed_rows(value: hg.TS[Event]) -> hg.TS[tuple[FrameRow, FrameRow]]:
        row = FrameRow(event_id=value.value.event_id)
        return row, row

    @hg.compute_node
    def frame_event_ids(
        value: hg.TS[hg.Frame[FrameRow]],
    ) -> hg.TS[tuple[str, ...]]:
        frame = value.value
        rows = frame.to_dicts() if hasattr(frame, "to_dicts") else frame.to_pylist()
        return tuple(row["event_id"] for row in rows)

    @hg.graph
    def fixed_tuple_frame(value: hg.TS[Event]) -> hg.TS[tuple[str, ...]]:
        frame = hg.convert[hg.TS[hg.Frame[FrameRow]]](fixed_rows(value))
        return frame_event_ids(frame)

    @hg.graph
    def singleton_set(value: hg.TS[Event]) -> hg.TS[Set[Event]]:
        return hg.convert[hg.TS[Set[Event]]](value)

    @hg.graph
    def singleton_mapping(
        key: hg.TS[str], value: hg.TS[Event]
    ) -> hg.TS[Mapping[str, Event]]:
        return hg.convert[hg.TS[Mapping[str, Event]]](key, value)

    @hg.graph
    def collected_values(
        key: hg.TS[str], value: hg.TS[Event]
    ) -> hg.TS[tuple[Event, ...]]:
        values = hg.collect[hg.TS[Mapping[str, Event]]](key, value)
        return hg.values_(values)

    @hg.graph
    def batched(
        trigger: hg.TS[bool], value: hg.TS[Event]
    ) -> hg.TS[tuple[Event, ...]]:
        return hg.batch(trigger, value, hg.MIN_TD)

    @hg.graph
    def windowed(value: hg.TS[Event]) -> hg.TS[tuple[Event, ...]]:
        return hg.window(value, 2).buffer

    @hg.graph
    def json_round_trip(value: hg.TS[Event]) -> hg.TS[Event]:
        return hg.from_json[hg.TS[Event]](hg.to_json(value))

    unary = {
        "compute": upcast,
        "emit": emitted,
        "feedback": delayed,
        "feedback_default": delayed_from_default,
        "tuple": singleton_tuple,
        "fixed_tuple_frame": fixed_tuple_frame,
        "set": singleton_set,
        "window": windowed,
        "json_round_trip": json_round_trip,
    }
    if operation in unary:
        return eval_node(unary[operation], events, __end_time__=end_time)
    if operation == "mapping":
        return eval_node(singleton_mapping, keys, events, __end_time__=end_time)
    if operation == "collect_values":
        return eval_node(collected_values, keys, events, __end_time__=end_time)
    if operation == "batch":
        return eval_node(batched, triggers, events, __end_time__=end_time)

    @hg.component
    def recorded_events(events: hg.TS[Event]) -> hg.TS[Event]:
        return events

    with hg.GlobalState() as state:
        hg.set_record_replay_model(hg.IN_MEMORY)
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECORD):
            live = eval_node(recorded_events, events, __end_time__=end_time)
        recording = state.get(":memory:recorded_events.events")
        recorded = [value for _, value in recording]
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.REPLAY):
            replayed = eval_node(recorded_events, [], __end_time__=end_time)
    return {"live": live, "recorded": recorded, "replayed": replayed}


def _polymorphic_event_map(hg, recipe):
    from hgraph.test import eval_node

    Event, _, _, _, upcast, decode = _polymorphic_event_model(hg)
    operation = recipe.parameters["operation"]

    ticks = []
    for tick in recipe.inputs["events"]:
        if tick is None:
            ticks.append(None)
            continue
        ticks.append({
            key: (
                _decode_value(hg, value)
                if isinstance(value, dict) and value == {"$remove": True}
                else decode(value)
            )
            for key, value in tick.items()
        })

    @hg.compute_node
    def singleton(value: hg.TS[Event]) -> hg.TS[tuple[Event, ...]]:
        return (value.value,)

    @hg.graph
    def emit_event(value: hg.TS[Event]) -> hg.TS[Event]:
        return hg.emit(singleton(value))

    @hg.graph
    def delayed_event(value: hg.TS[Event]) -> hg.TS[Event]:
        state = hg.feedback(hg.TS[Event])
        state(value)
        return state()

    @hg.graph
    def emit_then_delay(value: hg.TS[Event]) -> hg.TS[Event]:
        return delayed_event(emit_event(value))

    if operation == "map_compute":
        @hg.graph
        def app(events: hg.TSD[str, hg.TS[Event]]) -> hg.TSD[str, hg.TS[Event]]:
            return hg.map_(upcast, events)
    elif operation == "map_emit":
        @hg.graph
        def app(events: hg.TSD[str, hg.TS[Event]]) -> hg.TSD[str, hg.TS[Event]]:
            return hg.map_(emit_event, events)
    elif operation == "map_feedback":
        @hg.graph
        def app(events: hg.TSD[str, hg.TS[Event]]) -> hg.TSD[str, hg.TS[Event]]:
            return hg.map_(delayed_event, events)
    else:
        @hg.graph
        def app(
            events: hg.TSD[str, hg.TS[Event]],
        ) -> hg.TSB[hg.KeyValue[str, hg.TS[Event]]]:
            return hg.emit(hg.map_(emit_then_delay, events))

    return eval_node(
        app,
        ticks,
        __end_time__=hg.MIN_ST + (recipe.tick_count + 5) * hg.MIN_TD,
    )


def _structural_map_projection(hg, recipe):
    from hgraph.test import eval_node

    class Row(hg.TimeSeriesSchema):
        value: hg.TS[int]
        quantity: hg.TS[int]
        label: hg.TS[str]

    class Projection(hg.TimeSeriesSchema):
        value: hg.TS[int]
        quantity: hg.TS[int]
        label: hg.TS[str]

    @hg.graph
    def keyed_lookup(
        lookup: hg.TS[str], rows: hg.TSD[str, hg.TSB[Row]],
    ) -> hg.TSB[Row]:
        return rows[lookup]

    @hg.graph
    def materialize_lookup(
        lookup: hg.TS[str], rows: hg.TSD[str, hg.TSB[Row]],
    ) -> hg.TSB[Projection]:
        selected = rows[lookup]
        return hg.combine[hg.TSB[Projection]](
            value=selected.value,
            quantity=selected.quantity,
            label=selected.label,
        )

    projection = recipe.parameters["projection"]
    if projection == "lookup":
        @hg.graph
        def app(
            lookups: hg.TSD[str, hg.TS[str]],
            rows: hg.TSD[str, hg.TSB[Row]],
        ) -> hg.TSD[str, hg.REF[hg.TSB[Row]]]:
            return hg.map_(keyed_lookup, lookups, hg.pass_through(rows))
    elif projection == "combine":
        @hg.graph
        def app(
            lookups: hg.TSD[str, hg.TS[str]],
            rows: hg.TSD[str, hg.TSB[Row]],
        ) -> hg.TSD[str, hg.TSB[Projection]]:
            return hg.map_(materialize_lookup, lookups, hg.pass_through(rows))
    elif projection == "dispatch_combine":
        class Animal(hg.CompoundScalar):
            pass

        class Dog(Animal):
            pass

        @hg.operator
        def apply(
            animal: hg.TS[Animal], repository: hg.TSB[Projection],
        ) -> hg.TS[int]: ...

        @hg.graph(overloads=apply)
        def apply_dog(
            animal: hg.TS[Dog], repository: hg.TSB[Projection],
        ) -> hg.TS[int]:
            return repository.value + repository.quantity

        @hg.graph
        def dispatch_lookup(
            lookup: hg.TS[str], rows: hg.TSD[str, hg.TSB[Row]],
        ) -> hg.TS[int]:
            repository = materialize_lookup(lookup, rows)
            return hg.dispatch_(
                apply, hg.const(Dog()), repository=repository
            )

        @hg.graph
        def app(
            lookups: hg.TSD[str, hg.TS[str]],
            rows: hg.TSD[str, hg.TSB[Row]],
        ) -> hg.TSD[str, hg.TS[int]]:
            return hg.map_(dispatch_lookup, lookups, hg.pass_through(rows))
    elif projection == "dereference":
        @hg.graph
        def dereference_row(
            row: hg.TSB[hg.TS_SCHEMA],
        ) -> hg.TSB[hg.TS_SCHEMA]:
            return hg.dereference(row)

        @hg.graph
        def app(
            lookups: hg.TSD[str, hg.TS[str]],
            rows: hg.TSD[str, hg.TSB[Row]],
        ) -> hg.TSD[str, hg.TSB[Row]]:
            return hg.map_(dereference_row, rows)
    else:
        class CapturedProjection(hg.TimeSeriesSchema):
            lookup: hg.TS[str]
            row_value: hg.TS[int]

        @hg.graph
        def app(
            lookups: hg.TSD[str, hg.TS[str]],
            rows: hg.TSD[str, hg.TSB[Row]],
        ) -> hg.TSD[str, hg.TSB[CapturedProjection]]:
            row_values = rows.value
            return hg.map_(
                lambda key: hg.combine(
                    lookup=lookups[key], row_value=row_values[key],
                ),
                __keys__=lookups.key_set,
            )

    inputs = decoded_inputs(hg, recipe)
    return eval_node(
        app,
        inputs["lookups"],
        inputs["rows"],
        __end_time__=hg.MIN_ST + (recipe.tick_count + 4) * hg.MIN_TD,
    )


def _arrow_typed_projection(hg, recipe):
    from enum import Enum

    from hgraph.arrow import arrow, debug_, eval_, first, i, second
    from hgraph.test import eval_node

    Event, _, _, _, _, decode = _polymorphic_event_model(hg)

    class Side(Enum):
        BUY = "BUY"
        SELL = "SELL"

    @hg.compute_node
    def render(side: hg.TS[Side], event: hg.TS[Event]) -> hg.TS[str]:
        value = event.value
        return f"{side.value.value}:{type(value).__name__}:{value.event_id}"

    @hg.compute_node
    def render_side(side: hg.TS[Side]) -> hg.TS[str]:
        return side.value.value

    @hg.compute_node
    def render_event(event: hg.TS[Event]) -> hg.TS[str]:
        value = event.value
        return f"{type(value).__name__}:{value.event_id}"

    projection = recipe.parameters["projection"]
    debug = recipe.parameters["debug"]
    terminal = render if projection == "pair" else render_side if projection == "first" else render_event
    projection_arrow = i if projection == "pair" else first if projection == "first" else second
    debug_arrow = i if debug == "none" else debug_ if debug == "direct" else debug_("parity {}")
    pipeline = debug_arrow >> projection_arrow >> terminal
    sides = [Side[value] if value is not None else None for value in recipe.inputs["side"]]
    events = [decode(value) for value in recipe.inputs["events"]]

    if recipe.parameters["execution"] == "eval":
        return eval_(sides, events, type_map=(hg.TS[Side], hg.TS[Event])) | pipeline

    @hg.graph
    def app(side: hg.TS[Side], event: hg.TS[Event]) -> hg.TS[str]:
        return arrow(side, event) | pipeline

    return eval_node(app, sides, events)


def _stream_dataclass(hg, recipe):
    from dataclasses import dataclass

    from hgraph.reflection import fields, scalar_type
    from hgraph.stream import Stream
    from hgraph.test import eval_node

    @dataclass(frozen=True)
    class Payload:
        value: bool

    stream_fields = fields(hg.TSB[Stream[Payload]])
    payload_fields = fields(hg.TSB[Payload])

    @hg.graph
    def round_trip(value: hg.TS[Payload]) -> hg.TS[Payload]:
        return hg.convert[hg.TS[Payload]](hg.convert[hg.TSB](value))

    values = eval_node(
        round_trip,
        [Payload(value) if value is not None else None
         for value in decoded_inputs(hg, recipe)["probe"]],
    )
    return {
        "stream_fields": {
            name: (
                scalar_type(field_type).__module__
                + "."
                + scalar_type(field_type).__qualname__
            )
            for name, field_type in sorted(stream_fields.items())
        },
        "payload_fields": {
            name: (
                scalar_type(field_type).__module__
                + "."
                + scalar_type(field_type).__qualname__
            )
            for name, field_type in sorted(payload_fields.items())
        },
        "round_trip": values,
    }


def _validate_compound_scalar_downcast(recipe):
    if recipe.parameters:
        raise RecipeError("compound_scalar_downcast takes no parameters")
    for tick in recipe.inputs["event"]:
        if tick is None:
            continue
        if not isinstance(tick, dict) or set(tick) != {
            "event_id", "order_id", "quantity"
        }:
            raise RecipeError(
                "compound_scalar_downcast events require event_id, order_id, and quantity"
            )
        if not isinstance(tick["event_id"], str) or not tick["event_id"]:
            raise RecipeError("compound_scalar_downcast event_id must be a non-empty string")
        if not isinstance(tick["order_id"], str) or not tick["order_id"]:
            raise RecipeError("compound_scalar_downcast order_id must be a non-empty string")
        if not isinstance(tick["quantity"], int) or isinstance(tick["quantity"], bool):
            raise RecipeError("compound_scalar_downcast quantity must be an integer")


def _compound_scalar_downcast(hg, recipe):
    from dataclasses import dataclass

    from hgraph.test import eval_node

    @dataclass(frozen=True)
    class Event(hg.CompoundScalar):
        event_id: str

    @dataclass(frozen=True)
    class OrderEvent(Event):
        order_id: str

    @dataclass(frozen=True)
    class CreateEvent(OrderEvent):
        quantity: int

    @hg.graph
    def parity_graph(event: hg.TS[Event]) -> hg.TS[CreateEvent]:
        # The explicit positional target is the released 0.5 public contract.
        return hg.downcast_(CreateEvent, event)

    events = [
        CreateEvent(**tick) if tick is not None else None
        for tick in decoded_inputs(hg, recipe)["event"]
    ]
    return eval_node(parity_graph, events)


_POLYMORPHIC_FIELD_POSITIONS = (
    "nested_field",
    "top_level_cast",
    "tsd_value",
    "tsb_field",
    "plain_field",
)


def _validate_polymorphic_field_projection(recipe):
    if set(recipe.parameters) != {"position"}:
        raise RecipeError(
            "polymorphic_field_projection requires only the position parameter"
        )
    if recipe.parameters["position"] not in _POLYMORPHIC_FIELD_POSITIONS:
        raise RecipeError(
            "polymorphic_field_projection position must be one of "
            + ", ".join(_POLYMORPHIC_FIELD_POSITIONS)
        )
    for tick in recipe.inputs["series"]:
        if tick is None:
            continue
        if not isinstance(tick, dict) or set(tick) != {"symbol", "underlying"}:
            raise RecipeError(
                "polymorphic_field_projection ticks require symbol and underlying"
            )
        if not isinstance(tick["symbol"], str) or not tick["symbol"]:
            raise RecipeError(
                "polymorphic_field_projection symbol must be a non-empty string"
            )
        if not isinstance(tick["underlying"], str) or not tick["underlying"]:
            raise RecipeError(
                "polymorphic_field_projection underlying must be a non-empty string"
            )


def _polymorphic_field_projection(hg, recipe):
    """A CompoundScalar field whose DECLARED type is itself a polymorphic base.

    The declared type of ``DerivedSeries.underlying`` is ``Series``, which has a
    descendant, so the field carries the polymorphic (one-pointer) representation
    rather than a flat ``Series``.  Reading that field and passing it to a node
    declared on the base is the shape reported in issue #556.  ``position`` walks
    the same value through the other places a base-declared value can appear, so
    a failure is attributable to the position rather than to the hierarchy.
    """
    from dataclasses import dataclass

    from hgraph.test import eval_node

    @dataclass(frozen=True)
    class Series(hg.CompoundScalar):
        symbol: str

    @dataclass(frozen=True)
    class DerivedSeries(Series):
        underlying: Series

    @dataclass(frozen=True)
    class PlainInner(hg.CompoundScalar):
        symbol: str

    @dataclass(frozen=True)
    class HasPlainField(hg.CompoundScalar):
        symbol: str
        inner: PlainInner

    @hg.compute_node
    def read_base(series: hg.TS[Series]) -> hg.TS[str]:
        return series.value.symbol

    @hg.compute_node
    def read_plain(series: hg.TS[PlainInner]) -> hg.TS[str]:
        return series.value.symbol

    position = recipe.parameters["position"]
    ticks = decoded_inputs(hg, recipe)["series"]

    if position == "plain_field":
        # Control: identical projection whose field type has no descendant.
        values = [
            None if tick is None
            else HasPlainField(
                symbol=tick["symbol"],
                inner=PlainInner(symbol=tick["underlying"]),
            )
            for tick in ticks
        ]

        @hg.graph
        def parity_graph(series: hg.TS[HasPlainField]) -> hg.TS[str]:
            return read_plain(series.inner)

        return eval_node(parity_graph, values)

    values = [
        None if tick is None
        else DerivedSeries(
            symbol=tick["symbol"],
            underlying=Series(symbol=tick["underlying"]),
        )
        for tick in ticks
    ]

    if position == "nested_field":
        @hg.graph
        def parity_graph(series: hg.TS[DerivedSeries]) -> hg.TS[str]:
            return read_base(series.underlying)

        return eval_node(parity_graph, values)

    if position == "top_level_cast":
        @hg.graph
        def parity_graph(series: hg.TS[DerivedSeries]) -> hg.TS[str]:
            return read_base(series)

        return eval_node(parity_graph, values)

    if position == "tsd_value":
        @hg.graph
        def parity_graph(series: hg.TSD[str, hg.TS[Series]]) -> hg.TS[str]:
            return read_base(series["only"])

        return eval_node(
            parity_graph,
            [None if value is None else {"only": value} for value in values],
        )

    class Leg(hg.TimeSeriesSchema):
        leg: hg.TS[Series]

    @hg.graph
    def parity_graph(series: hg.TSB[Leg]) -> hg.TS[str]:
        return read_base(series.leg)

    return eval_node(
        parity_graph,
        [None if value is None else {"leg": value} for value in values],
    )


_POLYMORPHIC_KEY_OPERATIONS = (
    "passthrough",
    "map_compute",
    "map_nested_graph",
    "feedback",
    "key_set_size",
    "non_peered_map",
)


def _validate_polymorphic_tsd_key(recipe):
    if set(recipe.parameters) != {"operation"}:
        raise RecipeError(
            "polymorphic_tsd_key requires only the operation parameter"
        )
    if recipe.parameters["operation"] not in _POLYMORPHIC_KEY_OPERATIONS:
        raise RecipeError(
            "polymorphic_tsd_key operation must be one of "
            + ", ".join(_POLYMORPHIC_KEY_OPERATIONS)
        )
    for tick in recipe.inputs["entries"]:
        if tick is None:
            continue
        if not isinstance(tick, list) or not tick:
            raise RecipeError(
                "polymorphic_tsd_key ticks must be a non-empty list of entries"
            )
        for entry in tick:
            if not isinstance(entry, dict) or set(entry) != {
                "name", "tenor", "value"
            }:
                raise RecipeError(
                    "polymorphic_tsd_key entries require name, tenor, and value"
                )
            if not isinstance(entry["name"], str) or not entry["name"]:
                raise RecipeError(
                    "polymorphic_tsd_key name must be a non-empty string"
                )
            if entry["tenor"] is not None and not isinstance(entry["tenor"], str):
                raise RecipeError(
                    "polymorphic_tsd_key tenor must be a string or null"
                )
            if not isinstance(entry["value"], int) or isinstance(
                entry["value"], bool
            ):
                raise RecipeError("polymorphic_tsd_key value must be an integer")


def _polymorphic_tsd_key(hg, recipe):
    """A TSD KEYED by a polymorphic CompoundScalar.

    Issue #521 reported std::bad_alloc when polymorphic CompoundScalar keys
    reached TSD proxy and target-link adaptors.  Every generated tick mixes
    base and descendant keys in one dictionary, so key identity, hashing, and
    the keyed projections are exercised on the polymorphic representation
    rather than on the ``str``/``int`` keys the rest of the catalogue uses.
    """
    from dataclasses import dataclass

    from hgraph.test import eval_node

    @dataclass(frozen=True)
    class Key(hg.CompoundScalar):
        name: str

    @dataclass(frozen=True)
    class TenorKey(Key):
        tenor: str

    @hg.compute_node
    def double(value: hg.TS[int]) -> hg.TS[int]:
        return value.value * 2

    @hg.graph
    def double_graph(value: hg.TS[int]) -> hg.TS[int]:
        return double(value)

    def build_key(entry):
        if entry["tenor"] is None:
            return Key(name=entry["name"])
        return TenorKey(name=entry["name"], tenor=entry["tenor"])

    ticks = [
        None if tick is None
        else {build_key(entry): entry["value"] for entry in tick}
        for tick in decoded_inputs(hg, recipe)["entries"]
    ]

    operation = recipe.parameters["operation"]

    if operation == "key_set_size":
        @hg.graph
        def parity_graph(entries: hg.TSD[Key, hg.TS[int]]) -> hg.TS[int]:
            return hg.len_(entries.key_set)

        return eval_node(parity_graph, ticks)

    if operation == "feedback":
        @hg.graph
        def parity_graph(
            entries: hg.TSD[Key, hg.TS[int]],
        ) -> hg.TSD[Key, hg.TS[int]]:
            delayed = hg.feedback(hg.TSD[Key, hg.TS[int]])
            delayed(hg.map_(double, entries))
            return delayed()

        return eval_node(parity_graph, ticks)

    if operation == "passthrough":
        @hg.graph
        def parity_graph(
            entries: hg.TSD[Key, hg.TS[int]],
        ) -> hg.TSD[Key, hg.TS[int]]:
            return entries

        return eval_node(parity_graph, ticks)

    if operation == "non_peered_map":
        # Issue #521 named target-link adaptors alongside the TSD proxy, and a
        # structural TSL child is how this catalogue reaches a REF-transparent,
        # non-peered source.  The keys crossing that link are the polymorphic
        # ones.
        @hg.graph
        def parity_graph(
            entries: hg.TSD[Key, hg.TS[int]],
        ) -> hg.TSD[Key, hg.TS[int]]:
            return hg.map_(double, _via_reference(hg, entries, recipe))

        return eval_node(parity_graph, ticks)

    inner = double if operation == "map_compute" else double_graph

    @hg.graph
    def parity_graph(
        entries: hg.TSD[Key, hg.TS[int]],
    ) -> hg.TSD[Key, hg.TS[int]]:
        return hg.map_(inner, entries)

    return eval_node(parity_graph, ticks)


def _validate_enum_literal_selection(recipe):
    if set(recipe.parameters) != {"kind"}:
        raise RecipeError("enum_literal_selection requires only the kind parameter")
    if recipe.parameters["kind"] not in {"int", "str"}:
        raise RecipeError("enum_literal_selection kind must be 'int' or 'str'")
    for tick in recipe.inputs["condition"]:
        if tick is not None and not isinstance(tick, bool):
            raise RecipeError(
                "enum_literal_selection condition ticks must be bool or null"
            )


def _enum_literal_selection(hg, recipe):
    from enum import IntEnum, StrEnum

    from hgraph.test import eval_node

    if recipe.parameters["kind"] == "int":
        class IntegerChoice(IntEnum):
            FIRST = 1
            SECOND = 2

        Choice = IntegerChoice
    else:
        class StringChoice(StrEnum):
            FIRST = "first"
            SECOND = "second"

        Choice = StringChoice

    @hg.graph
    def parity_graph(condition: hg.TS[bool]) -> hg.TS[Choice]:
        # Released hgraph lifts nominal enum members without an explicit const.
        return hg.if_then_else(condition, Choice.FIRST, Choice.SECOND)

    return eval_node(
        parity_graph,
        decoded_inputs(hg, recipe)["condition"],
    )


def _validate_legacy_compound_scalar_json(recipe):
    if set(recipe.parameters) != {"mode"}:
        raise RecipeError(
            "legacy_compound_scalar_json requires only the mode parameter"
        )
    if recipe.parameters["mode"] not in {"default", "custom", "field"}:
        raise RecipeError(
            "legacy_compound_scalar_json mode must be default, custom, or field"
        )
    for tick in recipe.inputs["value"]:
        if tick is None:
            continue
        if not isinstance(tick, dict) or set(tick) != {"p1", "p2"}:
            raise RecipeError(
                "legacy_compound_scalar_json values require p1 and p2"
            )
        if not isinstance(tick["p1"], int) or isinstance(tick["p1"], bool):
            raise RecipeError("legacy_compound_scalar_json p1 must be an integer")
        if (
            not isinstance(tick["p2"], (int, float))
            or isinstance(tick["p2"], bool)
        ):
            raise RecipeError("legacy_compound_scalar_json p2 must be numeric")


def _legacy_compound_scalar_json(hg, recipe):
    from dataclasses import dataclass

    mode = recipe.parameters["mode"]
    if mode == "default":
        @dataclass
        class LegacyBase(hg.CompoundScalar):
            __serialise_base__ = True
            p1: int

        @dataclass
        class LegacyChild(LegacyBase):
            p2: float
    elif mode == "custom":
        @dataclass
        class LegacyBase(hg.CompoundScalar):
            __serialise_base__ = True
            __serialise_discriminator_field__ = "name"
            p1: int

        @dataclass
        class LegacyChild(LegacyBase):
            name = "LSCS"
            p2: float = 1.0
    else:
        @dataclass
        class LegacyBase(hg.CompoundScalar):
            __serialise_base__ = True
            __serialise_discriminator_field__ = "name"
            p1: int
            name: str

        @dataclass
        class LegacyChild(LegacyBase):
            name: str = "LSCS"
            p2: float = 1.0

    encode = hg.to_json_builder(LegacyBase)
    decode = hg.from_json_builder(LegacyBase)
    encoded = []
    decoded = []
    for tick in decoded_inputs(hg, recipe)["value"]:
        if tick is None:
            encoded.append(None)
            decoded.append(None)
            continue
        payload = json.loads(encode(LegacyChild(**tick)))
        encoded.append(payload)
        decoded.append(decode(payload))
    return {"encoded": encoded, "decoded": decoded}


def _json_operator_parameters(recipe):
    if set(recipe.parameters) != {"operation", "shape", "delta"}:
        raise RecipeError(
            "json_operator_contract requires operation, shape, and delta parameters"
        )
    operation = recipe.parameters["operation"]
    shape = recipe.parameters["shape"]
    delta = recipe.parameters["delta"]
    if operation not in {"from_json", "to_json"}:
        raise RecipeError(
            "json_operator_contract operation must be from_json or to_json"
        )
    if shape not in {"ts", "tss", "tsd", "tsl"}:
        raise RecipeError(
            "json_operator_contract shape must be ts, tss, tsd, or tsl"
        )
    if not isinstance(delta, bool):
        raise RecipeError("json_operator_contract delta must be a boolean")
    if operation == "from_json" and delta:
        raise RecipeError(
            "json_operator_contract cannot pass the non-public delta argument to from_json"
        )
    return operation, shape


def _is_json_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _is_json_int_list(value):
    return isinstance(value, list) and all(_is_json_int(item) for item in value)


def _is_json_set_delta(value):
    return (
        isinstance(value, dict)
        and set(value) == {"added", "removed"}
        and all(_is_json_int_list(value[name]) for name in ("added", "removed"))
        and not set(value["added"]) & set(value["removed"])
    )


def _is_json_ts_value(value, _encoded):
    return _is_json_int(value)


def _is_json_tss_value(value, encoded):
    if encoded and _is_json_int_list(value):
        return True
    if not isinstance(value, dict):
        return False
    if encoded:
        delta = value
    elif set(value) == {_SET_DELTA}:
        delta = value[_SET_DELTA]
    else:
        return False
    return _is_json_set_delta(delta)


def _is_json_tsd_value(value, encoded):
    def is_item(item):
        removal = item is None if encoded else item == {"$remove": True}
        return _is_json_int(item) or removal

    return isinstance(value, dict) and all(
        isinstance(key, str) and is_item(item)
        for key, item in value.items()
    )


def _is_json_tsl_value(value, _encoded):
    return (
        isinstance(value, list)
        and len(value) <= 2
        and all(item is None or _is_json_int(item) for item in value)
    )


_JSON_OPERATOR_VALUE_VALIDATORS = {
    "ts": _is_json_ts_value,
    "tss": _is_json_tss_value,
    "tsd": _is_json_tsd_value,
    "tsl": _is_json_tsl_value,
}


def _parse_json_operator_tick(tick):
    if not isinstance(tick, str):
        raise RecipeError(
            "json_operator_contract from_json values must be strings or null ticks"
        )
    try:
        return json.loads(tick)
    except json.JSONDecodeError as error:
        raise RecipeError(
            "json_operator_contract from_json values must contain valid JSON"
        ) from error


def _validate_json_operator_value(shape, value, *, encoded):
    if _JSON_OPERATOR_VALUE_VALIDATORS[shape](value, encoded):
        return
    representation = "decoded JSON" if encoded else "input"
    raise RecipeError(
        f"json_operator_contract invalid {representation} for {shape}: {value!r}"
    )


def _validate_json_operator_contract(recipe):
    operation, shape = _json_operator_parameters(recipe)
    encoded = operation == "from_json"

    for tick in recipe.inputs["value"]:
        if tick is None:
            continue
        value = _parse_json_operator_tick(tick) if encoded else tick
        _validate_json_operator_value(shape, value, encoded=encoded)


def _json_operator_contract(hg, recipe):
    import inspect

    from hgraph.test import eval_node

    shape = recipe.parameters["shape"]
    ts_type = {
        "ts": hg.TS[int],
        "tss": hg.TSS[int],
        "tsd": hg.TSD[str, hg.TS[int]],
        "tsl": hg.TSL[hg.TS[int], hg.Size[2]],
    }[shape]

    def call_shape(operator):
        return [
            {
                "name": parameter.name,
                "kind": str(parameter.kind),
                "default": (
                    repr(parameter.default)
                    if parameter.default is not inspect.Parameter.empty
                    else None
                ),
            }
            for parameter in inspect.signature(operator).parameters.values()
        ]

    if recipe.parameters["operation"] == "from_json":
        values = eval_node(
            hg.from_json[ts_type],
            list(recipe.inputs["value"]),
        )
    else:
        values = eval_node(
            hg.to_json[ts_type],
            decoded_inputs(hg, recipe)["value"],
            recipe.parameters["delta"],
        )

    return {
        "call_shapes": {
            "to_json": call_shape(hg.to_json),
            "from_json": call_shape(hg.from_json),
        },
        "values": values,
    }


# ---------------------------------------------------------------------------
# Type arguments (RFC 0033): the four fix shapes the 2026-09 fix-series
# retrospective catalogued -- a positional ``tp``, ``DEFAULT`` before
# ``AUTO_RESOLVE``, a size pin, a collection carrier -- each in its released
# 0.5 spelling, so the reference and the candidate resolve the same call.
# ---------------------------------------------------------------------------

_TYPE_ARGUMENT_POSITIONAL_MODES = ("const", "nothing")


def _validate_type_argument_positional(recipe):
    if set(recipe.parameters) - {"mode", "offset"}:
        raise RecipeError(
            "type_argument_positional accepts only the mode and offset parameters"
        )
    mode = recipe.parameters.get("mode", "const")
    if mode not in _TYPE_ARGUMENT_POSITIONAL_MODES:
        raise RecipeError(
            "type_argument_positional mode must be one of "
            + ", ".join(_TYPE_ARGUMENT_POSITIONAL_MODES)
        )
    offset = recipe.parameters.get("offset", 1)
    if not isinstance(offset, int) or isinstance(offset, bool) or not -100 <= offset <= 100:
        raise RecipeError("type_argument_positional offset must be an integer in [-100, 100]")
    for tick in recipe.inputs["value"]:
        if tick is None:
            continue
        if not isinstance(tick, (int, float)) or isinstance(tick, bool):
            raise RecipeError("type_argument_positional value ticks must be numbers")


def _type_argument_positional(hg, recipe):
    from hgraph.test import eval_node

    mode = recipe.parameters.get("mode", "const")
    offset = recipe.parameters.get("offset", 1)
    inputs = decoded_inputs(hg, recipe)

    if mode == "const":
        # ``const(value, tp)``: the type is the positional argument after the
        # value (the 0.5 signature ``const(value, tp=..., delay=...)``). The
        # offset is passed as a float: the candidate converts an int at the
        # selected output, released 0.5 raises at runtime (parity_matrix.rst),
        # and this recipe is about the positional carrier, not that.
        @hg.graph
        def parity_graph(value: hg.TS[float]) -> hg.TS[float]:
            return value + hg.const(float(offset), hg.TS[float])

        return eval_node(
            parity_graph,
            [None if tick is None else float(tick) for tick in inputs["value"]],
        )

    # ``nothing(tp)``: the type IS the argument; a never-valid source under a
    # default makes the stream observable tick by tick.
    @hg.graph
    def parity_graph(value: hg.TS[int]) -> hg.TS[int]:
        return hg.default(hg.nothing(hg.TS[int]), value + offset)

    return eval_node(
        parity_graph,
        [None if tick is None else int(tick) for tick in inputs["value"]],
    )


def _validate_type_argument_default_order(recipe):
    if set(recipe.parameters) - {"pinned", "inferred"}:
        raise RecipeError(
            "type_argument_default_order accepts only the pinned and inferred parameters"
        )
    for name in ("pinned", "inferred"):
        chosen = recipe.parameters.get(name, "int")
        if chosen not in _SCALAR_TYPES:
            raise RecipeError(
                f"type_argument_default_order {name} must be one of {sorted(_SCALAR_TYPES)}"
            )
    for tick in recipe.inputs["value"]:
        if tick is not None and (not isinstance(tick, int) or isinstance(tick, bool)):
            raise RecipeError("type_argument_default_order value ticks must be integers")


def _type_argument_default_order(hg, recipe):
    from typing import TypeVar

    from hgraph.test import eval_node

    pinned = _SCALAR_TYPES[recipe.parameters.get("pinned", "int")]
    inferred = _SCALAR_TYPES[recipe.parameters.get("inferred", "int")]
    inputs = decoded_inputs(hg, recipe)
    OTHER = TypeVar("OTHER")

    # A bare subscript fills the DEFAULT variable, not the AUTO_RESOLVE one
    # that appears first; the explicit keyword fills the other. The body
    # reports which type landed where.
    @hg.graph
    def parity_graph(
        value: hg.TS[int],
        other: type[OTHER] = hg.AUTO_RESOLVE,
        schema: type[hg.SCALAR] = hg.DEFAULT[hg.SCALAR],
    ) -> hg.TS[str]:
        label = f"{schema.__name__}/{other.__name__}:{{}}"
        return hg.format_(label, value)

    @hg.graph
    def outer(value: hg.TS[int]) -> hg.TS[str]:
        return parity_graph[pinned](value, other=inferred)

    return eval_node(outer, inputs["value"])


_TYPE_ARGUMENT_SIZE_MODES = ("auto", "subscript")


def _validate_type_argument_size_pin(recipe):
    if set(recipe.parameters) - {"mode", "size"}:
        raise RecipeError("type_argument_size_pin accepts only the mode and size parameters")
    mode = recipe.parameters.get("mode", "auto")
    if mode not in _TYPE_ARGUMENT_SIZE_MODES:
        raise RecipeError(
            "type_argument_size_pin mode must be one of " + ", ".join(_TYPE_ARGUMENT_SIZE_MODES)
        )
    size = recipe.parameters.get("size", 2)
    if not isinstance(size, int) or isinstance(size, bool) or not 1 <= size <= 4:
        raise RecipeError("type_argument_size_pin size must be an integer in [1, 4]")
    for tick in recipe.inputs["values"]:
        if tick is None:
            continue
        if (
            not isinstance(tick, list)
            or len(tick) != size
            or not all(isinstance(item, int) and not isinstance(item, bool) for item in tick)
        ):
            raise RecipeError(
                "type_argument_size_pin values ticks must be integer lists of the declared size"
            )


def _type_argument_size_pin(hg, recipe):
    from hgraph.test import eval_node

    mode = recipe.parameters.get("mode", "auto")
    size = recipe.parameters.get("size", 2)
    inputs = decoded_inputs(hg, recipe)
    ticks = [None if tick is None else tuple(tick) for tick in inputs["values"]]

    # ``type[SIZE]`` materialises as the Size object (``.SIZE``) whether it was
    # resolved from the TSL input or pinned by ``g[Size[n]]``; the body adds
    # the declared size to the element sum so both paths are observable.
    @hg.graph
    def parity_graph(
        values: hg.TSL[hg.TS[int], hg.SIZE], _sz: type[hg.SIZE] = hg.AUTO_RESOLVE
    ) -> hg.TS[int]:
        return hg.sum_(values) + hg.const(_sz.SIZE)

    tsl_type = hg.TSL[hg.TS[int], hg.Size[size]]
    if mode == "subscript":
        return eval_node(
            parity_graph[hg.Size[size]], ticks, resolution_dict={"values": tsl_type}
        )
    return eval_node(parity_graph, ticks, resolution_dict={"values": tsl_type})


_TYPE_ARGUMENT_COLLECTIONS = ("tuple", "frozenset")


def _validate_type_argument_collection(recipe):
    if set(recipe.parameters) - {"collection"}:
        raise RecipeError("type_argument_collection accepts only the collection parameter")
    collection = recipe.parameters.get("collection", "tuple")
    if collection not in _TYPE_ARGUMENT_COLLECTIONS:
        raise RecipeError(
            "type_argument_collection collection must be one of "
            + ", ".join(_TYPE_ARGUMENT_COLLECTIONS)
        )
    for tick in recipe.inputs["values"]:
        if tick is None:
            continue
        if not isinstance(tick, list) or not all(
            isinstance(item, int) and not isinstance(item, bool) for item in tick
        ):
            raise RecipeError("type_argument_collection values ticks must be integer lists")


def _collection_carrier_matches(tp, origins, args):
    """Structural comparison of a materialised collection carrier: released
    0.5 spells a frozenset carrier ``collections.abc.Set[int]`` and the
    candidate ``frozenset[int]`` (an accepted spelling deviation,
    ``parity_matrix.rst``); both are the same parameterised generic."""
    import typing

    return typing.get_origin(tp) in origins and typing.get_args(tp) == args


def _type_argument_collection(hg, recipe):
    import collections.abc

    from hgraph.test import eval_node

    collection = recipe.parameters.get("collection", "tuple")
    inputs = decoded_inputs(hg, recipe)
    if collection == "tuple":
        annotation, convert = hg.TS[tuple[int, ...]], tuple
        origins, args = (tuple,), (int, Ellipsis)
    else:
        annotation, convert = hg.TS[frozenset[int]], frozenset
        origins, args = (frozenset, collections.abc.Set), (int,)
    ticks = [None if tick is None else convert(tick) for tick in inputs["values"]]

    # A collection type argument resolved from the wired input materialises
    # as the parameterised generic the input was declared with, not a bare
    # ``tuple`` or ``frozenset``; the body adds the element count when the
    # carrier reads back as that generic, so a wrong form is visible on every
    # tick.
    @hg.graph
    def parity_graph(values: hg.TS[hg.SCALAR], tp: type[hg.SCALAR] = hg.AUTO_RESOLVE) -> hg.TS[int]:
        return hg.len_(values) + hg.const(1 if _collection_carrier_matches(tp, origins, args) else -1000)

    return eval_node(parity_graph, ticks, resolution_dict={"values": annotation})


# --------------------------------------------------------------------------
# Operator families (the 2026-09 coverage frontier).
#
# Each template below wires ONE operator named by the recipe's ``operation``
# parameter, so a single template covers a whole family and a recipe stays a
# data record. Every validator rejects an operation outside its own family,
# an input set the operation does not take, and a parameter it does not use.
#
# Operators the frontier deliberately still excludes, and why:
#   * ``clip``/``count``/``diff``/``ewma``/``resample``/``std``/``var`` --
#     relocated to the ``hgraph-analytics`` distribution (an accepted
#     deviation, ``parity_matrix.rst`` "Analytical"); the core parity
#     candidate environment does not install it, so a recipe would compare
#     packaging, not behaviour.
#   * ``last_modified_wall_clock_time`` -- reads the wall clock, so no two
#     runs agree.
#   * ``compare``/``zero`` -- the candidate rejects the released call shape
#     outright; recorded as a divergence rather than hidden in a recipe.
#   * ``replay_const`` -- needs a recordable store (RFC 0025).


def _family_operation(recipe, table):
    operation = recipe.parameters.get("operation")
    if operation not in table:
        raise RecipeError(
            f"{recipe.template} operation must be one of "
            f"{tuple(sorted(table))}, got {operation!r}"
        )
    return operation


def _family_inputs(recipe, *allowed):
    names = frozenset(recipe.inputs)
    if any(names == frozenset(option) for option in allowed):
        return
    raise RecipeError(
        f"{recipe.template} {recipe.parameters.get('operation')!r} requires "
        f"inputs {tuple(tuple(option) for option in allowed)}, got "
        f"{tuple(sorted(names))}"
    )


def _family_parameters(recipe, allowed):
    """Reject any parameter the selected operation does not read."""
    extra = set(recipe.parameters) - set(allowed) - {"operation"}
    if extra:
        raise RecipeError(
            f"{recipe.template} {recipe.parameters['operation']!r} does not "
            f"accept parameter(s) {sorted(extra)}"
        )


def _family_bounded_int(recipe, name, default, *, minimum, maximum):
    value = recipe.parameters.get(name, default)
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not minimum <= value <= maximum
    ):
        raise RecipeError(
            f"{recipe.template} {name} must be an integer in "
            f"[{minimum}, {maximum}]"
        )
    return value


def _family_bounded_str(recipe, name, default, *, maximum=32):
    value = recipe.parameters.get(name, default)
    if not isinstance(value, str) or len(value) > maximum:
        raise RecipeError(
            f"{recipe.template} {name} must be a string of at most "
            f"{maximum} characters"
        )
    return value


def _family_bool(recipe, name, default):
    value = recipe.parameters.get(name, default)
    if not isinstance(value, bool):
        raise RecipeError(f"{recipe.template} {name} must be a boolean")
    return value


def _family_choice(recipe, name, default, choices):
    value = recipe.parameters.get(name, default)
    if value not in choices:
        raise RecipeError(
            f"{recipe.template} {name} must be one of {tuple(sorted(choices))}"
        )
    return value


def _family_scalar_ticks(recipe, name, type_name):
    """Every non-null tick of ``name`` is exactly the declared scalar type."""
    expected = _SCALAR_TYPES[type_name]
    for tick in recipe.inputs[name]:
        if tick is None:
            continue
        if type(tick) is not expected:
            raise RecipeError(
                f"{recipe.template} {name} ticks must be {type_name}, got "
                f"{type(tick).__name__}"
            )


def _family_temporal_ticks(recipe, name, kind):
    """``date``/``datetime`` ticks carry the matching tag and a valid ISO string."""
    import datetime as _dt

    tag = "$date" if kind == "date" else "$datetime"
    decoder = _dt.date.fromisoformat if kind == "date" else _dt.datetime.fromisoformat
    for tick in recipe.inputs[name]:
        if tick is None:
            continue
        if (
            not isinstance(tick, dict)
            or set(tick) != {tag}
            or not isinstance(tick[tag], str)
        ):
            raise RecipeError(
                f'{recipe.template} {name} ticks must be {{"{tag}": '
                '"<iso-string>"}} or null'
            )
        try:
            decoded = decoder(tick[tag])
        except ValueError as error:
            raise RecipeError(
                f"{recipe.template} {name} tick {tick[tag]!r} is not a valid "
                f"ISO {kind}"
            ) from error
        if kind == "datetime" and decoded.tzinfo is not None:
            raise RecipeError(
                f"{recipe.template} {name} datetime ticks must be naive "
                "(the UTC convention)"
            )


def _validate_mapping_ticks(recipe, name):
    for tick in recipe.inputs[name]:
        if tick is not None and not isinstance(tick, dict):
            raise RecipeError(
                f"{recipe.template} {name} ticks must be a JSON object or null"
            )


#: The element type each ``TSS`` family input carries, so a validator can
#: check a ``$set_delta``'s elements against the type the executor wires.
_TSS_ELEMENT_TYPES = {"tss_int": "int", "tss_str": "str"}


def _family_annotation(hg, name):
    import datetime as _dt

    return {
        "bool": hg.TS[bool],
        "int": hg.TS[int],
        "float": hg.TS[float],
        "str": hg.TS[str],
        "date": hg.TS[_dt.date],
        "datetime": hg.TS[_dt.datetime],
        "tss_int": hg.TSS[int],
        "tss_str": hg.TSS[str],
        "tsd": hg.TSD[str, hg.TS[int]],
    }[name]


# --------------------------------------------------------------------------
# Unary scalar operators.

#: operation -> (accepted input types, output rule)
_UNARY_FAMILY = {
    "abs_": (("int", "float"), "same"),
    "cast_": (("bool", "int", "float", "str"), "target"),
    "invert_": (("int", "bool"), "int"),
    "ln": (("float",), "float"),
    "neg_": (("int", "float"), "same"),
    "not_": (("bool", "int", "str"), "bool"),
    "pos_": (("int", "float"), "same"),
    "sign": (("int", "float"), "same"),
    "str_": (("bool", "int", "float", "date", "datetime", "tss_int", "tsd"), "str"),
    "type_": (("bool", "int", "float", "str"), "type"),
}


def _validate_unary_operator(recipe):
    _family_inputs(recipe, ("ts",))
    operation = _family_operation(recipe, _UNARY_FAMILY)
    accepted, _rule = _UNARY_FAMILY[operation]
    input_type = recipe.parameters.get("input_type")
    if input_type not in accepted:
        raise RecipeError(
            f"unary_operator {operation} accepts input_type {accepted}, got "
            f"{input_type!r}"
        )
    allowed = ["input_type", "sink_result"]
    if operation == "cast_":
        allowed.append("target_type")
        target = recipe.parameters.get("target_type")
        if target not in _SCALAR_TYPES:
            raise RecipeError(
                "unary_operator cast_ target_type must be bool, float, int, or str"
            )
        # cast_(str -> number) is a released-only shape (the candidate has no
        # convert overload); keep it expressible so the campaign can retest it.
    _family_parameters(recipe, allowed)
    _family_bool(recipe, "sink_result", False)
    if input_type in ("date", "datetime"):
        _family_temporal_ticks(recipe, "ts", input_type)
    elif input_type in _SCALAR_TYPES:
        _family_scalar_ticks(recipe, "ts", input_type)
    elif input_type == "tsd":
        _validate_mapping_ticks(recipe, "ts")
    else:
        _validate_set_ticks(recipe, "ts", _TSS_ELEMENT_TYPES[input_type])


def _unary_operator(hg, recipe):
    from hgraph.test import eval_node

    operation = recipe.parameters["operation"]
    input_type = recipe.parameters["input_type"]
    _accepted, rule = _UNARY_FAMILY[operation]
    annotation = _family_annotation(hg, input_type)
    sink_result = recipe.parameters.get("sink_result", False)
    if operation == "cast_":
        target = _SCALAR_TYPES[recipe.parameters["target_type"]]
        apply = lambda ts: hg.cast_(target, ts)  # noqa: E731
        output = _family_annotation(hg, recipe.parameters["target_type"])
    else:
        node = getattr(hg, operation)
        apply = lambda ts: node(ts)  # noqa: E731
        output = {
            "same": lambda: annotation,
            "int": lambda: hg.TS[int],
            "float": lambda: hg.TS[float],
            "bool": lambda: hg.TS[bool],
            "str": lambda: hg.TS[str],
            "type": lambda: hg.TS[type],
        }[rule]()

    if sink_result:
        @hg.graph
        def parity_graph(ts: annotation) -> annotation:
            hg.null_sink(apply(ts))
            return ts
    else:
        @hg.graph
        def parity_graph(ts: annotation) -> output:
            return apply(ts)

    return eval_node(parity_graph, decoded_inputs(hg, recipe)["ts"])


# --------------------------------------------------------------------------
# Binary scalar operators.

_BINARY_FAMILY = {
    "bit_and": (("bool", "int", "tss_int"), "same"),
    "bit_or": (("bool", "int", "tss_int"), "same"),
    "bit_xor": (("bool", "int", "tss_int"), "same"),
    "cmp_": (("int", "float", "str"), "cmp"),
    "divmod_": (("int", "float"), "pair"),
    "if_cmp": (("int", "float", "str"), "int"),
    "lshift_": (("int",), "same"),
    "max_": (("int", "float", "str"), "same"),
    "min_": (("int", "float", "str"), "same"),
    "rshift_": (("int",), "same"),
}


def _validate_binary_operator(recipe):
    _family_inputs(recipe, ("lhs", "rhs"))
    operation = _family_operation(recipe, _BINARY_FAMILY)
    accepted, _rule = _BINARY_FAMILY[operation]
    input_type = recipe.parameters.get("input_type")
    if input_type not in accepted:
        raise RecipeError(
            f"binary_operator {operation} accepts input_type {accepted}, got "
            f"{input_type!r}"
        )
    _family_parameters(recipe, ("input_type",))
    for name in ("lhs", "rhs"):
        if input_type in _SCALAR_TYPES:
            _family_scalar_ticks(recipe, name, input_type)
        else:
            # ``tss_int`` wires TSS[int]: the ticks are set deltas over
            # integers. Skipping the check let an int or str tick through
            # validation and fail later in decode/wiring, where the harness
            # would report it as a runtime difference instead of an
            # out-of-language recipe.
            _validate_set_ticks(recipe, name, _TSS_ELEMENT_TYPES[input_type])


def _binary_operator(hg, recipe):
    from hgraph.test import eval_node

    operation = recipe.parameters["operation"]
    input_type = recipe.parameters["input_type"]
    _accepted, rule = _BINARY_FAMILY[operation]
    annotation = _family_annotation(hg, input_type)
    if rule == "pair":
        output = hg.TSL[annotation, hg.Size[2]]
    elif rule == "cmp":
        output = hg.TS[hg.CmpResult]
    elif rule == "int":
        output = hg.TS[int]
    else:
        output = annotation

    if operation == "if_cmp":
        @hg.graph
        def parity_graph(lhs: annotation, rhs: annotation) -> output:
            return hg.if_cmp(
                hg.cmp_(lhs, rhs), hg.const(-1), hg.const(0), hg.const(1)
            )
    else:
        node = getattr(hg, operation)

        @hg.graph
        def parity_graph(lhs: annotation, rhs: annotation) -> output:
            return node(lhs, rhs)

    inputs = decoded_inputs(hg, recipe)
    return eval_node(parity_graph, inputs["lhs"], inputs["rhs"])


# --------------------------------------------------------------------------
# String operators.

_STRING_FAMILY = ("join", "match_", "replace", "split", "substr")


def _validate_string_operator(recipe):
    operation = _family_operation(recipe, _STRING_FAMILY)
    if operation == "join":
        _family_inputs(recipe, ("s", "t"))
        _family_parameters(recipe, ("separator",))
        _family_bounded_str(recipe, "separator", "-", maximum=4)
    elif operation == "substr":
        _family_inputs(recipe, ("s",))
        _family_parameters(recipe, ("start", "end"))
        _family_bounded_int(recipe, "start", 0, minimum=-64, maximum=64)
        _family_bounded_int(recipe, "end", 1, minimum=-64, maximum=64)
    elif operation == "replace":
        _family_inputs(recipe, ("s",))
        _family_parameters(recipe, ("pattern", "replacement"))
        _family_bounded_str(recipe, "pattern", "a")
        _family_bounded_str(recipe, "replacement", "")
    elif operation == "match_":
        _family_inputs(recipe, ("s",))
        _family_parameters(recipe, ("pattern", "projection"))
        _family_bounded_str(recipe, "pattern", "a")
        _family_choice(recipe, "projection", "is_match", ("is_match", "groups"))
    else:  # split
        _family_inputs(recipe, ("s",))
        _family_parameters(recipe, ("separator", "to", "size"))
        _family_bounded_str(recipe, "separator", ",", maximum=4)
        to = _family_choice(recipe, "to", "tuple", ("tuple", "tsl"))
        if to == "tuple" and "size" in recipe.parameters:
            raise RecipeError("string_operator split size applies to the tsl target")
        _family_bounded_int(recipe, "size", 2, minimum=1, maximum=8)
    for name in recipe.inputs:
        _family_scalar_ticks(recipe, name, "str")


def _string_operator(hg, recipe):
    from hgraph.test import eval_node

    parameters = recipe.parameters
    operation = parameters["operation"]
    inputs = decoded_inputs(hg, recipe)
    if operation == "join":
        separator = parameters.get("separator", "-")

        @hg.graph
        def parity_graph(s: hg.TS[str], t: hg.TS[str]) -> hg.TS[str]:
            return hg.join(s, t, separator=separator)

        return eval_node(parity_graph, inputs["s"], inputs["t"])

    if operation == "substr":
        start = parameters.get("start", 0)
        end = parameters.get("end", 1)

        @hg.graph
        def parity_graph(s: hg.TS[str]) -> hg.TS[str]:
            return hg.substr(s, hg.const(start), hg.const(end))

    elif operation == "replace":
        pattern = parameters.get("pattern", "a")
        replacement = parameters.get("replacement", "")

        @hg.graph
        def parity_graph(s: hg.TS[str]) -> hg.TS[str]:
            return hg.replace(hg.const(pattern), hg.const(replacement), s)

    elif operation == "match_":
        pattern = parameters.get("pattern", "a")
        if parameters.get("projection", "is_match") == "is_match":

            @hg.graph
            def parity_graph(s: hg.TS[str]) -> hg.TS[bool]:
                return hg.match_(hg.const(pattern), s).is_match

        else:

            @hg.graph
            def parity_graph(s: hg.TS[str]) -> hg.TS[tuple[str, ...]]:
                return hg.match_(hg.const(pattern), s).groups

    else:  # split
        separator = parameters.get("separator", ",")
        if parameters.get("to", "tuple") == "tuple":

            @hg.graph
            def parity_graph(s: hg.TS[str]) -> hg.TS[tuple[str, ...]]:
                return hg.split[hg.OUT: hg.TS[tuple[str, ...]]](s, separator)

        else:
            size = parameters.get("size", 2)
            target = hg.TSL[hg.TS[str], hg.Size[size]]

            @hg.graph
            def parity_graph(s: hg.TS[str]) -> target:
                return hg.split[hg.OUT: target](s, separator)

    return eval_node(parity_graph, inputs["s"])


# --------------------------------------------------------------------------
# Stream shaping operators (one time-series in, one out).

_STREAM_FAMILY = {
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

#: The per-operation parameters, beside ``input_type``, each stream operation
#: reads. ``period_micros`` counts engine microseconds (``MIN_TD``).
_STREAM_PARAMETERS = {
    "drop": ("count", "period_micros"),
    "drop_dups": (),
    "freeze": ("threshold",),
    "lag": ("count", "period_micros"),
    "schedule": ("period_micros", "max_ticks", "initial_delay"),
    "slice_": ("start", "stop", "step_size"),
    "step": ("step_size",),
    "take": ("count", "period_micros"),
    "throttle": ("period_micros", "delay_first_tick"),
    "to_window": ("count", "min_count", "reduction"),
    "until_true": ("threshold",),
}


def _validate_stream_shape(recipe):
    _family_inputs(recipe, ("ts",))
    operation = _family_operation(recipe, _STREAM_FAMILY)
    input_type = recipe.parameters.get("input_type")
    if input_type not in _STREAM_FAMILY[operation]:
        raise RecipeError(
            f"stream_shape {operation} accepts input_type "
            f"{_STREAM_FAMILY[operation]}, got {input_type!r}"
        )
    _family_parameters(recipe, ("input_type", *_STREAM_PARAMETERS[operation]))
    _family_scalar_ticks(recipe, "ts", input_type)
    if operation in ("drop", "lag", "take"):
        if "count" in recipe.parameters and "period_micros" in recipe.parameters:
            raise RecipeError(
                f"stream_shape {operation} takes count or period_micros, not both"
            )
        if "period_micros" in recipe.parameters:
            _family_bounded_int(recipe, "period_micros", 1, minimum=1, maximum=1024)
        else:
            _family_bounded_int(recipe, "count", 1, minimum=0, maximum=64)
    elif operation == "schedule":
        _family_bounded_int(recipe, "period_micros", 2, minimum=1, maximum=1024)
        _family_bounded_int(recipe, "max_ticks", 3, minimum=1, maximum=64)
        _family_bool(recipe, "initial_delay", True)
    elif operation == "slice_":
        _family_bounded_int(recipe, "start", 0, minimum=0, maximum=64)
        _family_bounded_int(recipe, "stop", 4, minimum=0, maximum=64)
        _family_bounded_int(recipe, "step_size", 1, minimum=1, maximum=16)
    elif operation == "step":
        _family_bounded_int(recipe, "step_size", 2, minimum=1, maximum=16)
    elif operation == "throttle":
        _family_bounded_int(recipe, "period_micros", 3, minimum=1, maximum=1024)
        _family_bool(recipe, "delay_first_tick", False)
    elif operation == "to_window":
        count = _family_bounded_int(recipe, "count", 3, minimum=1, maximum=64)
        minimum = _family_bounded_int(recipe, "min_count", 1, minimum=1, maximum=64)
        if minimum > count:
            raise RecipeError("stream_shape to_window min_count exceeds count")
        reduction = _family_choice(recipe, "reduction", "sum_", ("sum_", "mean"))
        if reduction == "mean" and input_type != "float":
            raise RecipeError("stream_shape to_window mean requires a float input")
    elif operation in ("freeze", "until_true"):
        threshold = recipe.parameters.get("threshold", 0)
        if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
            raise RecipeError("stream_shape threshold must be a number")
        if not -1e6 <= threshold <= 1e6:
            raise RecipeError("stream_shape threshold must be bounded")


def _stream_shape(hg, recipe):
    import datetime as _dt

    from hgraph.test import eval_node

    parameters = recipe.parameters
    operation = parameters["operation"]
    annotation = _family_annotation(hg, parameters["input_type"])
    ticks = decoded_inputs(hg, recipe)["ts"]

    def _span():
        if "period_micros" in parameters:
            return _dt.timedelta(microseconds=parameters["period_micros"])
        return parameters.get("count", 1)

    if operation in ("drop", "lag", "take"):
        node = getattr(hg, operation)
        span = _span()

        @hg.graph
        def parity_graph(ts: annotation) -> annotation:
            return node(ts, span)

    elif operation == "drop_dups":

        @hg.graph
        def parity_graph(ts: annotation) -> annotation:
            return hg.drop_dups(ts)

    elif operation == "step":
        step_size = parameters.get("step_size", 2)

        @hg.graph
        def parity_graph(ts: annotation) -> annotation:
            return hg.step(ts, step_size)

    elif operation == "slice_":
        start = parameters.get("start", 0)
        stop = parameters.get("stop", 4)
        step_size = parameters.get("step_size", 1)

        @hg.graph
        def parity_graph(ts: annotation) -> annotation:
            return hg.slice_(ts, start, stop, step_size)

    elif operation == "throttle":
        period = _dt.timedelta(microseconds=parameters.get("period_micros", 3))
        delay_first_tick = parameters.get("delay_first_tick", False)

        @hg.graph
        def parity_graph(ts: annotation) -> annotation:
            return hg.throttle(ts, period, delay_first_tick=delay_first_tick)

    elif operation == "freeze":
        threshold = parameters.get("threshold", 0)

        @hg.graph
        def parity_graph(ts: annotation) -> annotation:
            return hg.freeze(lambda value: value > threshold, ts)

    elif operation == "until_true":
        threshold = parameters.get("threshold", 0)

        @hg.graph
        def parity_graph(ts: annotation) -> hg.TS[bool]:
            return hg.until_true(lambda value: value > threshold, ts)

    elif operation == "schedule":
        period = _dt.timedelta(microseconds=parameters.get("period_micros", 2))
        max_ticks = parameters.get("max_ticks", 3)
        initial_delay = parameters.get("initial_delay", True)

        @hg.graph
        def parity_graph(ts: annotation) -> hg.TS[bool]:
            hg.null_sink(ts)
            return hg.schedule(
                period, initial_delay=initial_delay, max_ticks=max_ticks
            )

    else:  # to_window
        count = parameters.get("count", 3)
        min_count = parameters.get("min_count", 1)
        if parameters.get("reduction", "sum_") == "sum_":

            @hg.graph
            def parity_graph(ts: annotation) -> annotation:
                return hg.sum_(hg.to_window(ts, count, min_count))

        else:

            @hg.graph
            def parity_graph(ts: annotation) -> hg.TS[float]:
                return hg.mean(hg.to_window(ts, count, min_count))

    return eval_node(parity_graph, ticks)


# --------------------------------------------------------------------------
# Flow-control operators (a control series steering a value series).

_FLOW_FAMILY = {
    "filter_": "bool",
    "gate": "bool",
    "if_": "bool",
    "if_true": "bool",
    "route_by_index": "int",
    "sample": "bool",
}


def _validate_flow_control(recipe):
    _family_inputs(recipe, ("condition", "ts"))
    operation = _family_operation(recipe, _FLOW_FAMILY)
    input_type = _family_choice(
        recipe, "input_type", "int", ("bool", "int", "float", "str", "tsd")
    )
    allowed = ["input_type"]
    if operation == "gate":
        allowed.append("buffer_length")
        _family_bounded_int(recipe, "buffer_length", 1, minimum=1, maximum=64)
    elif operation == "if_":
        allowed.append("branch")
        _family_choice(recipe, "branch", "true", ("true", "false"))
    elif operation == "if_true":
        allowed.append("tick_once_only")
        _family_bool(recipe, "tick_once_only", False)
    elif operation == "route_by_index":
        allowed.append("size")
        _family_bounded_int(recipe, "size", 2, minimum=1, maximum=8)
    _family_parameters(recipe, allowed)
    _family_scalar_ticks(recipe, "condition", _FLOW_FAMILY[operation])
    if input_type == "tsd":
        _validate_mapping_ticks(recipe, "ts")
    else:
        _family_scalar_ticks(recipe, "ts", input_type)


def _flow_control(hg, recipe):
    from hgraph.test import eval_node

    parameters = recipe.parameters
    operation = parameters["operation"]
    annotation = _family_annotation(hg, parameters.get("input_type", "int"))
    condition_annotation = _family_annotation(hg, _FLOW_FAMILY[operation])

    if operation == "filter_":

        @hg.graph
        def parity_graph(condition: condition_annotation, ts: annotation) -> annotation:
            return hg.filter_(condition, ts)

    elif operation == "gate":
        buffer_length = parameters.get("buffer_length", 1)

        @hg.graph
        def parity_graph(condition: condition_annotation, ts: annotation) -> annotation:
            return hg.gate(condition, ts, buffer_length=buffer_length)

    elif operation == "sample":

        @hg.graph
        def parity_graph(condition: condition_annotation, ts: annotation) -> annotation:
            return hg.sample(condition, ts)

    elif operation == "if_":
        branch = parameters.get("branch", "true")

        @hg.graph
        def parity_graph(condition: condition_annotation, ts: annotation) -> annotation:
            result = hg.if_(condition, ts)
            return result.true if branch == "true" else result.false

    elif operation == "if_true":
        tick_once_only = parameters.get("tick_once_only", False)

        @hg.graph
        def parity_graph(
            condition: condition_annotation, ts: annotation
        ) -> hg.TS[bool]:
            hg.null_sink(ts)
            return hg.if_true(condition, tick_once_only=tick_once_only)

    else:  # route_by_index
        size = parameters.get("size", 2)
        output = hg.TSL[annotation, hg.Size[size]]

        @hg.graph
        def parity_graph(condition: condition_annotation, ts: annotation) -> output:
            return hg.route_by_index[hg.SIZE : hg.Size[size]](condition, ts)

    inputs = decoded_inputs(hg, recipe)
    return eval_node(parity_graph, inputs["condition"], inputs["ts"])


# --------------------------------------------------------------------------
# Set (TSS) operators.

_SET_FAMILY = (
    "bit_and",
    "bit_or",
    "bit_xor",
    "difference",
    "intersection",
    "symmetric_difference",
    "union",
)


def _validate_set_ticks(recipe, name, element_type=None):
    """``name`` ticks are ``$set_delta`` records over ``element_type``.

    ``element_type`` is the scalar type the executor wires the ``TSS`` with;
    checking the elements here keeps a wrongly-typed element a rejected
    recipe rather than a decode/wiring failure reported as a runtime
    difference.
    """
    expected = _SCALAR_TYPES[element_type] if element_type is not None else None
    for tick in recipe.inputs[name]:
        if tick is None:
            continue
        if not isinstance(tick, dict) or set(tick) != {_SET_DELTA}:
            raise RecipeError(
                f"{recipe.template} {name} ticks must be a $set_delta or null"
            )
        if expected is None:
            continue
        delta = tick[_SET_DELTA]
        if not isinstance(delta, dict) or set(delta) != {"added", "removed"}:
            raise RecipeError(
                f"{recipe.template} {name} $set_delta requires added and "
                "removed lists"
            )
        for side in ("added", "removed"):
            items = delta[side]
            if not isinstance(items, list):
                raise RecipeError(
                    f"{recipe.template} {name} $set_delta {side} must be a list"
                )
            for item in items:
                if type(item) is not expected:
                    raise RecipeError(
                        f"{recipe.template} {name} $set_delta {side} elements "
                        f"must be {element_type}, got {type(item).__name__}"
                    )


def _validate_set_operator(recipe):
    operation = _family_operation(recipe, _SET_FAMILY)
    if operation in ("difference", "bit_and", "bit_or", "bit_xor"):
        _family_inputs(recipe, ("a", "b"))
    else:
        _family_inputs(recipe, ("a", "b"), ("a", "b", "c"))
    _family_parameters(recipe, ("element_type",))
    element_type = _family_choice(recipe, "element_type", "int", ("int", "str"))
    for name in recipe.inputs:
        _validate_set_ticks(recipe, name, element_type)


def _set_operator(hg, recipe):
    from hgraph.test import eval_node

    parameters = recipe.parameters
    operation = parameters["operation"]
    annotation = _family_annotation(
        hg, "tss_int" if parameters.get("element_type", "int") == "int" else "tss_str"
    )
    node = getattr(hg, operation)
    inputs = decoded_inputs(hg, recipe)
    if "c" in inputs:

        @hg.graph
        def parity_graph(
            a: annotation, b: annotation, c: annotation
        ) -> annotation:
            return node(a, b, c)

        return eval_node(parity_graph, inputs["a"], inputs["b"], inputs["c"])

    @hg.graph
    def parity_graph(a: annotation, b: annotation) -> annotation:
        return node(a, b)

    return eval_node(parity_graph, inputs["a"], inputs["b"])


# --------------------------------------------------------------------------
# Keyed-collection (TSD) operators.

#: operation -> (input names, the TSD shapes they take)
_TSD_FAMILY = {
    "collapse_keys": ("nested",),
    "flip": ("flat",),
    "flip_keys": ("nested",),
    "merge": ("flat", "flat"),
    "partition": ("flat", "keys"),
    "rekey": ("flat", "keys"),
    "uncollapse_keys": ("collapsed",),
    "unpartition": ("partitioned",),
}

_TSD_INPUT_NAMES = {
    "collapse_keys": ("ts",),
    "flip": ("ts",),
    "flip_keys": ("ts",),
    "merge": ("ts", "other"),
    "partition": ("ts", "keys"),
    "rekey": ("ts", "keys"),
    "uncollapse_keys": ("ts",),
    "unpartition": ("ts",),
}


def _tsd_shape(hg, shape):
    return {
        "flat": hg.TSD[str, hg.TS[int]],
        "keys": hg.TSD[str, hg.TS[str]],
        "nested": hg.TSD[str, hg.TSD[int, hg.TS[str]]],
        "collapsed": hg.TSD[tuple[str, int], hg.TS[str]],
        "partitioned": hg.TSD[str, hg.TSD[str, hg.TS[int]]],
    }[shape]


def _validate_tsd_operator(recipe):
    operation = _family_operation(recipe, _TSD_FAMILY)
    _family_inputs(recipe, _TSD_INPUT_NAMES[operation])
    _family_parameters(recipe, ("remove_empty",))
    if operation == "uncollapse_keys":
        _family_bool(recipe, "remove_empty", True)
    elif "remove_empty" in recipe.parameters:
        raise RecipeError(
            "tsd_operator remove_empty applies to uncollapse_keys only"
        )
    for name, ticks in recipe.inputs.items():
        for tick in ticks:
            if tick is None:
                continue
            if not isinstance(tick, dict):
                raise RecipeError(
                    f"tsd_operator {name} ticks must be a JSON object or null"
                )


def _tsd_operator(hg, recipe):
    from hgraph.test import eval_node

    operation = recipe.parameters["operation"]
    shapes = _TSD_FAMILY[operation]
    names = _TSD_INPUT_NAMES[operation]
    inputs = decoded_inputs(hg, recipe)
    first = _tsd_shape(hg, shapes[0])

    if operation == "flip":
        @hg.graph
        def parity_graph(ts: first) -> hg.TSD[int, hg.TS[str]]:
            return hg.flip(ts)

    elif operation == "flip_keys":
        @hg.graph
        def parity_graph(ts: first) -> hg.TSD[int, hg.TSD[str, hg.TS[str]]]:
            return hg.flip_keys(ts)

    elif operation == "collapse_keys":
        @hg.graph
        def parity_graph(ts: first) -> hg.TSD[tuple[str, int], hg.TS[str]]:
            return hg.collapse_keys(ts)

    elif operation == "uncollapse_keys":
        remove_empty = recipe.parameters.get("remove_empty", True)

        @hg.graph
        def parity_graph(ts: first) -> hg.TSD[str, hg.TSD[int, hg.TS[str]]]:
            return hg.uncollapse_keys(ts, remove_empty=remove_empty)

    elif operation == "unpartition":
        @hg.graph
        def parity_graph(ts: first) -> hg.TSD[str, hg.TS[int]]:
            return hg.unpartition(ts)

    elif operation == "rekey":
        keys_shape = _tsd_shape(hg, shapes[1])

        @hg.graph
        def parity_graph(ts: first, keys: keys_shape) -> hg.TSD[str, hg.TS[int]]:
            return hg.rekey(ts, keys)

    elif operation == "partition":
        keys_shape = _tsd_shape(hg, shapes[1])

        @hg.graph
        def parity_graph(
            ts: first, keys: keys_shape
        ) -> hg.TSD[str, hg.TSD[str, hg.TS[int]]]:
            return hg.partition(ts, keys)

    else:  # merge
        other = _tsd_shape(hg, shapes[1])

        @hg.graph
        def parity_graph(ts: first, other: other) -> hg.TSD[str, hg.TS[int]]:
            return hg.merge(ts, other)

    return eval_node(parity_graph, *(inputs[name] for name in names))


# --------------------------------------------------------------------------
# List (TSL) operators over free-standing time series.

_TSL_FAMILY = {
    "all_": ("bool",),
    "any_": ("bool",),
    "index_of": ("bool", "int", "float", "str"),
    "merge": ("bool", "int", "float", "str"),
    "race": ("bool", "int", "float", "str"),
}


def _validate_tsl_operator(recipe):
    operation = _family_operation(recipe, _TSL_FAMILY)
    if operation == "index_of":
        _family_inputs(recipe, ("a", "b", "item"))
    else:
        _family_inputs(recipe, ("a", "b"), ("a", "b", "c"))
    input_type = recipe.parameters.get("input_type", "int")
    if input_type not in _TSL_FAMILY[operation]:
        raise RecipeError(
            f"tsl_operator {operation} accepts input_type "
            f"{_TSL_FAMILY[operation]}, got {input_type!r}"
        )
    _family_parameters(recipe, ("input_type",))
    for name in recipe.inputs:
        _family_scalar_ticks(recipe, name, input_type)


def _tsl_operator(hg, recipe):
    from hgraph.test import eval_node

    operation = recipe.parameters["operation"]
    annotation = _family_annotation(hg, recipe.parameters.get("input_type", "int"))
    inputs = decoded_inputs(hg, recipe)

    if operation == "index_of":

        @hg.graph
        def parity_graph(
            a: annotation, b: annotation, item: annotation
        ) -> hg.TS[int]:
            return hg.index_of(hg.TSL.from_ts(a, b), item)

        return eval_node(parity_graph, inputs["a"], inputs["b"], inputs["item"])

    node = getattr(hg, operation)
    output = hg.TS[bool] if operation in ("all_", "any_") else annotation
    if "c" in inputs:

        @hg.graph
        def parity_graph(a: annotation, b: annotation, c: annotation) -> output:
            return node(a, b, c)

        return eval_node(parity_graph, inputs["a"], inputs["b"], inputs["c"])

    @hg.graph
    def parity_graph(a: annotation, b: annotation) -> output:
        return node(a, b)

    return eval_node(parity_graph, inputs["a"], inputs["b"])


# --------------------------------------------------------------------------
# Temporal component accessors.

_TEMPORAL_COMPONENT_FAMILY = {
    "day_of_month": ("date", "int"),
    "evaluation_time_in_range": ("int", "cmp"),
    "explode": ("date", "triple"),
    "last_modified_date": ("int", "date"),
    "last_modified_time": ("int", "datetime"),
    "month_of_year": ("date", "int"),
    "year": ("date", "int"),
}


def _validate_temporal_component(recipe):
    _family_inputs(recipe, ("ts",))
    operation = _family_operation(recipe, _TEMPORAL_COMPONENT_FAMILY)
    input_type, _output = _TEMPORAL_COMPONENT_FAMILY[operation]
    allowed = []
    if operation == "evaluation_time_in_range":
        allowed = ["start_micros", "end_micros"]
        start = _family_bounded_int(recipe, "start_micros", 2, minimum=1, maximum=1024)
        end = _family_bounded_int(recipe, "end_micros", 4, minimum=1, maximum=1024)
        if start > end:
            raise RecipeError(
                "temporal_component start_micros must not exceed end_micros"
            )
    _family_parameters(recipe, allowed)
    if input_type == "date":
        _family_temporal_ticks(recipe, "ts", "date")
    else:
        _family_scalar_ticks(recipe, "ts", "int")


def _temporal_component(hg, recipe):
    import datetime as _dt

    from hgraph.test import eval_node

    operation = recipe.parameters["operation"]
    input_type, output_kind = _TEMPORAL_COMPONENT_FAMILY[operation]
    annotation = _family_annotation(hg, input_type)
    ticks = decoded_inputs(hg, recipe)["ts"]

    if output_kind == "triple":

        @hg.graph
        def parity_graph(ts: annotation) -> hg.TSL[hg.TS[int], hg.Size[3]]:
            return hg.explode(ts)

    elif operation == "evaluation_time_in_range":
        start = hg.MIN_ST + hg.MIN_TD * recipe.parameters.get("start_micros", 2)
        end = hg.MIN_ST + hg.MIN_TD * recipe.parameters.get("end_micros", 4)

        @hg.graph
        def parity_graph(ts: annotation) -> hg.TS[hg.CmpResult]:
            hg.null_sink(ts)
            return hg.evaluation_time_in_range(hg.const(start), hg.const(end))

    else:
        node = getattr(hg, operation)
        output = {
            "int": hg.TS[int],
            "date": hg.TS[_dt.date],
            "datetime": hg.TS[_dt.datetime],
        }[output_kind]

        @hg.graph
        def parity_graph(ts: annotation) -> output:
            return node(ts)

    return eval_node(parity_graph, ticks)


# --------------------------------------------------------------------------
# TABLE protocol conversions.

_TABLE_SHAPES = ("ts_int", "ts_str", "tsd", "tsb")


def _table_shape(hg, shape):
    if shape == "ts_int":
        return hg.TS[int]
    if shape == "ts_str":
        return hg.TS[str]
    if shape == "tsd":
        return hg.TSD[str, hg.TS[int]]
    return hg.TSB[hg.ts_schema(x=hg.TS[int], y=hg.TS[str])]


def _validate_table_round_trip(recipe):
    _family_inputs(recipe, ("ts",))
    operation = _family_operation(recipe, ("round_trip", "schema"))
    shape = _family_choice(recipe, "shape", "ts_int", _TABLE_SHAPES)
    _family_parameters(recipe, ("shape",))
    if operation == "schema" and shape not in ("ts_int", "ts_str"):
        raise RecipeError(
            "table_round_trip schema covers the scalar shapes only"
        )
    if shape == "ts_int":
        _family_scalar_ticks(recipe, "ts", "int")
    elif shape == "ts_str":
        _family_scalar_ticks(recipe, "ts", "str")
    else:
        for tick in recipe.inputs["ts"]:
            if tick is not None and not isinstance(tick, dict):
                raise RecipeError(
                    "table_round_trip container ticks must be objects or null"
                )


def _table_round_trip(hg, recipe):
    from hgraph.test import eval_node

    operation = recipe.parameters["operation"]
    shape = recipe.parameters.get("shape", "ts_int")
    annotation = _table_shape(hg, shape)
    ticks = decoded_inputs(hg, recipe)["ts"]

    if operation == "schema":

        @hg.graph
        def parity_graph(ts: annotation) -> annotation:
            # The schema value itself is a python-owned surface whose
            # representation differs by runtime; the recipe records that the
            # graph wires and evaluates it.
            hg.null_sink(hg.table_schema(annotation))
            return ts

    else:

        @hg.graph
        def parity_graph(ts: annotation) -> annotation:
            return hg.from_table[hg.OUT:annotation](hg.to_table(ts))

    return eval_node(parity_graph, ticks)


# --------------------------------------------------------------------------
# JSON conversions.


def _validate_json_round_trip(recipe):
    _family_inputs(recipe, ("ts",))
    _family_operation(recipe, ("decode", "round_trip"))
    _family_parameters(recipe, ())
    for tick in recipe.inputs["ts"]:
        if tick is None:
            continue
        if not isinstance(tick, str):
            raise RecipeError("json_round_trip ts ticks must be JSON text or null")
        try:
            json.loads(tick)
        except json.JSONDecodeError as error:
            raise RecipeError(
                f"json_round_trip ts tick {tick!r} is not valid JSON"
            ) from error


def _json_round_trip(hg, recipe):
    from hgraph.test import eval_node

    operation = recipe.parameters["operation"]
    if operation == "decode":

        @hg.graph
        def parity_graph(ts: hg.TS[str]) -> hg.TS[str]:
            # ``json_decode``'s TS[JSON] value is a python-owned handle whose
            # representation differs by runtime; sinking it records that the
            # decode wires and evaluates.
            hg.null_sink(hg.json_decode(ts))
            return ts

    else:

        @hg.graph
        def parity_graph(ts: hg.TS[str]) -> hg.TS[str]:
            # The only encode spelling both runtimes accept: released hgraph
            # resolves ``_tp`` from SCALAR, the candidate from OUT.
            return hg.json_encode[hg.SCALAR:str, hg.OUT : hg.TS[str]](
                hg.json_decode(ts)
            )

    return eval_node(parity_graph, decoded_inputs(hg, recipe)["ts"])


# --------------------------------------------------------------------------
# Data-frame conversions.


def _validate_data_frame_conversion(recipe):
    _family_inputs(recipe, ("ts",))
    operation = _family_operation(recipe, ("to_data_frame", "from_data_frame"))
    if operation == "to_data_frame":
        _family_parameters(recipe, ())
    else:
        _family_parameters(recipe, ("rows",))
        rows = recipe.parameters.get("rows", [[1, 3], [2, 4]])
        if (
            not isinstance(rows, list)
            or not 1 <= len(rows) <= 32
            or not all(
                isinstance(row, list)
                and len(row) == 2
                and all(
                    isinstance(value, int) and not isinstance(value, bool)
                    for value in row
                )
                and 1 <= row[0] <= 1024
                for row in rows
            )
        ):
            raise RecipeError(
                "data_frame_conversion rows must be 1-32 [micros, value] "
                "integer pairs with micros in [1, 1024]"
            )
        offsets = [row[0] for row in rows]
        if sorted(offsets) != offsets or len(set(offsets)) != len(offsets):
            raise RecipeError(
                "data_frame_conversion rows must be strictly increasing in micros"
            )
    _family_scalar_ticks(recipe, "ts", "int")


def _data_frame_conversion(hg, recipe):
    from hgraph.test import eval_node

    operation = recipe.parameters["operation"]
    ticks = decoded_inputs(hg, recipe)["ts"]
    if operation == "to_data_frame":

        @hg.graph
        def parity_graph(ts: hg.TS[int]) -> hg.TS[int]:
            # The frame object is a runtime-owned surface (polars upstream,
            # pyarrow in the candidate); the recipe records that the
            # conversion wires and evaluates on the same ticks.
            hg.null_sink(hg.to_data_frame(ts))
            return ts

        return eval_node(parity_graph, ticks)

    import polars as pl

    rows = recipe.parameters.get("rows", [[1, 3], [2, 4]])
    frame = pl.DataFrame(
        {
            "date": [hg.MIN_ST + hg.MIN_TD * row[0] for row in rows],
            "value": [row[1] for row in rows],
        }
    )

    @hg.graph
    def parity_graph(ts: hg.TS[int]) -> hg.TS[int]:
        hg.null_sink(ts)
        return hg.from_data_frame[hg.OUT : hg.TS[int]](frame)

    return eval_node(parity_graph, ticks)


# --------------------------------------------------------------------------
# CompoundScalar field operators.


def _compound_scalar_field_model(hg):
    from dataclasses import dataclass

    # Deliberately NOT frozen: released hgraph refuses ``setattr_`` on a
    # frozen CompoundScalar, so a frozen model would only compare that
    # rejection.
    @dataclass
    class Base(hg.CompoundScalar):
        a: int

    @dataclass
    class Derived(Base):
        b: str

    return Base, Derived


def _validate_compound_scalar_field(recipe):
    operation = _family_operation(
        recipe, ("downcast_ref", "getattr_", "setattr_")
    )
    if operation == "setattr_":
        _family_inputs(recipe, ("event", "value"))
    else:
        _family_inputs(recipe, ("event",))
    _family_parameters(recipe, ())
    for tick in recipe.inputs["event"]:
        if tick is None:
            continue
        if not isinstance(tick, dict) or set(tick) - {"a", "b"} or "a" not in tick:
            raise RecipeError(
                "compound_scalar_field event ticks need an 'a' integer and an "
                "optional 'b' string"
            )
        if not isinstance(tick["a"], int) or isinstance(tick["a"], bool):
            raise RecipeError("compound_scalar_field event 'a' must be an integer")
        if "b" in tick and not isinstance(tick["b"], str):
            raise RecipeError("compound_scalar_field event 'b' must be a string")
        if operation == "downcast_ref" and "b" not in tick:
            raise RecipeError(
                "compound_scalar_field downcast_ref events must carry 'b'"
            )
    if operation == "setattr_":
        _family_scalar_ticks(recipe, "value", "int")


def _compound_scalar_field(hg, recipe):
    from hgraph.test import eval_node

    Base, Derived = _compound_scalar_field_model(hg)
    operation = recipe.parameters["operation"]
    events = [
        None
        if tick is None
        else (Derived(**tick) if "b" in tick else Base(**tick))
        for tick in recipe.inputs["event"]
    ]

    if operation == "getattr_":

        @hg.graph
        def parity_graph(event: hg.TS[Base]) -> hg.TS[int]:
            return hg.getattr_(event, "a")

        return eval_node(parity_graph, events)

    if operation == "downcast_ref":

        @hg.graph
        def parity_graph(event: hg.TS[Base]) -> hg.TS[str]:
            return hg.downcast_ref(Derived, event).b

        return eval_node(parity_graph, events)

    @hg.graph
    def parity_graph(event: hg.TS[Base], value: hg.TS[int]) -> hg.TS[Base]:
        return hg.setattr_(event, "a", value)

    return eval_node(
        parity_graph, events, decoded_inputs(hg, recipe)["value"]
    )


# --------------------------------------------------------------------------
# Sink operators: no output, so the recipe records what the GRAPH does --
# the passthrough value, and, when asked, the standard output the sink wrote.

_SINK_FAMILY = ("assert_", "debug_print", "log_", "null_sink", "print_")


def _validate_sink_operator(recipe):
    _family_inputs(recipe, ("ts",))
    operation = _family_operation(recipe, _SINK_FAMILY)
    allowed = ["input_type", "capture"]
    if operation == "assert_":
        allowed.extend(["threshold", "message"])
        _family_bounded_int(recipe, "threshold", 0, minimum=-1024, maximum=1024)
        _family_bounded_str(recipe, "message", "parity assertion")
    elif operation in ("debug_print", "log_", "print_"):
        allowed.append("label")
        _family_bounded_str(recipe, "label", "value")
    _family_parameters(recipe, allowed)
    input_type = _family_choice(
        recipe, "input_type", "int", ("bool", "int", "float", "str")
    )
    if operation == "assert_" and input_type not in ("int", "float"):
        raise RecipeError("sink_operator assert_ compares a numeric input")
    _family_choice(recipe, "capture", "none", ("none", "stdout"))
    _family_scalar_ticks(recipe, "ts", input_type)


#: Released hgraph installs its own stdout logging handler and writes the
#: framework's lifecycle records through it -- "Wiring graph", "Creating graph
#: engine", "Finished running graph" -- each stamped with a WALL CLOCK time
#: ("2026-09-09 07:59:58,781 [hgraph][DEBUG] ...") that no two runs can agree
#: on. Those records belong to the runtime, not to the graph, so
#: ``capture: "stdout"`` drops exactly them: the framework logger (``hgraph``)
#: at DEBUG.
_FRAMEWORK_LOG_LINE = re.compile(
    r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} \[hgraph\]\[DEBUG\] "
)

#: Every OTHER logging record on stdout is the graph's own output: a ``log_``
#: record goes through the same handler under the node's logger
#: ("... [hgraph.<graph>.<node>][INFO] [<engine time>] v=1"). Keep the line --
#: dropping it collapsed a missing ``log_`` record into an empty trace on both
#: sides -- and replace only the leading wall-clock stamp, which is the single
#: part of it that cannot be compared. The lookahead keeps the substitution to
#: a real logging preamble, so a ``print_`` label that merely starts with a
#: timestamp survives verbatim.
_LOG_RECORD_WALL_CLOCK = re.compile(
    r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} (?=\[)"
)
_WALL_CLOCK_PLACEHOLDER = "<wall-clock> "


def _sink_user_output(captured):
    return [
        _LOG_RECORD_WALL_CLOCK.sub(_WALL_CLOCK_PLACEHOLDER, line)
        for line in captured.splitlines()
        if not _FRAMEWORK_LOG_LINE.match(line)
    ]


def _sink_operator(hg, recipe):
    import contextlib
    import io

    from hgraph.test import eval_node

    parameters = recipe.parameters
    operation = parameters["operation"]
    annotation = _family_annotation(hg, parameters.get("input_type", "int"))
    label = parameters.get("label", "value")

    if operation == "null_sink":

        @hg.graph
        def parity_graph(ts: annotation) -> annotation:
            hg.null_sink(ts)
            return ts

    elif operation == "assert_":
        threshold = parameters.get("threshold", 0)
        message = parameters.get("message", "parity assertion")

        @hg.graph
        def parity_graph(ts: annotation) -> annotation:
            hg.assert_(ts > threshold, message)
            return ts

    elif operation == "debug_print":

        @hg.graph
        def parity_graph(ts: annotation) -> annotation:
            hg.debug_print(label, ts)
            return ts

    elif operation == "print_":

        @hg.graph
        def parity_graph(ts: annotation) -> annotation:
            hg.print_(label + "={value}", value=ts)
            return ts

    else:  # log_

        @hg.graph
        def parity_graph(ts: annotation) -> annotation:
            hg.log_(label + "={value}", value=ts)
            return ts

    ticks = decoded_inputs(hg, recipe)["ts"]
    if parameters.get("capture", "none") != "stdout":
        return eval_node(parity_graph, ticks)
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        result = eval_node(parity_graph, ticks)
    return {
        "result": result,
        "stdout": _sink_user_output(buffer.getvalue()),
    }


CATALOG = {
    "scalar_expression": TemplateSpec(
        name="scalar_expression",
        required_inputs=None,
        features=("shape:TS", "topology:expression"),
        operators=(
            "abs_",
            "add_",
            "dedup",
            "eq_",
            "ge_",
            "gt_",
            "le_",
            "lt_",
            "mul_",
            "ne_",
            "neg_",
            "pos_",
            "sub_",
        ),
        execute=_scalar_expression,
    ),
    "scalar_operator_arguments": TemplateSpec(
        name="scalar_operator_arguments",
        required_inputs=("value",),
        features=(
            "shape:TS",
            "topology:operator-overload",
            "argument:scalar",
        ),
        operators=tuple(sorted(_SCALAR_ARGUMENT_OPERATIONS.values())),
        execute=_scalar_operator_arguments,
    ),
    "feedback_accumulate": TemplateSpec(
        name="feedback_accumulate",
        required_inputs=("value",),
        features=(
            "shape:TS",
            "topology:feedback",
            "lifecycle:multi-cycle",
            "reference:REF",
        ),
        operators=("add_", "feedback", "passive"),
        execute=_feedback_accumulate,
    ),
    "switch_arithmetic": TemplateSpec(
        name="switch_arithmetic",
        required_inputs=("selector", "lhs", "rhs"),
        features=(
            "shape:TS",
            "topology:switch",
            "lifecycle:branch-rebind",
            "reference:REF",
        ),
        operators=("add_", "sub_", "switch_"),
        execute=_switch_arithmetic,
    ),
    "tsd_map_reduce": TemplateSpec(
        name="tsd_map_reduce",
        required_inputs=("values",),
        features=(
            "shape:TSD",
            "topology:map",
            "lifecycle:keyed",
            "reference:REF",
        ),
        operators=("add_", "map_", "reduce"),
        execute=_tsd_map_reduce,
    ),
    "service_reference": TemplateSpec(
        name="service_reference",
        required_inputs=("value",),
        features=(
            "shape:TS",
            "framework:service",
            "service:reference",
            "configuration:path",
            "reference:REF",
        ),
        operators=("add_", "const", "passive"),
        execute=_service_reference,
    ),
    "service_request_reply": TemplateSpec(
        name="service_request_reply",
        required_inputs=("value",),
        features=(
            "shape:TS",
            "shape:TSD",
            "framework:service",
            "service:request-reply",
            "lifecycle:transport-delay",
            "configuration:path",
            "reference:REF",
        ),
        operators=("add_", "map_"),
        execute=_service_request_reply,
    ),
    "service_subscription": TemplateSpec(
        name="service_subscription",
        required_inputs=("symbol",),
        features=(
            "shape:TS",
            "shape:TSS",
            "shape:TSD",
            "framework:service",
            "service:subscription",
            "lifecycle:keyed",
            "lifecycle:transport-delay",
            "reference:REF",
        ),
        operators=("len_", "map_", "mul_"),
        execute=_service_subscription,
    ),
    "adaptor_loopback": TemplateSpec(
        name="adaptor_loopback",
        required_inputs=("value",),
        features=(
            "shape:TS",
            "framework:adaptor",
            "adaptor:automatic",
            "adaptor:explicit-path",
            "configuration:path",
            "reference:REF",
        ),
        operators=("mul_",),
        execute=_adaptor_loopback,
    ),
    "service_adaptor_roundtrip": TemplateSpec(
        name="service_adaptor_roundtrip",
        required_inputs=("value",),
        features=(
            "shape:TS",
            "shape:TSD",
            "framework:adaptor",
            "adaptor:service",
            "adaptor:multi-client",
            "implementation:path-injection",
            "lifecycle:same-cycle",
            "reference:REF",
        ),
        operators=("add_", "map_"),
        execute=_service_adaptor_roundtrip,
    ),
    "service_adaptor_parameterized_clients": TemplateSpec(
        name="service_adaptor_parameterized_clients",
        required_inputs=("values", "direct", "trigger"),
        features=(
            "shape:TS",
            "shape:TSD",
            "shape:TSL",
            "framework:adaptor",
            "adaptor:service",
            "adaptor:multi-client",
            "configuration:scalar",
            "implementation:path-injection",
            "topology:map",
            "topology:feedback",
            "lifecycle:multi-cycle",
        ),
        operators=("add_", "combine", "feedback", "map_", "sample"),
        execute=_service_adaptor_parameterized_clients,
    ),
    "declaration_shape": TemplateSpec(
        name="declaration_shape",
        required_inputs=None,
        features=(
            "shape:TS",
            "shape:TSB",
            "shape:TSD",
            "declaration:derived-through-base",
            "declaration:partial-bundle",
            "declaration:element-or-whole",
            "declaration:branch-equivalence",
            "topology:map",
            "topology:switch",
        ),
        operators=("map_", "switch_", "convert", "add_"),
        execute=_declaration_shape,
    ),
    "context_switch": TemplateSpec(
        name="context_switch",
        required_inputs=("selector", "value", "offset"),
        features=(
            "shape:TS",
            "framework:context",
            "topology:context",
            "topology:switch",
            "lifecycle:branch-rebind",
            "reference:REF",
        ),
        operators=("add_", "sub_", "switch_"),
        execute=_context_switch,
    ),
    "operator_pipeline": TemplateSpec(
        name="operator_pipeline",
        required_inputs=("lhs", "rhs", "choose_minimum"),
        features=(
            "shape:TS",
            "shape:TSB",
            "topology:operator-composition",
            "type:int",
            "type:bool",
            "type:str",
            "reference:REF",
        ),
        operators=("add_", "and_", "combine", "floordiv_", "format_", "gt_", "if_then_else", "len_", "max_", "min_", "mod_", "modified", "not_", "or_", "valid"),
        execute=_operator_pipeline,
    ),
    "value_consumer_reference": TemplateSpec(
        name="value_consumer_reference",
        required_inputs=("lhs", "rhs", "choose_rhs"),
        features=(
            "shape:TS",
            "type:int",
            "type:bool",
            "reference:REF",
            "topology:operator-composition",
            "compatibility:release-0.5",
        ),
        operators=("apply", "if_then_else"),
        execute=_value_consumer_reference,
    ),
    "tsd_key_set_pipeline": TemplateSpec(
        name="tsd_key_set_pipeline",
        required_inputs=("values", "probe"),
        features=(
            "shape:TSS",
            "shape:TSD",
            "shape:TSB",
            "topology:operator-composition",
            "topology:key-set-projection",
            "lifecycle:keyed",
            "type:int",
            "type:bool",
            "type:float",
            "reference:REF",
        ),
        operators=("combine", "contains_", "dedup", "is_empty", "keys_", "len_", "max_", "mean", "min_", "sum_"),
        execute=_tsd_key_set_pipeline,
        float_abs_tolerance=1e-12,
    ),
    "mesh_key_set": TemplateSpec(
        name="mesh_key_set",
        required_inputs=("values",),
        features=(
            "shape:TSD",
            "shape:TSS",
            "topology:mesh",
            "topology:key-set-projection",
            "lifecycle:keyed",
            "lifecycle:nested-graph",
            "reference:REF",
        ),
        operators=("keys_", "mesh_", "mul_"),
        execute=_mesh_key_set,
    ),
    "issue_38_nested_tsd_feedback": TemplateSpec(
        name="issue_38_nested_tsd_feedback",
        required_inputs=("roll", "prices", "trigger"),
        features=(
            "shape:TSB",
            "shape:TSD",
            "topology:feedback",
            "topology:map",
            "topology:switch",
            "lifecycle:key-removal",
            "lifecycle:branch-rebind",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=(
            "combine",
            "const",
            "dedup",
            "default",
            "feedback",
            "lag",
            "map_",
            "sample",
            "switch_",
        ),
        execute=_issue_38_nested_tsd_feedback,
    ),
    "issue_40_no_key_rebind": TemplateSpec(
        name="issue_40_no_key_rebind",
        required_inputs=("target", "rebase", "prices", "trigger"),
        features=(
            "shape:TSB",
            "shape:TSD",
            "topology:feedback",
            "topology:map",
            "lifecycle:reference-rebind",
            "lifecycle:keyed",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=(
            "combine",
            "const",
            "default",
            "feedback",
            "if_then_else",
            "lag",
            "map_",
            "no_key",
            "sample",
        ),
        execute=_issue_40_no_key_rebind,
    ),
    "polymorphic_event_flow": TemplateSpec(
        name="polymorphic_event_flow",
        required_inputs=("events", "trigger", "key"),
        features=(
            "shape:TS",
            "type:CompoundScalar",
            "type:polymorphic",
            "boundary:python-owned",
            "lifecycle:multi-cycle",
            "topology:user-pipeline",
        ),
        operators=(
            "batch",
            "collect",
            "convert",
            "emit",
            "feedback",
            "from_json",
            "to_json",
            "values_",
            "window",
        ),
        execute=_polymorphic_event_flow,
    ),
    "polymorphic_event_map": TemplateSpec(
        name="polymorphic_event_map",
        required_inputs=("events",),
        features=(
            "shape:TS",
            "shape:TSD",
            "shape:TSB",
            "type:CompoundScalar",
            "type:polymorphic",
            "boundary:python-owned",
            "topology:map",
            "topology:feedback",
            "lifecycle:keyed",
            "lifecycle:nested-graph",
        ),
        operators=("emit", "feedback", "map_"),
        execute=_polymorphic_event_map,
    ),
    "structural_map_projection": TemplateSpec(
        name="structural_map_projection",
        required_inputs=("lookups", "rows"),
        features=(
            "shape:TS",
            "shape:TSB",
            "shape:TSD",
            "topology:map",
            "topology:keyed-lookup",
            "reference:REF",
            "binding:non-peered",
            "boundary:structural",
            "lifecycle:keyed",
        ),
        operators=(
            "combine", "dereference", "dispatch_", "getitem_", "map_",
            "pass_through",
        ),
        execute=_structural_map_projection,
    ),
    "arrow_typed_projection": TemplateSpec(
        name="arrow_typed_projection",
        required_inputs=("side", "events"),
        features=(
            "shape:TS",
            "shape:TSB",
            "type:Enum",
            "type:CompoundScalar",
            "type:polymorphic",
            "boundary:python-owned",
            "framework:arrow",
            "topology:projection",
        ),
        operators=("debug_", "getitem_"),
        execute=_arrow_typed_projection,
    ),
    "stream_dataclass": TemplateSpec(
        name="stream_dataclass",
        required_inputs=("probe",),
        features=(
            "boundary:python-owned",
            "shape:TSB",
            "type:dataclass",
            "conversion:round-trip",
        ),
        operators=("convert",),
        execute=_stream_dataclass,
    ),
    "compound_scalar_downcast": TemplateSpec(
        name="compound_scalar_downcast",
        required_inputs=("event",),
        features=(
            "boundary:python-owned",
            "shape:TS",
            "type:compound-scalar",
            "type:polymorphic",
            "conversion:checked-downcast",
        ),
        operators=("downcast_",),
        execute=_compound_scalar_downcast,
    ),
    "polymorphic_field_projection": TemplateSpec(
        name="polymorphic_field_projection",
        required_inputs=("series",),
        features=(
            "boundary:python-owned",
            "shape:TS",
            "type:CompoundScalar",
            "type:polymorphic",
            "type:base-declared-field",
            "topology:projection",
        ),
        operators=(),
        execute=_polymorphic_field_projection,
    ),
    "polymorphic_tsd_key": TemplateSpec(
        name="polymorphic_tsd_key",
        required_inputs=("entries",),
        features=(
            "boundary:python-owned",
            "shape:TSD",
            "type:CompoundScalar",
            "type:polymorphic",
            "type:polymorphic-key",
            "lifecycle:keyed",
        ),
        operators=("map_", "len_", "feedback"),
        execute=_polymorphic_tsd_key,
    ),
    "enum_literal_selection": TemplateSpec(
        name="enum_literal_selection",
        required_inputs=("condition",),
        features=(
            "boundary:python-owned",
            "shape:TS",
            "type:enum",
            "conversion:auto-const",
            "topology:branch-selection",
        ),
        operators=("if_then_else",),
        execute=_enum_literal_selection,
    ),
    "legacy_compound_scalar_json": TemplateSpec(
        name="legacy_compound_scalar_json",
        required_inputs=("value",),
        features=(
            "boundary:python-owned",
            "type:compound-scalar",
            "type:polymorphic",
            "conversion:json",
            "compatibility:release-0.5",
        ),
        operators=("to_json", "from_json"),
        execute=_legacy_compound_scalar_json,
    ),
    "json_operator_contract": TemplateSpec(
        name="json_operator_contract",
        required_inputs=("value",),
        features=(
            "conversion:json",
            "compatibility:release-0.5",
            "api:public-signature",
            "lifecycle:multi-cycle",
        ),
        operators=("to_json", "from_json"),
        execute=_json_operator_contract,
    ),
    "temporal_expression": TemplateSpec(
        name="temporal_expression",
        required_inputs=None,
        features=(
            "shape:TS",
            "topology:expression",
            "domain:temporal",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=("sub_", "add_", "getattr_"),
        execute=_temporal_expression,
    ),
    "collection_size": TemplateSpec(
        name="collection_size",
        required_inputs=None,
        features=(
            "topology:expression",
            "domain:collection-size",
            "reference:REF",
        ),
        operators=("len_", "is_empty", "contains_", "dedup"),
        execute=_collection_size,
    ),
    "lifecycle_state": TemplateSpec(
        name="lifecycle_state",
        required_inputs=("value",),
        features=(
            "shape:TS",
            "type:int",
            "boundary:python-owned",
            "domain:lifecycle-signature",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=(),
        execute=_lifecycle_state,
    ),
    "realtime_default_start": TemplateSpec(
        name="realtime_default_start",
        required_inputs=("probe",),
        features=(
            "shape:TS",
            "type:bool",
            "execution:real-time",
            "clock:wall-time",
            "compatibility:release-0.5",
        ),
        operators=("const",),
        execute=_realtime_default_start,
    ),
    "nested_higher_order": TemplateSpec(
        name="nested_higher_order",
        required_inputs=None,
        features=(
            "topology:nested-higher-order",
            "shape:TSD",
            "type:int",
            "reference:REF",
            "lifecycle:multi-cycle",
        ),
        operators=(
            "map_", "mesh_", "switch_", "reduce", "len_", "keys_", "dedup",
        ),
        execute=_nested_higher_order,
    ),
    "data_frame_recording": TemplateSpec(
        name="data_frame_recording",
        required_inputs=("ts",),
        features=(
            "shape:TS",
            "type:int",
            "boundary:python-owned",
            "domain:frame-surface",
            "topology:record-replay",
        ),
        operators=("record", "replay"),
        execute=_data_frame_recording,
    ),
    "type_argument_positional": TemplateSpec(
        name="type_argument_positional",
        required_inputs=("value",),
        features=(
            "shape:TS",
            "type-argument:positional",
            "syntax:positional-target",
            "compatibility:release-0.5",
            "api:public-signature",
        ),
        operators=("const", "nothing", "default", "add_"),
        execute=_type_argument_positional,
    ),
    "type_argument_default_order": TemplateSpec(
        name="type_argument_default_order",
        required_inputs=("value",),
        features=(
            "shape:TS",
            "type-argument:default-before-auto-resolve",
            "syntax:bare-subscript",
            "compatibility:release-0.5",
        ),
        operators=("format_",),
        execute=_type_argument_default_order,
    ),
    "type_argument_size_pin": TemplateSpec(
        name="type_argument_size_pin",
        required_inputs=("values",),
        features=(
            "shape:TSL",
            "type-argument:size",
            "syntax:bare-subscript",
            "compatibility:release-0.5",
        ),
        operators=("sum_", "const", "add_"),
        execute=_type_argument_size_pin,
    ),
    "type_argument_collection": TemplateSpec(
        name="type_argument_collection",
        required_inputs=("values",),
        features=(
            "shape:TS",
            "type-argument:collection",
            "type:collection",
            "compatibility:release-0.5",
        ),
        operators=("len_", "const", "add_"),
        execute=_type_argument_collection,
    ),
    "unary_operator": TemplateSpec(
        name="unary_operator",
        required_inputs=("ts",),
        features=(
            "shape:TS",
            "topology:operator-family",
            "family:unary",
        ),
        operators=tuple(sorted(_UNARY_FAMILY)),
        execute=_unary_operator,
    ),
    "binary_operator": TemplateSpec(
        name="binary_operator",
        required_inputs=("lhs", "rhs"),
        features=(
            "shape:TS",
            "topology:operator-family",
            "family:binary",
        ),
        operators=tuple(sorted(_BINARY_FAMILY)),
        execute=_binary_operator,
    ),
    "string_operator": TemplateSpec(
        name="string_operator",
        required_inputs=None,
        features=(
            "shape:TS",
            "type:str",
            "topology:operator-family",
            "family:string",
        ),
        operators=tuple(sorted(_STRING_FAMILY)),
        execute=_string_operator,
    ),
    "stream_shape": TemplateSpec(
        name="stream_shape",
        required_inputs=("ts",),
        features=(
            "shape:TS",
            "topology:operator-family",
            "family:stream",
            "lifecycle:multi-cycle",
        ),
        operators=tuple(sorted({*_STREAM_FAMILY, "sum_", "mean"})),
        execute=_stream_shape,
    ),
    "flow_control": TemplateSpec(
        name="flow_control",
        required_inputs=("condition", "ts"),
        features=(
            "shape:TS",
            "topology:operator-family",
            "family:flow-control",
        ),
        operators=tuple(sorted({*_FLOW_FAMILY, "null_sink"})),
        execute=_flow_control,
    ),
    "set_operator": TemplateSpec(
        name="set_operator",
        required_inputs=None,
        features=(
            "shape:TSS",
            "topology:operator-family",
            "family:set",
        ),
        operators=tuple(sorted(_SET_FAMILY)),
        execute=_set_operator,
    ),
    "tsd_operator": TemplateSpec(
        name="tsd_operator",
        required_inputs=None,
        features=(
            "shape:TSD",
            "lifecycle:keyed",
            "topology:operator-family",
            "family:keyed-collection",
        ),
        operators=tuple(sorted(_TSD_FAMILY)),
        execute=_tsd_operator,
    ),
    "tsl_operator": TemplateSpec(
        name="tsl_operator",
        required_inputs=None,
        features=(
            "shape:TSL",
            "topology:operator-family",
            "family:list-collection",
        ),
        operators=tuple(sorted(_TSL_FAMILY)),
        execute=_tsl_operator,
    ),
    "temporal_component": TemplateSpec(
        name="temporal_component",
        required_inputs=("ts",),
        features=(
            "shape:TS",
            "domain:temporal",
            "topology:operator-family",
            "family:temporal-component",
        ),
        operators=tuple(sorted({*_TEMPORAL_COMPONENT_FAMILY, "null_sink"})),
        execute=_temporal_component,
    ),
    "table_round_trip": TemplateSpec(
        name="table_round_trip",
        required_inputs=("ts",),
        features=(
            "conversion:table",
            "topology:operator-family",
            "family:conversion",
        ),
        operators=("from_table", "null_sink", "table_schema", "to_table"),
        execute=_table_round_trip,
    ),
    "json_round_trip": TemplateSpec(
        name="json_round_trip",
        required_inputs=("ts",),
        features=(
            "conversion:json",
            "shape:TS",
            "type:str",
            "topology:operator-family",
            "family:conversion",
        ),
        operators=("json_decode", "json_encode", "null_sink"),
        execute=_json_round_trip,
    ),
    "data_frame_conversion": TemplateSpec(
        name="data_frame_conversion",
        required_inputs=("ts",),
        features=(
            "domain:frame-surface",
            "boundary:python-owned",
            "topology:operator-family",
            "family:conversion",
        ),
        operators=("from_data_frame", "null_sink", "to_data_frame"),
        execute=_data_frame_conversion,
    ),
    "compound_scalar_field": TemplateSpec(
        name="compound_scalar_field",
        required_inputs=None,
        features=(
            "shape:TS",
            "type:CompoundScalar",
            "boundary:python-owned",
            "topology:operator-family",
            "family:field-access",
        ),
        operators=("downcast_ref", "getattr_", "setattr_"),
        execute=_compound_scalar_field,
    ),
    "sink_operator": TemplateSpec(
        name="sink_operator",
        required_inputs=("ts",),
        features=(
            "shape:TS",
            "topology:sink",
            "family:sink",
        ),
        operators=tuple(sorted({*_SINK_FAMILY, "gt_"})),
        execute=_sink_operator,
    ),
}


def _validate_bounded_int_parameter(recipe, name, default, *, minimum, maximum):
    value = recipe.parameters.get(name, default)
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not minimum <= value <= maximum
    ):
        raise RecipeError(
            f"{recipe.template} {name} must be an integer in "
            f"[{minimum}, {maximum}]"
        )


def _validate_path_parameter(recipe, default):
    path = recipe.parameters.get("path", default)
    if (
        not isinstance(path, str)
        or not 1 <= len(path) <= 32
        or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for character in path)
    ):
        raise RecipeError(
            f"{recipe.template} path must be 1-32 lowercase letters, "
            "digits, hyphens, or underscores"
        )


def validate_recipe(recipe):
    spec = CATALOG.get(recipe.template)
    if spec is None:
        raise RecipeError(f"unknown template {recipe.template!r}")
    if spec.required_inputs is not None and set(recipe.inputs) != set(
        spec.required_inputs
    ):
        raise RecipeError(
            f"{recipe.template} requires inputs {spec.required_inputs}, "
            f"got {tuple(recipe.inputs)}"
        )
    _validate_reference_source(recipe)
    if recipe.template == "declaration_shape":
        _validate_declaration_shape(recipe)
    if recipe.template == "scalar_expression":
        _validate_scalar_expression(recipe)
    elif recipe.template == "scalar_operator_arguments":
        _validate_scalar_operator_arguments(recipe)
    elif recipe.template == "temporal_expression":
        _validate_temporal_expression(recipe)
    elif recipe.template == "collection_size":
        _validate_collection_size(recipe)
    elif recipe.template == "lifecycle_state":
        _validate_lifecycle_state(recipe)
    elif recipe.template == "realtime_default_start":
        _validate_realtime_default_start(recipe)
    elif recipe.template == "nested_higher_order":
        _validate_nested_higher_order(recipe)
    elif recipe.template == "data_frame_recording":
        _validate_data_frame_recording(recipe)
    elif recipe.template == "compound_scalar_downcast":
        _validate_compound_scalar_downcast(recipe)
    elif recipe.template == "polymorphic_field_projection":
        _validate_polymorphic_field_projection(recipe)
    elif recipe.template == "polymorphic_tsd_key":
        _validate_polymorphic_tsd_key(recipe)
    elif recipe.template == "enum_literal_selection":
        _validate_enum_literal_selection(recipe)
    elif recipe.template == "legacy_compound_scalar_json":
        _validate_legacy_compound_scalar_json(recipe)
    elif recipe.template == "json_operator_contract":
        _validate_json_operator_contract(recipe)
    elif recipe.template == "polymorphic_event_flow":
        _validate_polymorphic_event_flow(recipe)
    elif recipe.template == "polymorphic_event_map":
        _validate_polymorphic_event_map(recipe)
    elif recipe.template == "structural_map_projection":
        _validate_structural_map_projection(recipe)
    elif recipe.template == "arrow_typed_projection":
        _validate_arrow_typed_projection(recipe)
    elif recipe.template == "type_argument_positional":
        _validate_type_argument_positional(recipe)
    elif recipe.template == "type_argument_default_order":
        _validate_type_argument_default_order(recipe)
    elif recipe.template == "type_argument_size_pin":
        _validate_type_argument_size_pin(recipe)
    elif recipe.template == "type_argument_collection":
        _validate_type_argument_collection(recipe)
    elif recipe.template == "unary_operator":
        _validate_unary_operator(recipe)
    elif recipe.template == "binary_operator":
        _validate_binary_operator(recipe)
    elif recipe.template == "string_operator":
        _validate_string_operator(recipe)
    elif recipe.template == "stream_shape":
        _validate_stream_shape(recipe)
    elif recipe.template == "flow_control":
        _validate_flow_control(recipe)
    elif recipe.template == "set_operator":
        _validate_set_operator(recipe)
    elif recipe.template == "tsd_operator":
        _validate_tsd_operator(recipe)
    elif recipe.template == "tsl_operator":
        _validate_tsl_operator(recipe)
    elif recipe.template == "temporal_component":
        _validate_temporal_component(recipe)
    elif recipe.template == "table_round_trip":
        _validate_table_round_trip(recipe)
    elif recipe.template == "json_round_trip":
        _validate_json_round_trip(recipe)
    elif recipe.template == "data_frame_conversion":
        _validate_data_frame_conversion(recipe)
    elif recipe.template == "compound_scalar_field":
        _validate_compound_scalar_field(recipe)
    elif recipe.template == "sink_operator":
        _validate_sink_operator(recipe)
    elif recipe.template == "feedback_accumulate":
        initial = recipe.parameters.get("initial", 0)
        if not isinstance(initial, int) or isinstance(initial, bool):
            raise RecipeError("feedback_accumulate initial must be an integer")
    elif recipe.template == "tsd_map_reduce":
        for name, default in (("increment", 1), ("zero", 0)):
            value = recipe.parameters.get(name, default)
            if not isinstance(value, int) or isinstance(value, bool):
                raise RecipeError(f"tsd_map_reduce {name} must be an integer")
    elif recipe.template == "service_reference":
        _validate_bounded_int_parameter(
            recipe, "base", 40, minimum=-100, maximum=100
        )
        _validate_path_parameter(recipe, "desk")
    elif recipe.template == "service_request_reply":
        _validate_bounded_int_parameter(
            recipe, "increment", 3, minimum=-20, maximum=20
        )
        _validate_path_parameter(recipe, "requests")
    elif recipe.template == "service_subscription":
        _validate_bounded_int_parameter(
            recipe, "multiplier", 10, minimum=-20, maximum=20
        )
        _validate_path_parameter(recipe, "quotes")
        if not isinstance(recipe.parameters.get("dependency", False), bool):
            raise RecipeError("service_subscription dependency must be a boolean")
    elif recipe.template == "adaptor_loopback":
        _validate_bounded_int_parameter(
            recipe, "factor", 2, minimum=-20, maximum=20
        )
        _validate_path_parameter(recipe, "loopback")
    elif recipe.template == "service_adaptor_roundtrip":
        _validate_bounded_int_parameter(
            recipe, "increment", 1, minimum=-20, maximum=20
        )
    elif recipe.template == "service_adaptor_parameterized_clients":
        if recipe.parameters:
            raise RecipeError(
                "service_adaptor_parameterized_clients takes no parameters")
    elif recipe.template == "operator_pipeline":
        if not isinstance(
            recipe.parameters.get("format_ref", False), bool
        ):
            raise RecipeError("operator_pipeline format_ref must be a boolean")
    elif recipe.template == "value_consumer_reference":
        if recipe.parameters:
            raise RecipeError("value_consumer_reference takes no parameters")
    elif recipe.template == "tsd_key_set_pipeline":
        if not isinstance(
            recipe.parameters.get("dedup_size", True), bool
        ):
            raise RecipeError(
                "tsd_key_set_pipeline dedup_size must be a boolean"
            )
    elif recipe.template == "mesh_key_set":
        _validate_bounded_int_parameter(
            recipe, "factor", 2, minimum=-20, maximum=20
        )
    return spec


def execute_recipe(hg, recipe):
    spec = validate_recipe(recipe)
    return spec.execute(hg, recipe)


def catalogue_manifest():
    return {
        name: {
            "features": list(spec.features),
            "operators": list(spec.operators),
            "float_abs_tolerance": spec.float_abs_tolerance,
        }
        for name, spec in sorted(CATALOG.items())
    }


def catalogue_json():
    return json.dumps(catalogue_manifest(), sort_keys=True, indent=2)
