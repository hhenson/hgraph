"""Bounded graph-template catalogue used by both runtime environments.

This module intentionally does not import hgraph at module import time. The
subprocess runner supplies whichever implementation is installed in its
isolated environment.
"""

import json
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
    if set(value) == {"$set_delta"}:
        delta = value["$set_delta"]
        if not isinstance(delta, dict) or set(delta) != {"added", "removed"}:
            raise RecipeError("$set_delta requires added and removed lists")
        added = [_decode_value(hg, item) for item in delta["added"]]
        removed = [_decode_value(hg, item) for item in delta["removed"]]
        overlap = set(added) & set(removed)
        if overlap:
            # Ruling 2026-07-28: added/removed must be disjoint — an element
            # in both is incorrect data, not a recipe to explore.
            raise RecipeError(f"$set_delta added/removed overlap: {sorted(overlap)!r}")
        return hg.set_delta(added=added, removed=removed)
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


def _via_non_peered_ref(hg, value):
    """Project a structural TSL child, producing a REF-transparent source."""
    return hg.getitem_(hg.TSL.from_ts(value, value), 0)


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
            result = hg.len_(hg.TSL.from_ts(_via_non_peered_ref(hg, a), b))
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
            result = hg.len_(_via_non_peered_ref(hg, ts))
            return hg.dedup(result) if normalize_output else result
    elif operation == "is_empty":
        @hg.graph
        def parity_graph(ts: annotation) -> hg.TS[bool]:
            result = hg.is_empty(_via_non_peered_ref(hg, ts))
            return hg.dedup(result) if normalize_output else result
    else:
        @hg.graph
        def parity_graph(ts: annotation) -> hg.TS[bool]:
            result = hg.contains_(_via_non_peered_ref(hg, ts), probe)
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
            values = _via_non_peered_ref(hg, values)
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
            values = _via_non_peered_ref(hg, values)
            result = pipeline(values, selector)
            return hg.dedup(result) if normalize_output else result
    else:
        @hg.graph
        def parity_graph(values: hg.TSD[str, hg.TS[int]],
                         selector: hg.TS[str]) -> hg.TSD[str, hg.TS[int]]:
            register()
            values = _via_non_peered_ref(hg, values)
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
        value = _via_non_peered_ref(hg, value)
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
        selector = _via_non_peered_ref(hg, selector)
        lhs = _via_non_peered_ref(hg, lhs)
        rhs = _via_non_peered_ref(hg, rhs)
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
        values = _via_non_peered_ref(hg, values)
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
        value = _via_non_peered_ref(hg, value)
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
        value = _via_non_peered_ref(hg, value)
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
        symbol = _via_non_peered_ref(hg, symbol)
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
        value = _via_non_peered_ref(hg, value)
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
        value = _via_non_peered_ref(hg, value)
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
        selector = _via_non_peered_ref(hg, selector)
        value = _via_non_peered_ref(hg, value)
        offset = _via_non_peered_ref(hg, offset)
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
        lhs = _via_non_peered_ref(hg, lhs)
        rhs = _via_non_peered_ref(hg, rhs)
        choose_minimum = _via_non_peered_ref(hg, choose_minimum)
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
        values = _via_non_peered_ref(hg, values)
        probe = _via_non_peered_ref(hg, probe)
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
        values = _via_non_peered_ref(hg, values)
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
_STRUCTURAL_MAP_PROJECTIONS = ("lookup", "combine", "dispatch_combine")
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
    from typing import Mapping, Set

    from hgraph.test import eval_node

    Event, _, CreateEvent, _, upcast, decode = _polymorphic_event_model(hg)
    operation = recipe.parameters["operation"]
    events = [decode(value) for value in recipe.inputs["events"]]
    triggers = list(recipe.inputs["trigger"])
    keys = list(recipe.inputs["key"])
    end_time = hg.MIN_ST + (recipe.tick_count + 4) * hg.MIN_TD

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
    else:
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
            "shape:TSL",
            "topology:feedback",
            "lifecycle:multi-cycle",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=("add_", "feedback", "getitem_", "passive"),
        execute=_feedback_accumulate,
    ),
    "switch_arithmetic": TemplateSpec(
        name="switch_arithmetic",
        required_inputs=("selector", "lhs", "rhs"),
        features=(
            "shape:TS",
            "shape:TSL",
            "topology:switch",
            "lifecycle:branch-rebind",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=("add_", "getitem_", "sub_", "switch_"),
        execute=_switch_arithmetic,
    ),
    "tsd_map_reduce": TemplateSpec(
        name="tsd_map_reduce",
        required_inputs=("values",),
        features=(
            "shape:TSD",
            "shape:TSL",
            "topology:map",
            "lifecycle:keyed",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=("add_", "getitem_", "map_", "reduce"),
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
            "shape:TSL",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=("add_", "const", "getitem_", "passive"),
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
            "shape:TSL",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=("add_", "getitem_", "map_"),
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
            "shape:TSL",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=("getitem_", "len_", "map_", "mul_"),
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
            "shape:TSL",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=("getitem_", "mul_"),
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
            "shape:TSL",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=("add_", "getitem_", "map_"),
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
    "context_switch": TemplateSpec(
        name="context_switch",
        required_inputs=("selector", "value", "offset"),
        features=(
            "shape:TS",
            "framework:context",
            "topology:context",
            "topology:switch",
            "lifecycle:branch-rebind",
            "shape:TSL",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=("add_", "getitem_", "sub_", "switch_"),
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
            "shape:TSL",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=(
            "add_",
            "and_",
            "combine",
            "floordiv_",
            "format_",
            "getitem_",
            "gt_",
            "if_then_else",
            "len_",
            "max_",
            "min_",
            "mod_",
            "modified",
            "not_",
            "or_",
            "valid",
        ),
        execute=_operator_pipeline,
    ),
    "tsd_key_set_pipeline": TemplateSpec(
        name="tsd_key_set_pipeline",
        required_inputs=("values", "probe"),
        features=(
            "shape:TSS",
            "shape:TSD",
            "shape:TSB",
            "shape:TSL",
            "topology:operator-composition",
            "topology:key-set-projection",
            "lifecycle:keyed",
            "type:int",
            "type:bool",
            "type:float",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=(
            "combine",
            "contains_",
            "dedup",
            "getitem_",
            "is_empty",
            "keys_",
            "len_",
            "max_",
            "mean",
            "min_",
            "sum_",
        ),
        execute=_tsd_key_set_pipeline,
        float_abs_tolerance=1e-12,
    ),
    "mesh_key_set": TemplateSpec(
        name="mesh_key_set",
        required_inputs=("values",),
        features=(
            "shape:TSD",
            "shape:TSS",
            "shape:TSL",
            "topology:mesh",
            "topology:key-set-projection",
            "lifecycle:keyed",
            "lifecycle:nested-graph",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=("getitem_", "keys_", "mesh_", "mul_"),
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
            "combine", "dispatch_", "getitem_", "map_", "pass_through",
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
            "binding:non-peered",
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
    "nested_higher_order": TemplateSpec(
        name="nested_higher_order",
        required_inputs=None,
        features=(
            "topology:nested-higher-order",
            "shape:TSD",
            "type:int",
            "reference:REF",
            "binding:non-peered",
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
    elif recipe.template == "nested_higher_order":
        _validate_nested_higher_order(recipe)
    elif recipe.template == "data_frame_recording":
        _validate_data_frame_recording(recipe)
    elif recipe.template == "compound_scalar_downcast":
        _validate_compound_scalar_downcast(recipe)
    elif recipe.template == "enum_literal_selection":
        _validate_enum_literal_selection(recipe)
    elif recipe.template == "legacy_compound_scalar_json":
        _validate_legacy_compound_scalar_json(recipe)
    elif recipe.template == "polymorphic_event_flow":
        _validate_polymorphic_event_flow(recipe)
    elif recipe.template == "polymorphic_event_map":
        _validate_polymorphic_event_map(recipe)
    elif recipe.template == "structural_map_projection":
        _validate_structural_map_projection(recipe)
    elif recipe.template == "arrow_typed_projection":
        _validate_arrow_typed_projection(recipe)
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
