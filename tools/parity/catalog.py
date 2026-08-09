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
# Data-frame recording surface (issue #92 class): the frames the recorder
# frameworks hand back to user code — column timezone presentation included.


def _validate_data_frame_recording(recipe):
    if set(recipe.inputs) != {"ts"}:
        raise RecipeError("data_frame_recording requires the ts input")
    as_of_offset = recipe.parameters.get("as_of_offset", 30)
    if (not isinstance(as_of_offset, int) or isinstance(as_of_offset, bool)
            or not 1 <= as_of_offset <= 10_000):
        raise RecipeError("data_frame_recording as_of_offset must be a bounded integer")


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
    inputs = decoded_inputs(hg, recipe)
    with hg.GlobalState(), MemoryDataFrameStorage() as storage:
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
        deviation: hg.TS[float]
        variance: hg.TS[float]

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
            deviation=hg.std(keys),
            variance=hg.var(keys),
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
            "std",
            "sum_",
            "var",
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
