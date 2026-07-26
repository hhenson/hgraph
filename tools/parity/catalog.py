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
    if set(value) == {"$remove"} and value["$remove"] is True:
        return hg.REMOVE
    if set(value) == {"$remove_if_exists"} and value["$remove_if_exists"] is True:
        return hg.REMOVE_IF_EXISTS
    if set(value) == {"$set_delta"}:
        delta = value["$set_delta"]
        if not isinstance(delta, dict) or set(delta) != {"added", "removed"}:
            raise RecipeError("$set_delta requires added and removed lists")
        return hg.set_delta(
            added=[_decode_value(hg, item) for item in delta["added"]],
            removed=[_decode_value(hg, item) for item in delta["removed"]],
        )
    if set(value) == {"$frozendict"}:
        from frozendict import frozendict

        return frozendict(
            {
                key: _decode_value(hg, item)
                for key, item in value["$frozendict"].items()
            }
        )
    return {key: _decode_value(hg, item) for key, item in value.items()}


def decoded_inputs(hg, recipe):
    return {
        name: [_decode_value(hg, value) for value in ticks]
        for name, ticks in recipe.inputs.items()
    }


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


def _scalar_expression(hg, recipe):
    from hgraph.test import eval_node

    input_types = recipe.parameters["input_types"]
    output_type = _expression_type(recipe.parameters["expression"], input_types)
    arguments = ", ".join(
        f"{name}: hg.TS[{type_name}]" for name, type_name in input_types.items()
    )
    source = (
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


def _feedback_accumulate(hg, recipe):
    from hgraph.test import eval_node

    initial = recipe.parameters.get("initial", 0)

    @hg.graph
    def parity_graph(value: hg.TS[int]) -> hg.TS[int]:
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
        mapped = hg.map_(lambda value: value + increment, values)
        return hg.reduce(lambda lhs, rhs: lhs + rhs, mapped, zero)

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

    @dataclass(frozen=True)
    class Payload:
        value: int

    stream_fields = fields(hg.TSB[Stream[Payload]])
    return {
        name: (
            scalar_type(field_type).__module__
            + "."
            + scalar_type(field_type).__qualname__
        )
        for name, field_type in sorted(stream_fields.items())
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
    "feedback_accumulate": TemplateSpec(
        name="feedback_accumulate",
        required_inputs=("value",),
        features=("shape:TS", "topology:feedback", "lifecycle:multi-cycle"),
        operators=("add_", "feedback", "passive"),
        execute=_feedback_accumulate,
    ),
    "switch_arithmetic": TemplateSpec(
        name="switch_arithmetic",
        required_inputs=("selector", "lhs", "rhs"),
        features=("shape:TS", "topology:switch", "lifecycle:branch-rebind"),
        operators=("add_", "sub_", "switch_"),
        execute=_switch_arithmetic,
    ),
    "tsd_map_reduce": TemplateSpec(
        name="tsd_map_reduce",
        required_inputs=("values",),
        features=("shape:TSD", "topology:map", "lifecycle:keyed"),
        operators=("add_", "map_", "reduce"),
        execute=_tsd_map_reduce,
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
        features=("boundary:python-owned", "shape:TSB", "type:dataclass"),
        operators=(),
        execute=_stream_dataclass,
    ),
}


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
    elif recipe.template == "feedback_accumulate":
        initial = recipe.parameters.get("initial", 0)
        if not isinstance(initial, int) or isinstance(initial, bool):
            raise RecipeError("feedback_accumulate initial must be an integer")
    elif recipe.template == "tsd_map_reduce":
        for name, default in (("increment", 1), ("zero", 0)):
            value = recipe.parameters.get(name, default)
            if not isinstance(value, int) or isinstance(value, bool):
                raise RecipeError(f"tsd_map_reduce {name} must be an integer")
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
