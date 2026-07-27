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

    @hg.subscription_service
    def quote(path: str, symbol: hg.TS[str]) -> hg.TS[int]: ...

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
        hg.register_service(path, quote_values)
        symbol = _via_non_peered_ref(hg, symbol)
        return quote(path, symbol)

    inputs = decoded_inputs(hg, recipe)
    return eval_node(
        parity_graph,
        inputs["symbol"],
        __end_time__=hg.MIN_ST + (recipe.tick_count + 3) * hg.MIN_TD,
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
            "lifecycle:transport-delay",
            "shape:TSL",
            "reference:REF",
            "binding:non-peered",
        ),
        operators=("add_", "getitem_", "map_"),
        execute=_service_adaptor_roundtrip,
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
    elif recipe.template == "adaptor_loopback":
        _validate_bounded_int_parameter(
            recipe, "factor", 2, minimum=-20, maximum=20
        )
        _validate_path_parameter(recipe, "loopback")
    elif recipe.template == "service_adaptor_roundtrip":
        _validate_bounded_int_parameter(
            recipe, "increment", 1, minimum=-20, maximum=20
        )
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
