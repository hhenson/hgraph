import inspect
from dataclasses import dataclass
from typing import TypeVar

import hgraph as hg
import pytest
import hgraph._wiring._core as wiring_core
import hgraph._wiring._graph as wiring_graph
import hgraph._wiring._node as wiring_node
import hgraph._wiring._resolution as wiring_resolution
import hgraph._types as hgraph_types


def test_fast_partial_binding_matches_inspect_for_ordinary_signatures():
    def fn(first=1, second=2, *, option=3):
        pass

    signature = inspect.signature(fn)
    plan = wiring_node._partial_binding_plan(signature)

    for args, kwargs in (
        ((), {}),
        ((10,), {}),
        ((10, 20), {"option": 30}),
        ((), {"second": 20}),
    ):
        expected = signature.bind_partial(*args, **kwargs)
        actual = wiring_node._bind_partial(signature, plan, args, kwargs)
        assert actual.arguments == expected.arguments
        assert actual.args == expected.args
        assert actual.kwargs == expected.kwargs


def test_fast_partial_binding_preserves_inspect_errors():
    def fn(first, *, option=None):
        pass

    signature = inspect.signature(fn)
    plan = wiring_node._partial_binding_plan(signature)

    with pytest.raises(TypeError, match="multiple values for argument 'first'"):
        wiring_node._bind_partial(signature, plan, (1,), {"first": 2})
    with pytest.raises(TypeError, match="unexpected keyword argument 'unknown'"):
        wiring_node._bind_partial(signature, plan, (), {"unknown": 2})
    with pytest.raises(TypeError, match="too many positional arguments"):
        wiring_node._bind_partial(signature, plan, (1, 2), {})


def test_variadic_partial_binding_uses_inspect():
    def fn(first, *args, **kwargs):
        pass

    signature = inspect.signature(fn)
    assert wiring_node._partial_binding_plan(signature) is None
    actual = wiring_node._bind_partial(
        signature, None, (1, 2), {"option": 3})
    assert actual.arguments == {
        "first": 1, "args": (2,), "kwargs": {"option": 3}}


def test_fast_complete_binding_materializes_defaults_and_preserves_errors():
    def fn(first, second=2, *, option=3):
        pass

    signature = inspect.signature(fn)
    plan = wiring_node._partial_binding_plan(signature)
    actual = wiring_node._bind_with_defaults(
        signature, plan, (1,), {"option": 30})

    assert actual.arguments == {"first": 1, "second": 2, "option": 30}
    assert actual.args == (1, 2)
    assert actual.kwargs == {"option": 30}

    with pytest.raises(TypeError, match="missing a required argument: 'first'"):
        wiring_node._bind_with_defaults(signature, plan, (), {})


def test_decorated_callable_wrapping_reuses_evaluated_signature(monkeypatch):
    @hg.graph
    def increment(value: hg.TS[int]) -> hg.TS[int]:
        return value + 1

    def unexpected_signature(*args, **kwargs):
        raise AssertionError("decorated callable was introspected while wrapping")

    monkeypatch.setattr(wiring_graph.inspect, "signature", unexpected_signature)

    wired = wiring_graph._as_wired(increment)

    assert wired is not None
    assert wiring_graph._as_wired(increment) is wired


def test_graph_wrapper_accepts_an_already_evaluated_signature(monkeypatch):
    def increment(value: hg.TS[int]) -> hg.TS[int]:
        return value + 1

    signature = inspect.signature(increment, eval_str=True)

    def unexpected_signature(*args, **kwargs):
        raise AssertionError("callable signature was evaluated twice")

    monkeypatch.setattr(wiring_graph.inspect, "signature", unexpected_signature)

    assert wiring_graph._wrap_graph_fn(
        increment, signature=signature) is not None


def test_map_reuses_inferred_output_mode_between_overload_candidates():
    calls = 0

    def wire_increment(value):
        nonlocal calls
        calls += 1
        return value + 1

    increment = lambda value: wire_increment(value)

    @hg.graph
    def mapped(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return hg.map_(increment, values)

    assert hg.eval_node(
        mapped,
        [{"one": 1}],
    ) == [{"one": 2}]
    assert calls == 1  # the selected map consumes its retained output probe


def test_switch_reuses_inferred_output_between_overload_candidates():
    calls = 0

    def wire_increment(value):
        nonlocal calls
        calls += 1
        return value + 1

    increment = lambda value: wire_increment(value)

    @hg.graph
    def selected(key: hg.TS[str], value: hg.TS[int]) -> hg.TS[int]:
        return hg.switch_(key, {"increment": increment}, value)

    assert hg.eval_node(selected, ["increment"], [1]) == [2]
    assert calls == 1  # the selected switch consumes its retained output probe


def test_resolution_callable_signature_is_cached(monkeypatch):
    def resolver(mapping, scalar):
        return int

    assert wiring_resolution._invoke_resolution_callable(
        resolver, {}, {"scalar": 1}
    ) is int

    def unexpected_signature(*args, **kwargs):
        raise AssertionError("resolver was introspected more than once")

    monkeypatch.setattr(inspect, "signature", unexpected_signature)

    assert wiring_resolution._invoke_resolution_callable(
        resolver, {}, {"scalar": 2}
    ) is int


def test_python_node_reference_is_cached(monkeypatch):
    def evaluate(value):
        return value

    first = wiring_node._node_ref(evaluate)

    def unexpected_node_ref(*args, **kwargs):
        raise AssertionError("Python node identity was registered more than once")

    monkeypatch.setattr(wiring_node._hgraph, "node_ref", unexpected_node_ref)

    assert wiring_node._node_ref(evaluate) is first


def test_tsb_attribute_access_uses_structural_projection(monkeypatch):
    class Pair(hg.TimeSeriesSchema):
        left: hg.TS[int]
        right: hg.TS[str]

    @hg.graph
    def select_left(pair: hg.TSB[Pair]) -> hg.TS[int]:
        return pair.left

    original_wire = wiring_core.wire

    def reject_getattr_dispatch(name, *args, **kwargs):
        if name == "getattr_":
            raise AssertionError("direct TSB field access used operator dispatch")
        return original_wire(name, *args, **kwargs)

    monkeypatch.setattr(wiring_core, "wire", reject_getattr_dispatch)

    assert hg.eval_node(select_left, [{"left": 1, "right": "a"}]) == [1]


def test_selected_python_overload_does_not_repeat_resolution_predicate():
    calls = 0

    @hg.operator
    def increment(value: hg.TS[int]) -> hg.TS[int]: ...

    def eligible(mapping):
        nonlocal calls
        calls += 1
        return True

    @hg.compute_node(overloads=increment, requires=eligible)
    def increment_impl(value: hg.TS[int]) -> hg.TS[int]:
        return value.value + 1

    assert hg.eval_node(increment, [1]) == [2]
    assert calls == 1


def test_ts_expression_is_cached_for_a_scalar_annotation(monkeypatch):
    class LocalValue:
        pass

    expression = hg.TS[LocalValue]

    def unexpected_value_type(*args, **kwargs):
        raise AssertionError("cached TS annotation rebuilt its value type")

    monkeypatch.setattr(hgraph_types, "_value_type", unexpected_value_type)

    assert hg.TS[LocalValue] is expression


def test_cached_ts_expression_restores_its_python_scalar_binding():
    from hgraph.reflection import scalar_type

    bounded = TypeVar("bounded", bound=int)
    expression = hg.TS[int]
    hg.TS[bounded]

    assert hg.TS[int] is expression
    assert scalar_type(expression) is int


def test_python_type_recursion_scan_stops_at_realized_types(monkeypatch):
    @dataclass(frozen=True)
    class Leaf:
        value: int

    hg.TS[Leaf]
    original_fields = hgraph_types._python_object_python_field_types

    def fields(scalar):
        if scalar is Leaf:
            raise AssertionError("recursion scan revisited a realized type")
        return original_fields(scalar)

    monkeypatch.setattr(hgraph_types, "_python_object_python_field_types", fields)

    @dataclass(frozen=True)
    class Root:
        leaf: Leaf

    assert hg.TS[Root].handle is not None


def test_plain_graph_skips_type_resolution_scope(monkeypatch):
    @hg.graph
    def increment(value: hg.TS[int]) -> hg.TS[int]:
        return value + 1

    def unexpected_resolution(*args, **kwargs):
        raise AssertionError("plain graph entered auto-resolution")

    monkeypatch.setattr(wiring_graph, "_graph_auto_resolve", unexpected_resolution)

    assert hg.eval_node(increment, [1]) == [2]
