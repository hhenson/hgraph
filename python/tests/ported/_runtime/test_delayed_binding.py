from dataclasses import dataclass

import pytest

from hgraph import (
    CompoundScalar,
    REMOVE,
    Size,
    TS,
    TSB,
    TSD,
    TSL,
    combine,
    const,
    delayed_binding,
    graph,
    map_,
    reduce,
)
from hgraph.nodes import pass_through_node
from hgraph.test import eval_node


def test_delayed_binding():
    @graph
    def g(v: TS[int]) -> TS[int]:
        value = delayed_binding(TS[int])
        out = pass_through_node(value())
        value(const(1))
        return out + v

    assert eval_node(g, [1]) == [2]


def test_delayed_tsl_binding():
    @graph
    def g(v: TSL[TS[int], Size[2]]) -> TS[int]:
        value = delayed_binding(TSL[TS[int], Size[2]])
        out = pass_through_node(value()[0] + value()[1])
        value(v)
        return out

    assert eval_node(g, [(1, 2)]) == [3]


def test_delayed_binding_accepts_a_fixed_structural_source():
    @graph
    def g(a: TS[int], b: TS[int]) -> TSL[TS[int], Size[2]]:
        value = delayed_binding(TSL[TS[int], Size[2]])
        out = pass_through_node(value())
        value(combine[TSL[TS[int], Size[2]]](a, b))
        return out

    assert eval_node(g, [1, 2], [10, 20]) == [
        {0: 1, 1: 10},
        {0: 2, 1: 20},
    ]

    @dataclass
    class Pair(CompoundScalar):
        first: int
        second: int

    @graph
    def bundle_g(a: TS[int], b: TS[int]) -> TSB[Pair]:
        value = delayed_binding(TSB[Pair])
        out = pass_through_node(value())
        value(combine[TSB[Pair]](first=a, second=b))
        return out

    assert eval_node(bundle_g, [1, 2], [10, 20]) == [
        {"first": 1, "second": 10},
        {"first": 2, "second": 20},
    ]


def test_delayed_tsd_binding_uses_port_output_type():
    @dataclass
    class AB(CompoundScalar):
        a: int
        b: int

    @graph
    def g(v: TSD[str, TSB[AB]]) -> TS[int]:
        value = delayed_binding(v.output_type)
        out = pass_through_node(map_(lambda x, y: x + y, value().a, value().b))
        result = reduce(lambda x, y: x + y, out, 0)
        value(v)
        return result

    assert eval_node(g, [{"a": {"a": 1, "b": 2}, "b": {"a": -1, "b": -2}}]) == [0]


def test_delayed_binding_accepts_a_port_as_its_type_source():
    @graph
    def g(v: TS[int]) -> TS[int]:
        value = delayed_binding(v)
        out = pass_through_node(value())
        value(v)
        return out

    assert eval_node(g, [1, 2]) == [1, 2]


def test_delayed_binding_resolves_inside_a_mapped_graph():
    @graph
    def late_identity(v: TS[int]) -> TS[int]:
        value = delayed_binding(TS[int])
        out = pass_through_node(value())
        value(v)
        return out

    @graph
    def g(v: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return map_(late_identity, v)

    assert eval_node(g, [{"a": 1, "b": 2}, {"a": 3, "b": REMOVE}]) == [
        {"a": 1, "b": 2},
        {"a": 3, "b": REMOVE},
    ]


def test_delayed_binding_rejects_unbound_mismatched_and_duplicate_bindings():
    @graph
    def unbound(v: TS[int]) -> TS[int]:
        value = delayed_binding(TS[int])
        return pass_through_node(value()) + v

    @graph
    def mismatched(v: TS[float]) -> TS[int]:
        value = delayed_binding(TS[int])
        out = pass_through_node(value())
        value(v)
        return out

    @graph
    def duplicate(a: TS[int], b: TS[int]) -> TS[int]:
        value = delayed_binding(TS[int])
        out = pass_through_node(value())
        value(a)
        value(b)
        return out

    with pytest.raises(RuntimeError, match="unbound delayed_binding"):
        eval_node(unbound, [1])
    with pytest.raises(ValueError, match="output type does not match"):
        eval_node(mismatched, [1.0])
    with pytest.raises(RuntimeError, match="already bound"):
        eval_node(duplicate, [1], [2])


@pytest.mark.parametrize("diamond", [False, True])
def test_delayed_binding_does_not_permit_cycles(diamond):
    @graph
    def g(v: TS[int]) -> TS[int]:
        value = delayed_binding(TS[int])
        out = pass_through_node(value())
        first = out + v
        bound = first + (out + 2 * v) if diamond else first
        value(bound)
        return out

    with pytest.raises(RuntimeError, match="cycle"):
        eval_node(g, [1])
