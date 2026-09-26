# Ported from release/0.5:hgraph_unit_tests/_wiring/test_var_args.py
import pytest

from inspect import signature

from hgraph import (
    compute_node,
    TS,
    TIME_SERIES_TYPE,
    TSL,
    SIZE,
    graph,
    TSD,
    Size,
    with_signature,
    TSB,
    TS_SCHEMA,
    TimeSeriesSchema,
    const,
    operator,
)
from hgraph.test import eval_node


def test_var_args0():
    @compute_node
    def n(a: TS[int], *b: TSL[TIME_SERIES_TYPE, SIZE]) -> TS[int]:
        return a.value + sum(b_.value for b_ in b)

    @graph
    def g(a: TS[int], b: TS[int], c: TS[int], d: TS[int]) -> TS[int]:
        return n(a, b, c, d)

    assert eval_node(g, 1, 2, 3, 4) == [10]


def test_var_args1():
    @compute_node
    def n(a: TS[int], **bundle: TSB[TS_SCHEMA]) -> TS[int]:
        return a.value + sum(b.value for b in bundle.values())

    @graph
    def g(a: TS[int], b: TS[int], c: TS[int], d: TS[int]) -> TS[int]:
        return n(a, b=b, c=c, d=d)

    assert eval_node(g, 1, 2, 3, 4) == [10]


def test_graph_var_kwargs_are_packed_as_declared_tsb():
    @graph
    def n(a: TS[int], **bundle: TSB[TS_SCHEMA]) -> TS[int]:
        fields = bundle.as_dict()
        return a + fields["b"] + fields["c"]

    @graph
    def g(a: TS[int], b: TS[int], c: TS[int]) -> TS[int]:
        return n(a, b=b, c=c)

    assert eval_node(g, 1, 2, 3) == [6]


def test_graph_var_kwargs_are_packed_as_concrete_tsb():
    class Pair(TimeSeriesSchema):
        left: TS[int]
        right: TS[int]

    @graph
    def select_right(**bundle: TSB[Pair]) -> TS[int]:
        return bundle.right

    assert eval_node(select_right, left=1, right=2) == [2]


def test_empty_graph_var_kwargs_are_packed_as_tsb():
    @graph
    def field_count(**bundle: TSB[TS_SCHEMA]) -> TS[int]:
        return const(len(bundle.as_dict()))

    assert eval_node(field_count) == [0]


def test_graph_var_args_are_packed_as_declared_tsl():
    @graph
    def pack(*values: TSL[TS[int], SIZE]) -> TSL[TS[int], SIZE]:
        return values

    @graph
    def g(a: TS[int], b: TS[int]) -> TS[int]:
        return pack(a, b)[1]

    assert eval_node(g, [1, 2], [3, 4]) == [3, 4]


def test_graph_var_args_are_packed_as_declared_tsb():
    @graph
    def select_second(*bundle: TSB[TS_SCHEMA]) -> TS[int]:
        return bundle[1]

    assert eval_node(select_second, 1, 2) == [2]


def test_var_args2():
    @compute_node
    def n(a: TS[int], *b: TSL[TIME_SERIES_TYPE, SIZE], c: TS[int], **dundle: TSB[TS_SCHEMA]) -> TS[int]:
        return a.value + sum(b_.value for b_ in b) + c.value + sum(d.value for d in dundle.values())

    @graph
    def g(a: TS[int], b: TS[int], c: TS[int], d: TS[int], e: TS[int]) -> TS[int]:
        return n(a, b, c=c, d=d, e=e)

    assert eval_node(g, 1, 2, 3, 4, 5) == [15]


def test_var_args3():
    @compute_node
    def n(a: TS[int], *b: TSL[TIME_SERIES_TYPE, SIZE], c: TS[int], **dundle: TSB[TS_SCHEMA]) -> TS[int]:
        return a.value + sum(b_.value for b_ in b) + c.value + sum(d.value for d in dundle.values())

    @graph
    def g(a: TS[int], b: TS[int], c: TS[int], d: TS[int], e: TS[int]) -> TS[int]:
        return n(a, 2, c=c, d=d, e=5)

    assert eval_node(g, 1, 2, 3, 4, 5) == [15]


def test_var_args4():
    @operator
    def n(**args: TSL[TIME_SERIES_TYPE, SIZE]) -> TS[int]:
        pass

    @compute_node(overloads=n)
    def n_1(a: TIME_SERIES_TYPE) -> TS[int]:
        return -a.value

    @compute_node(overloads=n)
    def n_n(a: TS[int], *b: TSL[TIME_SERIES_TYPE, SIZE]) -> TS[int]:
        return a.value + sum(b_.value for b_ in b)

    @graph
    def g(a: TS[int], b: TS[int], c: TS[int]) -> TSL[TS[int], Size[2]]:
        return TSL.from_ts(n(a, b, c), n(a))

    assert eval_node(g, 1, 2, 3) == [{0: 6, 1: -1}]


def test_var_args_tsb():
    @compute_node
    def n(a: TS[int], *bundle: TSB[TS_SCHEMA]) -> TS[int]:
        return a.value + sum(int(b.value) for b in bundle.values())

    @graph
    def g(a: TS[int], b: TS[int], c: TS[float], d: TS[str]) -> TS[int]:
        return n(a, b, c, d)

    assert eval_node(g, 1, 2, 3.0, "4") == [10]


def test_var_kwarg_tsd():
    @compute_node
    def n(a: TS[int], **bundle: TSD[str, TS[int]]) -> TS[int]:
        return a.value + sum(b.value for b in bundle.values())

    @graph
    def g(a: TS[int], b: TS[int], c: TS[int], d: TS[int]) -> TS[int]:
        return n(a, b=b, c=c, d=d)

    assert eval_node(g, 1, 2, 3, 4) == [10]
