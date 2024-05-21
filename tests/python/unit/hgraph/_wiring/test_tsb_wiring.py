from dataclasses import dataclass
from typing import Generic

import pytest
from frozendict import frozendict

from hgraph import TSB, TimeSeriesSchema, TS, compute_node, graph, IncorrectTypeBinding, ParseError, TIME_SERIES_TYPE, \
    SCALAR, SCALAR_1, AUTO_RESOLVE, CompoundScalar

from hgraph.test import eval_node


class MyTsb(TimeSeriesSchema):
    p1: TS[int]
    p2: TS[str]


@compute_node(valid=[])
def create_my_tsb(ts1: TS[int], ts2: TS[str]) -> TSB[MyTsb]:
    out = {}
    if ts1.modified:
        out['p1'] = ts1.value
    if ts2.modified:
        out['p2'] = ts2.value
    return out


@compute_node
def split_my_tsb(tsb: TSB[MyTsb]) -> TS[int]:
    return tsb.as_schema.p1.delta_value


@pytest.mark.parametrize("ts1,ts2,expected", [
    [[1, 2], ["a", "b"], [dict(p1=1, p2="a"), dict(p1=2, p2="b")]],
    [[None, 2], ["a", None], [dict(p2="a"), dict(p1=2)]],
    [[1, None], [None, "b"], [dict(p1=1), dict(p2="b")]],
])
def test_tsb_output(ts1, ts2, expected):

    assert eval_node(create_my_tsb, ts1, ts2) == expected


def test_tsb_splitting():

    @graph
    def split_tester(ts1: TS[int], ts2: TS[str]) -> TS[int]:
        tsb: TSB[MyTsb] = TSB[MyTsb].from_ts(p1=ts1, p2=ts2)
        return tsb.as_schema.p1

    assert eval_node(split_tester, [1, 2], ["a", "b"]) == [1, 2]


def test_tsb_from_ts_with_nothing_defaults():

    @graph
    def nothing_tester(ts1: TS[int]) -> TSB[MyTsb]:
        return TSB[MyTsb].from_ts(p1=ts1)

    assert eval_node(nothing_tester, [1, 2]) == [frozendict(p1=1), frozendict(p1=2)]


def test_tsb_splitting_peered():

    @graph
    def split_tester(ts1: TS[int], ts2: TS[str]) -> TS[int]:
        tsb: TSB[MyTsb] = create_my_tsb(ts1, ts2)
        return tsb.as_schema.p1

    assert eval_node(split_tester, [1, 2], ["a", "b"]) == [1, 2]


def test_tsb_input_not_peered():

    @graph
    def tsb_in_non_peered(ts1: TS[int], ts2: TS[str]) -> TS[int]:
        tsb: TSB[MyTsb] = TSB[MyTsb].from_ts(p1=ts1, p2=ts2)
        return split_my_tsb(tsb)

    assert eval_node(tsb_in_non_peered, [1, 2], ["a", "b"]) == [1, 2]


def test_tsb_input_peered():
    @graph
    def tsb_in_non_peered(ts1: TS[int], ts2: TS[str]) -> TS[int]:
        tsb: TSB[MyTsb] = create_my_tsb(ts1, ts2)
        return split_my_tsb(tsb)

    assert eval_node(tsb_in_non_peered, [1, 2], ["a", "b"]) == [1, 2]


def test_ts_schema_error():

    try:
        @graph
        def tsb_bad_return_type() -> MyTsb:
            ...
        assert False, "Should have raised an exception"
    except ParseError:
        ...


def test_tsb_from_scalar():
    @dataclass
    class MyScalar(CompoundScalar):
        p1: int
        p2: str

    @graph
    def g(ts: TSB[MyScalar]) -> TS[int]:
        return ts.p1

    assert eval_node(g, MyScalar(1, "a")) == [1]

    
def test_generic_tsb():
    from frozendict import frozendict as fd

    class GenericTSB(TimeSeriesSchema, Generic[SCALAR]):
        p1: TS[SCALAR]

    @graph
    def tsb_multi_type(ts: TSB[GenericTSB[SCALAR]], v: TS[SCALAR_1],
                       _v_tp: type[SCALAR_1] = AUTO_RESOLVE) -> TSB[GenericTSB[SCALAR_1]]:
        return TSB[GenericTSB[_v_tp]].from_ts(p1=v)

    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TSB[GenericTSB[str]]:
        return tsb_multi_type(TSB[GenericTSB[int]].from_ts(p1=ts1), ts2)

    assert eval_node(g, [1, 2], ["a", "b"]) == [fd({"p1": "a"}), fd({"p1": "b"})]

