import pytest
from frozendict import frozendict as fd

from hgraph import TimeSeriesSchema, TS, TSB, TSD, WiringError, graph, TIME_SERIES_TYPE, combine, convert
from hgraph.test import eval_node


class ATimeSeriesSchema(TimeSeriesSchema):
    p1: TS[float]
    p2: TS[float]
    p3: TS[float]


def test_tsb_convert_to_bool():
    assert eval_node(convert, [dict(p1=1.0)], TS[bool], resolution_dict=dict(ts=TSB[ATimeSeriesSchema])) == [True]


def test_tsb_convert_to_tsd():
    assert eval_node(convert, [dict(p1=1.0)], TSD[str, TS[float]], resolution_dict=dict(ts=TSB[ATimeSeriesSchema])) == [
        fd(p1=1.0)
    ]


class AnotherTimeSeriesSchema(ATimeSeriesSchema):
    p4: TS[bool]


def test_tsb_convert_to_tsb_failure():
    with pytest.raises(WiringError):
        eval_node(convert, [dict(p1=1.0)], TSD[str, TS[float]], resolution_dict=dict(ts=TSB[AnotherTimeSeriesSchema]))


def test_tsb_convert_to_tsd_keys():

    @graph
    def convert_g(ts: TSB[AnotherTimeSeriesSchema]) -> TSD[str, TS[float]]:
        return convert(ts=ts, to=TSD[str, TS[float]], keys=("p1", "p2", "p3"))

    assert eval_node(convert_g, [dict(p1=1.0)]) == [fd(p1=1.0)]


def test_combine_unnamed_tsb():
    @graph
    def g(a: TS[int], b: TS[str]) -> TIME_SERIES_TYPE:
        return combine(a=a, b=b)

    assert eval_node(g, [None, 1], "a") == [dict(b="a"), dict(a=1)]


def test_combine_unnamed_tsb_strict():
    @graph
    def g(a: TS[int], b: TS[str]) -> TIME_SERIES_TYPE:
        return combine(a=a, b=b, __strict__=True)

    assert eval_node(g, [None, 1], ["a", None, "b"]) == [None, dict(a=1, b="a"), dict(b="b")]


def test_combine_named_tsb():
    class AB(TimeSeriesSchema):
        a: TS[int]
        b: TS[str]

    @graph
    def g(a: TS[int], b: TS[str]) -> TSB[AB]:
        return combine[TSB[AB]](a=a, b=b)

    assert eval_node(g, 1, "a") == [dict(a=1, b="a")]


def test_combine_named_tsb2():
    class AB(TimeSeriesSchema):
        a: TS[int]
        b: TS[str]

    class C(TimeSeriesSchema):
        c: TS[int]

    class ABC(AB, C): ...

    @graph
    def g(a: TS[int], b: TS[str]) -> TSB[ABC]:
        return combine[TSB[ABC]](a=a, b=b, c=1)

    assert eval_node(g, 1, "a") == [dict(a=1, b="a", c=1)]


def test_combine_named_tsb_partial():
    class AB(TimeSeriesSchema):
        a: TS[int]
        b: TS[str]

    @graph
    def g(a: TS[int]) -> TSB[AB]:
        return combine[TSB[AB]](a=a)

    assert eval_node(g, 1) == [dict(a=1)]
