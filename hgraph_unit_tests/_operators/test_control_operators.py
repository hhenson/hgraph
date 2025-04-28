from dataclasses import dataclass

import pytest

from hgraph import (
    graph,
    TS,
    all_,
    any_,
    TSB,
    TimeSeriesSchema,
    Size,
    TSL,
    SIZE,
    merge,
    REF,
    const,
    BoolResult,
    if_,
    route_by_index,
    race,
    if_true,
    if_then_else,
    nothing,
    compute_node,
    TSD,
    map_,
    REMOVE,
    combine,
    switch_,
    filter_,
    CmpResult,
    if_cmp,
    reduce_tsd_with_race,
    reduce_tsd_of_bundles_with_race,
    TimeSeriesReference, CompoundScalar,
)
from hgraph.test import eval_node


def test_all_false():
    @graph
    def app() -> TS[bool]:
        return all_(const(True), const(False), const(True))

    assert eval_node(app) == [False]


def test_all_true():
    @graph
    def app() -> TS[bool]:
        return all_(const(True), const(True), const(True))

    assert eval_node(app) == [True]


def test_all_invalid():
    @graph
    def app() -> TS[bool]:
        invalid = if_(True, True).false
        return all_(const(True), const(True), const(True), invalid)

    assert eval_node(app) == [False]


def test_any_false():
    @graph
    def app() -> TS[bool]:
        return any_(const(False), const(False), const(False))

    assert eval_node(app) == [False]


def test_any_true():
    @graph
    def app() -> TS[bool]:
        return any_(const(True), const(False), const(True))

    assert eval_node(app) == [True]


def test_any_invalid():
    @graph
    def app() -> TS[bool]:
        invalid = if_(True, True).false
        return any_(const(True), invalid, const(True), const(True))

    assert eval_node(app) == [True]


def test_if_then_else():
    expected = [None, 2, 6, 3]

    assert eval_node(if_then_else, [None, True, False, True], [1, 2, 3], [4, 5, 6]) == expected


def test_if_cmp():
    lt = [1, 2, 3, 4, 5]
    eq = [10, 20, 30, 40, 50]
    gt = [100, 200, 300, 400, None]
    cmp = [None, CmpResult.LT, CmpResult.EQ, CmpResult.GT, CmpResult.GT]
    exp = [None, 2, 30, 400, None]
    assert eval_node(if_cmp, cmp, lt, eq, gt) == exp


@pytest.mark.parametrize(
    "condition,tick_once_only,expected",
    [
        ([True, False, True], False, [True, None, True]),
        ([True, False, True], True, [True, None, None]),
    ],
)
def test_if_true(condition, tick_once_only, expected):
    assert eval_node(if_true, condition, tick_once_only) == expected


def test_if_():
    @graph
    def g(condition: TS[bool], ts: TS[str]) -> TSB[BoolResult[TS[str]]]:
        return if_(condition, ts)

    from frozendict import frozendict as fd

    assert eval_node(g, [True, False, True], ["a", "b", "c"]) == [
        fd({"true": "a"}),
        fd({"false": "b"}),
        fd({"true": "c"}),
    ]


def test_route_by_index():
    @graph
    def g(index: TS[int], ts: TS[str]) -> TSL[TS[str], Size[4]]:
        return route_by_index[SIZE : Size[4]](index, ts)

    assert eval_node(g, [1, 2, 0, 4], ["1", "2", "2", "2"]) == [{1: "1"}, {2: "2"}, {0: "2"}, None]


def test_merge():
    assert eval_node(
        merge,
        [None, 2, None, None, 6],
        [1, None, 4, None, None],
        [None, 3, 5, None, None],
        resolution_dict={"tsl": TSL[TS[int], Size[3]]},
    ) == [1, 2, 4, None, 6]


def test_merge_compound_scalars():
    @dataclass
    class SimpleCS(CompoundScalar):
        p1: str
        p2: str

    @dataclass
    class LessSimpleCS(CompoundScalar):
        p3: SimpleCS
        p4: int

    @graph
    def g(orig: TS[LessSimpleCS], delta: TS[LessSimpleCS]) -> TS[LessSimpleCS]:
        return merge(orig, delta)

    initial = LessSimpleCS(p3=SimpleCS(p1="a", p2="b"), p4=1)
    second = LessSimpleCS(p3=SimpleCS(p1="a", p2="c"), p4=1)

    assert eval_node(
        g,
        [initial],
        [None, LessSimpleCS(p3=SimpleCS(p1=None, p2="c"), p4=None)],
       # __trace_wiring__=True,
    ) == [initial, second]


def test_race_scalars():
    @graph
    def app(tsl: TSL[TS[int], Size[3]]) -> REF[TS[int]]:
        return race(*tsl)

    assert eval_node(app, [{1: 100}, {0: 200, 2: 300}, {1: 300, 2: 500}, {0: 600}]) == [100, None, 300, None]


def test_race_scalars_invalid():
    @graph
    def app(ts: TS[int]) -> REF[TS[int]]:
        return race(ts, nothing[TS[int]]())

    assert eval_node(app, [None, 1, None]) == [None, 1, None]


def test_race_tsls():
    @graph
    def app(invalidate: TS[bool]) -> TS[int]:
        v11 = if_(invalidate, const(11)).false
        v12 = if_(invalidate, const(12)).false
        tsl1 = TSL.from_ts(v11, v12)
        v21 = const(21)
        v22 = const(22)
        tsl2 = TSL.from_ts(v21, v22)
        return race(tsl1, tsl2)[0]

    assert eval_node(app, [False, True]) == [11, 21]


def test_race_tsbs():
    class S(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def app(invalidate: TS[bool]) -> TSB[S]:
        v11 = if_(invalidate, const(11)).false
        v12 = if_(invalidate, const(12)).false
        tsb1 = TSB[S].from_ts(a=v11, b=v12)
        tsb2 = TSB[S].from_ts(a=21, b=22)
        return race(tsb1, tsb2)

    assert eval_node(app, [False, True]) == [{"a": 11, "b": 12}, {"a": 21, "b": 22}]


def test_race_tsd():
    @compute_node
    def make_ref(ts: TS[int], ref: REF[TS[int]]) -> REF[TS[int]]:
        return ref.value if ts.value != 0 else TimeSeriesReference.make()

    @graph
    def g(tsd: TSD[int, TS[int]]) -> REF[TS[int]]:
        refs = map_(make_ref, tsd, tsd)
        return reduce_tsd_with_race(tsd=refs)

    assert eval_node(g, [None, {1: 0, 2: 0}, {1: 1}, {2: 2, 3: 3}, {1: REMOVE}, {2: 0}]) == [None, None, 1, None, 2, 3]


def test_race_tsd_of_bundles_all_free_bundles():
    class S(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @compute_node
    def make_ref(ts: TS[int], ref: REF[TS[int]]) -> REF[TS[int]]:
        return ref.value if ts.value != 0 else TimeSeriesReference.make()

    @graph
    def g(a: TSD[int, TS[int]], b: TSD[int, TS[int]]) -> REF[TSB[S]]:
        refs = map_(lambda a, b: combine[TSB[S]](a=make_ref(a, a), b=make_ref(b, b)), a, b)
        return reduce_tsd_of_bundles_with_race(tsd=refs)

    assert eval_node(
        g,
        a=[None, {1: 0, 2: 0}, {2: 2}, {2: 2, 3: 3}, {1: REMOVE}, {2: 0}],
        b=[None, {1: 0, 2: 0}, {1: 1}, {2: 2, 3: 3}, {1: REMOVE}, {2: 0}],
    ) == [None, None, {"a": 2, "b": 1}, {"a": 2}, {"b": 2}, {"a": 3, "b": 3}]


def test_race_tsd_of_bundles_switch_bundle_types():
    class S(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    class SC(S):
        free: TS[bool]
        cond: TS[bool]

    @compute_node
    def make_ref(ts: TS[int], ref: REF[TS[int]]) -> REF[TS[int]]:
        return ref.value if ts.value != 0 else TimeSeriesReference.make()

    @graph
    def make_bundle(ts: TSB[SC]) -> TSB[S]:
        return switch_(
            ts.free,
            {
                False: lambda a, b, cond: filter_(cond, combine[TSB[S]](a=a, b=b)),  # normal bundle
                True: lambda a, b, cond: combine[TSB[S]](a=make_ref(a, a), b=make_ref(b, b)),  # free bundle
            },
            reload_on_ticked=True,
            a=ts.a,
            b=ts.b,
            cond=ts.cond,
        )

    @graph
    def g(ts: TSD[int, TSB[SC]]) -> REF[TSB[S]]:
        refs = map_(make_bundle, ts)
        return reduce_tsd_of_bundles_with_race(tsd=refs)

    assert eval_node(
        g,
        __trace__=True,
        ts=[
            {1: {"free": False}, 2: {"free": True}},
            {1: {"a": 0, "cond": False}},
            {1: {"a": 0, "cond": True}},
            {2: {"a": 2, "b": 1}},
            {1: {"a": 1, "b": 2}},
            {1: {"free": False, "cond": False}},  # reset the switch
            {1: {"a": 3, "b": 3, "cond": True}},  # rebuild the bundle
            {2: REMOVE},
            {2: {"a": 0, "b": 0}},
        ],
    ) == [
        None,
        None,
        {"a": 0},
        {"b": 1},
        {"a": 1},
        {"a": 2},
        None,
        {"a": 3, "b": 3},
        None,
    ]
