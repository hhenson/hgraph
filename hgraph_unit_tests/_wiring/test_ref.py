from typing import cast

from hgraph import (
    TIME_SERIES_TYPE,
    compute_node,
    REF,
    TS,
    TSL,
    Size,
    SIZE,
    graph,
    TSS,
    TSD,
    REMOVE,
    Removed,
    K,
    KEYABLE_SCALAR,
    if_,
    TSB,
)
from hgraph._impl._operators._tss_operators import contains_tss
from hgraph.test import eval_node

import pytest
pytestmark = pytest.mark.smoke

@compute_node
def create_ref(ts: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]:
    return ts.value


def test_ref():
    assert eval_node(create_ref[TIME_SERIES_TYPE : TS[int]], ts=[1, 2]) == [1, 2]


def test_route_ref():
    assert eval_node(if_[TIME_SERIES_TYPE : TS[int]], condition=[True, None, False, None], ts=[1, 2, None, 4]) == [
        {"true": 1},
        {"true": 2},
        {"false": 2},
        {"false": 4},
    ]


@compute_node
def merge_ref(index: TS[int], ts: TSL[REF[TIME_SERIES_TYPE], SIZE]) -> REF[TIME_SERIES_TYPE]:
    return cast(REF, ts[index.value].value)


def test_merge_ref():
    assert eval_node(
        merge_ref[TIME_SERIES_TYPE : TS[int], SIZE : Size[2]],
        index=[0, None, 1, None],
        ts=[(1, -1), (2, -2), None, (4, -4)],
    ) == [1, 2, -2, -4]


@graph
def merge_ref_non_peer(index: TS[int], ts1: TIME_SERIES_TYPE, ts2: TIME_SERIES_TYPE) -> REF[TIME_SERIES_TYPE]:
    return merge_ref(index, TSL.from_ts(ts1, ts2))


def test_merge_ref_non_peer():
    assert eval_node(
        merge_ref_non_peer[TIME_SERIES_TYPE : TS[int]],
        index=[0, None, 1, None],
        ts1=[1, 2, None, 4],
        ts2=[-1, -2, None, -4],
    ) == [1, 2, -2, -4]


def test_merge_ref_non_peer_complex_inner_ts():
    assert eval_node(
        merge_ref_non_peer[TIME_SERIES_TYPE : TSL[TS[int], Size[2]]],
        index=[0, None, 1, None],
        ts1=[(1, 1), (2, None), None, (None, 4)],
        ts2=[(-1, -1), (-2, -2), None, (-4, None)],
    ) == [{0: 1, 1: 1}, {0: 2}, {0: -2, 1: -2}, {0: -4}]


@graph
def merge_ref_non_peer_inner(
    index: TS[int], ts1: TIME_SERIES_TYPE, ts2: TIME_SERIES_TYPE, ts3: TIME_SERIES_TYPE, ts4: TIME_SERIES_TYPE
) -> REF[TSL[TIME_SERIES_TYPE, Size[2]]]:
    return merge_ref(index, TSL.from_ts(TSL.from_ts(ts1, ts2), TSL.from_ts(ts3, ts4)))


def test_merge_ref_inner_non_peer_ts():
    assert eval_node(
        merge_ref_non_peer_inner[TIME_SERIES_TYPE : TS[int]],
        index=[0, None, 1, None],
        ts1=[1, 2, None, None],
        ts2=[1, None, None, 4],
        ts3=[-1, -2, None, -4],
        ts4=[-1, -2, None, None],
    ) == [{0: 1, 1: 1}, {0: 2}, {0: -2, 1: -2}, {0: -4}]


def test_merge_ref_set():
    assert eval_node(
        merge_ref_non_peer[TIME_SERIES_TYPE : TSS[int]],
        index=[0, None, 1, None],
        ts1=[{1, 2}, None, None, {4}],
        ts2=[{-1}, {-2}, {-3, Removed(-1)}, {-4}],
    ) == [{1, 2}, None, {-2, -3, Removed(1), Removed(2)}, {-4}]


def test_merge_ref_set1():
    assert eval_node(
        merge_ref_non_peer[TIME_SERIES_TYPE : TSS[int]],
        index=[0, None, 1, None],
        ts1=[{1, 2}, None, None, {4}],
        ts2=[{1}, None, {2}, {4}],
    ) == [{1, 2}, None, set(), {4}]


def test_merge_ref_set2():
    assert eval_node(
        merge_ref_non_peer[TIME_SERIES_TYPE : TSS[int]],
        index=[0, None, 1, None],
        ts1=[{1, 2}, None, {3}, {4}],
        ts2=[{1}, None, {2, 3}, {4}],
    ) == [{1, 2}, None, {3}, {4}]


def test_merge_ref_set3():
    assert eval_node(
        merge_ref_non_peer[TIME_SERIES_TYPE : TSS[int]],
        index=[0, None, 1, None],
        ts1=[{1, 2}, None, {3}, {4}],
        ts2=[{1}, None, {Removed(1)}, {4}],
    ) == [{1, 2}, None, {Removed(1), Removed(2)}, {4}]


def test_tss_ref_contains():
    assert eval_node(
        contains_tss[KEYABLE_SCALAR:int], ts=[{1}, {2}, None, {Removed(2)}], item=[2, None, None, None, 1]
    ) == [False, True, None, False, True]


def test_merge_with_tsd():
    assert eval_node(
        merge_ref_non_peer[TIME_SERIES_TYPE : TSD[int, TS[int]]],
        index=[0, None, 1, None],
        ts1=[{1: 1, 2: 2}, None, None, {4: 4}],
        ts2=[{-1: -1}, {-2: -2}, {-3: -3, -1: REMOVE}, {-4: -4}],
    ) == [{1: 1, 2: 2}, None, {-2: -2, -3: -3, 1: REMOVE, 2: REMOVE}, {-4: -4}]


@compute_node
def merge_tsd(
    tsd1: TSD[K, REF[TIME_SERIES_TYPE]], tsd2: TSD[K, REF[TIME_SERIES_TYPE]]
) -> TSD[K, REF[TIME_SERIES_TYPE]]:
    tick = {}
    tick.update({k: v.value for k, v in tsd1.modified_items()})
    tick.update({k: v.value for k, v in tsd2.modified_items() if k not in tsd1})
    tick.update({k: tsd2[k].value if k in tsd2 else REMOVE for k in tsd1.removed_keys()})
    tick.update({k: REMOVE for k in tsd2.removed_keys() if k not in tsd1})
    return tick


def test_merge_tsd():
    assert eval_node(
        merge_tsd[K:int, TIME_SERIES_TYPE : TS[int]],
        tsd1=[{1: 1}, {2: 2}, {3: 3}, {1: REMOVE}, {1: 11}],
        tsd2=[{1: -1}, {-2: -2}, {1: -1, 3: -3}, None, {-2: REMOVE, 3: REMOVE}],
    ) == [{1: 1}, {2: 2, -2: -2}, {3: 3}, {1: -1}, {-2: REMOVE, 1: 11}]


def test_free_bundle_ref():
    from hgraph import TimeSeriesSchema

    class AB(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @compute_node
    def ref_signal(ts: REF[TSB[AB]]) -> TS[bool]:
        return ts.valid

    @graph
    def g(a: TS[int], b: TS[int]) -> TS[bool]:
        from hgraph import combine

        return ref_signal(combine[TSB[AB]](a=a, b=b))

    assert eval_node(g, a=[1, 2], b=[3, 4]) == [True, None]
