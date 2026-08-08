# Ported from ext/main/hgraph_unit_tests/_wiring/test_ref.py at 4760fccadd5368b0482393e5acb0ceaac48518e9
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
    if_,
    TSB,
    switch_,
    contains_,
    TimeSeriesReference,
    pass_through_node,
)
from hgraph.test import eval_node

import pytest

pytestmark = pytest.mark.smoke


@compute_node
def create_ref(ts: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]:
    return ts.value


def test_ref():
    assert eval_node(create_ref[TIME_SERIES_TYPE : TS[int]], ts=[1, 2]) == [1, 2]


@compute_node
def concrete_int_ref(ts: REF[TS[int]]) -> REF[TS[int]]:
    return ts.value


@compute_node
def concrete_str_ref(ts: REF[TS[str]]) -> REF[TS[str]]:
    return ts.value


@compute_node
def concrete_float_ref(ts: REF[TS[float]]) -> REF[TS[float]]:
    return ts.value


@compute_node
def concrete_set_ref(ts: REF[TSS[int]]) -> REF[TSS[int]]:
    return ts.value


@compute_node
def concrete_dict_ref(ts: REF[TSD[int, TS[int]]]) -> REF[TSD[int, TS[int]]]:
    return ts.value


def test_eval_node_seeds_concrete_scalar_ref_inputs():
    assert eval_node(concrete_int_ref, ts=[1, 2, 3]) == [1, 2, 3]
    assert eval_node(concrete_str_ref, ts=["a", "b", "c"]) == ["a", "b", "c"]
    assert eval_node(concrete_float_ref, ts=[1.0, 2.5, 3.7]) == [1.0, 2.5, 3.7]


def test_eval_node_seeds_concrete_collection_ref_inputs():
    set_result = eval_node(concrete_set_ref, ts=[{1, 2}, {3}, {Removed(1), 4}])
    assert set_result[0].added == frozenset({1, 2})
    assert set_result[1].added == {3}
    assert set_result[2].added == {4}
    assert 1 in set_result[2].removed

    assert eval_node(
        concrete_dict_ref,
        ts=[{1: 10, 2: 20}, {3: 30}, {1: REMOVE, 4: 40}],
    ) == [{1: 10, 2: 20}, {3: 30}, {1: REMOVE, 4: 40}]


def test_eval_node_seeds_concrete_ref_with_sparse_ticks():
    assert eval_node(concrete_int_ref, ts=[1, None, 3, None]) == [1, None, 3, None]


@compute_node
def ref_delta_matches_value(ref: REF[TS[int]]) -> REF[TS[int]]:
    return ref.value if ref.delta_value == ref.value else None


@graph
def ref_delta_graph(ts: TS[int]) -> REF[TS[int]]:
    return ref_delta_matches_value(ts)


def test_ref_delta_value_is_the_reference_token():
    assert eval_node(ref_delta_graph, [10, 20, 30]) == [10, 20, 30]


@graph
def pass_through_late_tsd_item(ts: TSD[str, TS[str]], key: TS[str]) -> TS[str]:
    return pass_through_node(ts[key])


def test_python_node_delta_samples_existing_value_on_ref_rebind():
    assert eval_node(
        pass_through_late_tsd_item,
        [{"topic": "value"}, None, None],
        [None, None, "topic"],
    ) == [None, None, "value"]


@compute_node
def inspect_ref_metadata(ts: REF[TS[int]]) -> TS[bool]:
    ref = ts.value
    return (
        TimeSeriesReference.is_instance(ref)
        and not ref.is_empty
        and ref.has_output
        and ref.is_valid
    )


@graph
def inspect_bound_ref(ts: TS[int]) -> TS[bool]:
    return inspect_ref_metadata(ts)


def test_bound_ref_exposes_safe_metadata():
    assert eval_node(inspect_bound_ref, [1]) == [True]


def test_route_ref():
    assert eval_node(if_[TIME_SERIES_TYPE : TS[int]], condition=[True, None, False, None], ts=[1, 2, None, 4]) == [
        {"true": 1},
        {"true": 2},
        {"false": 2},
        {"false": 4},
    ]


def test_route_ref_with_positional_inputs():
    assert eval_node(if_[TIME_SERIES_TYPE : TS[int]], [True, None, False, None], [1, 2, None, 4]) == [
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
    @graph
    def tss_contains(ts: TSS[int], item: TS[int]) -> TS[bool]:
        return contains_(ts, item)

    assert eval_node(
        tss_contains, ts=[{1}, {2}, None, {Removed(2)}], item=[2, None, None, None, 1]
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


def test_free_bundle_ref_in_switch():
    from hgraph import TimeSeriesSchema

    class AB(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def g(s: TS[bool], a: TS[int], b: TS[int]) -> TSB[AB]:
        from hgraph import combine

        bun = combine[TSB[AB]](a=if_(s, a).true, b=b)

        return switch_(
            s,
            {
                True: lambda b: b,
                False: lambda b: combine[TSB[AB]](a=1, b=2),
            },
            bun,
        )

    assert eval_node(g, s=[False, True, False, True], a=[1, 2, 3, 4], b=[-1, -2, -3, -4]) == [
        {"a": 1, "b": 2},
        {"a": 2, "b": -2},
        {"a": 1, "b": 2},
        {"a": 4, "b": -4},
    ]


def test_free_tsl_ref_in_switch():
    @graph
    def g(s: TS[bool], a: TS[int], b: TS[int]) -> TSL[TS[int], Size[2]]:
        from hgraph import combine

        bun = combine[TSL](if_(s, a).true, b)

        return switch_(
            s,
            {
                True: lambda b: b,
                False: lambda b: combine[TSL](1, 2),
            },
            bun,
        )

    assert eval_node(g, s=[False, True, False, True], a=[1, 2, 3, 4], b=[-1, -2, -3, -4]) == [
        {0: 1, 1: 2},
        {0: 2, 1: -2},
        {0: 1, 1: 2},
        {0: 4, 1: -4},
    ]
