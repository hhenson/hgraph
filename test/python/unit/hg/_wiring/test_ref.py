from typing import cast, Type

from hg import TIME_SERIES_TYPE, compute_node, REF, TS, TSL, Size, SIZE, graph, TSS, SCALAR
from hg._impl._types._ref import PythonTimeSeriesReference
from hg._impl._types._tss import Removed
from hg._types._type_meta_data import AUTO_RESOLVE
from hg.test import eval_node


@compute_node
def create_ref(ts: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]:
    return ts.value


def test_ref():
    assert eval_node(create_ref[TIME_SERIES_TYPE: TS[int]], ts=[1, 2]) == [1, 2]


@compute_node
def route_ref(condition: TS[bool], ts: REF[TIME_SERIES_TYPE]) -> TSL[REF[TIME_SERIES_TYPE], Size[2]]:
    return cast(TSL, (ts.value, PythonTimeSeriesReference()) if condition.value else (PythonTimeSeriesReference(), ts.value))


def test_route_ref():
    assert eval_node(route_ref[TIME_SERIES_TYPE: TS[int]], condition=[True, None, False, None], ts=[1, 2, None, 4]) == [
        {0: 1}, {0: 2}, {1: 2}, {1: 4}]


@compute_node
def merge_ref(index: TS[int], ts: TSL[REF[TIME_SERIES_TYPE], SIZE]) -> REF[TIME_SERIES_TYPE]:
    return cast(REF, ts[index.value].value)


def test_merge_ref():
    assert eval_node(merge_ref[TIME_SERIES_TYPE: TS[int], SIZE: Size[2]], index=[0, None, 1, None], ts=[(1, -1), (2, -2), None, (4, -4)]) == [1, 2, -2, -4]


@graph
def merge_ref_non_peer(index: TS[int], ts1: TIME_SERIES_TYPE, ts2: TIME_SERIES_TYPE, tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE) -> REF[TIME_SERIES_TYPE]:
    return merge_ref(index, TSL.from_ts(ts1, ts2))  # TODO: This TSL building syntax is quite a mouthful, TSL(ts1, ts2) would be preferrable, ideally wiring should accept just (ts1, ts2) here


def test_merge_ref_non_peer():
    assert eval_node(merge_ref_non_peer[TIME_SERIES_TYPE: TS[int]],
                     index=[0, None, 1, None],
                     ts1=[1, 2, None, 4],
                     ts2=[-1, -2, None, -4]
                     ) == [1, 2, -2, -4]


def test_merge_ref_non_peer_complex_inner_ts():
    assert eval_node(merge_ref_non_peer[TIME_SERIES_TYPE: TSL[TS[int], Size[2]]],
                     index=[0, None, 1, None],
                     ts1=[(1, 1), (2, None), None, (None, 4)],
                     ts2=[(-1, -1), (-2, -2), None, (-4, None)]
                     ) == [{0: 1, 1: 1}, {0: 2}, {0: -2, 1: -2}, {0: -4}]


def test_merge_ref_set():
    assert eval_node(merge_ref_non_peer[TIME_SERIES_TYPE: TSS[int]],
                     index=[0, None, 1, None],
                     ts1=[{1, 2}, None, None, {4}],
                     ts2=[{-1}, {-2}, {-3, Removed(-1)}, {-4}]
                     ) == [{1, 2}, None, {-2, -3, Removed(1), Removed(2)}, {-4}]


@compute_node
def ref_contains(tss: REF[TSS[SCALAR]], item: TS[SCALAR]) -> REF[TS[bool]]:
    return PythonTimeSeriesReference(tss.value.output.ts_contains(item.value))


def test_tss_ref_contains():
    assert eval_node(ref_contains[SCALAR: int],
                     tss=[{1}, {2}, None, {Removed(2)}],
                     item=[2, None, None, None]
                     ) == [False, True, None, False]
