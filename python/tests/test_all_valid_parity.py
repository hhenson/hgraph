"""TSD, TSB and TSL all_valid checks immediate child validity only.

A nested child need not itself be all_valid. These tests specify the C++-first
contract, including cases where the released Python implementation differs.
"""

from __future__ import annotations

from dataclasses import dataclass

from frozendict import frozendict as fd

from hgraph import TS, TSB, TSD, TSL, TSS, Size, TimeSeriesSchema, compute_node, graph
from hgraph.test import eval_node


def test_nested_tsl_all_valid_does_not_recurse():
    @compute_node(valid=tuple())
    def probe(x: TSL[TSL[TS[int], Size[2]], Size[2]]) -> TS[str]:
        return f"outer={x.all_valid} inner0={x[0].all_valid} inner1={x[1].all_valid}"

    # Each inner list holds one of its two elements: the inner lists are valid
    # but not all_valid, and the outer list only asks them for valid.
    assert eval_node(probe, [{0: {0: 1}, 1: {0: 2}}]) == [
        "outer=True inner0=False inner1=False"
    ]

    # A direct child that is not valid at all does make the outer false.
    assert eval_node(probe, [{0: {0: 1}}]) == [
        "outer=False inner0=False inner1=False"
    ]

    # Fully populated is all_valid at every level.
    assert eval_node(probe, [{0: {0: 1, 1: 9}, 1: {0: 2, 1: 8}}]) == [
        "outer=True inner0=True inner1=True"
    ]


@dataclass
class _Inner(TimeSeriesSchema):
    a: TS[int]
    b: TS[int]


@dataclass
class _Outer(TimeSeriesSchema):
    inner: TSB[_Inner]


def test_nested_tsb_all_valid_does_not_recurse():
    @compute_node(valid=tuple())
    def probe(x: TSB[_Outer]) -> TS[str]:
        return f"outer={x.all_valid} inner={x.inner.all_valid}"

    # `inner` is valid (a ticked) but not all_valid (b never ticked).
    assert eval_node(probe, [{"inner": {"a": 1}}]) == ["outer=True inner=False"]

    assert eval_node(probe, [{"inner": {"a": 1, "b": 2}}]) == [
        "outer=True inner=True"
    ]


def test_tsl_all_valid_checks_its_own_children():
    @compute_node(valid=tuple())
    def probe(x: TSL[TS[int], Size[2]]) -> TS[bool]:
        return x.all_valid

    assert eval_node(probe, [{0: 1}, {1: 2}]) == [False, True]


def test_tsd_all_valid_does_not_recurse_into_a_partially_valid_child():
    @compute_node(valid=tuple())
    def probe(tsd: TSD[str, TSL[TS[int], Size[2]]], tss: TSS[int]) -> TS[str]:
        return f"tsd={tsd.valid == tsd.all_valid} tss={tss.valid == tss.all_valid}"

    # The TSD value is a partially populated TSL; that must not make the TSD
    # all_valid-false: its immediate list child is valid despite the leaf hole.
    assert eval_node(probe, [fd(a={0: 1})], [frozenset({1})]) == [
        "tsd=True tss=True"
    ]


def test_ts_all_valid_matches_valid():
    # `tick` drives evaluation so `ts` can be observed while still invalid.
    @compute_node(valid=tuple())
    def probe(ts: TS[int], tick: TS[int]) -> TS[str]:
        return f"valid={ts.valid} all_valid={ts.all_valid}"

    assert eval_node(probe, [None, 1], [1, 1]) == [
        "valid=False all_valid=False",
        "valid=True all_valid=True",
    ]


@compute_node
def _dictionary_membership(event: TS[int], _output: TSD = None) -> TSD[str, TS[int]]:
    if event.value == 1:
        _output.get_or_create("a")
    elif event.value in (2, 5):
        _output.get_or_create("a").value = event.value
    elif event.value == 3:
        _output["a"].invalidate()
    else:
        del _output["a"]


@compute_node(valid=tuple())
def _dictionary_all_valid(values: TSD[str, TS[int]], tick: TS[int]) -> TS[bool]:
    return values.all_valid


@compute_node(all_valid=("values",))
def _dictionary_all_valid_gate(values: TSD[str, TS[int]], tick: TS[int]) -> TS[int]:
    return tick.value


def test_tsd_all_valid_tracks_invalid_live_children_and_removal():
    @graph
    def probe(event: TS[int]) -> TS[bool]:
        return _dictionary_all_valid(_dictionary_membership(event), event)

    assert eval_node(probe, [1, 2, 3, 4, 5]) == [False, True, False, True, True]


def test_tsd_all_valid_gate_rechecks_child_validity_on_every_evaluation():
    @graph
    def probe(event: TS[int]) -> TS[int]:
        return _dictionary_all_valid_gate(_dictionary_membership(event), event)

    assert eval_node(probe, [1, 2, 3, 4, 5]) == [None, 2, None, 4, 5]
