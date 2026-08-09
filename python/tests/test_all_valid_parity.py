"""``all_valid`` is a one-level check, matching the Python-first implementation.

A collection asks each of its direct children for ``valid``, never for their
``all_valid``. So a partially populated collection nested inside another does
not make the outer one ``all_valid``-false.

Upstream (``release/0.5``) defines this in ``hgraph/_impl/_types``:
``PythonTimeSeriesBundleOutput.all_valid`` and
``PythonTimeSeriesListOutput.all_valid`` are both
``all(ts.valid for ts in self.values())``, while ``TSD`` and ``TSS`` inherit
``PythonTimeSeriesOutput.all_valid``, which is just ``valid``.
"""

from __future__ import annotations

from dataclasses import dataclass

from frozendict import frozendict as fd

from hgraph import TS, TSB, TSD, TSL, TSS, Size, TimeSeriesSchema, compute_node
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


def test_tsd_and_tss_all_valid_match_valid():
    @compute_node(valid=tuple())
    def probe(tsd: TSD[str, TSL[TS[int], Size[2]]], tss: TSS[int]) -> TS[str]:
        return f"tsd={tsd.valid == tsd.all_valid} tss={tss.valid == tss.all_valid}"

    # The TSD value is a partially populated TSL; that must not make the TSD
    # all_valid-false, because a TSD's all_valid is defined as its valid.
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
