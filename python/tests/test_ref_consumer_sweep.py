"""REF-by-consumer sweep.

Design record: ``docs/source/developer_guide/testing.rst`` ("Authoring-shape
sweeps"). The rule under test is the one the developer guide states twice: a
consumer that does not declare ``REF`` observes the dereferenced value because
binding inserts the from-REF adaptation, and a type variable binds the
dereferenced schema. The 2026-09-04 fix-series retrospective found that rule
patched at ten consumers (collect, mesh keys, CompoundScalar fields, ungroup,
tuple combine, nested bundle projections, ...), which is why this sweep
exists.

Oracle: for every (shape, source, consumer) product, the graph that feeds the
consumer through a REF-producing source yields the same ``eval_node`` trace
as the graph that feeds it the plain source. No released-hgraph oracle is
needed, so the sweep also covers C++-first-only shapes. Equivalent native
public-wiring coverage lives in ``test_graph_wiring.cpp`` (scalar, TSS, TSD,
fixed TSL, TSB, and nested collection REF round trips) and
``test_ref_executor.cpp`` (dynamic retargeting through a plain consumer).

``KNOWN_GAPS`` lists the products that fail today and ``KNOWN_GAP_SOURCES``
the sources that fail for every consumer. Each entry specifies the precise
exception or trace produced by the known defect before the test is marked
``xfail``, so another failure mode remains a regression. The first run found
#649 (reduce/mesh_ over a REF-valued collection) and #650 (a REF-output switch
going silent after a branch change); both are fixed and the tables are empty.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Callable, Mapping, Set, Tuple

import pytest

from hgraph import (
    REF,
    REMOVE,
    TIME_SERIES_TYPE,
    TS,
    TSB,
    TSD,
    TSL,
    TSS,
    CompoundScalar,
    Removed,
    Size,
    TimeSeriesSchema,
    abs_,
    add_,
    cast_,
    collect,
    combine,
    compute_node,
    const,
    contains_,
    convert,
    dedup,
    default,
    dispatch,
    drop,
    eq_,
    filter_,
    flip,
    gate,
    getattr_,
    graph,
    if_,
    if_then_else,
    is_empty,
    keys_,
    lag,
    len_,
    map_,
    merge,
    mesh_,
    modified,
    nothing,
    race,
    reduce,
    str_,
    sum_,
    switch_,
    take,
    valid,
)
from hgraph.test import eval_node

# --------------------------------------------------------------------------
# Scalar and bundle schemas used by the shapes
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class SweepPoint(CompoundScalar):
    x: int
    label: str


@dataclass(frozen=True)
class SweepDerived(SweepPoint):
    extra: int


@dataclass(frozen=True)
class SweepKey(CompoundScalar):
    key: str
    group: str


@dataclass(frozen=True)
class SweepDerivedKey(SweepKey):
    """Registered so ``SweepKey`` is polymorphic in the realized type system (#521)."""

    detail: str


class SweepPair(TimeSeriesSchema):
    a: TS[int]
    b: TS[str]


@dataclass(frozen=True)
class SweepHolder(CompoundScalar):
    point: SweepPoint


# --------------------------------------------------------------------------
# REF-producing sources
# --------------------------------------------------------------------------


@compute_node
def _as_ref(ts: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]:
    return ts.value


@compute_node
def _keyed(ts: REF[TIME_SERIES_TYPE]) -> TSD[str, REF[TIME_SERIES_TYPE]]:
    return {"k": ts.value}


def _flip_key(ts):
    return default(if_then_else(valid(lag(ts, 1)), const("b"), const("a")), const("a"))


def _switch_flip(ts, tp):
    """A REF-output switch whose active branch changes after the first tick.

    Both branches publish a reference to the same outer source, so the flip
    retires one token for an equal one: the trace must match the plain arm
    exactly (#650 was this shape going silent after the flip).
    """
    return switch_(_flip_key(ts), {"a": lambda t: _as_ref(t), "b": lambda t: _as_ref(t)}, ts)


def _switch_flip_mixed(ts, tp):
    """A switch whose active branch flips from a value body to a REF body.

    The value branch owns a copy of the value in its terminal and the switch
    publishes a reference to that copy; the REF branch publishes a reference
    to the outer source. The flip therefore re-points consumers to a different
    output, which samples it: for a collection that is a full-value tick at
    the flip (documented binding semantics), so this source only sweeps the
    scalar shapes where a sample equals the tick it coincides with.
    """
    return switch_(_flip_key(ts), {"a": lambda t: t, "b": lambda t: _as_ref(t)}, ts)


def _invalid_after_first(ts, tp):
    """The exact observable source shape of #650: valid once, then unbound."""
    active = default(if_then_else(valid(lag(ts, 1)), const(False), const(True)), const(True))
    return if_(active, ts).true


SOURCES: dict[str, Callable] = {
    "plain": lambda ts, tp: ts,
    "ref_node": lambda ts, tp: _as_ref(ts),
    "tsl_projection": lambda ts, tp: TSL.from_ts(ts, ts)[0],
    "tsd_getitem": lambda ts, tp: _keyed(ts)[const("k")],
    "map_element": lambda ts, tp: map_(lambda v: v, _keyed(ts))[const("k")],
    "switch_branch": lambda ts, tp: switch_(const("a"), {"a": lambda t: t}, ts),
    "switch_flip": _switch_flip,
    "switch_flip_mixed": _switch_flip_mixed,
    "if_true": lambda ts, tp: if_(const(True), ts).true,
    "default_ref": lambda ts, tp: default(nothing(tp), ts),
    # A collection whose ELEMENTS are references (the map_ output shape): the
    # top level is a value, the REF is nested one level down.
    "map_nested": lambda ts, tp: map_(lambda v: v, ts),
}

# Sources that only make sense for a subset of shapes.
_SOURCE_SHAPES: dict[str, tuple[str, ...]] = {
    "map_nested": ("TSD[str, TS[int]]", "TSD[Key, TS[int]]", "TSL[TS[int], Size[2]]"),
    "switch_flip_mixed": ("TS[int]", "TS[Point]", "TS[Point]/polymorphic", "TS[tuple[int, ...]]"),
}


# --------------------------------------------------------------------------
# Consumers, grouped by the shape they accept
# --------------------------------------------------------------------------


@compute_node
def _py_int(ts: TS[int]) -> TS[int]:
    return ts.value * 10


@compute_node
def _py_point(ts: TS[SweepPoint]) -> TS[int]:
    return ts.value.x


@compute_node
def _py_tuple(ts: TS[Tuple[int, ...]]) -> TS[int]:
    return sum(ts.value)


@compute_node
def _py_tsd(ts: TSD[str, TS[int]]) -> TS[int]:
    return sum(v for v in ts.value.values())


@compute_node
def _py_keyed_tsd(ts: TSD[SweepKey, TS[int]]) -> TS[str]:
    return ",".join(sorted(k.key for k in ts.value))


@compute_node
def _py_tss(ts: TSS[int]) -> TS[int]:
    return len(ts.value)


@compute_node
def _py_tsl(ts: TSL[TS[int], Size[2]]) -> TS[int]:
    return sum(v.value for v in ts.values() if v.valid)


@compute_node
def _py_tsb(ts: TSB[SweepPair]) -> TS[str]:
    return f"{ts.a.value if ts.a.valid else None}:{ts.b.value if ts.b.valid else None}"


@compute_node
def _describe_default(p: TS[SweepPoint]) -> TS[str]:
    return f"point:{p.value.x}"


@dispatch
def _describe(p: TS[SweepPoint]) -> TS[str]:
    return _describe_default(p)


@graph(overloads=_describe)
def _describe_derived(p: TS[SweepDerived]) -> TS[str]:
    return const("derived")


CONSUMERS: dict[str, dict[str, Callable]] = {
    "TS[int]": {
        "add_one": lambda ts: ts + 1,
        "abs_": lambda ts: abs_(ts),
        "str_": lambda ts: str_(ts),
        "eq_const": lambda ts: eq_(ts, 2),
        "lag": lambda ts: lag(ts, 1),
        "take": lambda ts: take(ts, 2),
        "drop": lambda ts: drop(ts, 1),
        "dedup": lambda ts: dedup(ts),
        "default": lambda ts: default(nothing(TS[int]), ts),
        "filter_": lambda ts: filter_(const(True), ts),
        "if_then_else": lambda ts: if_then_else(const(True), ts, ts),
        "merge": lambda ts: merge(ts, ts),
        "race": lambda ts: race(ts, nothing(TS[int])),
        "gate": lambda ts: gate(const(True), ts),
        "sum_": lambda ts: sum_(ts),
        "convert_float": lambda ts: convert[TS[float]](ts),
        "cast_float": lambda ts: cast_(float, ts),
        "convert_tss": lambda ts: convert[TSS](ts),
        "collect_set": lambda ts: collect[TS[Set]](ts),
        "collect_tuple": lambda ts: collect[TS[Tuple]](ts),
        "combine_tsl": lambda ts: combine[TSL](ts, ts),
        "tsl_from_ts": lambda ts: TSL.from_ts(ts, ts)[1],
        "switch_": lambda ts: switch_(const("a"), {"a": lambda t: t + 1}, ts),
        "valid": lambda ts: valid(ts),
        "modified": lambda ts: modified(ts),
        "python_node": lambda ts: _py_int(ts),
    },
    "TS[Point]": {
        "field": lambda ts: ts.x,
        "getattr_": lambda ts: getattr_(ts, "label"),
        "combine_compound": lambda ts: combine[TS[SweepHolder]](point=ts),
        "str_": lambda ts: str_(ts),
        "eq_": lambda ts: eq_(ts, ts),
        "dispatch": lambda ts: _describe(ts),
        "python_node": lambda ts: _py_point(ts),
    },
    "TS[tuple[int, ...]]": {
        "len_": lambda ts: len_(ts),
        "getitem": lambda ts: ts[0],
        "str_": lambda ts: str_(ts),
        "python_node": lambda ts: _py_tuple(ts),
    },
    "TSD[str, TS[int]]": {
        "getitem": lambda ts: ts[const("a")],
        "key_set": lambda ts: ts.key_set,
        "self_keyed": lambda ts: ts[ts.key_set],
        "is_empty": lambda ts: is_empty(ts),
        "keys_": lambda ts: keys_(ts),
        "reduce": lambda ts: reduce(add_, ts, 0),
        "map_": lambda ts: map_(lambda v: v + 1, ts),
        # Not ``v + 1``: a ported test registers a mesh_ overload whose
        # ``requires`` captures that exact lambda for the rest of the process.
        "mesh_": lambda ts: mesh_(lambda v: v * 3, ts),
        "merge": lambda ts: merge(ts, ts),
        "filter_": lambda ts: filter_(const(True), ts),
        "flip": lambda ts: flip(ts),
        "collect_tsd": lambda ts: collect[TSD](ts),
        "dedup": lambda ts: dedup(ts),
        "convert_mapping": lambda ts: convert[TS[Mapping[str, int]]](ts),
        "default": lambda ts: default(nothing(TSD[str, TS[int]]), ts),
        "python_node": lambda ts: _py_tsd(ts),
    },
    "TSD[Key, TS[int]]": {
        "self_keyed": lambda ts: ts[ts.key_set],
        "key_set": lambda ts: ts.key_set,
        "merge": lambda ts: merge(ts, ts),
        "filter_": lambda ts: filter_(const(True), ts),
        "default": lambda ts: default(nothing(TSD[SweepKey, TS[int]]), ts),
        "collect_tsd": lambda ts: collect[TSD](ts),
        "python_node": lambda ts: _py_keyed_tsd(ts),
    },
    "TSS[int]": {
        "len_": lambda ts: len_(ts),
        "is_empty": lambda ts: is_empty(ts),
        "contains_": lambda ts: contains_(ts, const(2)),
        "str_": lambda ts: str_(ts),
        "python_node": lambda ts: _py_tss(ts),
    },
    "TSL[TS[int], Size[2]]": {
        "getitem_0": lambda ts: ts[0],
        "getitem_1": lambda ts: ts[1],
        "reduce": lambda ts: reduce(add_, ts, 0),
        "map_": lambda ts: map_(lambda v: v + 1, ts),
        "abs_": lambda ts: abs_(ts),
        "python_node": lambda ts: _py_tsl(ts),
    },
    "TSB[Pair]": {
        "field_a": lambda ts: ts.a,
        "field_b": lambda ts: ts.b,
        "getattr_": lambda ts: getattr_(ts, "a"),
        "recombine": lambda ts: combine[TSB[SweepPair]](a=ts.a, b=ts.b),
        "python_node": lambda ts: _py_tsb(ts),
    },
}


@dataclass(frozen=True)
class Shape:
    tp: object
    ticks: list


SHAPES: dict[str, Shape] = {
    "TS[int]": Shape(TS[int], [1, 2, None, 4]),
    "TS[Point]": Shape(TS[SweepPoint], [SweepPoint(1, "a"), SweepPoint(2, "b"), None, SweepPoint(4, "d")]),
    "TS[Point]/polymorphic": Shape(
        TS[SweepPoint], [SweepPoint(1, "a"), SweepDerived(2, "b", 3), None, SweepDerived(4, "d", 5)]
    ),
    "TS[tuple[int, ...]]": Shape(TS[Tuple[int, ...]], [(1, 2), (3,), None, (4, 5, 6)]),
    "TSD[str, TS[int]]": Shape(
        TSD[str, TS[int]], [{"a": 1, "b": 2}, {"a": 3}, None, {"b": REMOVE, "c": 5}]
    ),
    "TSD[Key, TS[int]]": Shape(
        TSD[SweepKey, TS[int]],
        [
            {
                SweepKey("one", "a"): 1,
                SweepDerivedKey("two", "b", "derived"): 2,
            },
            {SweepKey("one", "a"): 3},
            None,
            {
                SweepDerivedKey("two", "b", "derived"): REMOVE,
                SweepKey("three", "c"): 5,
            },
        ],
    ),
    "TSS[int]": Shape(TSS[int], [{1, 2}, {3}, None, {Removed(1), 4}]),
    "TSL[TS[int], Size[2]]": Shape(
        TSL[TS[int], Size[2]], [(1, 2), (3, None), None, (None, 4)]
    ),
    "TSB[Pair]": Shape(TSB[SweepPair], [{"a": 1, "b": "x"}, {"a": 2}, None, {"b": "y"}]),
}

_CONSUMER_SHAPE = {
    "TS[Point]/polymorphic": "TS[Point]",
}


def _consumers_for(shape_id: str) -> dict[str, Callable]:
    return CONSUMERS[_CONSUMER_SHAPE.get(shape_id, shape_id)]


# --------------------------------------------------------------------------
# Known gaps: validate the recorded failure before marking it xfail
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class KnownGap:
    reason: str
    error: type[Exception] | None = None
    match: str | None = None
    matches_invalidated_source: bool = False
    expected_trace: list | None = None


KNOWN_GAPS: dict[str, KnownGap] = {}

# A source in this table fails for every consumer; the defect is the source
# itself, so every one of its products is an expected failure.
KNOWN_GAP_SOURCES: dict[str, KnownGap] = {}

# Products of a gap source that nevertheless match the plain arm. They stay
# ordinary tests.
KNOWN_GAP_SOURCE_PASSES: dict[str, frozenset[str]] = {}


# --------------------------------------------------------------------------
# Harness
# --------------------------------------------------------------------------


def _build(shape: Shape, source: Callable, consumer: Callable):
    def g(ts):
        return consumer(source(ts, shape.tp))

    g.__annotations__ = {"ts": shape.tp}
    return graph(g)


def _normalize(trace):
    if trace is None:
        return None
    out = []
    for item in trace:
        if item is not None and hasattr(item, "added") and hasattr(item, "removed"):
            item = (frozenset(item.added), frozenset(item.removed))
        out.append(item)
    return out


@lru_cache(maxsize=None)
def _expected(shape_id: str, consumer_id: str):
    shape = SHAPES[shape_id]
    consumer = _consumers_for(shape_id)[consumer_id]
    return _normalize(eval_node(_build(shape, SOURCES["plain"], consumer), shape.ticks))


def _cases():
    for shape_id in SHAPES:
        for consumer_id in _consumers_for(shape_id):
            for source_id in SOURCES:
                if source_id == "plain":
                    continue
                if source_id in _SOURCE_SHAPES and shape_id not in _SOURCE_SHAPES[source_id]:
                    continue
                yield shape_id, source_id, consumer_id


def _case_id(case):
    return "-".join(case)


def _known_gap(case):
    shape_id, source_id, consumer_id = case
    case_id = _case_id(case)
    if case_id in KNOWN_GAPS:
        return KNOWN_GAPS[case_id]
    if case_id in KNOWN_GAP_SOURCE_PASSES.get(source_id, ()):
        return None
    return KNOWN_GAP_SOURCES.get(source_id)


_CASES = list(_cases())


@pytest.mark.parametrize(
    "shape_id,consumer_id",
    [(s, c) for s in SHAPES for c in _consumers_for(s)],
    ids=lambda v: v,
)
def test_plain_arm_evaluates(shape_id, consumer_id):
    """The oracle arm must itself run: a failure here is a consumer definition bug."""
    trace = _expected(shape_id, consumer_id)
    assert isinstance(trace, list) and trace


@pytest.mark.parametrize(
    "case",
    _CASES,
    ids=_case_id,
)
def test_ref_source_matches_plain(case):
    shape_id, source_id, consumer_id = case
    shape = SHAPES[shape_id]
    consumer = _consumers_for(shape_id)[consumer_id]
    expected = _expected(shape_id, consumer_id)
    gap = _known_gap(case)
    if gap is not None and gap.error is not None:
        with pytest.raises(gap.error, match=gap.match):
            eval_node(_build(shape, SOURCES[source_id], consumer), shape.ticks)
        pytest.xfail(gap.reason)

    actual = _normalize(eval_node(_build(shape, SOURCES[source_id], consumer), shape.ticks))
    if gap is not None and gap.expected_trace is not None:
        assert actual == gap.expected_trace
        assert actual != expected
        pytest.xfail(gap.reason)

    if gap is not None and gap.matches_invalidated_source:
        defect = _normalize(
            eval_node(_build(shape, _invalid_after_first, consumer), shape.ticks)
        )
        assert actual == defect
        assert actual != expected
        pytest.xfail(gap.reason)

    assert actual == expected
