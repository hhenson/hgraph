# Ported from ext/main/hgraph_unit_tests/_operators/test_data_frame_operators.py
# Changes from upstream:
#  - polars -> pyarrow (Arrow ruling): pl.DataFrame -> pa.table, pl.concat ->
#    pa.concat_tables, .sort(...) -> .sort_by(...), assert_frame_equal ->
#    Table.equals. The empty frame gets explicit column types (pyarrow cannot
#    infer from []).
#  - deviation: RFC 0002 version-2 Instant columns are timestamp[us, "UTC"],
#    so Arrow returns timezone-aware UTC datetime values.
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Tuple

import pyarrow as pa
import pytest
from frozendict import frozendict as fd, frozendict

from hgraph import from_data_frame, TS, MIN_ST, MIN_TD, TSB, ts_schema, TSD, Frame, COMPOUND_SCALAR, graph, \
    compound_scalar, to_data_frame, CompoundScalar, REMOVE
from hgraph.adaptors.data_frame import group_by
from hgraph.test import eval_node


def _instant(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc)


def test_data_frame_ts():
    df = pa.table({"date": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD], "value": [1, 2, 3]})
    assert eval_node(from_data_frame[TS[int]], df=df) == [1, 2, 3]


def test_data_frame_tsb():
    df = pa.table({"date": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD], "a": [1, 2, 3], "b": [4, 5, 6]})
    assert eval_node(from_data_frame[TSB[ts_schema(a=TS[int], b=TS[int])]], df=df) == [
        fd(a=1, b=4),
        fd(a=2, b=5),
        fd(a=3, b=6),
    ]

def test_to_data_frame_tsb():
    @graph
    def g(tsb: TSB[ts_schema(a=TS[int], b=TS[int])]) -> TS[Frame[compound_scalar(date=datetime, a=int, b=int)]]:
        return to_data_frame(tsb)

    actual = eval_node(g, tsb=[fd(a=1, b=4), fd(a=2, b=5), fd(a=3, b=6)])
    expected = pa.table({
        "date": [_instant(MIN_ST), _instant(MIN_ST + MIN_TD), _instant(MIN_ST + 2 * MIN_TD)],
        "a": [1, 2, 3],
        "b": [4, 5, 6],
    })
    assert pa.concat_tables(actual).equals(expected)


def test_data_frame_tsd_k_v():
    @graph
    def g(df: Frame[compound_scalar(date=datetime, a=int, b=int)]) -> TSD[int, TS[int]]:
        return from_data_frame[TSD[int, TS[int]]](df, key_col="a")

    df = pa.table({"date": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD], "a": [1, 2, 3], "b": [4, 5, 6]})
    assert eval_node(g, df=df) == [
        fd({1: 4}),
        fd({2: 5}),
        fd({3: 6}),
    ]


def test_to_data_frame_ts():
    @graph
    def g(ts: TS[int]) -> TS[Frame[compound_scalar(date=datetime, value=int)]]:
        return to_data_frame(ts)

    actual = pa.concat_tables(eval_node(g, ts=[1, 2, 3]))
    expected = pa.table({
        "date": [_instant(MIN_ST), _instant(MIN_ST + MIN_TD), _instant(MIN_ST + 2 * MIN_TD)],
        "value": [1, 2, 3],
    })
    assert actual.equals(expected)


def test_to_data_frame_tsd_k_v():
    @graph
    def g(ts: TSD[int, TS[int]]) -> TS[Frame[compound_scalar(date=datetime, key=int, value=int)]]:
        return to_data_frame(ts)

    actual = pa.concat_tables(eval_node(g, ts=[fd({1: 1}), fd({2: 2}), fd({2: 3})]))
    expected = pa.table({
        "date": [
            _instant(MIN_ST),
            _instant(MIN_ST + MIN_TD),
            _instant(MIN_ST + MIN_TD),
            _instant(MIN_ST + 2 * MIN_TD),
            _instant(MIN_ST + 2 * MIN_TD),
        ],
        "key": [1, 1, 2, 1, 2],
        "value": [1, 1, 2, 1, 3]}).sort_by([("date", "ascending"), ("key", "ascending"), ("value", "ascending")])
    actual = actual.sort_by([("date", "ascending"), ("key", "ascending"), ("value", "ascending")])
    assert actual.equals(expected)

def test_to_data_frame_tsd_k_tsb():
    @graph
    def g(ts: TSD[int, TSB[ts_schema(a=TS[int], b=TS[int])]]) -> TS[Frame[compound_scalar(date=datetime, key=int, a=int, b=int)]]:
        return to_data_frame(ts)

    actual = pa.concat_tables(eval_node(g, ts=[fd({1: fd(a=1, b=4)}), fd({2: fd(a=2, b=5)}), fd({2: fd(a=3, b=6)})]))
    expected = pa.table({
        "date": [
            _instant(MIN_ST),
            _instant(MIN_ST + MIN_TD),
            _instant(MIN_ST + MIN_TD),
            _instant(MIN_ST + 2 * MIN_TD),
            _instant(MIN_ST + 2 * MIN_TD),
        ],
        "key": [1, 1, 2, 1, 2],
        "a": [1, 1, 2, 1, 3],
        "b": [4, 4, 5, 4, 6]}).sort_by([("date", "ascending"), ("key", "ascending"), ("a", "ascending"), ("b", "ascending")])
    actual = actual.sort_by([("date", "ascending"), ("key", "ascending"), ("a", "ascending"), ("b", "ascending")])
    assert actual.equals(expected)


def test_from_data_frame_rejects_explicit_tsd_key_mismatch():

    df = pa.table({
        "date": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD, MIN_ST + 2 * MIN_TD],
        "key": [1, 1, 2, 1, 2],
        "a": [1, 1, 2, 2, 3],
        "b": [4, 4, 5, 5, 6]})

    with pytest.raises(RuntimeError, match=r"column 'key'.*int64.*expected 'string'.*native scalar 'str'"):
        eval_node(from_data_frame[TSD[str, TSB[ts_schema(a=TS[int], b=TS[int])]]], df=df)


def test_group_by_single():

    @dataclass(frozen=True)
    class D(CompoundScalar):
        name: str
        value: int

    df1 = pa.table({"name": ["N1", "N1", "N2", "N3", "N3", "N4"], "value": [1, 1, 2, 3, 3, 4]})
    df2 = pa.table({"name": ["N1", "N1", "N2", "N4"], "value": [1, 1, 2, 4]})
    df3 = pa.table({"name": pa.array([], type=pa.string()), "value": pa.array([], type=pa.int64())})
    expected1 = {
        "N1": pa.table({"name": ["N1", "N1"], "value": [1, 1]}),
        "N2": pa.table({"name": ["N2"], "value": [2]}),
        "N3": pa.table({"name": ["N3", "N3"], "value": [3, 3]}),
        "N4": pa.table({"name": ["N4"], "value": [4]}),
    }
    expected2 = {
        "N1": pa.table({"name": ["N1", "N1"], "value": [1, 1]}),
        "N2": pa.table({"name": ["N2"], "value": [2]}),
        "N3": REMOVE,
        "N4": pa.table({"name": ["N4"], "value": [4]}),
    }
    expected3 = {
        "N1": REMOVE,
        "N2": REMOVE,
        "N4": REMOVE,
    }

    @graph
    def g(ts: TS[Frame[D]]) -> TSD[str, TS[Frame[D]]]:
        return group_by(ts, "name")

    results = eval_node(g, [df1, df2, df3], __elide__=True)

    assert len(results) == 3
    actual1, actual2, actual3 = results

    check_frames_equal(actual1, expected1)
    check_frames_equal(actual2, expected2)
    check_frames_equal(actual3, expected3)


def test_group_by_tuple():
    @dataclass(frozen=True)
    class D(CompoundScalar):
        parent: str
        child: str
        value: int

    df = pa.table({"parent": ["P1", "P1", "P1", "P2", "P2", "P2"], "child": ["C1", "C2", "C3", "C1", "C4", "C5"], "value": [1, 2, 3, 4, 5, 6]})
    expected = {
        ("P1", "C1"): pa.table({"parent": ["P1"], "child": ["C1"], "value": [1]}),
        ("P1", "C2"): pa.table({"parent": ["P1"], "child": ["C2"], "value": [2]}),
        ("P1", "C3"): pa.table({"parent": ["P1"], "child": ["C3"], "value": [3]}),

        ("P2", "C1"): pa.table({"parent": ["P2"], "child": ["C1"], "value": [4]}),
        ("P2", "C4"): pa.table({"parent": ["P2"], "child": ["C4"], "value": [5]}),
        ("P2", "C5"): pa.table({"parent": ["P2"], "child": ["C5"], "value": [6]}),
    }

    @graph
    def g(ts: TS[Frame[D]]) -> TSD[Tuple[str, str], TS[Frame[D]]]:
        return group_by(ts, ("parent", "child"))

    results = eval_node(g, [df], __elide__=True)

    check_frames_equal(results[-1], expected)


def check_frames_equal(actual, expected):
    assert len(actual) == len(expected)
    for k in expected:
        if expected[k] is REMOVE:
            assert actual[k] is REMOVE
        else:
            assert actual[k].equals(expected[k])
