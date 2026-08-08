# Ported from ext/main/hgraph_unit_tests/_operators/_conversion_operators/
# test_frame_conversion_operators.py
# Changes from upstream:
#  - polars -> pyarrow (Arrow ruling); assert_frame_equal -> Table.equals.
#  - `from polars import DataFrame` + TS[DataFrame]: our frame value IS the
#    Arrow table, so the untyped-input case is written TS[Frame] (a schema on
#    an input is a minimum requirement; no schema = untyped frame).
#  - The upstream `from hgraph.adaptors import data_frame` import-for-effect
#    is not needed (conversion overloads register with the stdlib).
from dataclasses import dataclass
from typing import Tuple

import pyarrow as pa
import pytest
from frozendict import frozendict

from hgraph import CompoundScalar, TS, TSD, convert, Frame, graph, combine
from hgraph.test import eval_node


def test_convert_tsd_to_frame():
    @dataclass
    class ABStruct(CompoundScalar):
        a: int
        b: str

    @graph
    def g(ts: TSD[str, TS[ABStruct]]) -> TS[Frame[ABStruct]]:
        return convert[TS[Frame]](ts)

    frame = pa.table({"a": [1], "b": ["1"]})
    assert eval_node(g, ts=[{"a": ABStruct(1, "1")}])[-1].equals(frame)


def test_convert_tsd_to_frame_key():
    @dataclass
    class ABStruct(CompoundScalar):
        a: int
        b: str

    @dataclass
    class KABStruct(CompoundScalar):
        k: str
        a: int
        b: str

    @graph
    def g(ts: TSD[str, TS[ABStruct]]) -> TS[Frame[KABStruct]]:
        return convert[TS[Frame[KABStruct]]](ts, mapping=frozendict({"key_col": "k"}))

    frame = pa.table({"k": ["a"], "a": [1], "b": ["1"]})
    assert eval_node(g, ts=[{"a": ABStruct(1, "1")}])[-1].equals(frame)


def test_convert_df_to_frame():
    @dataclass
    class ABStruct(CompoundScalar):
        a: int
        b: str

    @graph
    def g(ts: TS[Frame]) -> TS[Frame[ABStruct]]:
        return convert[TS[Frame[ABStruct]]](ts)

    frame = pa.table({"a": [1], "b": ["1"]})
    assert eval_node(g, ts=[frame])[-1].equals(frame)


def test_convert_frame_to_frame():
    @dataclass
    class ABStruct(CompoundScalar):
        a: int
        b: str

    @dataclass
    class CDStruct(CompoundScalar):
        c: int
        d: str

    @graph
    def g(ts: TS[Frame[ABStruct]]) -> TS[Frame[CDStruct]]:
        return convert[TS[Frame[CDStruct]]](ts, mapping=frozendict({"a": "c", "b": "d"}))

    frame = pa.table({"a": [1], "b": ["1"]})
    frame_r = pa.table({"c": [1], "d": ["1"]})
    assert eval_node(g, ts=[frame])[-1].equals(frame_r)


def test_combine_frame():
    @dataclass
    class ABStruct(CompoundScalar):
        a: int
        b: str

    @graph
    def g(ts1: TS[Tuple[int, ...]], ts2: TS[Tuple[str, ...]]) -> TS[Frame[ABStruct]]:
        return combine[TS[Frame[ABStruct]]](a=ts1, b=ts2)

    frame = pa.table({"a": [1], "b": ["1"]})
    assert eval_node(g, ts1=[(1,)], ts2=[("1",)])[-1].equals(frame)


def test_convert_cs_frame():
    @dataclass
    class ABStruct(CompoundScalar):
        a: int
        b: str

    @graph
    def g(ts: TS[ABStruct]) -> TS[Frame[ABStruct]]:
        return convert[TS[Frame[ABStruct]]](ts)

    frame = pa.table({"a": [1], "b": ["1"]})
    assert eval_node(g, ts=ABStruct(1, "1"))[-1].equals(frame)


def test_convert_tuple_to_frame():
    @dataclass
    class ABStruct(CompoundScalar):
        a: int
        b: str

    @graph
    def g(ts: TS[Tuple[ABStruct, ...]]) -> TS[Frame[ABStruct]]:
        return convert[TS[Frame[ABStruct]]](ts)

    frame = pa.table({"a": [1, 2], "b": ["1", "2"]})
    assert eval_node(g, ts=[(ABStruct(1, "1"), ABStruct(2, "2"))])[-1].equals(frame)
