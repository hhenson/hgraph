from dataclasses import dataclass
from typing import Tuple

import polars as pl
from frozendict import frozendict
from polars import DataFrame
from polars.testing import assert_frame_equal

from hgraph import CompoundScalar, TS, TSD, convert, Frame, graph, combine
from hgraph.adaptors import data_frame  # noqa  # TODO: this is a gotcha - have to import the adaptor package
from hgraph.test import eval_node


def test_convert_tsd_to_frame():
    @dataclass
    class ABStruct(CompoundScalar):
        a: int
        b: str

    @graph
    def g(ts: TSD[str, TS[ABStruct]]) -> TS[Frame[ABStruct]]:
        return convert[TS[Frame]](ts)

    frame = pl.DataFrame({"a": [1], "b": ["1"]})
    assert_frame_equal(eval_node(g, ts=[{"a": ABStruct(1, "1")}])[-1], frame)


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

    frame = pl.DataFrame({"k": ["a"], "a": [1], "b": ["1"]})
    assert_frame_equal(eval_node(g, ts=[{"a": ABStruct(1, "1")}])[-1], frame)


def test_convert_df_to_frame():
    @dataclass
    class ABStruct(CompoundScalar):
        a: int
        b: str

    @graph
    def g(ts: TS[DataFrame]) -> TS[Frame[ABStruct]]:
        return convert[TS[Frame[ABStruct]]](ts)

    frame = pl.DataFrame({"a": [1], "b": ["1"]})
    assert_frame_equal(eval_node(g, ts=[frame])[-1], frame)


def test_combine_frame():
    @dataclass
    class ABStruct(CompoundScalar):
        a: int
        b: str

    @graph
    def g(ts1: TS[Tuple[int, ...]], ts2: TS[Tuple[str, ...]]) -> TS[Frame[ABStruct]]:
        return combine[TS[Frame[ABStruct]]](a=ts1, b=ts2)

    frame = pl.DataFrame({"a": [1], "b": ["1"]})
    assert_frame_equal(eval_node(g, ts1=[(1,)], ts2=[("1",)])[-1], frame)


def test_convert_cs_frame():
    @dataclass
    class ABStruct(CompoundScalar):
        a: int
        b: str

    @graph
    def g(ts: TS[ABStruct]) -> TS[Frame[ABStruct]]:
        return convert[TS[Frame[ABStruct]]](ts)

    frame = pl.DataFrame({"a": [1], "b": ["1"]})
    assert_frame_equal(eval_node(g, ts=ABStruct(1, "1"))[-1], frame)
