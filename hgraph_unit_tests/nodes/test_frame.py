from dataclasses import dataclass
from datetime import date

import polars as pl

from hgraph import (
    TIME_SERIES_TYPE,
    TS,
    Frame,
    CompoundScalar,
    graph,
    HgDataFrameScalarTypeMetaData,
    HgCompoundScalarType,
    HgTypeMetaData,
    Series,
    const,
    pass_through_node
)
from hgraph.test import eval_node


@dataclass
class _TestSchema(CompoundScalar):
    a: date
    b: float

def test_data_frame():
    assert HgTypeMetaData.parse_type(Frame[_TestSchema]) == HgDataFrameScalarTypeMetaData(
        HgCompoundScalarType.parse_type(_TestSchema)
    )

def test_data_frame_parse_value_instance():
    tp = HgTypeMetaData.parse_type(Frame[_TestSchema])
    assert tp is tp.parse_value(pl.DataFrame({"a": [date(2021, 1, 1)], "b": [1.0]}))

def test_data_frame_parse_value_type():
    tp = HgTypeMetaData.parse_type(Frame[_TestSchema])
    assert tp is not HgTypeMetaData.parse_value(pl.DataFrame({"a": [date(2021, 1, 1)], "b": [1.0]}))
    assert tp.matches(HgTypeMetaData.parse_value(pl.DataFrame({"a": [date(2021, 1, 1)], "b": [1.0]})))

def test_data_frame1():
    frame = pl.DataFrame({"a": [date(2021, 1, 1)], "b": [1.0]})
    assert eval_node(pass_through_node[TIME_SERIES_TYPE : TS[Frame[_TestSchema]]], [frame]) == [frame]

def test_data_frame_operator_1():
    frame = pl.DataFrame({"a": [date(2021, 1, 1)], "b": [1.0]})

    @graph
    def g(ts: TIME_SERIES_TYPE) -> TS[_TestSchema]:
        return ts[0]

    assert eval_node(g[TIME_SERIES_TYPE : TS[Frame[_TestSchema]]], [frame]) == [
        _TestSchema(a=date(2021, 1, 1), b=1.0)
    ]

def test_data_frame_operator_2():
    frame = pl.DataFrame({"a": [date(2021, 1, 1)], "b": [1.0]})

    @graph
    def g(ts: TIME_SERIES_TYPE) -> TS[Series[date]]:
        return ts["a"]

    assert eval_node(g[TIME_SERIES_TYPE : TS[Frame[_TestSchema]]], [frame])[0][0] == date(2021, 1, 1)

def test_data_frame_operator_3():
    frame = pl.DataFrame({"a": [date(2021, 1, 1)], "b": [1.0]})

    @graph
    def g(ts: TIME_SERIES_TYPE) -> TS[_TestSchema]:
        return ts[const(0)]

    assert eval_node(g[TIME_SERIES_TYPE : TS[Frame[_TestSchema]]], [frame]) == [
        _TestSchema(a=date(2021, 1, 1), b=1.0)
    ]


