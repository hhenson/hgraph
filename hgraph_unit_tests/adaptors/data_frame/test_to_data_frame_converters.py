from datetime import datetime, date

from frozendict import frozendict as fd

from hgraph import (
    compound_scalar,
    COMPOUND_SCALAR,
    MIN_ST,
    MIN_TD,
    TSB,
    ts_schema,
    TS,
    convert,
    Frame,
    TIME_SERIES_TYPE,
    graph,
)
import hgraph.adaptors.data_frame
from hgraph.test import eval_node


def test_to_frame_ts_value():
    result = eval_node(convert[TS[Frame[compound_scalar(value=int)]]], [1, 2, 3])
    assert len(result) == 3
    assert [r["value"][0] for r in result] == [1, 2, 3]


def test_to_frame_ts_value_resolver():
    @graph
    def g(ts: TS[int]) -> TIME_SERIES_TYPE:
        return convert[TS[Frame]](ts, value_col="value")

    result = eval_node(g, [1, 2, 3])
    assert len(result) == 3
    assert [r["value"][0] for r in result] == [1, 2, 3]


def test_to_frame_ts_dt_value():
    @graph
    def g(ts: TS[int]) -> TIME_SERIES_TYPE:
        return convert[TS[Frame[compound_scalar(dt=datetime, value=int)]]](ts)

    result = eval_node(g, [1, 2, 3])
    assert len(result) == 3
    assert [r["value"][0] for r in result] == [1, 2, 3]
    assert [r["dt"][0] for r in result] == [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2]


def test_to_frame_ts_dt_value_resolver():
    @graph
    def g(ts: TS[int]) -> TIME_SERIES_TYPE:
        return convert[TS[Frame]](ts, value_col="value", dt_col="dt")

    result = eval_node(g, [1, 2, 3])
    assert len(result) == 3
    assert [r["value"][0] for r in result] == [1, 2, 3]
    assert [r["dt"][0] for r in result] == [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2]


def test_to_frame_ts_date_value():
    @graph
    def g(ts: TS[int]) -> TIME_SERIES_TYPE:
        return convert[TS[Frame[compound_scalar(dt=date, value=int)]]](ts)

    result = eval_node(g, [1, 2, 3])

    assert len(result) == 3
    assert [r["value"][0] for r in result] == [1, 2, 3]
    assert [r["dt"][0] for r in result] == [MIN_ST.date(), MIN_ST.date(), MIN_ST.date()]


def test_to_frame_ts_date_value_resolver():
    @graph
    def g(ts: TS[int]) -> TIME_SERIES_TYPE:
        return convert[TS[Frame]](ts, dt_col="dt", value_col="value", dt_is_date=True)

    result = eval_node(g, [1, 2, 3])

    assert len(result) == 3
    assert [r["value"][0] for r in result] == [1, 2, 3]
    assert [r["dt"][0] for r in result] == [MIN_ST.date(), MIN_ST.date(), MIN_ST.date()]


def test_to_frame_tsb():
    ts_schema_ = ts_schema(p1=TS[int], p2=TS[str])
    schema = compound_scalar(p1=int, p2=str)

    @graph
    def g(ts: TSB[ts_schema_]) -> TIME_SERIES_TYPE:
        return convert[TS[Frame[schema]]](ts)

    result = eval_node(g, [fd(p1=1, p2="a"), fd(p1=2, p2="b"), fd(p1=3, p2="c")])

    assert len(result) == 3
    assert [r["p1"][0] for r in result] == [1, 2, 3]
    assert [r["p2"][0] for r in result] == ["a", "b", "c"]


def test_to_frame_tsb_dt():
    ts_schema_ = ts_schema(p1=TS[int], p2=TS[str])
    schema = compound_scalar(dt=datetime, p1=int, p2=str)

    @graph
    def g(ts: TSB[ts_schema_]) -> TIME_SERIES_TYPE:
        return convert[TS[Frame[schema]]](ts)

    result = eval_node(g, [fd(p1=1, p2="a"), fd(p1=2, p2="b"), fd(p1=3, p2="c")])

    assert len(result) == 3
    assert [r["dt"][0] for r in result] == [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2]
    assert [r["p1"][0] for r in result] == [1, 2, 3]
    assert [r["p2"][0] for r in result] == ["a", "b", "c"]


def test_to_frame_tsb_date():
    ts_schema_ = ts_schema(p1=TS[int], p2=TS[str])
    schema = compound_scalar(dt=date, p1=int, p2=str)

    @graph
    def g(ts: TSB[ts_schema_]) -> TIME_SERIES_TYPE:
        return convert[TS[Frame[schema]]](ts)

    result = eval_node(g, [fd(p1=1, p2="a"), fd(p1=2, p2="b"), fd(p1=3, p2="c")])

    assert len(result) == 3
    assert [r["dt"][0] for r in result] == [MIN_ST.date(), MIN_ST.date(), MIN_ST.date()]
    assert [r["p1"][0] for r in result] == [1, 2, 3]
    assert [r["p2"][0] for r in result] == ["a", "b", "c"]


def test_to_frame_tsb_guess_schema():
    ts_schema_ = ts_schema(p1=TS[int], p2=TS[str])

    @graph
    def g(ts: TSB[ts_schema_]) -> TIME_SERIES_TYPE:
        return convert[TS[Frame]](ts, dt_col="dt", dt_is_date=True)

    result = eval_node(g, [fd(p1=1, p2="a"), fd(p1=2, p2="b"), fd(p1=3, p2="c")])

    assert len(result) == 3
    assert [r["dt"][0] for r in result] == [MIN_ST.date(), MIN_ST.date(), MIN_ST.date()]
    assert [r["p1"][0] for r in result] == [1, 2, 3]
    assert [r["p2"][0] for r in result] == ["a", "b", "c"]


def test_to_frame_tsb_guess_schema_with_mapping_date():
    ts_schema_ = ts_schema(p1_=TS[int], p2=TS[str])

    @graph
    def g(ts: TSB[ts_schema_]) -> TIME_SERIES_TYPE:
        return convert[TS[Frame]](ts, dt_col="dt", dt_is_date=True, map_=fd(p1_="p1"))

    result = eval_node(g, [fd(p1_=1, p2="a"), fd(p1_=2, p2="b"), fd(p1_=3, p2="c")])

    assert len(result) == 3
    assert [r["dt"][0] for r in result] == [MIN_ST.date(), MIN_ST.date(), MIN_ST.date()]
    assert [r["p1"][0] for r in result] == [1, 2, 3]
    assert [r["p2"][0] for r in result] == ["a", "b", "c"]


def test_to_frame_tsb_guess_schema_with_mapping():
    ts_schema_ = ts_schema(p1_=TS[int], p2=TS[str])

    @graph
    def g(ts: TSB[ts_schema_]) -> TIME_SERIES_TYPE:
        return convert[TS[Frame]](ts, dt_col="dt", map_=fd(p1_="p1"))

    result = eval_node(g, [fd(p1_=1, p2="a"), fd(p1_=2, p2="b"), fd(p1_=3, p2="c")])

    assert len(result) == 3
    assert [r["dt"][0] for r in result] == [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2]
    assert [r["p1"][0] for r in result] == [1, 2, 3]
    assert [r["p2"][0] for r in result] == ["a", "b", "c"]
