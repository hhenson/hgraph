from datetime import datetime

import polars as pl
from frozendict import frozendict as fd

from hgraph import from_data_frame, TS, MIN_ST, MIN_TD, TSB, ts_schema, TSD, Frame, COMPOUND_SCALAR, graph, \
    compound_scalar, to_data_frame
from hgraph.test import eval_node


def test_data_frame_ts():
    df = pl.DataFrame({"date": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD], "value": [1, 2, 3]})
    assert eval_node(from_data_frame[TS[int]], df=df) == [1, 2, 3]


def test_data_frame_tsb():
    df = pl.DataFrame({"date": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD], "a": [1, 2, 3], "b": [4, 5, 6]})
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
    expected = pl.DataFrame({"date": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD], "a": [1, 2, 3], "b": [4, 5, 6]})
    assert pl.concat(actual).equals(expected)
    

def test_data_frame_tsd_k_v():
    @graph
    def g(df: Frame[compound_scalar(date=datetime, a=int, b=int)]) -> TSD[int, TS[int]]:
        return from_data_frame[TSD[int, TS[int]]](df, key_col="a")

    df = pl.DataFrame({"date": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD], "a": [1, 2, 3], "b": [4, 5, 6]})
    assert eval_node(g, df=df) == [
        fd({1: 4}),
        fd({2: 5}),
        fd({3: 6}),
    ]


def test_to_data_frame_ts():
    @graph
    def g(ts: TS[int]) -> TS[Frame[compound_scalar(date=datetime, value=int)]]:
        return to_data_frame(ts)

    actual = pl.concat(eval_node(g, ts=[1, 2, 3]))
    expected = pl.DataFrame({"date": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD], "value": [1, 2, 3]})
    assert actual.equals(expected)


def test_to_data_frame_tsd_k_v():
    @graph
    def g(ts: TSD[int, TS[int]]) -> TS[Frame[compound_scalar(date=datetime, key=int, value=int)]]:
        return to_data_frame(ts)

    actual = pl.concat(eval_node(g, ts=[fd({1: 1}), fd({2: 2}), fd({2: 3})]))
    expected = pl.DataFrame({
        "date": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD, MIN_ST + 2 * MIN_TD],
        "key": [1, 1, 2, 1, 2],
        "value": [1, 1, 2, 1, 3]}).sort("date", "key", "value")
    actual = actual.sort("date", "key", "value")
    assert actual.equals(expected)

def test_to_data_frame_tsd_k_tsb():
    @graph
    def g(ts: TSD[int, TSB[ts_schema(a=TS[int], b=TS[int])]]) -> TS[Frame[compound_scalar(date=datetime, key=int, a=int, b=int)]]:
        return to_data_frame(ts)
    
    actual = pl.concat(eval_node(g, ts=[fd({1: fd(a=1, b=4)}), fd({2: fd(a=2, b=5)}), fd({2: fd(a=3, b=6)})]))
    expected = pl.DataFrame({
        "date": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD, MIN_ST + 2 * MIN_TD],
        "key": [1, 1, 2, 1, 2],
        "a": [1, 1, 2, 1, 3],
        "b": [4, 4, 5, 4, 6]}).sort("date", "key", "a", "b")
    actual = actual.sort("date", "key", "a", "b")
    assert actual.equals(expected) 


def test_from_data_frame_tsd_k_tsb():

    df = pl.DataFrame({
        "date": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD, MIN_ST + 2 * MIN_TD],
        "key": [1, 1, 2, 1, 2],
        "a": [1, 1, 2, 2, 3],
        "b": [4, 4, 5, 5, 6]})

    result = eval_node(from_data_frame[TSD[str, TSB[ts_schema(a=TS[int], b=TS[int])]]], df=df)
    assert result == [ {1: fd(a=1, b=4)}, {1: fd(a=1, b=4), 2: fd(a=2, b=5)}, {1: fd(a=2, b=5), 2: fd(a=3, b=6)} ]
