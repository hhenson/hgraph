from datetime import date, datetime

from hgraph import from_data_frame, TS, MIN_ST, MIN_TD, TSB, ts_schema, TSD, Frame, COMPOUND_SCALAR, graph, \
    compound_scalar

from hgraph.test import eval_node

import polars as pl
from frozendict import frozendict as fd


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
