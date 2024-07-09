import polars as pl
from frozendict import frozendict as fd

from hgraph import MIN_ST, MIN_TD
from hgraph.adaptors.data_frame import (
    PolarsDataFrameSource,
    tsb_from_data_source,
    tsd_k_v_from_data_source,
    tsd_k_tsd_from_data_source,
    tsd_k_b_from_data_source,
    ts_of_array_from_data_source,
    tsd_k_a_from_data_source,
    ts_of_matrix_from_data_source,
)
from hgraph.adaptors.data_frame._data_source_generators import ts_of_frames_from_data_source
from hgraph.test import eval_node

_1 = MIN_ST
_2 = MIN_ST + MIN_TD
_3 = MIN_ST + MIN_TD * 2
_4 = MIN_ST + MIN_TD * 3


class TsbMockDataSource(PolarsDataFrameSource):

    def __init__(self):
        df = pl.DataFrame({"dt": [_1, _2, _3], "a": [1.0, 2.0, 3.0], "b": [4, 5, 6]})
        super().__init__(df)


def test_tsb_from_dadta_source():
    assert eval_node(tsb_from_data_source, TsbMockDataSource, "dt") == [
        fd({"a": 1.0, "b": 4}),
        fd({"a": 2.0, "b": 5}),
        fd({"a": 3.0, "b": 6}),
    ]


class TsdMockDataSource(PolarsDataFrameSource):

    def __init__(self):
        df = pl.DataFrame({
            "dt": [_1, _1, _2, _3],
            "k": [4, 5, 6, 6],
            "v": [1.0, 2.0, 3.0, 4.0],
        })
        super().__init__(df)


def test_k_v_from_data_source():
    assert eval_node(tsd_k_v_from_data_source, TsdMockDataSource, "dt", "k") == [
        fd({4: 1.0, 5: 2.0}),
        fd({6: 3.0}),
        fd({6: 4.0}),
    ]


class TsdTsdMockDataSource(PolarsDataFrameSource):

    def __init__(self):
        df = pl.DataFrame({
            "dt": [_1, _1, _1, _2, _2, _2, _3, _3, _3],
            "k": [4, 4, 5, 5, 6, 6, 6, 6, 6],
            "p": ["a", "b", "c", "a", "b", "c", "a", "b", "c"],
            "v": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
        })
        super().__init__(df)


def test_k_tsd_from_data_source():
    assert eval_node(tsd_k_tsd_from_data_source, TsdTsdMockDataSource, "dt", "k", "p") == [
        fd({4: fd({"a": 1.0, "b": 2.0}), 5: fd({"c": 3.0})}),
        fd({5: fd({"a": 4.0}), 6: fd({"b": 5.0, "c": 6.0})}),
        fd({6: fd({"a": 7.0, "b": 8.0, "c": 9.0})}),
    ]


class TsdTsbMockDataSource(PolarsDataFrameSource):

    def __init__(self):
        df = pl.DataFrame({
            "dt": [_1, _2, _2, _3],
            "k": ["a", "b", "c", "d"],
            "p1": [1.0, 2.0, 3.0, 4.0],
            "p2": [4, 5, 6, 7],
        })
        super().__init__(df)


def test_k_b_from_data_source():
    assert eval_node(tsd_k_b_from_data_source, TsdTsbMockDataSource, "dt", "k") == [
        fd({"a": fd({"p1": 1.0, "p2": 4})}),
        fd({"b": fd({"p1": 2.0, "p2": 5}), "c": fd({"p1": 3.0, "p2": 6})}),
        fd({"d": fd({"p1": 4.0, "p2": 7})}),
    ]


class TsArrayMockDataSource(PolarsDataFrameSource):

    def __init__(self):
        df = pl.DataFrame({
            "dt": [_1, _2, _3],
            "p1": [1, 2, 3],
            "p2": [4, 5, 6],
        })
        super().__init__(df)


def test_ts_of_array_from_data_source():
    results = eval_node(ts_of_array_from_data_source, TsArrayMockDataSource, "dt")
    results = [list(row) for row in results]
    assert results == [
        [1, 4],
        [2, 5],
        [3, 6],
    ]


class TsdKeyArrayMockDataSource(PolarsDataFrameSource):

    def __init__(self):
        df = pl.DataFrame({
            "dt": [_1, _2, _3],
            "k": ["a", "b", "c"],
            "p1": [1, 2, 3],
            "p2": [4, 5, 6],
        })
        super().__init__(df)


def test_tsd_k_of_array_from_data_source():
    results = eval_node(tsd_k_a_from_data_source, TsdKeyArrayMockDataSource, "dt", "k")
    results = [{k: list(v) for k, v in row.items()} for row in results]
    assert results == [
        fd({"a": [1, 4]}),
        fd({"b": [2, 5]}),
        fd({"c": [3, 6]}),
    ]


class TsdMatrixMockDataSource(PolarsDataFrameSource):

    def __init__(self):
        df = pl.DataFrame({
            "dt": [_1, _1, _2, _2, _3, _3],
            "p1": [1, 2, 3, 4, 5, 6],
            "p2": [4, 5, 6, 7, 8, 9],
        })
        super().__init__(df)


def test_ts_matrix_from_data_source():
    results = eval_node(ts_of_matrix_from_data_source, TsdMatrixMockDataSource, "dt")
    results = [[list(i) for i in row] for row in results]
    assert results == [
        [[1, 4], [2, 5]],
        [[3, 6], [4, 7]],
        [[5, 8], [6, 9]],
    ]


def test_ts_of_frames_from_data_source():
    results = eval_node(ts_of_frames_from_data_source, TsdMatrixMockDataSource, "dt")
    results = [[list(i) for i in row] for row in results]  # for i in row is columnar not row wise
    assert results == [
        [[1, 2], [4, 5]],
        [[3, 4], [6, 7]],
        [[5, 6], [8, 9]],
    ]


def test_ts_of_frames_from_data_source_with_dt():
    results = eval_node(ts_of_frames_from_data_source, TsdMatrixMockDataSource, "dt", remove_dt_col=False)
    results = [[list(i) for i in row] for row in results]  # for i in row is columnar not row wise
    assert results == [
        [[_1, _1], [1, 2], [4, 5]],
        [[_2, _2], [3, 4], [6, 7]],
        [[_3, _3], [5, 6], [8, 9]],
    ]
