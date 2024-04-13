import polars as pl
from frozendict import frozendict as fd

from hgraph import MIN_ST, MIN_TD
from hgraph.adaptors.data_frame.data_frame_source import PolarsDataFrameSource
from hgraph.adaptors.data_frame.data_source_generators import tsb_from_data_source, tsd_k_v_from_data_source, \
    tsd_k_tsd_from_data_source, tsd_k_b_from_data_source
from hgraph.test import eval_node

_1 = MIN_ST
_2 = MIN_ST + MIN_TD
_3 = MIN_ST + MIN_TD * 2
_4 = MIN_ST + MIN_TD * 3


class TsbMockDataSource(PolarsDataFrameSource):

    def __init__(self):
        df = pl.DataFrame({
            'dt': [_1, _2, _3],
            'a': [1.0, 2.0, 3.0],
            'b': [4, 5, 6]
        })
        super().__init__(df)


def test_tsb_from_dadta_source():
    assert eval_node(tsb_from_data_source, TsbMockDataSource, 'dt') == [
        fd({'a': 1.0, 'b': 4}), fd({'a': 2.0, 'b': 5}), fd({'a': 3.0, 'b': 6})]


class TsdMockDataSource(PolarsDataFrameSource):

    def __init__(self):
        df = pl.DataFrame({
            'dt': [_1, _1, _2, _3],
            'k': [4, 5, 6, 6],
            'v': [1.0, 2.0, 3.0, 4.0],
        })
        super().__init__(df)


def test_k_v_from_data_source():
    assert eval_node(tsd_k_v_from_data_source, TsdMockDataSource, 'dt', 'k') == [
        fd({4: 1.0, 5: 2.0}), fd({6: 3.0}), fd({6: 4.0})]


class TsdTsdMockDataSource(PolarsDataFrameSource):

    def __init__(self):
        df = pl.DataFrame({
            'dt': [_1, _1, _1, _2, _2, _2, _3, _3, _3],
            'k': [4, 4, 5, 5, 6, 6, 6, 6, 6],
            'p': ['a', 'b', 'c', 'a', 'b', 'c', 'a', 'b', 'c'],
            'v': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
        })
        super().__init__(df)


def test_k_tsd_from_data_source():
    assert eval_node(tsd_k_tsd_from_data_source, TsdTsdMockDataSource, 'dt', 'k', 'p') == [
        fd({4: fd({'a': 1.0, 'b': 2.0}), 5: fd({'c': 3.0})}),
        fd({5: fd({'a': 4.0}), 6: fd({'b': 5.0, 'c': 6.0})}),
        fd({6: fd({'a': 7.0, 'b': 8.0, 'c': 9.0})})
    ]


class TsdTsbMockDataSource(PolarsDataFrameSource):

    def __init__(self):
        df = pl.DataFrame({
            'dt': [_1, _2, _2, _3],
            'k': ['a', 'b', 'c', 'd'],
            'p1': [1.0, 2.0, 3.0, 4.0],
            'p2': [4, 5, 6, 7],
        })
        super().__init__(df)


def test_k_b_from_data_source():
    assert eval_node(tsd_k_b_from_data_source, TsdTsbMockDataSource, 'dt', 'k') == [
        fd({'a': fd({'p1': 1.0, 'p2': 4})}),
        fd({'b': fd({'p1': 2.0, 'p2': 5}), 'c': fd({'p1': 3.0, 'p2': 6})}),
        fd({'d': fd({'p1': 4.0, 'p2': 7})}),
    ]
