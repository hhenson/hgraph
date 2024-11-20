import pytest

from hgraph import MIN_ST, MIN_TD, Size, TS, Array, BuffSize, SCALAR, graph, compute_node, TIME_SERIES_TYPE, BUFF
from hgraph.nodes._numpy import np_rolling_window, np_quantile, rolling_window_exp
from hgraph.test import eval_node
import numpy as np


@pytest.mark.parametrize(
    ["values", "sz", "expected"],
    [
        [[1, 2, 3, 4, 5], Size[3], [
            None,
            None,
            {'buffer': np.array((1, 2, 3)), 'index': np.array((MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD))},
            {'buffer': np.array((2, 3, 4)),
             'index': np.array((MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD))},
            {'buffer': np.array((3, 4, 5)),
             'index': np.array((MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD, MIN_ST + 4 * MIN_TD))},
        ]],
        [[1.0, 2.0, 3.0, 4.0, 5.0], Size[3], [
            None,
            None,
            {'buffer': np.array((1.0, 2.0, 3.0)), 'index': np.array((MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD))},
            {'buffer': np.array((2.0, 3.0, 4.0)),
             'index': np.array((MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD))},
            {'buffer': np.array((3.0, 4.0, 5.0)),
             'index': np.array((MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD, MIN_ST + 4 * MIN_TD))},
        ]],
    ]
)
def test_np_rolling_window(values, sz, expected):
    result = eval_node(np_rolling_window, values, sz)
    assert len(result) == len(expected)
    assert all((r['buffer'] == e['buffer']).all() for r, e in zip(result, expected) if
               not (r is None and e is None))


def test_np_quantile():
    assert eval_node(np_quantile, [np.array([1, 2])], 0.8, resolution_dict={'ts': TS[Array[int, Size[2]]]}) == [1.8]


def test_rolling_window_delta():
    result = eval_node(rolling_window_exp[SCALAR: int], [1, 2, 3, 4, 5], BuffSize[3])
    assert result == [None, None, 3, 4, 5]

def test_rolling_window_value():

    @compute_node
    def _as_value(ts: BUFF[int, BuffSize[3]]) -> TS[Array[int, Size[3]]]:
        return ts.value

    @graph
    def g(ts: TS[int]) -> TS[Array[int, Size[3]]]:
        return _as_value(rolling_window_exp(ts, BuffSize[3]))

    result = eval_node(g, [1, 2, 3, 4, 5])
    assert all((a == b).all() for a, b in zip(result, [None, None, np.array((1.0, 2.0, 3.0)), np.array((2, 3, 4)), np.array((3, 4, 5))]) if
               not (a is None and b is None))
