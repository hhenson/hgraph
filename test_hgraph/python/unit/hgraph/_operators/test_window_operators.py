import pytest

from hgraph import MIN_TD
from hgraph.nodes._window_operators import rolling_average
from hgraph.test import eval_node


@pytest.mark.parametrize(
    ["ts", "period", "min_period", "expected"],
    [
        [[1, 2, 3, 4, 5], 3, None, [None, None, None, 3.0, 4.0,]],
        [[1.0, 2.0, 3.0, 4.0, 5.0], 3, None, [None, None, None, 3.0, 4.0,]],
        [[1, 2, 3, 4, 5], 3, 2, [None, 1.5, 2.0, 3.0, 4.0,]],
        [[1.0, 2.0, 3.0, 4.0, 5.0], 3, 2, [None, 1.5, 2.0, 3.0, 4.0,]],
    ]
)
def test_rolling_average_int(ts, period, min_period, expected):
    assert eval_node(rolling_average, ts, period, min_period) == expected

    
def test_rolling_average_time_delta():
    expected = [
        None,
        None,
        None,
        3.0,
        4.0,
        4.5,
        5.0,
        float('NaN'),
    ]
    # Note NaN does not match equal without some assistance, so for now just drop it
    assert eval_node(rolling_average, [1, 2, 3, 4, 5], MIN_TD*3)[:-1] == expected[:-1]
