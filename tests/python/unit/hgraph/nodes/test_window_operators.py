from hgraph import MIN_ST, MIN_TD, TS
from hgraph.nodes import window, lag, count, accumulate, average
from hgraph.nodes._window_operators import diff, rolling_average
from hgraph.test import eval_node


def test_cyclic_operator():
    expected = [
        None,
        None,
        {'buffer': (1, 2, 3), 'index': (MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD)},
        {'buffer': (2, 3, 4), 'index': (MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD)},
        {'buffer': (3, 4, 5), 'index': (MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD, MIN_ST + 4 * MIN_TD)},
    ]

    assert eval_node(window, [1, 2, 3, 4, 5], 3) == expected


def test_time_delta_operator():
    expected = [
        {'buffer': (1,), 'index': (MIN_ST,)},
        {'buffer': (1, 2,), 'index': (MIN_ST, MIN_ST + MIN_TD,)},
        {'buffer': (1, 2, 3), 'index': (MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD)},
        {'buffer': (2, 3, 4), 'index': (MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD)},
        {'buffer': (3, 4, 5), 'index': (MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD, MIN_ST + 4 * MIN_TD)},
    ]

    assert eval_node(window, [1, 2, 3, 4, 5], MIN_TD * 2, False) == expected


def test_tick_lag():
    expected = [
        None,
        None,
        None,
        1,
        2,
    ]

    assert eval_node(lag, [1, 2, 3, 4, 5], 3) == expected


def test_tick_lag_time_delta():
    expected = [
        None,
        None,
        1,
        2,
        3,
        4,
        5,
    ]

    assert eval_node(lag, [1, 2, 3, 4, 5], MIN_TD*2) == expected


def test_count():
    expected = [
        1,
        2,
        3,
    ]

    assert eval_node(count, [3, 2, 1,], resolution_dict={'ts': TS[int]}) == expected


def test_accumulate():
    expected = [
        1,
        3,
        6,
        10,
    ]

    assert eval_node(accumulate, [1, 2, 3, 4,]) == expected


def test_average():
    expected = [
        1,
        1.5,
        2,
        2.5,
    ]

    assert eval_node(average, [1, 2, 3, 4,]) == expected


def test_diff():
    expected = [
        None,
        1,
        1,
        1,
    ]

    assert eval_node(diff, [1, 2, 3, 4,]) == expected


def test_rolling_average_int():
    expected = [
        None,
        None,
        None,
        3.0,
        4.0,
    ]

    assert eval_node(rolling_average, [1, 2, 3, 4, 5], 3) == expected

    
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