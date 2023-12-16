from hgraph import MIN_ST, MIN_TD
from hgraph.nodes import window
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
