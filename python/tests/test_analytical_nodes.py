from hgraph.nodes import rolling_average, rolling_window
from hgraph.test import eval_node


def test_public_window_aliases_remain_wirable():
    assert eval_node(rolling_average, [1, 2, 3, 4], 2) == [None, None, 2.5, 3.5]
    result = eval_node(rolling_window, [1, 2, 3], 2)
    assert result[0] is None
    assert result[1]["buffer"] == (1, 2)
    assert result[2]["buffer"] == (2, 3)
