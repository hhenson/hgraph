from hgraph.nodes import rolling_window
from hgraph.test import eval_node


def test_public_window_alias_remains_wirable():
    result = eval_node(rolling_window, [1, 2, 3], 2)
    assert result[0] is None
    assert result[1]["buffer"] == (1, 2)
    assert result[2]["buffer"] == (2, 3)
