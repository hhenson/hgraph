from typing import Tuple

from hgraph import compute_node, TS
from hgraph.test import eval_node


def test_valid_lambda():
    @compute_node(valid=lambda m, s: None if s["__strict__"] else ())
    def f(x: TS[int], y: TS[int], __strict__: bool) -> TS[Tuple[int, int]]:
        return x.value, y.value

    assert eval_node(f, [None, 2], [3, 4], __strict__=True) == [None, (2, 4)]
    assert eval_node(f, [None, 2], [3, 4], __strict__=False) == [(None, 3), (2, 4)]


def test_active_lambda():
    @compute_node(active=lambda m, s: {"x"} if s["__lazy__"] else None, valid=())
    def f(x: TS[int], y: TS[int], __lazy__: bool = False) -> TS[Tuple[int, int]]:
        return x.value, y.value

    assert eval_node(f, [1, None], [3, 4]) == [(1, 3), (1, 4)]
    assert eval_node(f, [1, None], [3, 4], __lazy__=True) == [(1, 3), None]
