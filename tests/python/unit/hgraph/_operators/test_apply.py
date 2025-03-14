from typing import Callable

from hgraph import graph, apply, TS, const
from hgraph.test import eval_node


def test_apply_arg():

    @graph
    def g(i: TS[int]) -> TS[int]:
        fn = const(lambda x: x + 1, TS[Callable])
        return apply[TS[int]](fn, i)

    assert eval_node(g, [1, 2, 3]) == [2, 3, 4]


def test_apply_karg():

    @graph
    def g(i: TS[int]) -> TS[int]:
        fn = const(lambda x: x + 1, TS[Callable])
        return apply[TS[int]](fn, x=i)

    assert eval_node(g, [1, 2, 3]) == [2, 3, 4]

def test_apply_arg_kwarg():

    @graph
    def g(i: TS[int]) -> TS[int]:
        fn = const(lambda x, y: x + y, TS[Callable])
        return apply[TS[int]](fn, i, y=1)

    assert eval_node(g, [1, 2, 3]) == [2, 3, 4]