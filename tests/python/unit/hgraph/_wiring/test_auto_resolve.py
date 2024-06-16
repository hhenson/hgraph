from typing import Callable

from hgraph import graph, TSL, TS, SIZE, Size, AUTO_RESOLVE, SCALAR, SCALAR_1, compute_node
from hgraph import const
from hgraph.test import eval_node


def test_auto_resolve():

    @graph
    def g(tsl: TSL[TS[int], SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TS[int]:
        return const(_sz.SIZE)

    assert eval_node(g, [(1, 2)], resolution_dict={"tsl": TSL[TS[int], Size[2]]}) == [2]


def test_func_resolve():
    def x(x) -> str:
        return str(x)

    @compute_node(resolvers={SCALAR_1: lambda mapping, scalars: scalars['f'].__annotations__['return']})
    def call(ts: TS[SCALAR], f: type(x)) -> TS[SCALAR_1]:
        return f(ts.value)

    assert eval_node(call[SCALAR: int], [1, 2], f=x) == ['1', '2']