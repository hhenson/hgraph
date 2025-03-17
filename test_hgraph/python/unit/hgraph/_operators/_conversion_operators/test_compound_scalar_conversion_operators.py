from dataclasses import dataclass

from hgraph import graph, TS, combine, CompoundScalar, TSB, convert
from hgraph.test import eval_node


def test_combine_cs():
    @dataclass
    class AB(CompoundScalar):
        a: int
        b: str = 'b'

    @graph
    def g(a: TS[int], b: TS[str]) -> TS[AB]:
        return combine[TS[AB]](a=a, b=b)

    assert eval_node(g, [None, 1], 'a') == [None, AB(a=1, b='a')]

    @graph
    def h(a: TS[int], b: TS[str]) -> TS[AB]:
        return combine[TS[AB]](a=a, b=b, __strict__=False)

    assert eval_node(h, [None, 1], 'a') == [AB(a=None, b='a'), AB(a=1, b='a')]

    @graph
    def u(a: TS[int]) -> TS[AB]:
        return combine[TS[AB]](a=a, b='a')

    assert eval_node(u, [None, 1]) == [None, AB(a=1, b='a')]

    @graph
    def v(a: TS[int]) -> TS[AB]:
        return combine[TS[AB]](a=a)

    assert eval_node(v, [None, 1]) == [None, AB(a=1, b='b')]


def test_convert_cs():
    @dataclass
    class AB(CompoundScalar):
        a: int
        b: str

    @graph
    def g(x: TSB[AB]) -> TS[AB]:
        return convert[TS[AB]](x)

    assert eval_node(g, [dict(a=None, b='a'), dict(a=1)]) == [None, AB(a=1, b='a')]

    @graph
    def h(x: TSB[AB]) -> TS[AB]:
        return convert[TS[CompoundScalar]](x)

    assert eval_node(h, [dict(a=None, b='a'), dict(a=1)]) == [None, AB(a=1, b='a')]

    @graph
    def g1(x: TSB[AB]) -> TS[AB]:
        return convert[TS[AB]](x, __strict__=False)

    assert eval_node(g1, [dict(b='a')]) == [AB(a=None, b='a')]

    @graph
    def h1(x: TSB[AB]) -> TS[AB]:
        return convert[TS[CompoundScalar]](x, __strict__=False)

    assert eval_node(h1, [dict(b='a')]) == [AB(a=None, b='a')]
