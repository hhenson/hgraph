from hgraph import round_, graph, TS, call
from hgraph.test import eval_node


def test_round():

    assert eval_node(round_, [1.23456789, 1.235], [2]) == [1.23, 1.24]


def test_call():

    @graph
    def g(v: TS[str]):
        call(print, v)

    eval_node(g, ["hello", "world"])
