import numpy as np

from hgraph import TS, Array, Size, graph
from hgraph.numpy import corrcoef
from hgraph.test import eval_node


def test_corrcoef_1():

    @graph
    def g(ts: TS[Array[float, Size[4]]]) -> TS[float]:
        return corrcoef(ts)

    actual = eval_node(g, [np.array([1, 2, 3, 4])])
    assert actual == [1.0]


def test_corrcoef_2():

    @graph
    def g(ts: TS[Array[float, Size[2], Size[4]]]) -> TS[Array[float, Size[2], Size[2]]]:
        return corrcoef(ts)

    actual = eval_node(g, [np.array([[1, 2, 3, 4],[1, 2, 3, 4]])])
    assert (actual[0] == np.array([[1.0, 1.0], [1.0, 1.0]])).all()


def test_corrcoef_3():

    @graph
    def g(ts: TS[Array[float, Size[4]]]) -> TS[Array[float, Size[2], Size[2]]]:
        return corrcoef(ts, ts)

    actual = eval_node(g, [np.array([1, 2, 3, 4])])
    assert (actual[0] == np.array([[1.0, 1.0], [1.0, 1.0]])).all()
    