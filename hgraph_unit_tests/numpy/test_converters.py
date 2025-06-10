from hgraph import graph, TS, Array, Size, to_window
from hgraph.numpy import as_array
from hgraph.test import eval_node
import numpy as np


def test_as_array():

    @graph
    def g(ts: TS[int]) -> TS[Array[int, Size[3]]]:
        w = to_window(ts, 3, 3)
        return as_array(w)

    actual = eval_node(g, [1, 2, 3])
    assert actual[:2] == [None, None]
    assert (actual[2] == np.array([1, 2, 3])).all()

def test_as_array_min_size():

    @graph
    def g(ts: TS[int]) -> TS[Array[int, Size[3]]]:
        w = to_window(ts, 3, 2)
        return as_array(w)

    actual = eval_node(g, [1, 2, 3])
    assert actual[:1] == [None]
    assert (actual[1] == np.array([1, 2, 0])).all()
    assert (actual[2] == np.array([1, 2, 3])).all()