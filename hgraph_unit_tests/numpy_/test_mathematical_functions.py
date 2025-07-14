from hgraph import Array, Size
from hgraph.numpy_ import cumsum, ARRAY
from hgraph.test import eval_node
import numpy as nd

def test_cumsum_1():

    actual = eval_node(cumsum[ARRAY: Array[float, Size[3]]], [nd.array([1, 2, 3])])
    assert (actual[0] == nd.array([1, 3, 6])).all()


def test_cumsum_2():

    actual = eval_node(cumsum[ARRAY: Array[float, Size[3]]], [nd.array([1, 2, 3])], 0)
    assert (actual[0] == nd.array([1, 3, 6])).all()