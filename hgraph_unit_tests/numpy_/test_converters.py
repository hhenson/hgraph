from hgraph import graph, TS, Array, Size, to_window
from hgraph.numpy_ import as_array, get_item
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


def test_get_item_1():
    @graph
    def g(ts: TS[Array[int, Size[3]]]) -> TS[int]:
        return get_item(ts, 1)

    assert eval_node(g, [np.array([1, 2, 3])]) == [2]


def test_get_item_2():
    @graph
    def g(ts: TS[Array[int, Size[3], Size[2]]]) -> TS[Array[int, Size[2]]]:
        return get_item(ts, 1)

    assert (eval_node(g, [np.array([[1, 2], [3, 4], [5, 6]])])[0] == np.array([3, 4])).all()


def test_get_item_3():
    @graph
    def g(ts: TS[Array[int, Size[3], Size[2]]]) -> TS[int]:
        return get_item(ts, (1, 0))

    assert eval_node(g, [np.array([[1, 2], [3, 4], [5, 6]])]) == [3]


def test_get_item_4():
    @graph
    def g(ts: TS[Array[int, Size[2], Size[2], Size[2]]]) -> TS[Array[int, Size[2]]]:
        return get_item(ts, (0, 1))

    assert (eval_node(g, [np.array(
        [
            [
                [1, 2],
                [3, 4]
            ],
            [
                [5, 6],
                [7, 8]
            ]
        ]
    )])[0] == np.array([3, 4])).all()
