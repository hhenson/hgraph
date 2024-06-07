from typing import Tuple

from hgraph import SCALAR, mul_, getitem_, and_, TIME_SERIES_TYPE, TS, graph, or_, min_, max_, contains_, sum_
from hgraph.nodes._tuple_operators import unroll
from hgraph.test import eval_node


def test_unroll():
    assert eval_node(unroll[SCALAR: int], [(1, 2, 3), (4,), None, None, (5, 6)]) == [1, 2, 3, 4, 5, 6]


def test_mul_tuples():
    assert eval_node(mul_, [(1, 2, 3)], [2]) == [(1, 2, 3, 1, 2, 3)]


def test_getitem_tuples():
    assert eval_node(getitem_, [(1, 2, 3)], [1]) == [2]


def test_and_tuples():
    @graph
    def app(lhs: TS[Tuple[int, ...]], rhs: TS[Tuple[int, ...]]) -> TS[bool]:
        return and_(lhs, rhs)

    assert eval_node(app, [(1, 2), ()],  [(3, 4), (3, 4)]) == [True, False]


def test_or_tuples():
    @graph
    def app(lhs: TS[Tuple[int, ...]], rhs: TS[Tuple[int, ...]]) -> TS[bool]:
        return or_(lhs, rhs)

    assert eval_node(app, [(1, 2), (), ()],  [(3, 4), (3, 4), ()]) == [True, True, False]


def test_min_tuple_unary():
    @graph
    def app(ts: TS[Tuple[int, ...]]) -> TS[int]:
        return min_(ts)

    assert eval_node(app, [(6, 3, 4, 5, 8)]) == [3]


def test_min_tuple_unary_default():
    @graph
    def app(ts: TS[Tuple[int, ...]]) -> TS[int]:
        return min_(ts, default_value=-1)

    assert eval_node(app, [()]) == [-1]


def test_max_tuple_unary():
    @graph
    def app(ts: TS[Tuple[int, ...]]) -> TS[int]:
        return max_(ts)

    assert eval_node(app, [(6, 3, 4, 5, 8)]) == [8]


def test_max_tuple_unary_default():
    @graph
    def app(ts: TS[Tuple[int, ...]]) -> TS[int]:
        return max_(ts, default_value=-1)

    assert eval_node(app, [()]) == [-1]


def test_sum_tuple_unary():
    @graph
    def app(ts: TS[Tuple[int, ...]]) -> TS[int]:
        return sum_(ts)

    assert eval_node(app, [(1, 2, 3), ()]) == [6, 0]


def test_contains_tuple():
    @graph
    def app(lhs: TS[Tuple[int, ...]], rhs: TS[int]) -> TS[bool]:
        return contains_(lhs, rhs)

    assert eval_node(app, [(1, 2, 3), None, ()], [4, 2, 3]) == [False, True, False]