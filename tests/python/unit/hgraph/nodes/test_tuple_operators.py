import math
from typing import Tuple

from hgraph import mul_, getitem_, and_, TS, graph, or_, min_, max_, contains_, sum_, mean, std, var
from hgraph.test import eval_node

def test_mul_tuples():
    assert eval_node(mul_, [(1, 2, 3)], [2]) == [(1, 2, 3, 1, 2, 3)]


def test_getitem_tuples():
    assert eval_node(getitem_, [(1, 2, 3)], [1]) == [2]


def test_getitem_fixed_tuple():
    @graph
    def g(a: TS[Tuple[int, int]], i: TS[int]) -> TS[int]:
        return a[i]

    assert eval_node(g, [(1, 2, 3)], [1]) == [2]


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


def test_min_tuple_binary():
    @graph
    def app(ts1: TS[Tuple[int, ...]], ts2: TS[Tuple[int, ...]]) -> TS[Tuple[int, ...]]:
        return min_(ts1, ts2)

    assert eval_node(app, [(10, 100)], [(999,)]) == [(10, 100)]


def test_min_tuple_multi():
    @graph
    def app(ts1: TS[Tuple[int, ...]],
            ts2: TS[Tuple[int, ...]],
            ts3: TS[Tuple[int, ...]]) -> TS[Tuple[int, ...]]:
        return min_(ts1, ts2, ts3)

    assert eval_node(app, [(10, 100)], [(999,)], [(1, 2, 3, 4)]) == [(1, 2, 3, 4)]


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


def test_mean_tuple_unary():
    @graph
    def app(ts: TS[Tuple[int, ...]]) -> TS[float]:
        return mean(ts)

    output = eval_node(app, [(1, 2, 3), ()])
    assert output[0] == 2.0
    assert math.isnan(output[1])


def test_std_tuple_unary():
    @graph
    def app(ts: TS[Tuple[int, ...]]) -> TS[float]:
        return std(ts)

    assert eval_node(app, [(1, 2, 3), ()]) == [1.0, 0.0]


def test_var_tuple_unary():
    @graph
    def app(ts: TS[Tuple[int, ...]]) -> TS[float]:
        return var(ts)

    assert eval_node(app, [(1, 2, 3), ()]) == [1.0, 0.0]


def test_contains_tuple():
    @graph
    def app(lhs: TS[Tuple[int, ...]], rhs: TS[int]) -> TS[bool]:
        return contains_(lhs, rhs)

    assert eval_node(app, [(1, 2, 3), None, ()], [4, 2, 3]) == [False, True, False]