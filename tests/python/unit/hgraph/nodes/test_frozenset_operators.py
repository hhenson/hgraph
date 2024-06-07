from hgraph import TS, graph, and_, KEYABLE_SCALAR, or_, sum_, str_, min_, max_
from hgraph.test import eval_node


def test_and_frozensets():
    @graph
    def app(lhs: TS[frozenset[KEYABLE_SCALAR]], rhs: TS[frozenset[KEYABLE_SCALAR]]) -> TS[bool]:
        return and_(lhs, rhs)

    assert eval_node(app, [frozenset({1, 2}), frozenset()],
                          [frozenset({3, 4}), frozenset({3, 4})]) == [True, False]


def test_or_frozensets():
    @graph
    def app(lhs: TS[frozenset[KEYABLE_SCALAR]], rhs: TS[frozenset[KEYABLE_SCALAR]]) -> TS[bool]:
        return or_(lhs, rhs)

    assert eval_node(app, [frozenset({1, 2}), frozenset()],
                          [frozenset({3, 4}), frozenset({3, 4})]) == [True, True]


def test_min_frozenset_unary():
    @graph
    def app(ts: TS[frozenset[int]]) -> TS[int]:
        return min_(ts)

    assert eval_node(app, [frozenset({1, 2, -1})]) == [-1]


def test_min_frozenset_unary_default():
    @graph
    def app(ts: TS[frozenset[int]]) -> TS[int]:
        return min_(ts, default_value=-1)

    assert eval_node(app, [frozenset({})]) == [-1]


def test_max_frozenset_unary():
    @graph
    def app(ts: TS[frozenset[int]]) -> TS[int]:
        return max_(ts)

    assert eval_node(app, [frozenset({1, 2, -1})]) == [2]


def test_max_frozenset_unary_default():
    @graph
    def app(ts: TS[frozenset[int]]) -> TS[int]:
        return max_(ts, default_value=-1)

    assert eval_node(app, [frozenset({})]) == [-1]


def test_sum_frozenset_unary():
    @graph
    def app(ts: TS[frozenset[int]]) -> TS[int]:
        return sum_(ts)

    assert eval_node(app, [frozenset({1, 2}), frozenset()]) == [3, 0]


def test_str_frozenset():
    @graph
    def app(ts: TS[frozenset[int]]) -> TS[str]:
        return str_(ts)

    assert eval_node(app, [frozenset({1, 2})]) == ["{1, 2}"]







