from enum import Enum

from hgraph import TS, graph, min_, max_, ENUM
from hgraph.test import eval_node


class TestEnum(Enum):
    A = 1
    B = 2


def test_eq_enums():
    @graph
    def app(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
        return lhs == rhs

    assert eval_node(app, [TestEnum.A, None], [TestEnum.B, TestEnum.A]) == [False, True]


def test_min_enums_unary():
    @graph
    def app(ts: TS[ENUM]) -> TS[ENUM]:
        return min_(ts)

    assert eval_node(app, [TestEnum.B, TestEnum.A]) == [TestEnum.B, TestEnum.A]


def test_min_enums_binary():
    @graph
    def app(ts1: TS[ENUM], ts2: TS[ENUM]) -> TS[ENUM]:
        return min_(ts1, ts2)

    assert eval_node(app, [TestEnum.B, TestEnum.A], [TestEnum.A, TestEnum.B]) == [TestEnum.A, TestEnum.A]


def test_min_enums_multi():
    @graph
    def app(i1: TS[ENUM], i2: TS[ENUM], i3: TS[ENUM], i4: TS[ENUM]) -> TS[ENUM]:
        return min_(i1, i2, i3, i4)

    assert eval_node(app, TestEnum.B, TestEnum.A, TestEnum.A, TestEnum.B) == [TestEnum.A]


def test_max_enums_unary():
    @graph
    def app(ts: TS[ENUM]) -> TS[ENUM]:
        return max_(ts)

    assert eval_node(app, [TestEnum.B, TestEnum.A]) == [TestEnum.B, None]


def test_max_enums_binary():
    @graph
    def app(ts1: TS[ENUM], ts2: TS[ENUM]) -> TS[ENUM]:
        return max_(ts1, ts2)

    assert eval_node(app, [TestEnum.B, TestEnum.A], [TestEnum.A, TestEnum.B]) == [TestEnum.B, TestEnum.B]


def test_max_enums_multi():
    @graph
    def app(i1: TS[ENUM], i2: TS[ENUM], i3: TS[ENUM], i4: TS[ENUM]) -> TS[ENUM]:
        return max_(i1, i2, i3, i4)

    assert eval_node(app, TestEnum.B, TestEnum.A, TestEnum.A, TestEnum.B) == [TestEnum.B]


def test_lt_enums():
    @graph
    def app(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
        return lhs < rhs

    assert eval_node(app, [TestEnum.A, None], [TestEnum.B, TestEnum.A]) == [True, False]


def test_le_enums():
    @graph
    def app(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
        return lhs <= rhs

    assert eval_node(app, [TestEnum.A, None], [TestEnum.B, TestEnum.A]) == [True, True]


def test_gt_enums():
    @graph
    def app(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
        return lhs > rhs

    assert eval_node(app, [TestEnum.A, None], [TestEnum.B, TestEnum.A]) == [False, False]


def test_ge_enums():
    @graph
    def app(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
        return lhs >= rhs

    assert eval_node(app, [TestEnum.A, None], [TestEnum.B, TestEnum.A]) == [False, True]