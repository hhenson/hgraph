from enum import Enum

from hgraph import TS, graph, min_, max_, ENUM, nothing
from hgraph.test import eval_node


class _TestEnum(Enum):
    A = 1
    B = 2


def test_eq_enums():
    @graph
    def app(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
        return lhs == rhs

    assert eval_node(app, [_TestEnum.A, None], [_TestEnum.B, _TestEnum.A]) == [False, True]


def test_min_enums_unary():
    @graph
    def app(ts: TS[ENUM]) -> TS[ENUM]:
        return min_(ts)

    assert eval_node(app, [_TestEnum.B, _TestEnum.A]) == [_TestEnum.B, _TestEnum.A]


def test_min_enums_binary():
    @graph
    def app(ts1: TS[ENUM], ts2: TS[ENUM]) -> TS[ENUM]:
        return min_(ts1, ts2)

    assert eval_node(app, [_TestEnum.B, _TestEnum.A], [_TestEnum.A, _TestEnum.B]) == [_TestEnum.A, _TestEnum.A]


def test_min_enums_binary_not_strict():
    @graph
    def app(ts1: TS[ENUM]) -> TS[ENUM]:
        return min_(ts1, nothing(TS[_TestEnum]), __strict__=False)

    assert eval_node(app, [_TestEnum.B, _TestEnum.A]) == [_TestEnum.B, _TestEnum.A]


def test_min_enums_multi():
    @graph
    def app(i1: TS[ENUM], i2: TS[ENUM], i3: TS[ENUM], i4: TS[ENUM]) -> TS[ENUM]:
        return min_(i1, i2, i3, i4)

    assert eval_node(app, _TestEnum.B, _TestEnum.A, _TestEnum.A, _TestEnum.B) == [_TestEnum.A]


def test_min_enums_multi_non_strict():
    @graph
    def app(i1: TS[ENUM], i2: TS[ENUM], i3: TS[ENUM]) -> TS[ENUM]:
        return min_(i1, i2, i3, nothing(TS[_TestEnum]), __strict__=False)

    assert eval_node(app, _TestEnum.B, _TestEnum.A, _TestEnum.A) == [_TestEnum.A]


def test_min_enums_multi_strict_not_all_valid():
    @graph
    def app(i1: TS[ENUM], i2: TS[ENUM], i3: TS[ENUM]) -> TS[ENUM]:
        return min_(i1, i2, i3, nothing(TS[_TestEnum]))

    assert eval_node(app, _TestEnum.B, _TestEnum.A, _TestEnum.A) is None


def test_max_enums_unary():
    @graph
    def app(ts: TS[ENUM]) -> TS[ENUM]:
        return max_(ts)

    assert eval_node(app, [_TestEnum.B, _TestEnum.A]) == [_TestEnum.B, None]


def test_max_enums_binary():
    @graph
    def app(ts1: TS[ENUM], ts2: TS[ENUM]) -> TS[ENUM]:
        return max_(ts1, ts2)

    assert eval_node(app, [_TestEnum.B, _TestEnum.A], [_TestEnum.A, _TestEnum.B]) == [_TestEnum.B, _TestEnum.B]


def test_max_enums_binary_not_strict():
    @graph
    def app(ts1: TS[ENUM]) -> TS[ENUM]:
        return max_(ts1, nothing(TS[_TestEnum]), __strict__=False)

    assert eval_node(app, [_TestEnum.B, _TestEnum.A]) == [_TestEnum.B, _TestEnum.A]


def test_max_enums_multi():
    @graph
    def app(i1: TS[ENUM], i2: TS[ENUM], i3: TS[ENUM], i4: TS[ENUM]) -> TS[ENUM]:
        return max_(i1, i2, i3, i4)

    assert eval_node(app, _TestEnum.B, _TestEnum.A, _TestEnum.A, _TestEnum.B) == [_TestEnum.B]


def test_max_enums_multi_non_strict():
    @graph
    def app(i1: TS[ENUM], i2: TS[ENUM], i3: TS[ENUM]) -> TS[ENUM]:
        return max_(i1, i2, i3, nothing(TS[_TestEnum]), __strict__=False)

    assert eval_node(app, _TestEnum.B, _TestEnum.A, _TestEnum.A) == [_TestEnum.B]


def test_max_enums_multi_strict_not_all_valid():
    @graph
    def app(i1: TS[ENUM], i2: TS[ENUM], i3: TS[ENUM]) -> TS[ENUM]:
        return max_(i1, i2, i3, nothing(TS[_TestEnum]))

    assert eval_node(app, _TestEnum.B, _TestEnum.A, _TestEnum.A) is None


def test_lt_enums():
    @graph
    def app(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
        return lhs < rhs

    assert eval_node(app, [_TestEnum.A, None], [_TestEnum.B, _TestEnum.A]) == [True, False]


def test_le_enums():
    @graph
    def app(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
        return lhs <= rhs

    assert eval_node(app, [_TestEnum.A, None], [_TestEnum.B, _TestEnum.A]) == [True, True]


def test_gt_enums():
    @graph
    def app(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
        return lhs > rhs

    assert eval_node(app, [_TestEnum.A, None], [_TestEnum.B, _TestEnum.A]) == [False, False]


def test_ge_enums():
    @graph
    def app(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
        return lhs >= rhs

    assert eval_node(app, [_TestEnum.A, None], [_TestEnum.B, _TestEnum.A]) == [False, True]


def test_enum_name():
    @graph
    def app(enum: TS[ENUM]) -> TS[str]:
        return enum.name

    assert eval_node(app, [_TestEnum.A])[0] == "A"
