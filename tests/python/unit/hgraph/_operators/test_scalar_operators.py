from datetime import timedelta, date, datetime
from typing import Tuple

import pytest
from frozendict import frozendict

from hgraph import WiringError, add_, sub_, mul_, lshift_, rshift_, bit_and, bit_or, bit_xor, eq_, neg_, pos_, TS, \
    abs_, len_, and_, or_, min_, max_, graph, str_, invert_, sum_, lt_, gt_, le_, ge_, mean, std, var
from hgraph.test import eval_node


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([1, 2, 3],                 [4, 5, 6],              [5, 7, 9]),
    ([1.0, 2.0, 3.0],           [4.0, 5.0, 6.0, 7.0],   [5.0, 7.0, 9.0, 10.0]),
    ([timedelta(seconds=1)],    [timedelta(hours=1)],   [timedelta(seconds=3601)]),
    ([None, 2, None, 4],        [4, 5, 6],              [None, 7, 8, 10]),
    ([None, 1],                 [1, 1],                 [None, 2]),
])
def test_add_scalars(lhs, rhs, expected):
    assert eval_node(add_, lhs, rhs) == expected
    assert eval_node(add_, rhs, lhs) == expected


def test_add_str():
    assert eval_node(add_, ["abc"], ["xyz"]) == ["abcxyz"]


def test_add_tuples():
    assert eval_node(add_, [(1, 2)], [(3, 4)]) == [(1, 2, 3, 4)]


def test_add_fail():
    with pytest.raises(WiringError) as e:
        assert eval_node(add_, [1, 2, 3], ["1", "2", "3"])
    assert "Cannot wire overload" in str(e)


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([1, 2, 3],                 [4, 5, 6],              [-3, -3, -3]),
    ([1.0, 2.0, 3.0],           [4.0, 5.0, 6.0, 7.0],   [-3.0, -3.0, -3.0, -4.0]),
    ([timedelta(hours=1)],    [timedelta(seconds=1)],   [timedelta(seconds=3599)]),
    ([None, 2, None, 4],        [4, 5, 6],              [None, -3, -4, -2]),
    ([None, 1],                 [1, 1],                 [None, 0]),
])
def test_sub_scalars(lhs, rhs, expected):
    assert eval_node(sub_, lhs, rhs) == expected


def test_sub_fail():
    with pytest.raises(WiringError) as e:
        eval_node(sub_, ["x"], ["y"])
    assert "Cannot subtract one string from another" in str(e)


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([1, 2, 3], [6, 5, 4], [-5, -3, -1]),
    ([1, 2, 3], [6, 5, 4, 3], [-5, -3, -1, 0]),
    ([None, 2, None, 4], [4, 5, 6], [None, -3, -4, -2]),
])
def test_sub_scalars(lhs, rhs, expected):
    assert eval_node(sub_, lhs, rhs) == expected


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([1, 2, 3], [6, 5, 4], [6, 10, 12]),
    ([1, 2, 3], [6, 5, 4, 3], [6, 10, 12, 9]),
    ([None, 2, None, 4], [4, 5, 6], [None, 10, 12, 24]),
])
def test_mult_scalars(lhs, rhs, expected):
    assert eval_node(mul_, lhs, rhs) == expected


def test_lshift_scalars():
    assert eval_node(lshift_, [3], [2]) == [12]


def test_rshift_scalars():
    assert eval_node(rshift_, [64], [2]) == [16]


@pytest.mark.parametrize(
"lhs,rhs,expected",
[
    (65, 64, 64),
    (True, True, True),
    (True, False, False),
    (False, True, False),
    (False, False, False),
])
def test_bit_and_scalars(lhs, rhs, expected):
    assert eval_node(bit_and, [lhs], [rhs]) == [expected]


@pytest.mark.parametrize(
"lhs,rhs,expected",
[
    (8, 3, 11),
    (True, True, True),
    (True, False, True),
    (False, True, True),
    (False, False, False),
])
def test_bit_or_scalars(lhs, rhs, expected):
    assert eval_node(bit_or, [lhs], [rhs]) == [expected]


@pytest.mark.parametrize(
"lhs,rhs,expected",
[
    (5, 13, 8),
    (True, True, False),
    (True, False, True),
    (False, True, True),
    (False, False, False),
])
def test_bit_xor_scalars(lhs, rhs, expected):
    assert eval_node(bit_xor, [lhs], [rhs]) == [expected]


def test_sub_frozensets():
    assert eval_node(sub_, [frozenset({1, 2})], [frozenset({2, 3})]) == [frozenset({1,})]


def test_bit_and_frozensets():
    assert eval_node(bit_and, [frozenset({1, 2})], [frozenset({2, 3})]) == [frozenset({2,})]


def test_bit_or_frozensets():
    assert eval_node(bit_or, [frozenset({1, 2})], [frozenset({2, 3})]) == [frozenset({1, 2, 3})]


def test_bit_xor_frozensets():
    assert eval_node(bit_xor, [frozenset({1, 2})], [frozenset({2, 3})]) == [frozenset({1, 3})]


def test_bit_or_frozendicts():
    assert eval_node(bit_or, [frozendict({'1': 10, '2': 20})], [frozendict({'2': 20, '3': 30})]) == [frozendict({'1': 10, '2': 20, '3': 30})]


@pytest.mark.parametrize(
"lhs,rhs,expected_equal",
[(1, 1, True),
 (1, 0, False),
 (1 / 10, 0.1, True),
 (1 / 10, 0.11, False),
 (1, 1.0, True),
 (1.0, 1, True),
 (True, False, False),
 (True, True, True),
 (False, True, False),
 (False, False, True),
 ("x", "x", True),
 ("x", "X", False),
 (date(2024, 1, 1), date(2024, 1, 1), True),
 (datetime(2024, 1, 1, 9, 0), datetime(2024, 1, 1, 9, 0), True),
 (timedelta(seconds=3600), timedelta(hours=1), True),
 (timedelta(seconds=3601), timedelta(hours=1), False),
 ((1, 2, 3), (1, 2, 3), True),
 ((1, 2, 3), (1, 2, 3, 4), False),
 ({1, 2, 3}, {1, 2, 3}, True),
 ({1, 2, 3}, {1, 2, 3, 4}, False),
 ({1: 1}, {1: 1}, True),
 ({1: 1}, {1: 2}, False),
])
def test_eq_scalars(lhs, rhs, expected_equal):
    assert eval_node(eq_, [lhs], [rhs]) == [expected_equal]


def test_neg_scalars():
    assert eval_node(neg_, [1]) == [-1]
    assert eval_node(neg_, [1.0]) == [-1.0]


def test_pos_scalars():
    assert eval_node(pos_, [1]) == [1]
    assert eval_node(pos_, [1.0]) == [1.0]


def test_invert_scalars():
    assert eval_node(invert_, [True, False]) == [-2, -1]
    assert eval_node(invert_, [1, 0, 12, -1]) == [-2, -1, -13, 0]


def test_abs_scalars():
    assert eval_node(abs_, [-100, 0, 100]) == [100, 0, 100]
    assert eval_node(abs_, [-100.0, -0.0, 100.0]) == [100.0, 0.0, 100.0]


@pytest.mark.parametrize(
    ['tp', 'values', 'expected'],
    [
        (TS[Tuple[int, ...]], [tuple(), (1,), (1, 2)], [0, 1, 2]),
        (TS[frozenset[int]],  [frozenset(), frozenset({1}), frozenset({1, 2})], [0, 1, 2]),
        (TS[frozendict[int, int]],  [frozendict(), frozendict({1: 1}), frozendict({1: 1, 2: 2})], [0, 1, 2]),
    ]
)
def test_len_scalar(tp, expected, values):
    assert eval_node(len_, values, resolution_dict={'ts': tp}) == expected


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([1, 1, 0, 0],  [0, 1, 0, 1],  [False, True, False, False]),
    ([True, True, False, False],  [False, True, False, True],  [False, True, False, False]),
    (["True", "True", "", ""],  ["", "True", "", "True"],  [False, True, False, False]),
])
def test_and_scalars(lhs, rhs, expected):
    assert eval_node(and_, lhs, rhs) == expected


@pytest.mark.parametrize("lhs,rhs,expected", [
    ([1, 1, 0, 0],  [0, 1, 0, 1],  [True, True, False, True]),
    ([True, True, False, False],  [False, True, False, True],  [True, True, False, True]),
    (["True", "True", "", ""],  ["", "True", "", "True"],  [True, True, False, True]),
])
def test_or_scalars(lhs, rhs, expected):
    assert eval_node(or_, lhs, rhs) == expected


def test_min_scalars_unary():
    @graph
    def app(ts: TS[int]) -> TS[int]:
        return min_(ts)

    assert eval_node(app, [4, 5, 3, 6, 2]) == [4, None, 3, None, 2]


def test_min_scalars_binary():
    assert eval_node(min_, [1, 2, 3], [3, 2, 1]) == [1, 2, 1]


def test_min_scalar_multi():
    @graph
    def app(i1: TS[int], i2: TS[int], i3: TS[int], i4: TS[int]) -> TS[int]:
        return min_(i1, i2, i3, i4)

    assert eval_node(app, [4], [5], [3], [6]) == [3]


def test_max_scalars_unary():
    @graph
    def app(ts: TS[int]) -> TS[int]:
        return max_(ts)

    assert eval_node(app, [4, 5, 3, 6, 2]) == [4, 5, None, 6, None]


def test_max_scalars_binary():
    assert eval_node(max_, [1, 2, 3], [3, 2, 1]) == [3, 2, 3]


def test_max_scalar_multi():
    @graph
    def app(i1: TS[int], i2: TS[int], i3: TS[int], i4: TS[int]) -> TS[int]:
        return max_(i1, i2, i3, i4)

    assert eval_node(app, [4], [9], [3], [6]) == [9]


@pytest.mark.parametrize(
["lhs", "rhs", "expected"],
[
    ([1, 2], [2, 3], [3, 5]),
    ([1.0, 2.0], [2.0, 3.0], [3.0, 5.0]),
]
)
def test_sum_scalars(lhs, rhs, expected):
    tp = type(lhs[0])
    @graph
    def app(lhs: TS[tp], rhs: TS[tp]) -> TS[tp]:
        return sum_(lhs, rhs)

    assert eval_node(app, lhs, rhs) == expected


def test_sum_scalars_multi():
    @graph
    def app(ts1: TS[float], ts2: TS[float], ts3: TS[float]) -> TS[float]:
        return sum_(ts1, ts2, ts3)

    assert eval_node(app, 4.0, 5.0, 6.0) == [15.0]


def test_mean_scalars_unary():
    @graph
    def app(ts: TS[int]) -> TS[float]:
        return mean(ts)

    assert eval_node(app, [1, 3, 5, 11], __trace_wiring__=True) == [1.0, 2.0, 3.0, 5.0]


@pytest.mark.parametrize(
["lhs", "rhs", "expected"],
[
    ([1, 2], [2, 3], [1.5, 2.5]),
    ([1.0, 2.0], [2.0, 3.0], [1.5, 2.5]),
]
)
def test_mean_scalars_binary(lhs, rhs, expected):
    tp = type(lhs[0])
    @graph
    def app(lhs: TS[tp], rhs: TS[tp]) -> TS[float]:
        return mean(lhs, rhs)

    assert eval_node(app, lhs, rhs) == expected


def test_mean_scalars_multi():
    @graph
    def app(ts1: TS[float], ts2: TS[float], ts3: TS[float]) -> TS[float]:
        return mean(ts1, ts2, ts3)

    assert eval_node(app, 4.0, 5.0, 6.0) == [5.0]


def test_std_scalars_unary():
    @graph
    def app(ts: TS[int]) -> TS[float]:
        return std(ts)

    assert eval_node(app, [1, 2, 3, 5]) == [0.0, 0.5, 0.8164965809277263, 1.479019945774904]


@pytest.mark.parametrize(
["lhs", "rhs", "expected"],
[
    ([1, 2], [2, 3], [0.7071067811865476, 0.7071067811865476]),
    ([1.0, 2.0], [2.0, 3.0], [0.7071067811865476, 0.7071067811865476]),
])
def test_std_scalars_binary(lhs, rhs, expected):
    tp = type(lhs[0])
    @graph
    def app(lhs: TS[tp], rhs: TS[tp]) -> TS[float]:
        return std(lhs, rhs)

    assert eval_node(app, lhs, rhs) == expected


@pytest.mark.parametrize(
["lhs", "rhs", "expected"],
[
    ([1, 2], [2, 3], [0.5, 0.5]),
    ([1.0, 2.0], [2.0, 3.0], [0.5, 0.5]),
])
def test_var_scalars_binary(lhs, rhs, expected):
    tp = type(lhs[0])
    @graph
    def app(lhs: TS[tp], rhs: TS[tp]) -> TS[float]:
        return var(lhs, rhs)

    assert eval_node(app, lhs, rhs) == expected


def test_std_scalars_multi():
    @graph
    def app(ts1: TS[float], ts2: TS[float], ts3: TS[float]) -> TS[float]:
        return mean(ts1, ts2, ts3)

    assert eval_node(app, 4.0, 5.0, 6.0) == [5.0]


def test_str_scalars():
    assert eval_node(str_, [100]) == ['100']


@pytest.mark.parametrize(
    ['op', 'd1', 'd2', 'expected'],
    [
        [sub_, 3, 1, 2],
        [add_, 3, 1, 4],
        [eq_, 3, 1, False],
        [eq_, 3, 3, True],
        [lt_, 3, 2, False],
        [lt_, 2, 3, True],
        [gt_, 3, 2, True],
        [gt_, 2, 3, False],
        [le_, 3, 2, False],
        [le_, 2, 3, True],
        [le_, 3, 3, True],
        [ge_, 3, 2, True],
        [ge_, 3, 3, True],
        [ge_, 2, 3, False],
    ]
)
def test_comparison_ops(op, d1, d2, expected):
    assert eval_node(op, d1, d2) == [expected]
