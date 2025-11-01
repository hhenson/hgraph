import math

import pytest

from hgraph import add_, sub_, div_, exception_time_series, TS, graph, mod_, divmod_, pow_, eq_, const, DivideByZero, ln
from hgraph._impl._operators._number_operators import div_numbers
from hgraph.test import eval_node

import pytest
pytestmark = pytest.mark.smoke

@pytest.mark.parametrize(
    "lhs,rhs,expected",
    [
        ([1, 2, 3], [4.0, 5.0, 6.0, 7.0], [5.0, 7.0, 9.0, 10.0]),
    ],
)
def test_add_int_and_float(lhs, rhs, expected):
    assert eval_node(add_, lhs, rhs) == expected
    assert eval_node(add_, rhs, lhs) == expected


def test_sub_float_from_int():
    assert eval_node(sub_, [10], [0.1]) == [9.9]


def test_sub_int_from_float():
    assert eval_node(sub_, [0.1], [10]) == [-9.9]


@pytest.mark.parametrize(
    ["lhs", "rhs", "divide_by_zero", "expected"],
    [
        (1, 2, DivideByZero.ERROR, 0.5),
        (1, 0, DivideByZero.INF, math.inf),
        (1, 0, DivideByZero.NAN, math.nan),
        (1, 0, DivideByZero.ZERO, 0.0),
        (1, 0, DivideByZero.ONE, 1.0),
        (1.0, 2, DivideByZero.NAN, 0.5),
        (1, 2.0, DivideByZero.NAN, 0.5),
    ],
)
def test_div_numbers(lhs, rhs, divide_by_zero, expected):
    output = eval_node(div_numbers, [lhs], [rhs], divide_by_zero=divide_by_zero)[0]
    assert math.isnan(output) if math.isnan(expected) else output == expected


def test_divide_by_zero_error():
    @graph
    def app() -> TS[str]:
        ts = const(1) / const(0)
        return exception_time_series(ts).error_msg

    output = eval_node(app)
    assert "division by zero" in output[0]


@pytest.mark.parametrize(
    "lhs,rhs,expected",
    [
        ([6, 4, 2], [3, 2, 1], [2.0, 2.0, 2.0]),
        ([6, 4, 2], [3, 2, 1, 2], [2.0, 2.0, 2.0, 1.0]),
        ([None, 6, None, 12], [2, 3, 6], [None, 2.0, 1.0, 2.0]),
    ],
)
def test_div(lhs, rhs, expected):
    assert eval_node(div_, lhs, rhs) == expected


def test_mod_int():
    assert eval_node(mod_, [1, 2, 3, 4, 5], [3]) == [1, 2, 0, 1, 2]


def test_divmod_int():
    assert eval_node(divmod_, [5], [2]) == [{0: 2, 1: 1}]


def test_divmod_float_int():
    assert eval_node(divmod_, [5.0], [2]) == [{0: 2.0, 1: 1.0}]


def test_divmod_int_float():
    assert eval_node(divmod_, [5], [2.0]) == [{0: 2.0, 1: 1.0}]


@pytest.mark.parametrize(
    "lhs,rhs,expected",
    [
        ([1, 2, 3], [1, 1, 1], [1, 2, 3]),
        ([2, 2, 2, None], [0, 1, 2, 3], [1, 2, 4, 8]),
        ([2.0], [3], [8.0]),
        ([2], [3.0], [8.0]),
        ([4], [0.5], [2.0]),
    ],
)
def test_pow_numbers(lhs, rhs, expected):
    assert eval_node(pow_, lhs, rhs) == expected


@pytest.mark.parametrize(
    ["lhs", "rhs", "expected", "epsilon"],
    [
        (1.0, 1.0, True, None),
        (1, 1.0, True, None),
        (1.0, 1, True, None),
        (1, 1, True, None),
        (1.0, -1.0, False, None),
        (1, -1.0, False, None),
        (1.0, -1, False, None),
        (1, -1, False, None),
        (1.0, 1.0 + 1e-16, True, None),
        (1.0, 1.0 + 1e-5, False, None),
        (1.0, 1.0 + 1e-5, True, 1e-4),
        (1, 1.0 + 1e-5, True, 1e-4),
        (1.0 + 1e-5, 1.0, True, 1e-4),
        (-1.0, -1.0 + 1e-5, True, 1e-4),
    ],
)
def test_eq_floats(lhs, rhs, expected, epsilon):
    @graph
    def app(lhs: TS[lhs.__class__], rhs: TS[rhs.__class__]) -> TS[bool]:
        if epsilon is not None:
            return eq_(lhs, rhs, epsilon=epsilon)
        else:
            return eq_(lhs, rhs)

    assert eval_node(app, [lhs], [rhs]) == [expected]


def test_ln():

    assert eval_node(ln, [math.e]) == [1.0]
