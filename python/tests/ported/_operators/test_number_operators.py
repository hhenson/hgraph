import math
import sys

import pytest

from hgraph import add_, sub_, div_, exception_time_series, TS, graph, mod_, divmod_, pow_, eq_, const, DivideByZero, ln, sign
# deviation: python-implementation internals - div_numbers adapts to the div_ OP
from hgraph import div_ as div_numbers
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


def test_mod_int_boundaries_and_negative_divisors():
    lhs = [-(2**63), -(2**63), 2**63 - 1, 7, -7]
    rhs = [3, -1, -3, -3, 3]
    assert eval_node(mod_, lhs, rhs) == [a % b for a, b in zip(lhs, rhs)]


@pytest.mark.parametrize("policy", [None, DivideByZero.ERROR])
@pytest.mark.parametrize("lhs,rhs", [
    (1.0, math.inf), (-1.0, math.inf), (1.0, -math.inf), (-1.0, -math.inf),
    (sys.float_info.max, sys.float_info.min), (-sys.float_info.max, -sys.float_info.min),
    (-sys.float_info.min, sys.float_info.max), (sys.float_info.min, -sys.float_info.max),
    (0.0, -2.0), (-0.0, 2.0), (4.0, -2.0), (-4.0, 2.0),
    (math.inf, 2.0), (-math.inf, 2.0), (math.nan, 2.0), (1.0, math.nan),
    (1, math.inf), (-1, math.inf), (sys.float_info.max, 2),
])
def test_mod_float_extremes(lhs, rhs, policy):
    kwargs = {} if policy is None else {"divide_by_zero": policy}
    actual = eval_node(mod_, [lhs], [rhs], **kwargs)[0]
    expected = lhs % rhs
    if math.isnan(expected):
        assert math.isnan(actual)
    else:
        assert actual == expected
        assert math.copysign(1.0, actual) == math.copysign(1.0, expected)


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
    actual = eval_node(pow_, lhs, rhs)
    assert actual == expected
    assert [type(value) if value is not None else None for value in actual] == [
        type(value) if value is not None else None for value in expected
    ]


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


@pytest.mark.parametrize(
    "values,expected",
    [
        ([-10, 0, 3], [-1, 1, 1]),
        ([-10.0, 0.0, 3.1], [-1.0, 1.0, 1.0]),
    ],
)
def test_sign(values, expected):
    assert eval_node(sign, values) == expected
