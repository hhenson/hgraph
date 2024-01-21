import pytest

from hgraph import Size, TS, TSL, TSD, MIN_TD
from hgraph.nodes import ewma, center_of_mass_to_alpha, span_to_alpha, mean, clip, diff, average, lag, count, \
    accumulate, sum_
from hgraph.test import eval_node


def test_ewma():
    assert eval_node(ewma,
                     [1.0, 2.0, 3.0, 4.0, 3.0, 2.0, 1.0],
                     0.5) == [1.0, 1.5, 2.25, 3.125, 3.0625, 2.53125, 1.765625]


def test_conversions():
    assert center_of_mass_to_alpha(1.0) == 0.5
    assert span_to_alpha(1.0) == 1.0


@pytest.mark.parametrize(
    ['value', 'expected', 'tp'],
    [
        [[1.0, 2.0, 3.0, 4.0, 5.0], [3.0], TSL[TS[float], Size[5]]],
        [{0: 1.0, 1: 2.0, 2: 3.0, 3: 4.0, 4: 5.0}, [3.0], TSD[int, TS[float]]],
        [[1, 2, 3, 4, 5], [3.0], TSL[TS[int], Size[5]]],
        [{0: 1, 1: 2, 2: 3, 3: 4, 4: 5}, [3.0], TSD[int, TS[int]]],
    ]
)
def test_mean(value, expected, tp):
    assert eval_node(mean, [value, ], resolution_dict={'ts': tp}) == expected


@pytest.mark.parametrize(
    ["values", "min", "max", "expected"],
    [
        [[2, 1, 0, -1, -2], -1, 1, [1, 1, 0, -1, -1]],
        [[2.0, 1.0, 0.0, -1.0, -2.0], -1.0, 1.0, [1.0, 1.0, 0.0, -1.0, -1.0]],
    ]
)
def test_abs(values, min, max, expected):
    assert eval_node(clip, values, min, max) == expected


def test_abs_failure():
    with pytest.raises(RuntimeError):
        assert eval_node(clip, [1.0], 1.0, -1.0) == [1.0]


def test_tick_lag():
    expected = [
        None,
        None,
        None,
        1,
        2,
    ]

    assert eval_node(lag, [1, 2, 3, 4, 5], 3) == expected


def test_tick_lag_time_delta():
    expected = [
        None,
        None,
        1,
        2,
        3,
        4,
        5,
    ]

    assert eval_node(lag, [1, 2, 3, 4, 5], MIN_TD*2) == expected


def test_count():
    expected = [
        1,
        2,
        3,
    ]

    assert eval_node(count, [3, 2, 1,], resolution_dict={'ts': TS[int]}) == expected


def test_accumulate():
    expected = [
        1,
        3,
        6,
        10,
    ]

    assert eval_node(accumulate, [1, 2, 3, 4,]) == expected


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        [[1, 2, 3, 4,], [1.0, 1.5, 2.0, 2.5]],
        [[1.0, 2.0, 3.0, 4.0,], [1.0, 1.5, 2.0, 2.5]],
    ]
)
def test_average(value, expected):
    assert eval_node(average, value) == expected


def test_diff():
    expected = [
        None,
        1,
        1,
        1,
    ]

    assert eval_node(diff, [1, 2, 3, 4,]) == expected


@pytest.mark.parametrize(
    ["inputs", "expected"],
    [
        [[(1, 2), (2, 3)], [3, 5]],
        [[(1.0, 2.0), (2.0, 3.0)], [3.0, 5.0]],
    ]
)
def test_sum(inputs, expected):
    assert eval_node(sum_, inputs) == expected
