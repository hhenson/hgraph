import pytest

from hgraph import MIN_TD
from hgraph.nodes import center_of_mass_to_alpha, span_to_alpha, lag
from hgraph.nodes._stream_analytical_operators import clip
from hgraph.test import eval_node


def test_conversions():
    assert center_of_mass_to_alpha(1.0) == 0.5
    assert span_to_alpha(1.0) == 1.0


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


