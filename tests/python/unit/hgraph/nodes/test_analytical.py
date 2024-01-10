import pytest

from hgraph import Size, TS, TSL, TSD
from hgraph.nodes import ewma, center_of_mass_to_alpha, span_to_alpha, mean, clip
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
