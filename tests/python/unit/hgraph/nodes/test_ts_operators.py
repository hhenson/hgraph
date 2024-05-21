import pytest

from hgraph import TSS, Removed, TSL, TS, Size, TSD, REMOVE, len_, abs_, mod_, min_, max_
from hgraph.test import eval_node


@pytest.mark.parametrize(
    ['tp', 'expected', 'values'],
    [
        [TSS[int], [0, 1, 3, 2], [{}, {1}, {2, 3}, {Removed(1)}]],
        [TSL[TS[int], Size[2]], [2, None, None], [{}, {0: 1}, {1: 2}]],
        [TSD[int, TS[int]], [None, 1, 0], [{}, {0: 1}, {0: REMOVE}]],
        [TS[tuple[int]], [0, 1, 2], [tuple(), (1,), (1, 2)]]
    ]
)
def test_len(tp, expected, values):
    assert eval_node(len_, values, resolution_dict={'ts': tp}) == expected


@pytest.mark.parametrize(
    ["values", "expected"],
    [
        [[1, 0, -1], [1, 0, 1]],
        [[1.0, 0.0, -1.0], [1.0, 0.0, 1.0]],
    ]
)
def test_abs(values, expected):
    assert eval_node(abs_, values) == expected


def test_mod():
    assert eval_node(mod_, [1, 2, 3, 4, 5], [3]) == [1, 2, 0, 1, 2]


def test_min():
    assert eval_node(min_, [1, 2, 3], [3, 2, 1]) == [1, 2, 1]


def test_max():
    assert eval_node(max_, [1, 2, 3], [3, 2, 1]) == [3, 2, 3]
