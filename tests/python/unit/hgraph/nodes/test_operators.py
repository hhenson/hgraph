import pytest

from hgraph import TSS, Removed, TSL, TS, Size, TSD, REMOVE, abs_
from hgraph.nodes import cast_, len_, drop, take
from hgraph.test import eval_node


def test_cast():
    expected = [
        1.0,
        2.0,
        3.0
    ]

    assert eval_node(cast_, float, [1, 2, 3]) == expected


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


def test_drop():
    assert eval_node(drop, [1, 2, 3, 4, 5], 3) == [None, None, None, 4, 5]


def test_take():
    assert eval_node(take, [1, 2, 3, 4, 5], 3) == [1, 2, 3, None, None]


@pytest.mark.parametrize(
    ["values", "expected"],
    [
        [[1, 0, -1], [1, 0, 1]],
        [[1.0, 0.0, -1.0], [1.0, 0.0, 1.0]],
    ]
)
def test_abs(values, expected):
    assert eval_node(abs_, values) == expected
