import pytest

from hgraph import TSS, Removed, TSL, TS, Size, TSD, REMOVE, abs_, len_, mod_, sub_, add_, eq_, lt_, gt_, le_, ge_
from hgraph.nodes import cast_, drop, take
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


def test_mod():
    assert eval_node(mod_, [1, 2, 3, 4, 5], [3]) == [1, 2, 0, 1, 2]


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
def test_date_ops(op, d1, d2, expected):
    assert eval_node(op, d1, d2) == [expected]
