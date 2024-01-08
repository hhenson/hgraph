import pytest

from hgraph import TSS, Removed, TSL, TS, Size, TSD, REMOVE
from hgraph.nodes._operators import cast_, len_
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
