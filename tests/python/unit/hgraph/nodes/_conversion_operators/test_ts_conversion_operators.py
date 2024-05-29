import pytest

from hgraph import OUT, TS, TSS
from hgraph.nodes import convert
from hgraph.test import eval_node


@pytest.mark.parametrize(
    ["from_tp", "from_", "to_tp", "expected"],
    [
        [TS[int], [1, 2, 3], TS[int], [1, 2, 3]],
        [TS[int], [1, 2, 3], TS[float], [1., 2., 3.]],
        [TS[int], [0, 1, 2], TS[bool], [False, True, True]],
        [TS[int], [0, 1, 2], TS[str], ["0", "1", "2"]],
        [TS[int], [0, 1, 2], TSS[int], [frozenset({0}), frozenset({1}), frozenset({2})]],
    ]
)
def test_convert_ts(from_, from_tp, to_tp, expected):
    assert eval_node(convert, from_, to_tp, resolution_dict=dict(ts=from_tp)) == expected


