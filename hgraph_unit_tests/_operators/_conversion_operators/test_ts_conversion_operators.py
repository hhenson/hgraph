from typing import Type

import pytest

from hgraph import TS, TSS, SCALAR, SCALAR_1, AUTO_RESOLVE, DEFAULT, Removed
from hgraph import convert
from hgraph.test import eval_node


class base_test_class:
    def __eq__(self, other):
        return isinstance(other, base_test_class)


class derived_test_class(base_test_class):
    def __eq__(self, other):
        return isinstance(other, derived_test_class)


@pytest.mark.parametrize(
    ["from_tp", "from_", "to_tp", "expected"],
    [
        [TS[int], [1, 2, 3], TS[int], [1, 2, 3]],
        [TS[float], [1.0, 2.0, 3.0], TS[int], [1, 2, 3]],
        [TS[int], [1, 2, 3], TS[float], [1.0, 2.0, 3.0]],
        [TS[int], [0, 1, 2], TS[bool], [False, True, True]],
        [TS[float], [0.0, 1.0, 2.0], TS[bool], [False, True, True]],
        [TS[int], [0, 1, 2], TS[str], ["0", "1", "2"]],
        [TS[float], [0.0, 1.0, 2.0], TS[str], ["0.0", "1.0", "2.0"]],
        [TS[int], [0, 1, 2], TSS[int], [frozenset({0}), frozenset({1, Removed(0)}), frozenset({2, Removed(1)})]],
        [TS[bool], [True, False], TS[int], [1, 0]],
        [TS[bool], [True, False], TS[float], [1.0, 0.0]],
        [TS[bool], [True, False], TS[str], ["True", "False"]],
        [TS[bool], [True, False], TSS[bool], [frozenset({True}), frozenset({False, Removed(True)})]],
        [TS[tuple[int, ...]], [tuple(), (1,), (1, 2)], TS[bool], [False, True, True]],
        [TS[tuple[int, ...]], [tuple(), (1,), (1, 2)], TS[str], ["()", "(1,)", "(1, 2)"]],
        [
            TS[tuple[int, ...]],
            [tuple(), (1,), (1, 2)],
            TSS[tuple[int, ...]],
            [frozenset({tuple()}), frozenset({(1,), Removed(tuple())}), frozenset({(1, 2), Removed((1,))})],
        ],
        [TS[derived_test_class], [derived_test_class()], TS[base_test_class], [derived_test_class()]],
        [TS[base_test_class], [derived_test_class()], TS[derived_test_class], [derived_test_class()]],
    ],
)
def test_convert_ts(from_, from_tp, to_tp, expected):
    assert eval_node(convert, from_, to_tp, resolution_dict=dict(ts=from_tp)) == expected


def test_convert_wiring():
    from hgraph import graph

    @graph
    def g(a: TS[SCALAR], to: Type[SCALAR_1] = DEFAULT[SCALAR_1]) -> TS[SCALAR_1]:
        return convert[TS[to]](a)

    assert eval_node(g[str], 1) == ["1"]
