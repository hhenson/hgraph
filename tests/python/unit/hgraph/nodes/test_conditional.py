import pytest

from hgraph import graph, TS, TSB
from hgraph.nodes._conditional import if_then_else, if_true, if_, BoolResult
from hgraph.test import eval_node


def test_if_then_else():
    expected = [
        None,
        2,
        6,
        3
    ]

    assert eval_node(if_then_else, [None, True, False, True], [1, 2, 3], [4, 5, 6]) == expected


@pytest.mark.parametrize("condition,tick_once_only,expected", [
    ([True, False, True], False, [True, None, True]),
    ([True, False, True], True, [True, None, None]),
])
def test_if_true(condition, tick_once_only, expected):
    assert eval_node(if_true, condition, tick_once_only) == expected


def test_if_():

    @graph
    def g(condition: TS[bool], ts: TS[str]) -> TSB[BoolResult[TS[str]]]:
        return if_(condition, ts)

    from frozendict import frozendict as fd
    assert eval_node(g, [True, False, True], ['a', 'b', 'c']) == [
        fd({'true': 'a'}),
        fd({'false': 'b'}),
        fd({'true': 'c'}),
    ]
