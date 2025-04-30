import pytest

from hgraph import REMOVE_IF_EXISTS, graph, TSD, TS, reduce, add_, Size, TSL, SIZE, map_, default, format_
from hgraph.test import eval_node


@pytest.mark.parametrize(
    ["inputs", "expected"],
    [
        [[None, {"a": 1}, {"a": REMOVE_IF_EXISTS}], [0, 1, 0]],
        [[None, {"a": 1}, {"b": 2}, {"b": REMOVE_IF_EXISTS}, {"a": REMOVE_IF_EXISTS}], [0, 1, 3, 1, 0]],
        [[{"a": 1, "b": 2, "c": 3}, {"b": REMOVE_IF_EXISTS}, {"a": REMOVE_IF_EXISTS}], [6, 4, 3]],
        [[{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}, {"e": 5}], [1, 3, 6, 10, 15]],
        [
            [
                {(chr(ord("a") + i)): i for i in range(26)},
                {(chr(ord("a") + i)): REMOVE_IF_EXISTS for i in range(26)},
            ],
            [325, 0],
        ],
        [
            [
                {(chr(ord("a") + i)): i for i in range(26)},
                {(chr(ord("a") + i)): REMOVE_IF_EXISTS for i in range(20)},
                {(chr(ord("a") + i)): i for i in range(20)},
                {(chr(ord("a") + i)): REMOVE_IF_EXISTS for i in range(26)},
                {(chr(ord("a") + i)): i for i in range(26)},
            ],
            [325, 135, 325, 0, 325],
        ],
    ],
)
def test_tsd_reduce(inputs, expected):
    @graph
    def reduce_test(tsd: TSD[str, TS[int]]) -> TS[int]:
        return reduce(add_, tsd, 0)

    assert eval_node(reduce_test, inputs) == expected


@pytest.mark.parametrize(
    ["inputs", "expected"],
    [
        [[None, {"a": 1}, {"a": REMOVE_IF_EXISTS}], [0, 1, 0]],
        [[None, {"a": 1}, {"b": 2}, {"b": REMOVE_IF_EXISTS}, {"a": REMOVE_IF_EXISTS}], [0, 1, 3, 1, 0]],
        [[{"a": 1, "b": 2, "c": 3}, {"b": REMOVE_IF_EXISTS}, {"a": REMOVE_IF_EXISTS}], [6, 4, 3]],
        [[{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}, {"e": 5}], [1, 3, 6, 10, 15]],
        [
            [
                {(chr(ord("a") + i)): i for i in range(26)},
                {(chr(ord("a") + i)): REMOVE_IF_EXISTS for i in range(26)},
            ],
            [325, 0],
        ],
        [
            [
                {(chr(ord("a") + i)): i for i in range(26)},
                {(chr(ord("a") + i)): REMOVE_IF_EXISTS for i in range(20)},
                {(chr(ord("a") + i)): i for i in range(20)},
                {(chr(ord("a") + i)): REMOVE_IF_EXISTS for i in range(26)},
                {(chr(ord("a") + i)): i for i in range(26)},
            ],
            [325, 135, 325, 0, 325],
        ],
    ],
)
def test_tsd_reduce_no_zero(inputs, expected):
    @graph
    def reduce_test(tsd: TSD[str, TS[int]]) -> TS[int]:
        return reduce(add_, tsd)

    assert eval_node(reduce_test, inputs) == expected


@pytest.mark.parametrize(
    ["inputs", "size", "expected"],
    [
        [[None, {0: 1}, None, {1: 2}], Size[2], [0, 1, None, 3]],
        [[None, {0: 1, 3: 4}, {1: 2, 2: 3}], Size[4], [0, 5, 10]],
        [[None, {0: 1, 3: 4}, {1: 2, 2: 3}, {4: 8}], Size[5], [0, 5, 10, 18]],
        [[None, {0: 1, 3: 4}, {1: 2, 2: 3}, {4: 8, 5: 9}], Size[6], [0, 5, 10, 27]],
    ],
)
def test_tsl_reduce(inputs, size, expected):
    @graph
    def reduce_test(tsl: TSL[TS[int], SIZE]) -> TS[int]:
        return reduce(add_, tsl, 0)

    assert eval_node(reduce_test, inputs, resolution_dict={"tsl": TSL[TS[int], size]}) == expected


def test_reduce_map():
    @graph
    def g(items: TSD[int, TSD[int, TS[int]]]) -> TSD[int, TS[int]]:
        return items.reduce(lambda x, y: map_(lambda i, j: default(i, 0) + default(j, 0), x, y))

    assert eval_node(
        g, [{1: {1: 1, 2: 2}}, {2: {1: 3, 2: 4}}, {3: {2: 1, 3: 3}}], __trace__={"start": False, "stop": False}
    ) == [{1: 1, 2: 2}, {1: 4, 2: 6}, {1: 4, 2: 7, 3: 3}]


def test_reduce_tuple():

    @graph
    def g(items: TS[tuple[int, ...]], zero: TS[str]) -> TS[str]:
        return reduce(lambda x, y: format_("{x}, {y}",x=x, y=y), items, zero, is_associative=False)

    assert eval_node(g, [(1, 2,), (1,), tuple()], ["a"]) == ["a, 1, 2", "a, 1", "a"]

def test_reduce_simple():
    @graph
    def g(items: TSD[int, TS[int]]) -> TS[int]:
        return reduce(lambda x, y: x + y, items, 0)

    assert eval_node(
        g, [{1: 1, 2: 2}],
    ) == [3]