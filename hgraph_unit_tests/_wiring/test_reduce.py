import pytest
pytestmark = pytest.mark.smoke

from hgraph import DEFAULT, REMOVE, REMOVE_IF_EXISTS, const, debug_print, graph, TSD, TS, log_, reduce, add_, Size, TSL, SIZE, map_, default, format_, sum_, switch_
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
        g, [{1: {1: 1, 2: 2}}, {2: {1: 3, 2: 4}}, {3: {2: 1, 3: 3}}]
    ) == [{1: 1, 2: 2}, {1: 4, 2: 6}, {1: 4, 2: 7, 3: 3}]


def test_reduce_tuple():

    @graph
    def g(items: TS[tuple[int, ...]], zero: TS[str]) -> TS[str]:
        return reduce(lambda x, y: format_("{x}, {y}", x=x, y=y), items, zero, is_associative=False)

    assert eval_node(
        g,
        [
            (
                1,
                2,
            ),
            (1,),
            tuple(),
        ],
        ["a"],
    ) == ["a, 1, 2", "a, 1", "a"]


def test_reduce_simple():
    @graph
    def g(items: TSD[int, TS[int]]) -> TS[int]:
        return reduce(lambda x, y: x + y, items, 0)

    assert eval_node(
        g,
        [{1: 1, 2: 2}],
    ) == [3]


def test_reduce_map_and_switch():
    @graph
    def g(items: TSD[int, TS[int]]) -> TS[int]:
        return map_(lambda i: switch_(i, {0: lambda: const(0), DEFAULT: lambda: const(1)}), items).reduce(lambda x, y: x + y, 0)

    res = eval_node(g, [
        {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9, 10: 10, 11: 11, 12: 12, 13: 13, 14: 14, 15: 15, 16: 16},
        None,
        {1: REMOVE, 3: REMOVE, 5: REMOVE, 7: REMOVE, 9: REMOVE, 11: REMOVE, 13: REMOVE, 15: REMOVE},
        None,
        {0: 0, 2: 0, 4: 0, 6: 0, 8: 0, 10: 0, 12: 0, 14: 0, 16: 0},
        None,
        {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9, 10: 10, 11: 11, 12: 12, 13: 13, 14: 14, 15: 15, 16: 16},
        None
        ])
    
    assert res == [16, None, 8, None, 0, None, 16, None]
    
    
def test_reduce_map_and_switch_2():
    @graph
    def g(items: TSD[int, TS[int]]) -> TS[int]:
        a = map_(lambda i: switch_(i, {
                0: lambda: const({0: 0}, TSD[int, TS[int]]), 
                DEFAULT: lambda: const({1: 1}, TSD[int, TS[int]])}), items)
        b = a.reduce(lambda x, y: map_(lambda i, j: default(i, 0) + default(j, 0), x, y))
        c = b.reduce(lambda x, y: x + y, 0)
        return c

    res = eval_node(g, [
        {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9, 10: 10, 11: 11, 12: 12, 13: 13, 14: 14, 15: 15, 16: 16},
        None,
        {1: REMOVE, 2: REMOVE, 3: REMOVE, 5: REMOVE, 6: REMOVE, 7: REMOVE, 9: REMOVE, 10: REMOVE, 11: REMOVE, 13: REMOVE, 15: REMOVE},
        None,
        {0: 0, 2: 0, 4: 0, 6: 0, 8: 0, 10: 0, 12: 0, 14: 0, 16: 0},
        None,
        {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9, 10: 10, 11: 11, 12: 12, 13: 13, 14: 14, 15: 15, 16: 16},
        None
        ])
    
    assert res == [16, None, 5, None, 0, None, 16, None]
