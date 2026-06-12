from typing import Any, Callable

import logging
import pytest
pytestmark = pytest.mark.smoke

from hgraph import DEFAULT, REMOVE, REMOVE_IF_EXISTS, const, graph, TSD, TS, log_, reduce, add_, Size, TSL, SIZE, map_, default, format_, sum_, switch_, compute_node, if_, TS_OUT, TimeSeriesSchema, TSB, equal_lambdas
from hgraph.nodes import keys_where_true
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
        [None, {1: 1, 2: 2}],
    ) == [0, 3]


def test_reduce_none_zero():
    @graph
    def g(items: TSD[int, TS[int]]) -> TS[int]:
        return reduce(lambda x, y: x + y, items, None)

    assert eval_node(
        g,
        [None, {1: 1, 2: 2}],
    ) == [None, 3]


def test_reduce_overload():
    @graph
    def only_even(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return lhs + rhs

    @compute_node(overloads=reduce, requires=lambda m, func: func == only_even)
    def my_reduce_overload(
        func: Callable[..., Any], ts: TSD[str, TS[int]], zero: int = 0, is_associative: bool = True
    ) -> TS[int]:
        return sum(v.value for v in ts.values() if v.value % 2 == 0)

    @graph
    def g(items: TSD[str, TS[int]]) -> TS[int]:
        return reduce(only_even, items, 0)

    assert eval_node(g, [{"a": 1, "b": 2, "c": 4}]) == [6]


def test_reduce_overload_lambda_shape():
    @compute_node(overloads=reduce, requires=lambda m, func: equal_lambdas(func, lambda a, b: a + b))
    def my_reduce_lambda_shape_overload(
        func: Callable[..., Any], ts: TSL[TS[int], Size[2]], zero: int = 0, is_associative: bool = True
    ) -> TS[int]:
        return 99

    @graph
    def g_match(items: TSL[TS[int], Size[2]]) -> TS[int]:
        return reduce(lambda lhs, rhs: lhs + rhs, items, 0)

    @graph
    def g_renamed(items: TSL[TS[int], Size[2]]) -> TS[int]:
        return reduce(lambda a, b: a + b, items, 0)

    @graph
    def g_no_match(items: TSL[TS[int], Size[2]]) -> TS[int]:
        return reduce(lambda lhs, rhs: lhs + rhs + 1, items, 0)

    assert eval_node(g_match, [(1, 2)]) == [99]
    assert eval_node(g_renamed, [(1, 2)]) == [99]
    assert eval_node(g_no_match, [(1, 2)]) == [4]


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


def test_reduce_17():
    @compute_node(valid=())
    def add_non_strict(lhs: TS[int], rhs: TS[int], _output: TS_OUT[float] = None) -> TS[int]:
        if lhs.valid and rhs.valid:
            return lhs.value + rhs.value
        elif lhs.valid:
            return lhs.value
        elif rhs.valid:
            return rhs.value
        else:
            _output.invalidate()
            

    @graph
    def g(items: TSD[int, TS[int]]) -> TS[int]:
        a = map_(lambda i: i + 1, items)
        b = a.reduce(lambda x, y: add_non_strict(x, y), None)
        return b

    res = eval_node(g, [
        {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9, 10: 10, 11: 11, 12: 12, 13: 13, 14: 14, 15: 15, 16: 16},
        None,
        {1: REMOVE},
        None,
        ],
        __trace__=True)

    assert res == [153, None, 151, None]


def test_reduce_ref():
    class RB(TimeSeriesSchema):
        a: TS[int]
        b: TS[bool]

    @graph
    def g(items: TSD[int, TSB[RB]], use: TS[bool]) -> TS[int]:
        a = if_(use, items).true
        b = a.a.reduce(lambda x, y: x + y, 0)
        return b

    iterations, flip_at = 5, 3
    ticks = []
    flips = []
    for i in range(iterations):
        ticks.append({i: {'a': i, 'b': True}})
    for i in range(iterations):
        ticks.append({i: REMOVE})
    for i in range(iterations * 2):
        flips.append({0: False, 1: True}.get(i % flip_at, None))
    on = False
    outs = []
    for i in range(iterations * 2):
        on = on if flips[i] is None else flips[i]
        if on:
            outs.append(sum(i['a'] if i is not REMOVE else -k for x in ticks[:i+1] for k, i in x.items()))
        else:
            outs.append(0)

    res = eval_node(g,
                    ticks,
                    flips,
                    __trace__=True)

    assert res == outs


def test_reduce_preexisting_items():
    @graph
    def g(items: TSD[int, TS[int]], trigger: TS[bool]) -> TS[int]:
        return switch_(trigger, {
            False: lambda i: const(0),
            True: lambda i: reduce(lambda x, y: x + y, i, 0)
        }, items)

    assert eval_node(
        g,
        [
            {1: 1, 2: 2, 3: 3},
            {4: 4, 5: 5},
            {6: 6},
        ],
        [
            None,
            True,
            None
        ],
        __trace__=True
    ) == [None, 15, 21]
    
    
def test_reduce_unset_refs():
    class RB(TimeSeriesSchema):
        a: TS[int]
        b: TS[bool]

    @graph
    def g(items: TSD[int, TSB[RB]], use: TS[bool]) -> TS[int]:
        a = map_(lambda i, use: if_(use, i).true, items, use)
        b = a.a.reduce(lambda x, y: default(x, 0) + default(y, 0), 0)
        return b

    res = eval_node(g,
                    [
                        {0: {'a': 1, 'b': True}, 1: {'a': 1, 'b': True}, 2: {'a': 1, 'b': True}},
                        None
                    ],
                    [False, False, True],
                    __trace__=False)

    assert res == [0, None, 3]


def test_reduce_map_shrink_no_subscriber_leak():
    """
    Reproduces the 'Output instance still has subscribers when released' bug.

    When a reduce node uses a map_ lambda and the binary tree shrinks (due to removals),
    the MAP node's per-key TSD outputs are released while still having subscribers.

    The pattern:
      - reduce over TSD[str, TSD[str, TS[float]]] using map_ inside the lambda
      - downstream nodes subscribe to the reduce's reference output
        (via keys_where_true + map_)
      - enough items to grow the tree past capacity 8 (requires >8 super-nodes)
      - enough removals to trigger _re_balance_nodes -> _shrink_tree

    Previously this logged "Output instance still has subscribers when released" errors
    and caused crashes due to dangling references to released outputs.
    """
    errors = []

    class ErrorCapture(logging.Handler):
        def emit(self, record):
            if record.levelno >= logging.ERROR:
                errors.append(record.getMessage())

    handler = ErrorCapture()
    logging.getLogger("hgraph").addHandler(handler)
    try:

        @graph
        def g(outer: TSD[str, TSD[str, TS[float]]]) -> TSD[str, TS[float]]:
            reduced = outer.reduce(lambda x, y: map_(lambda i, j: default(i, 0.0) + default(j, 0.0), x, y))
            non_zero = reduced[keys_where_true(map_(lambda v: v != 0.0, reduced))]
            return non_zero

        n = 17
        keys = ["s" + str(i).zfill(2) for i in range(n)]

        result = eval_node(
            g,
            [
                {k: {"a": float(i + 1), "b": float(i + 2)} for i, k in enumerate(keys)},
                {k: REMOVE_IF_EXISTS for k in keys[:11]},
                {keys[0]: {"a": 100.0}},
                {k: REMOVE_IF_EXISTS for k in keys[11:]},
                {keys[0]: {"a": 200.0}},
            ],
        )

        assert result[-1] == {"a": 200.0}
        assert not errors, f"Unexpected error logs (subscriber leak):\n" + "\n".join(
            e.split("\n")[0] for e in errors
        )
    finally:
        logging.getLogger("hgraph").removeHandler(handler)


def test_switch_reduce_map_shrink_no_subscriber_leak(capfd):
    """
    Distills the crash-stack shape down to a pure hgraph repro:
    shrink a nested reduce/map tree, then flip a switch that owns it.

    The C++ runtime must not release branch outputs while downstream nodes still
    hold subscriptions into the reduced map.
    """

    @graph
    def reduced_non_zero(outer: TSD[str, TSD[str, TS[float]]]) -> TSD[str, TS[float]]:
        reduced = outer.reduce(lambda x, y: map_(lambda i, j: default(i, 0.0) + default(j, 0.0), x, y))
        return reduced[keys_where_true(map_(lambda v: v != 0.0, reduced))]

    @graph
    def g(outer: TSD[str, TSD[str, TS[float]]], enabled: TS[bool]) -> TSD[str, TS[float]]:
        return switch_(
            enabled,
            {
                True: lambda o: reduced_non_zero(o),
                False: lambda o: reduced_non_zero(o),
            },
            outer,
        )

    keys = [f"s{i:02d}" for i in range(17)]

    result = eval_node(
        g,
        [
            {k: {"a": float(i + 1), "b": float(i + 2)} for i, k in enumerate(keys)},
            {k: REMOVE_IF_EXISTS for k in keys[:11]},
            None,
            {k: REMOVE_IF_EXISTS for k in keys[11:]},
            None,
        ],
        [
            True,
            None,
            False,
            None,
            True,
        ],
    )

    assert result[-1] == {}

    captured = "".join(capfd.readouterr())
    assert "Output instance still has subscribers when released" not in captured


def test_switch_reduce_map_shrink_with_trace():
    """
    A stronger version of the switch/reduce repro above.

    Trace output should remain safe while a switched branch tears down a reduced
    nested map and downstream nodes still inspect the resulting references.
    """

    @graph
    def reduced_non_zero(outer: TSD[str, TSD[str, TS[float]]]) -> TSD[str, TS[float]]:
        reduced = outer.reduce(lambda x, y: map_(lambda i, j: default(i, 0.0) + default(j, 0.0), x, y))
        return reduced[keys_where_true(map_(lambda v: v != 0.0, reduced))]

    @graph
    def switched_reduce(outer: TSD[str, TSD[str, TS[float]]], enabled: TS[bool]) -> TSD[str, TS[float]]:
        return switch_(
            enabled,
            {
                True: lambda o: reduced_non_zero(o),
                False: lambda o: reduced_non_zero(o),
            },
            outer,
        )

    @graph
    def g(values: TSD[str, TSD[str, TSD[str, TS[float]]]], enabled: TSD[str, TS[bool]]) -> TSD[str, TSD[str, TS[float]]]:
        return map_(switched_reduce, values, enabled)

    keys = [f"s{i:02d}" for i in range(17)]

    result = eval_node(
        g,
        [
            {"p1": {k: {"a": float(i + 1), "b": float(i + 2)} for i, k in enumerate(keys)}},
            {"p1": {k: REMOVE_IF_EXISTS for k in keys[:11]}},
            None,
            {"p1": {k: REMOVE_IF_EXISTS for k in keys[11:]}},
            {"p1": REMOVE_IF_EXISTS},
            {"p1": {"r1": {"a": 10.0, "b": 20.0}, "r2": {"a": 1.0}}},
            {"p1": {"r2": REMOVE_IF_EXISTS}},
            None,
        ],
        [
            {"p1": True},
            None,
            {"p1": False},
            None,
            None,
            {"p1": True},
            None,
            {"p1": False},
        ],
        __trace__=True,
    )

    assert result == [
        {"p1": {"a": 153.0, "b": 170.0}},
        {"p1": {"a": 87.0, "b": 93.0}},
        {"p1": {"a": 87.0, "b": 93.0}},
        {"p1": {"a": REMOVE, "b": REMOVE}},
        None,
        {"p1": {"a": 11.0, "b": 20.0}},
        {"p1": {"a": 10.0}},
        {"p1": {"a": 10.0, "b": 20.0}},
    ]


def test_switch_reduce_map_shrink_on_final_tick_cleanup(capfd):
    """
    If shrink happens on the final tick, deferred release should still run during clean shutdown.
    """

    @graph
    def reduced_non_zero(outer: TSD[str, TSD[str, TS[float]]]) -> TSD[str, TS[float]]:
        reduced = outer.reduce(lambda x, y: map_(lambda i, j: default(i, 0.0) + default(j, 0.0), x, y))
        return reduced[keys_where_true(map_(lambda v: v != 0.0, reduced))]

    @graph
    def g(outer: TSD[str, TSD[str, TS[float]]], enabled: TS[bool]) -> TSD[str, TS[float]]:
        return switch_(
            enabled,
            {
                True: lambda o: reduced_non_zero(o),
                False: lambda o: reduced_non_zero(o),
            },
            outer,
        )

    keys = [f"s{i:02d}" for i in range(17)]

    result = eval_node(
        g,
        [
            {k: {"a": float(i + 1), "b": float(i + 2)} for i, k in enumerate(keys)},
            {k: REMOVE_IF_EXISTS for k in keys[:11]},
            {k: REMOVE_IF_EXISTS for k in keys[11:]},
        ],
        [
            True,
            None,
            False,
        ],
    )

    assert result[-1] == {"a": REMOVE, "b": REMOVE}

    captured = "".join(capfd.readouterr())
    assert "Output instance still has subscribers when released" not in captured
