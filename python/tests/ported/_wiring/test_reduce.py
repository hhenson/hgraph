# Ported from ext/main/hgraph_unit_tests/_wiring/test_reduce.py.
from typing import Any, Callable

from hgraph import (
    DEFAULT,
    REMOVE,
    REMOVE_IF_EXISTS,
    Size,
    TS,
    TSD,
    TSL,
    add_,
    compute_node,
    const,
    default,
    graph,
    map_,
    reduce,
    switch_,
)
from hgraph.nodes import keys_where_true
from hgraph.test import eval_node


@graph
def _sum_without_explicit_zero(values: TSD[str, TS[int]]) -> TS[int]:
    return reduce(add_, values)


@graph
def _sum_with_none_zero(values: TSD[str, TS[int]]) -> TS[int]:
    return reduce(lambda lhs, rhs: lhs + rhs, values, None)


@graph
def _offset_sum(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs + rhs + 100


@graph
def _offset_sum_without_zero(values: TSD[str, TS[int]]) -> TS[int]:
    return reduce(_offset_sum, values)


@graph
def _offset_sum_with_zero(values: TSD[str, TS[int]]) -> TS[int]:
    return reduce(_offset_sum, values, 10)


@graph
def _offset_sum_tsl_without_zero(
    values: TSL[TS[int], Size[3]],
) -> TS[int]:
    return reduce(_offset_sum, values)


@graph
def _offset_sum_tsl_with_zero(
    values: TSL[TS[int], Size[3]],
) -> TS[int]:
    return reduce(_offset_sum, values, 10)


@graph
def _merge_int_dicts(
    lhs: TSD[int, TS[int]], rhs: TSD[int, TS[int]]
) -> TSD[int, TS[int]]:
    return map_(
        lambda lhs_value, rhs_value: default(lhs_value, 0)
        + default(rhs_value, 0),
        lhs,
        rhs,
    )


@graph
def _reduce_nested_dicts(
    values: TSD[int, TSD[int, TS[int]]],
) -> TSD[int, TS[int]]:
    return reduce(_merge_int_dicts, values)


@graph
def _zero_or_one_dict(value: TS[int]) -> TSD[int, TS[int]]:
    return switch_(
        value,
        {
            0: lambda: const({0: 0}, TSD[int, TS[int]]),
            DEFAULT: lambda: const({1: 1}, TSD[int, TS[int]]),
        },
    )


@graph
def _reduce_mapped_switches(values: TSD[int, TS[int]]) -> TS[int]:
    mapped = map_(_zero_or_one_dict, values)
    merged = reduce(_merge_int_dicts, mapped)
    return reduce(add_, merged, 0)


@graph
def _only_even(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs + rhs


@compute_node(overloads=reduce, requires=lambda mapping, func: func == _only_even)
def _reduce_only_even(
    func: Callable[..., Any],
    values: TSD[str, TS[int]],
    zero: int = 0,
    is_associative: bool = True,
) -> TS[int]:
    return sum(value.value for value in values.values() if value.value % 2 == 0)


@graph
def _use_callable_annotated_reduce(values: TSD[str, TS[int]]) -> TS[int]:
    return reduce(_only_even, values, 0)


@graph
def _reduced_non_zero(
    outer: TSD[str, TSD[str, TS[float]]],
) -> TSD[str, TS[float]]:
    reduced = reduce(
        lambda lhs, rhs: map_(
            lambda lhs_value, rhs_value: default(lhs_value, 0.0)
            + default(rhs_value, 0.0),
            lhs,
            rhs,
        ),
        outer,
    )
    non_zero_keys = keys_where_true(map_(lambda value: value != 0.0, reduced))
    return reduced[non_zero_keys]


@graph
def _switch_reduced_non_zero(
    outer: TSD[str, TSD[str, TS[float]]], enabled: TS[bool]
) -> TSD[str, TS[float]]:
    return switch_(
        enabled,
        {
            True: lambda values: _reduced_non_zero(values),
            False: lambda values: _reduced_non_zero(values),
        },
        outer,
    )


@graph
def _map_switch_reduced_non_zero(
    values: TSD[str, TSD[str, TSD[str, TS[float]]]],
    enabled: TSD[str, TS[bool]],
) -> TSD[str, TSD[str, TS[float]]]:
    return map_(_switch_reduced_non_zero, values, enabled)


def test_tsd_reduce_without_zero_is_invalid_when_empty_and_aliases_singleton():
    assert eval_node(
        _sum_without_explicit_zero,
        [None, {"a": 1}, {"b": 2}, {"b": REMOVE_IF_EXISTS}, {"a": REMOVE_IF_EXISTS}],
    ) == [None, 1, 3, 1, None]


def test_tsd_reduce_zero_is_used_only_for_empty_and_singleton():
    deltas = [
        {},
        {"a": 1},
        {"b": 2},
        {"b": REMOVE_IF_EXISTS},
        {"a": REMOVE_IF_EXISTS},
    ]
    assert eval_node(_offset_sum_without_zero, deltas) == [None, 1, 103, 1, None]
    assert eval_node(_offset_sum_with_zero, deltas) == [10, 111, 103, 111, 10]


def test_tsl_reduce_uses_the_same_zero_cardinality_rules():
    deltas = [{}, {0: 1}, {1: 2}]
    assert eval_node(_offset_sum_tsl_without_zero, deltas) == [None, 1, 103]
    assert eval_node(_offset_sum_tsl_with_zero, deltas) == [10, 111, 103]


def test_tsd_reduce_preserves_explicit_none_as_an_unset_zero():
    assert eval_node(_sum_with_none_zero, [None, {"a": 1, "b": 2}]) == [None, 3]


def test_tsd_reduce_wires_three_argument_map_inside_combiner():
    assert eval_node(
        _reduce_nested_dicts,
        [{1: {1: 1, 2: 2}}, {2: {1: 3, 2: 4}}, {3: {2: 1, 3: 3}}],
    ) == [{1: 1, 2: 2}, {1: 4, 2: 6}, {1: 4, 2: 7, 3: 3}]


def test_tsd_reduce_wires_switch_inside_mapped_values():
    assert eval_node(
        _reduce_mapped_switches,
        [
            {index: index for index in range(17)},
            None,
            {index: REMOVE for index in (1, 2, 3, 5, 6, 7, 9, 10, 11, 13, 15)},
            None,
            {index: 0 for index in range(0, 17, 2)},
            None,
            {index: index for index in range(17)},
            None,
        ],
    ) == [16, None, 5, None, 0, None, 16, None]


def test_callable_ellipsis_annotation_supports_reduce_overload_resolution():
    assert eval_node(
        _use_callable_annotated_reduce, [{"a": 1, "b": 2, "c": 4}]
    ) == [6]


def test_switch_samples_an_empty_nested_reduce_branch():
    keys = [f"s{index:02d}" for index in range(17)]
    assert eval_node(
        _switch_reduced_non_zero,
        [
            {
                key: {"a": float(index + 1), "b": float(index + 2)}
                for index, key in enumerate(keys)
            },
            {key: REMOVE_IF_EXISTS for key in keys[:11]},
            None,
            {key: REMOVE_IF_EXISTS for key in keys[11:]},
            None,
        ],
        [True, None, False, None, True],
    ) == [
        {"a": 153.0, "b": 170.0},
        {"a": 87.0, "b": 93.0},
        {"a": 87.0, "b": 93.0},
        {"a": REMOVE, "b": REMOVE},
        {},
    ]


def test_switch_reduce_map_shrink_with_trace():
    keys = [f"s{index:02d}" for index in range(17)]
    assert eval_node(
        _map_switch_reduced_non_zero,
        [
            {
                "p1": {
                    key: {"a": float(index + 1), "b": float(index + 2)}
                    for index, key in enumerate(keys)
                }
            },
            {"p1": {key: REMOVE_IF_EXISTS for key in keys[:11]}},
            None,
            {"p1": {key: REMOVE_IF_EXISTS for key in keys[11:]}},
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
    ) == [
        {"p1": {"a": 153.0, "b": 170.0}},
        {"p1": {"a": 87.0, "b": 93.0}},
        {"p1": {"a": 87.0, "b": 93.0}},
        {"p1": {"a": REMOVE, "b": REMOVE}},
        None,
        {"p1": {"a": 11.0, "b": 20.0}},
        {"p1": {"a": 10.0}},
        {"p1": {"a": 10.0, "b": 20.0}},
    ]
