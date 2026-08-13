"""mesh_ follows map_'s applicable TSD argument-classification rules."""

import pytest

from hgraph import (
    TIME_SERIES_TYPE,
    TS,
    TSB,
    TSD,
    TS_SCHEMA,
    TSS,
    TimeSeriesSchema,
    WiringError,
    compute_node,
    dereference,
    graph,
    if_then_else,
    len_,
    mesh_,
    no_key,
    passive,
    pass_through,
)
from hgraph.test import eval_node


@compute_node
def _add_pair(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs.value + rhs.value


@graph
def _add_one_generic(value: TIME_SERIES_TYPE) -> TS[int]:
    return value + 1


@graph
def _add_two_generics(
    lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE
) -> TS[int]:
    return lhs + rhs


@graph
def _add_generic_dict_size(
    value: TS[int], whole: TIME_SERIES_TYPE
) -> TS[int]:
    return value + len_(whole)


@graph
def _add_generic_offset(
    value: TS[int], offset: TIME_SERIES_TYPE
) -> TS[int]:
    return value + offset


@graph
def _add_concrete_dict_size(
    whole: TSD[int, TS[int]], value: TS[int]
) -> TS[int]:
    return value + len_(whole)


@graph
def _return_rhs(_lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return rhs


@graph
def _whole_dict_size(
    _value: TS[int], whole: TSD[str, TS[int]]
) -> TS[int]:
    return len_(whole)


@compute_node
def _add_one(value: TS[int]) -> TS[int]:
    return value.value + 1


def test_mesh_first_generic_tsd_is_multiplexed():
    @graph
    def g(tsd: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return mesh_(_add_one_generic, tsd)

    assert eval_node(g, [{"a": 1, "b": 2}]) == [{"a": 2, "b": 3}]


def test_mesh_later_same_key_generic_tsd_is_multiplexed():
    @graph
    def g(
        lhs: TSD[str, TS[int]], rhs: TSD[str, TS[int]]
    ) -> TSD[str, TS[int]]:
        return mesh_(_add_two_generics, lhs, rhs)

    assert eval_node(
        g,
        [{"a": 1, "b": 2}],
        [{"a": 10, "b": 20}],
    ) == [{"a": 11, "b": 22}]


def test_mesh_later_different_key_generic_tsd_is_direct():
    @graph
    def g(
        mux: TSD[str, TS[int]], whole: TSD[int, TS[int]]
    ) -> TSD[str, TS[int]]:
        return mesh_(_add_generic_dict_size, mux, whole)

    assert eval_node(
        g,
        [{"a": 1, "b": 2}, None],
        [{1: 10, 2: 20}, {3: 30}],
    ) == [{"a": 3, "b": 4}, {"a": 4, "b": 5}]


def test_mesh_non_tsd_generic_input_is_direct():
    @graph
    def g(
        mux: TSD[str, TS[int]], offset: TS[int]
    ) -> TSD[str, TS[int]]:
        return mesh_(_add_generic_offset, mux, offset)

    assert eval_node(
        g,
        [{"a": 1, "b": 2}, None],
        [10, 20],
    ) == [{"a": 11, "b": 12}, {"a": 21, "b": 22}]


def test_mesh_generic_bundle_parameter_accepts_keyword_mapping():
    @graph
    def handler(
        value: TS[int], flag: TS[bool], limit: TS[int],
    ) -> TS[int]:
        return if_then_else(flag, value + limit, value - limit)

    @graph
    def child(value: TS[int], params: TSB[TS_SCHEMA]) -> TS[int]:
        resolved = dereference(params)
        return handler(value=value, **resolved)

    @graph
    def g(
        values: TSD[str, TS[int]], flag: TS[bool], limit: TS[int],
    ) -> TSD[str, TS[int]]:
        return mesh_(
            child,
            value=values,
            params={"flag": flag, "limit": limit},
            __keys__=values.key_set,
        )

    assert eval_node(
        g,
        [{"a": 2, "b": 3}, None],
        [True, False],
        [10, 5],
    ) == [
        {"a": 12, "b": 13},
        {"a": -3, "b": -2},
    ]


def test_mesh_passive_structural_bundle_materializes_before_child_binding():
    class Params(TimeSeriesSchema):
        flag: TS[bool]
        limit: TS[int]

    @graph
    def child(value: TS[int], params: TSB[TS_SCHEMA]) -> TS[int]:
        resolved = dereference(params)
        return if_then_else(
            resolved.flag, value + resolved.limit, value - resolved.limit,
        )

    @graph
    def g(
        values: TSD[str, TS[int]], flag: TS[bool], limit: TS[int],
    ) -> TSD[str, TS[int]]:
        params = passive(TSB[Params].from_ts(flag=flag, limit=limit))
        return mesh_(
            child, value=values, params=params, __keys__=values.key_set,
        )

    assert eval_node(
        g,
        [{"a": 2, "b": 3}, {"a": 4, "b": 5}],
        [True, False],
        [10, 5],
    ) == [
        {"a": 12, "b": 13},
        {"a": -1, "b": 0},
    ]


def test_mesh_concrete_tsd_before_first_multiplexed_input_is_direct():
    @graph
    def g(
        whole: TSD[int, TS[int]], mux: TSD[str, TS[int]]
    ) -> TSD[str, TS[int]]:
        return mesh_(_add_concrete_dict_size, whole, mux)

    assert eval_node(
        g,
        [{1: 10, 2: 20}, {3: 30}],
        [{"a": 1, "b": 2}, None],
    ) == [{"a": 3, "b": 4}, {"a": 4, "b": 5}]


def test_mesh_mismatched_multiplexed_key_types_keep_the_diagnostic():
    @graph
    def g(
        lhs: TSD[str, TS[int]], rhs: TSD[int, TS[int]]
    ) -> TSD[str, TS[int]]:
        return mesh_(_add_pair, lhs, rhs)

    with pytest.raises(
        WiringError,
        match="every multiplexed TSD must share the same key type",
    ):
        eval_node(g, [{"a": 1}], [{1: 2}])


def test_mesh_no_key_input_is_excluded_from_inferred_key_union():
    @graph
    def g(
        keys_source: TSD[str, TS[int]], values: TSD[str, TS[int]]
    ) -> TSD[str, TS[int]]:
        return mesh_(_return_rhs, keys_source, no_key(values))

    # If values contributed to the inferred union, its independently usable
    # "z" value would produce an observable output child.
    assert eval_node(
        g,
        [{"x": 1, "y": 2}],
        [{"x": 10, "z": 30}],
    ) == [{"x": 10}]


def test_mesh_pass_through_input_is_excluded_from_inferred_key_union():
    @graph
    def g(
        keys_source: TSD[str, TS[int]], whole: TSD[str, TS[int]]
    ) -> TSD[str, TS[int]]:
        return mesh_(_whole_dict_size, keys_source, pass_through(whole))

    # The mapped output does not depend on keys_source's values, so any key
    # incorrectly inferred from whole would be visible in the output.
    assert eval_node(
        g,
        [{"a": 1, "b": 2}],
        [{"p": 10, "q": 20, "r": 30}],
    ) == [{"a": 3, "b": 3}]


def test_mesh_all_no_key_inputs_require_explicit_keys():
    @graph
    def g(values: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return mesh_(_add_one, no_key(values))

    with pytest.raises(
        WiringError,
        match="every multiplexed input is no_key",
    ):
        eval_node(g, [{"a": 1}])


def test_mesh_no_key_with_explicit_keys_uses_that_lifecycle():
    @graph
    def g(
        values: TSD[str, TS[int]], keys: TSS[str]
    ) -> TSD[str, TS[int]]:
        return mesh_(_add_one, no_key(values), __keys__=keys)

    assert eval_node(
        g,
        [{"a": 1, "b": 2, "c": 3}],
        [{"a", "c"}],
    ) == [{"a": 2, "c": 4}]
