"""map_'s upstream-parity kwargs (theme-F ruling 2026-08-01): __keys__,
__key_arg__, __label__."""

import pytest

from hgraph import TS, TSD, TSS, WiringError, compute_node, graph, map_
from hgraph.test import eval_node


@compute_node
def _add1(v: TS[int]) -> TS[int]:
    return v.value + 1


@compute_node
def _add_pair(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs.value + rhs.value


def test_map_explicit_keys_restrict_children():
    @graph
    def g(tsd: TSD[str, TS[int]], keys: TSS[str]) -> TSD[str, TS[int]]:
        return map_(_add1, tsd, __keys__=keys)

    assert eval_node(g, [{"a": 1, "b": 2}], [{"a"}]) == [{"a": 2}]


def test_map_key_arg_names_the_key_parameter():
    @compute_node
    def keyed(my_key: TS[str], v: TS[int]) -> TS[int]:
        return v.value * 10

    @graph
    def g(tsd: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return map_(keyed, tsd, __key_arg__="my_key")

    assert eval_node(g, [{"a": 5}]) == [{"a": 50}]


def test_map_label_is_accepted_and_traces():
    @graph
    def g(tsd: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return map_(_add1, tsd, __label__="my_map")

    assert eval_node(g, [{"a": 1}]) == [{"a": 2}]


def test_map_mismatched_key_types_retain_the_classifier_diagnostic():
    @graph
    def g(
        lhs: TSD[str, TS[int]], rhs: TSD[int, TS[int]]
    ) -> TSD[str, TS[int]]:
        return map_(_add_pair, lhs, rhs)

    with pytest.raises(
        WiringError,
        match="map_: every multiplexed TSD must share the same key type",
    ):
        eval_node(g, [{"a": 1}], [{1: 2}])
