"""Sparse shared lookups retain their per-element subscriptions across map lifecycle changes."""
from collections import Counter

from hgraph import REMOVE, STATE, TS, TSD, compute_node, default, graph, map_, pass_through
from hgraph.test import eval_node


def test_shared_lookup_retargets_and_removes_consumers():
    calls = Counter()

    @compute_node
    def count(key: TS[str], value: TS[int], _state: STATE = None) -> TS[int]:
        calls[key.value] += 1
        _state.count = getattr(_state, "count", 0) + 1
        return _state.count

    @graph
    def child(key: TS[str], lookup: TS[str], shared: TSD[str, TS[int]]) -> TS[int]:
        return count(key, shared[lookup])

    @graph
    def app(lookups: TSD[str, TS[str]], shared: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return map_(child, lookups, pass_through(shared))

    assert eval_node(
        app,
        [{"a": "x", "b": "y", "c": "x"}, None, None, {"a": "y"}, {"c": REMOVE}, None, {"c": "x"}],
        [{"x": 1, "y": 2, "z": 3}, {"x": 4}, {"z": 5}, None, None, {"x": 6}, None],
    ) == [
        {"a": 1, "b": 1, "c": 1}, {"a": 2, "c": 2}, None, {"a": 3}, {"c": REMOVE}, None, {"c": 1},
    ]
    assert calls == {"a": 3, "b": 1, "c": 3}


def test_reported_shared_lookup_with_default_reads_only_changed_elements():
    calls = []

    @compute_node
    def read(value: TS[int]) -> TS[int]:
        calls.append(value.value)
        return value.value

    @graph
    def app(keys: TSD[str, TS[int]], shared: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return map_(lambda key, x, s: read(default(s[key], 0)), keys, pass_through(shared))

    assert eval_node(
        app,
        [{"a": 1, "b": 2}],
        [{"a": 10, "b": 20, "unselected": 30}, {"a": 11}, {"unselected": 31}, {"b": 21}],
    ) == [{"a": 10, "b": 20}, {"a": 11}, None, {"b": 21}]
    assert sorted(calls) == [10, 11, 20, 21]
