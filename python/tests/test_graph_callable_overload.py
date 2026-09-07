import inspect
from typing import Callable

import pytest

from hgraph import (
    K,
    K_1,
    TS,
    TSD,
    V,
    WiringError,
    const,
    graph,
    map_,
    operator,
    partition,
    subscription_service,
    until_true,
)
from hgraph.test import eval_node, wiring_context


def test_graph_overload_accepts_lambda_and_resolves_output_from_composition():
    @operator
    def partition_by(
        tsd: TSD[K, V],
        key_fn: Callable[..., TS[K_1]],
    ) -> TSD[K_1, TSD[K, V]]: ...

    @graph(overloads=partition_by)
    def partition_by_impl(
        tsd: TSD[K, V],
        key_fn: Callable[..., TS[K_1]],
    ) -> TSD[K_1, TSD[K, V]]:
        assert tuple(inspect.signature(key_fn).parameters) == ("key",)
        keys = map_(key_fn, __keys__=tsd.key_set)
        return partition(tsd, keys)

    @graph
    def app(tsd: TSD[str, TS[int]]) -> TSD[int, TSD[str, TS[int]]]:
        return partition_by(tsd, lambda key: const(1))

    assert eval_node(app, [{"a": 1}, {"b": 2}]) == [
        {1: {"a": 1}},
        {1: {"b": 2}},
    ]


def test_graph_overload_validates_requested_output_after_composition():
    @operator
    def identity_as(ts: TS[int]) -> TS[K_1]: ...

    @graph(overloads=identity_as)
    def identity_as_impl(ts: TS[int]) -> TS[K_1]:
        return ts

    @graph
    def app(ts: TS[int]) -> TS[str]:
        return identity_as[K_1:str](ts)

    with pytest.raises(WiringError, match="output"):
        eval_node(app, [1])


def test_runtime_callable_overload_keeps_value_callable_semantics():
    @graph
    def app(ts: TS[int]) -> TS[bool]:
        return until_true(lambda value: value >= 2, ts)

    assert eval_node(app, [1, 2, 3]) == [False, True, None]


def test_graph_overload_accepts_a_service_as_a_callable():
    @subscription_service
    def values(key: TS[str]) -> TS[int]: ...

    @operator
    def call_source(source: Callable) -> TS[int]: ...

    @graph(overloads=call_source)
    def call_source_impl(source: Callable) -> TS[int]:
        return source("key")

    with wiring_context():
        call_source(values)
