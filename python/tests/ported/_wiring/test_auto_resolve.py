# Ported from release/0.5:hgraph_unit_tests/_wiring/test_auto_resolve.py
from dataclasses import dataclass
from typing import Callable, Type, TypeVar

from hgraph import CompoundScalar, DEFAULT, K, OUT, TIME_SERIES_TYPE, graph, TSD, TSL, TS, SIZE, Size, AUTO_RESOLVE, SCALAR, SCALAR_1, compute_node
from hgraph import const
from hgraph.reflection import fields
from hgraph.test import eval_node


def test_auto_resolve():

    @graph
    def g(tsl: TSL[TS[int], SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TS[int]:
        return const(_sz.SIZE)

    assert eval_node(g, [(1, 2)], resolution_dict={"tsl": TSL[TS[int], Size[2]]}) == [2]


def test_func_resolve():
    def x(x) -> str:
        return str(x)

    @compute_node(resolvers={SCALAR_1: lambda mapping, f: f.__annotations__["return"]})
    def call(ts: TS[SCALAR], f: type(x)) -> TS[SCALAR_1]:
        return f(ts.value)

    assert eval_node(call[SCALAR:int], [1, 2], f=x) == ["1", "2"]


def test_compute_node_receives_auto_resolved_scalar_type():
    @compute_node
    def type_name(ts: TS[SCALAR], tp: Type[SCALAR] = AUTO_RESOLVE) -> TS[str]:
        return tp.__name__

    assert eval_node(type_name, [1, 2]) == ["int", "int"]


def test_graph_receives_auto_resolved_compound_scalar_type():
    @dataclass(frozen=True)
    class Config(CompoundScalar):
        count: int

    config_type = TypeVar("config_type", bound=Config)

    @graph(requires=lambda mapping: "count" in fields(mapping[config_type]))
    def field_count(
        ts: TS[config_type], tp: type[config_type] = AUTO_RESOLVE
    ) -> TS[int]:
        return const(len(fields(tp)))

    assert eval_node(field_count, [Config(7)]) == [1]


def test_graph_requires_receives_type_resolved_from_type_argument():
    key_type_var = TypeVar("key_type_var")
    observed = []

    def requires(mapping, key_type, value_col):
        observed.append((key_type, value_col))
        return key_type is str and value_col == "value"

    @graph(requires=requires)
    def key_name(
        tsd_type: type[TSD[key_type_var, TS[int]]],
        key_type: type[key_type_var] = AUTO_RESOLVE,
        value_col: str = "value",
    ) -> TS[str]:
        return key_type.__name__

    @graph
    def app() -> TS[str]:
        return key_name(TSD[str, TS[int]])

    assert eval_node(app) == ["str"]
    assert observed == [(str, "value")]


def test_node_auto_resolve_uses_explicit_output_specialization():
    observed = []

    @compute_node
    def keyed(
        value: TS[int],
        key_type: type[K] = AUTO_RESOLVE,
        value_type: type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
        _output_type: type[TSD[K, TIME_SERIES_TYPE]] = DEFAULT[OUT],
    ) -> TSD[K, TIME_SERIES_TYPE]:
        observed.append((key_type, value_type))
        return {"key": value.value}

    assert eval_node(keyed[TSD[str, TS[int]]], [3]) == [{"key": 3}]
    assert observed == [(str, TS[int])]
