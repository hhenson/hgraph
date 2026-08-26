# Ported from release/0.5:hgraph_unit_tests/_wiring/test_auto_resolve.py
from dataclasses import dataclass
from typing import Callable, Type, TypeVar

import pytest

from hgraph import (
    AUTO_RESOLVE, DEFAULT, K, OUT, SCALAR, SCALAR_1, SIZE,
    TIME_SERIES_TYPE, CompoundScalar, Size, TS, TSD, TSL, WiringError,
    compute_node, graph, operator,
)
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


def test_graph_subscript_prioritizes_default_type_carrier():
    @graph
    def type_name(
        inferred: type[SCALAR_1] = AUTO_RESOLVE,
        schema: type[SCALAR] = DEFAULT[SCALAR],
    ) -> TS[str]:
        return const(schema.__name__)

    @graph
    def app() -> TS[str]:
        return type_name[int](inferred=str)

    assert eval_node(app) == ["int"]


@pytest.mark.parametrize(
    "expected_key_type",
    [
        tuple[str, int],
        tuple[str, ...],
        frozenset[str],
        dict[str, int],
    ],
)
def test_graph_materializes_collection_type_resolved_from_type_argument(
    expected_key_type,
):
    observed = []

    def requires(mapping, key_type):
        observed.append(key_type)
        return key_type == expected_key_type

    @graph(requires=requires)
    def key_type_matches(
        tsd_type: type[TSD[K, TS[int]]],
        key_type: type[K] = AUTO_RESOLVE,
    ) -> TS[bool]:
        return const(key_type == expected_key_type)

    @graph
    def app() -> TS[bool]:
        return key_type_matches(TSD[expected_key_type, TS[int]])

    assert eval_node(app) == [True]
    assert observed == [expected_key_type]


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


def test_node_materializes_fixed_tuple_from_output_specialization():
    observed = []

    @compute_node
    def keyed(
        value: TS[int],
        key_type: type[K] = AUTO_RESOLVE,
        value_type: type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
        _output_type: type[TSD[K, TIME_SERIES_TYPE]] = DEFAULT[OUT],
    ) -> TSD[K, TIME_SERIES_TYPE]:
        observed.append(key_type)
        return {("key", value.value): value.value}

    output_type = TSD[tuple[str, int], TS[int]]
    assert eval_node(keyed[output_type], [3]) == [{("key", 3): 3}]
    assert observed == [tuple[str, int]]


def test_node_materializes_default_scalar_type_argument():
    @dataclass(frozen=True)
    class Row(CompoundScalar):
        value: int

    @compute_node
    def schema_name(
        value: TS[int],
        schema: type[SCALAR] = DEFAULT[SCALAR],
    ) -> TS[str]:
        return schema.__name__

    assert eval_node(schema_name[Row], [1]) == ["Row"]


def test_output_type_carrier_binds_nested_scalar_and_rejects_wrong_ts_kind():
    @dataclass(frozen=True)
    class Row(CompoundScalar):
        value: int

    observed = []

    @operator
    def typed(value: TS[int], to: type[OUT] = DEFAULT[OUT]) -> OUT: ...

    @compute_node(
        overloads=typed,
        resolvers={SCALAR_1: lambda mapping: Row if SCALAR in mapping else str},
    )
    def typed_ts(
        value: TS[int],
        to: type[TS[SCALAR]] = OUT,
        scalar_type: type[SCALAR] = AUTO_RESOLVE,
        resolved_after_carrier: type[SCALAR_1] = AUTO_RESOLVE,
    ) -> OUT:
        observed.append((to, scalar_type, resolved_after_carrier))
        return Row(value.value)

    assert eval_node(typed[TS[Row]], [7]) == [Row(7)]
    assert observed == [(TS[Row], Row, Row)]

    assert eval_node(typed, [8], to=TS[Row]) == [Row(8)]
    assert observed == [(TS[Row], Row, Row), (TS[Row], Row, Row)]

    with pytest.raises(WiringError):
        eval_node(typed[TSD[str, TS[int]]], [7])

    with pytest.raises(WiringError):
        eval_node(typed[TS[Row]], [7], to=TSD[str, TS[int]])


def test_output_type_carrier_materializes_fixed_tuple_scalar():
    observed = []

    @operator
    def typed(value: TS[int], to: type[OUT] = DEFAULT[OUT]) -> OUT: ...

    @compute_node(overloads=typed)
    def typed_tuple(
        value: TS[int],
        to: type[TS[SCALAR]] = OUT,
        scalar_type: type[SCALAR] = AUTO_RESOLVE,
    ) -> OUT:
        observed.append((to, scalar_type))
        return "value", value.value

    output_type = TS[tuple[str, int]]
    assert eval_node(typed[output_type], [7]) == [("value", 7)]
    assert observed == [(output_type, tuple[str, int])]
