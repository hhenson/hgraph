"""Type-carrier pin sweep.

Design record: ``docs/source/developer_guide/testing.rst`` ("Authoring-shape
sweeps"). A *type carrier* is a ``type[...]`` parameter, however it is
supplied: a bare subscript ``fn[X]``, a named one ``fn[VAR: X]``, an explicit
keyword ``to=X``, a ``DEFAULT[X]`` default, a bare ``= X`` default,
``AUTO_RESOLVE``, a scalar argument that carries a TS type, a scalar argument
that carries a type variable, a collection type, or a ``Size[n]``.

The 2026-09-04 retrospective found the carrier rules implemented once per
decorator kind in Python wiring (seven subscript rules, three type-variable
collectors, two shadow schema dictionaries, a two-pass materialisation). The
type-carrier blueprint moves the matching into the C++ resolver in stages.
This sweep pins how every decorator kind x carrier source x consumer behaves
TODAY, including the per-kind inconsistencies (marked ``blueprint risk N``)
and the cells that raise, so each stage can be verified against it. Where a
kind's behaviour is intentionally changed later, the pin changes in the same
PR and the diff says so.
"""

# No ``from __future__ import annotations``: nested signatures reference
# locally declared type variables and classes, which string annotations
# (resolved against module globals) cannot see.

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Generic, Mapping, Type, TypeVar

import _hgraph
import pytest

import hgraph as hg
from hgraph import (
    AUTO_RESOLVE,
    DEFAULT,
    K,
    OUT,
    SCALAR,
    SCALAR_1,
    SIZE,
    TIME_SERIES_TYPE,
    TS,
    TSB,
    TSD,
    TSL,
    V,
    CompoundScalar,
    Size,
    TimeSeriesSchema,
    WiringError,
    compute_node,
    const,
    graph,
    nothing,
    operator,
)
from hgraph._types import _value_type
from hgraph.test import eval_node

# Sweep-prefixed: named scalars intern on their bare name (#653).


@dataclass(frozen=True)
class SweepRow(CompoundScalar):
    value: int


@dataclass(frozen=True)
class SweepOther(CompoundScalar):
    label: str


class SweepColour(Enum):
    RED = 1
    BLUE = 2


class SweepPair(TimeSeriesSchema):
    a: TS[int]
    b: TS[str]


# --------------------------------------------------------------------------
# A. Decorator kind x carrier source: the body reads the materialised value
# --------------------------------------------------------------------------


def _name_of(carrier):
    """A stable, printable identity for what a body received."""
    if isinstance(carrier, type):
        return carrier.__name__
    return repr(carrier)


class TestNodeCarriers:
    """``@compute_node``: every spelling of a carrier on a node."""

    def test_bare_subscript_binds_default_carrier(self):
        @compute_node
        def schema_name(value: TS[int], schema: type[SCALAR] = DEFAULT[SCALAR]) -> TS[str]:
            return schema.__name__

        assert eval_node(schema_name[SweepRow], [1]) == ["SweepRow"]

    def test_named_subscript_binds_the_named_variable(self):
        @compute_node
        def schema_name(value: TS[int], schema: type[SCALAR] = DEFAULT[SCALAR]) -> TS[str]:
            return schema.__name__

        assert eval_node(schema_name[SCALAR: SweepOther], [1]) == ["SweepOther"]

    def test_explicit_keyword_type_value_binds_and_materialises(self):
        @compute_node
        def schema_name(value: TS[int], schema: type[SCALAR] = DEFAULT[SCALAR]) -> TS[str]:
            return schema.__name__

        assert eval_node(schema_name, [1], schema=SweepRow) == ["SweepRow"]

    def test_auto_resolve_from_a_time_series_input(self):
        @compute_node
        def type_name(ts: TS[SCALAR], tp: Type[SCALAR] = AUTO_RESOLVE) -> TS[str]:
            return tp.__name__

        assert eval_node(type_name, [1, 2]) == ["int", "int"]
        assert eval_node(type_name, [SweepRow(1)]) == ["SweepRow"]

    def test_ts_type_scalar_argument_binds_nested_variable(self):
        observed = []

        @compute_node
        def key_name(
            value: TS[int],
            tsd_type: type[TSD[K, TS[int]]],
            key_type: type[K] = AUTO_RESOLVE,
        ) -> TS[str]:
            observed.append((tsd_type, key_type))
            return key_type.__name__

        assert eval_node(key_name, [1], tsd_type=TSD[str, TS[int]]) == ["str"]
        assert observed == [(TSD[str, TS[int]], str)]

    def test_type_variable_scalar_argument_without_default(self):
        @compute_node
        def schema_name(value: TS[int], schema: type[SCALAR]) -> TS[str]:
            return schema.__name__

        assert eval_node(schema_name, [1], schema=SweepRow) == ["SweepRow"]

    def test_collection_type_argument_materialises_the_collection(self):
        observed = []

        @compute_node
        def collection(
            value: TS[int],
            tsd_type: type[TSD[K, TS[int]]],
            key_type: type[K] = AUTO_RESOLVE,
        ) -> TS[bool]:
            observed.append(key_type)
            return key_type == tuple[str, ...]

        assert eval_node(collection, [1], tsd_type=TSD[tuple[str, ...], TS[int]]) == [True]
        assert observed == [tuple[str, ...]]

    def test_explicit_output_specialisation_binds_every_variable(self):
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

    def test_size_carrier_from_a_fixed_list_input(self):
        @compute_node
        def size_of(tsl: TSL[TS[int], SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TS[int]:
            return _sz.SIZE

        assert eval_node(size_of, [(1, 2, 3)], resolution_dict={"tsl": TSL[TS[int], Size[3]]}) == [3]

    def test_bare_class_default_is_used_as_the_carrier_value(self):
        # A concrete class as the default of a type[...] parameter: the body
        # receives it as a plain default when the call omits it.
        @compute_node
        def schema_name(value: TS[int], schema: type[SCALAR] = SweepRow) -> TS[str]:
            return schema.__name__

        assert eval_node(schema_name, [1]) == ["SweepRow"]


class TestGraphCarriers:
    """``@graph``: the same spellings on a graph function."""

    def test_bare_subscript_binds_default_carrier(self):
        @graph
        def schema_name(value: TS[int], schema: type[SCALAR] = DEFAULT[SCALAR]) -> TS[str]:
            return const(schema.__name__)

        assert eval_node(schema_name[SweepRow], [1]) == ["SweepRow"]

    def test_named_subscript_binds_the_named_variable(self):
        @graph
        def schema_name(value: TS[int], schema: type[SCALAR] = DEFAULT[SCALAR]) -> TS[str]:
            return const(schema.__name__)

        assert eval_node(schema_name[SCALAR: SweepOther], [1]) == ["SweepOther"]

    def test_explicit_keyword_type_value_binds_and_materialises(self):
        @graph
        def schema_name(value: TS[int], schema: type[SCALAR] = DEFAULT[SCALAR]) -> TS[str]:
            return const(schema.__name__)

        assert eval_node(schema_name, [1], schema=SweepRow) == ["SweepRow"]

    def test_auto_resolve_from_a_time_series_input(self):
        @graph
        def type_name(ts: TS[SCALAR], tp: type[SCALAR] = AUTO_RESOLVE) -> TS[str]:
            return const(tp.__name__)

        assert eval_node(type_name, [1]) == ["int"]
        assert eval_node(type_name, [SweepRow(1)]) == ["SweepRow"]

    def test_ts_type_scalar_argument_binds_nested_variable(self):
        key_type_var = TypeVar("key_type_var")

        @graph
        def key_name(
            tsd_type: type[TSD[key_type_var, TS[int]]],
            key_type: type[key_type_var] = AUTO_RESOLVE,
        ) -> TS[str]:
            return const(key_type.__name__)

        @graph
        def app() -> TS[str]:
            return key_name(TSD[str, TS[int]])

        assert eval_node(app) == ["str"]

    def test_type_variable_scalar_argument_without_default(self):
        @graph
        def schema_name(value: TS[int], schema: type[SCALAR]) -> TS[str]:
            return const(schema.__name__)

        assert eval_node(schema_name, [1], schema=SweepRow) == ["SweepRow"]

    @pytest.mark.parametrize(
        "collection", [tuple[str, int], tuple[str, ...], frozenset[str], dict[str, int]]
    )
    def test_collection_type_argument_materialises_the_collection(self, collection):
        observed = []

        @graph
        def key_type_matches(
            tsd_type: type[TSD[K, TS[int]]],
            key_type: type[K] = AUTO_RESOLVE,
        ) -> TS[bool]:
            observed.append(key_type)
            return const(key_type == collection)

        @graph
        def app() -> TS[bool]:
            return key_type_matches(TSD[collection, TS[int]])

        assert eval_node(app) == [True]
        assert observed == [collection]

    def test_size_carrier_from_a_fixed_list_input(self):
        observed = []

        @graph
        def size_of(tsl: TSL[TS[int], SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TS[int]:
            observed.append(_sz)
            return const(_sz.SIZE)

        assert eval_node(size_of, [(1, 2)], resolution_dict={"tsl": TSL[TS[int], Size[2]]}) == [2]
        assert not isinstance(observed[0], int) and observed[0].SIZE == 2

    def test_size_carrier_by_subscript_materialises_as_a_plain_int(self):
        # blueprint risk 3: a Size pinned by subscript reaches the body as the
        # int 3, whereas the auto-resolved form (previous test) is a Size
        # object with a SIZE attribute.
        observed = []

        @graph
        def size_of(tsl: TSL[TS[int], SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TS[int]:
            observed.append(_sz)
            return const(_sz if isinstance(_sz, int) else _sz.SIZE)

        assert eval_node(size_of[Size[3]], [(1, 2, 3)]) == [3]
        assert observed == [3]

    def test_bare_class_default_is_used_as_the_carrier_value(self):
        @graph
        def schema_name(value: TS[int], schema: type[SCALAR] = SweepRow) -> TS[str]:
            return const(schema.__name__)

        assert eval_node(schema_name, [1]) == ["SweepRow"]


# One operator family for the whole module. An operator registers under a
# name derived from ``id(self)``; a family built inside each test was garbage
# collected between tests and its id reused, so the second build registered a
# second candidate set under the first's registry name and every call became
# "ambiguous overloads". Finding, not a sweep concern: keep the object alive.
_TYPED_OBSERVED = []


@operator
def _sweep_typed(value: TS[int], to: type[OUT] = DEFAULT[OUT]) -> OUT: ...


@compute_node(overloads=_sweep_typed)
def _sweep_typed_ts(
    value: TS[int],
    to: type[TS[SCALAR]] = OUT,
    scalar_type: type[SCALAR] = AUTO_RESOLVE,
) -> OUT:
    _TYPED_OBSERVED.append((to, scalar_type))
    return SweepRow(value.value)


class TestOperatorCarriers:
    """``@operator`` overloads: carriers on the contract and on the implementation."""

    def test_bare_subscript_selects_the_output_carrier(self):
        _TYPED_OBSERVED.clear()
        assert eval_node(_sweep_typed[TS[SweepRow]], [7]) == [SweepRow(7)]
        assert _TYPED_OBSERVED == [(TS[SweepRow], SweepRow)]

    def test_named_subscript_selects_the_output_carrier(self):
        _TYPED_OBSERVED.clear()
        assert eval_node(_sweep_typed[OUT: TS[SweepRow]], [7]) == [SweepRow(7)]
        assert _TYPED_OBSERVED == [(TS[SweepRow], SweepRow)]

    def test_explicit_keyword_carrier_on_the_call(self):
        _TYPED_OBSERVED.clear()
        assert eval_node(_sweep_typed, [8], to=TS[SweepRow]) == [SweepRow(8)]
        assert _TYPED_OBSERVED == [(TS[SweepRow], SweepRow)]

    def test_graph_overload_materialises_the_default_output_carrier(self):
        observed = []

        @operator
        def typed(value: TS[int]) -> DEFAULT[OUT]: ...

        @graph(overloads=typed)
        def typed_impl(value: TS[int], output_type: type[OUT] = DEFAULT[OUT]) -> OUT:
            observed.append(output_type)
            return value

        @graph
        def app(value: TS[int]) -> TS[int]:
            return typed[TS[int]](value)

        assert eval_node(app, [1]) == [1]
        assert observed == [TS[int]]

    def test_bare_subscript_binds_the_sole_scalar_variable(self):
        value_type = TypeVar("SWEEP_VALUE", SweepRow, SweepOther)

        class Result(TimeSeriesSchema, Generic[value_type]):
            value: TS[value_type]

        @operator
        def load(tp: type[value_type] = AUTO_RESOLVE) -> TSD[str, TSB[Result[value_type]]]: ...

        @graph(overloads=load)
        def load_row() -> TSD[str, TSB[Result[value_type]]]:
            return const[TSD[str, TSB[Result[SweepRow]]]]({})

        assert eval_node(load[SweepRow]) == [{}]

    def test_collection_output_carrier_materialises_a_fixed_tuple(self):
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

        assert eval_node(typed[TS[tuple[str, int]]], [7]) == [("value", 7)]
        assert observed == [(TS[tuple[str, int]], tuple[str, int])]


class TestServiceCarriers:
    """Generic reference services materialise one implementation per carrier.

    The single-interface implementation idiom *returns* its output; the
    ``wire_impl_out_stub`` idiom is for multi-interface implementations
    (``python/tests/test_generic_service_specializations.py``).
    """

    def test_reference_service_specialisation_materialises_per_subscript(self):
        value_type = TypeVar("SWEEP_SERVICE_VALUE", int, str)
        materialized = []

        @hg.reference_service
        def sweep_value(path: str = "shared") -> hg.TS[value_type]: ...

        @hg.service_impl(interfaces=sweep_value)
        def impl(path: str, tp: Type[value_type] = hg.AUTO_RESOLVE) -> hg.TS[value_type]:
            materialized.append(tp)
            return hg.const(1 if tp is int else "text")

        @hg.graph
        def app() -> hg.TS[str]:
            hg.register_service("shared", impl)
            return hg.format_("{}:{}", sweep_value[int](), sweep_value[str]())

        assert eval_node(app) == ["1:text"]
        assert materialized == [int, str]

    def test_single_interface_stub_idiom_is_rejected_with_a_scope_error(self):
        # Finding: a single-interface implementation is a registered
        # implementation graph, yet using the multi-interface stub idiom in it
        # (instead of returning the output) is refused as if it were not. The
        # same body with ``interfaces=(sweep_value, other)`` is the supported
        # multi-interface form. Pinned so the message can be corrected once.
        value_type = TypeVar("SWEEP_SERVICE_VALUE", int, str)

        @hg.reference_service
        def sweep_value(path: str = "shared") -> hg.TS[value_type]: ...

        @hg.service_impl(interfaces=sweep_value)
        def impl(path: str, tp: Type[value_type] = hg.AUTO_RESOLVE):
            sweep_value[tp].wire_impl_out_stub(path, hg.const(1 if tp is int else "text"))

        @hg.graph
        def app() -> hg.TS[int]:
            hg.register_service("shared", impl)
            return sweep_value[int]()

        with pytest.raises(ValueError, match="may only be used inside a registered implementation graph"):
            eval_node(app)


class TestAdaptorCarriers:
    """Generic adaptors and service adaptors specialise by subscript too, and
    the two stubs have their own rules: ``_AdaptorStub`` takes only
    ``TYPEVAR: concrete`` entries while ``_ServiceAdaptorStub`` (like a
    reference service) also binds a bare item to its sole variable. That is
    blueprint risk 1's per-kind inconsistency on the adaptor side; the pins
    flip together when one subscript rule lands.
    """

    @staticmethod
    def _adaptors():
        payload = TypeVar("SWEEP_PAYLOAD", int, str)

        @hg.adaptor
        def sweep_adaptor(value: TS[payload], path: str = "sweep_adaptor") -> TS[payload]: ...

        @hg.service_adaptor
        def sweep_service_adaptor(
            request: TS[payload], path: str = "sweep_service_adaptor"
        ) -> TS[payload]: ...

        return payload, sweep_adaptor, sweep_service_adaptor

    def test_adaptor_named_subscript_specialises_and_runs(self):
        payload, sweep_adaptor, _ = self._adaptors()
        int_adaptor = sweep_adaptor[payload:int]

        @hg.adaptor_impl(interfaces=(int_adaptor,))
        def impl(path: str):
            value = hg.from_graph(int_adaptor, path=path)
            hg.to_graph(int_adaptor, value + 1, path=path)

        @graph
        def app(value: TS[int]) -> TS[int]:
            hg.register_adaptor("sweep", impl)
            return sweep_adaptor(value, path="sweep")

        assert eval_node(app, [2, None, 4]) == [3, None, 5]

    def test_adaptor_bare_subscript_is_rejected_even_for_a_sole_variable(self):
        # blueprint risk 1 (per-kind inconsistency): the adaptor stub refuses
        # a bare item outright; a reference service and a service adaptor
        # bind it to the sole unresolved variable.
        _, sweep_adaptor, _ = self._adaptors()
        with pytest.raises(TypeError, match="requires TYPEVAR: concrete"):
            sweep_adaptor[int]

    def test_adaptor_constraint_violation_is_rejected(self):
        payload, sweep_adaptor, _ = self._adaptors()
        with pytest.raises(TypeError, match="must be one of"):
            sweep_adaptor[payload:float]

    def test_service_adaptor_named_subscript_specialises_and_runs(self):
        payload, _, sweep_service_adaptor = self._adaptors()
        assert sweep_service_adaptor[payload:int] is not None

        @hg.service_adaptor_impl(interfaces=sweep_service_adaptor)
        def impl(requests: TSD[int, TS[payload]]) -> TSD[int, TS[payload]]:
            return requests

        @graph
        def app(value: TS[int]) -> TS[int]:
            hg.register_adaptor("sweep_service", impl)
            return sweep_service_adaptor(value, path="sweep_service")

        assert eval_node(app, [2, None, 4]) == [2, None, 4]

    def test_service_adaptor_bare_subscript_binds_the_sole_variable(self):
        # blueprint risk 1: the service-adaptor stub accepts exactly what the
        # adaptor stub above refuses.
        _, _, sweep_service_adaptor = self._adaptors()
        assert sweep_service_adaptor[int] is not None


# --------------------------------------------------------------------------
# B. Consumers and ordering: resolvers see the carrier; requires sees the value
# --------------------------------------------------------------------------


class TestConsumersAndOrdering:
    def test_requires_sees_the_materialised_carrier_value(self):
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
            return const(key_type.__name__)

        @graph
        def app() -> TS[str]:
            return key_name(TSD[str, TS[int]])

        assert eval_node(app) == ["str"]
        assert observed == [(str, "value")]

    def test_requires_on_a_node_sees_the_default_carrier_value(self):
        observed = []

        @compute_node(requires=lambda mapping, schema: observed.append(schema) or schema is SweepRow)
        def schema_name(value: TS[int], schema: type[SCALAR] = DEFAULT[SCALAR]) -> TS[str]:
            return schema.__name__

        assert eval_node(schema_name[SweepRow], [1]) == ["SweepRow"]
        assert observed == [SweepRow]

    def test_resolvers_run_after_the_carrier_is_bound(self):
        observed = []

        @operator
        def typed(value: TS[int], to: type[OUT] = DEFAULT[OUT]) -> OUT: ...

        @compute_node(
            overloads=typed,
            resolvers={SCALAR_1: lambda mapping: SweepRow if SCALAR in mapping else str},
        )
        def typed_ts(
            value: TS[int],
            to: type[TS[SCALAR]] = OUT,
            scalar_type: type[SCALAR] = AUTO_RESOLVE,
            resolved_after_carrier: type[SCALAR_1] = AUTO_RESOLVE,
        ) -> OUT:
            observed.append((to, scalar_type, resolved_after_carrier))
            return SweepRow(value.value)

        assert eval_node(typed[TS[SweepRow]], [7]) == [SweepRow(7)]
        assert observed == [(TS[SweepRow], SweepRow, SweepRow)]

    def test_resolver_on_a_node_sees_the_input_binding(self):
        def to_string(x) -> str:
            return str(x)

        @compute_node(resolvers={SCALAR_1: lambda mapping, f: f.__annotations__["return"]})
        def call(ts: TS[SCALAR], f: type(to_string)) -> TS[SCALAR_1]:
            return f(ts.value)

        assert eval_node(call[SCALAR: int], [1, 2], f=to_string) == ["1", "2"]

    def test_requires_after_resolver_sees_the_resolved_carrier(self):
        observed = []

        @graph(
            resolvers={SCALAR_1: lambda mapping: str},
            requires=lambda mapping, out: observed.append(out) or out is str,
        )
        def g(value: TS[int], out: type[SCALAR_1] = AUTO_RESOLVE) -> TS[str]:
            return const(out.__name__)

        assert eval_node(g, [1]) == ["str"]
        assert observed == [str]


# --------------------------------------------------------------------------
# C. Negative cells
# --------------------------------------------------------------------------


class TestNegatives:
    def test_mismatching_output_carrier_raises_at_wiring(self):
        @operator
        def typed(value: TS[int], to: type[OUT] = DEFAULT[OUT]) -> OUT: ...

        @compute_node(overloads=typed)
        def typed_ts(
            value: TS[int],
            to: type[TS[SCALAR]] = OUT,
            scalar_type: type[SCALAR] = AUTO_RESOLVE,
        ) -> OUT:
            return SweepRow(value.value)

        with pytest.raises(WiringError):
            eval_node(typed[TSD[str, TS[int]]], [7])
        with pytest.raises(WiringError):
            eval_node(typed[TS[SweepRow]], [7], to=TSD[str, TS[int]])

    def test_two_variables_and_no_default_makes_a_bare_item_ambiguous_on_a_node(self):
        @compute_node
        def ambiguous(lhs: TS[K], rhs: TS[V]) -> TS[bool]:
            return True

        with pytest.raises(WiringError, match="DEFAULT"):
            ambiguous[str]

    def test_too_many_bare_items_raise_on_a_node(self):
        @compute_node
        def one_var(ts: TS[SCALAR]) -> TS[SCALAR]:
            return ts.value

        with pytest.raises(WiringError):
            one_var[int, str]

    def test_graph_ts_type_argument_binds_variables_without_validating_the_rest(self):
        # Finding (blueprint PR B target): the graph path matches a
        # type[TSD[K, TS[int]]] argument only to bind K; the concrete TS[int]
        # element is not checked, so TSD[str, TS[str]] is accepted. The C++
        # matcher rejects the outer mismatch; the pin flips in PR C.
        @graph
        def key_name(
            tsd_type: type[TSD[K, TS[int]]],
            key_type: type[K] = AUTO_RESOLVE,
        ) -> TS[str]:
            return const(key_type.__name__)

        @graph
        def app() -> TS[str]:
            return key_name(TSD[str, TS[str]])

        assert eval_node(app) == ["str"]


# --------------------------------------------------------------------------
# D. Reverse binding: T -> _value_type(T) -> Python type -> T
# --------------------------------------------------------------------------


_LATTICE = [
    int,
    float,
    str,
    bool,
    date,
    datetime,
    SweepRow,
    SweepColour,
    tuple[int, ...],
    frozenset[int],
    Mapping[str, int],
]


@pytest.mark.parametrize("python_type", _LATTICE, ids=lambda t: getattr(t, "__name__", repr(t)))
def test_reverse_binding_round_trips_through_a_constructed_ts(python_type):
    # The path AUTO_RESOLVE takes: TS[T] records T against the value schema
    # in the bridge's reverse-binding registry (RFC 0033, PR C; formerly the
    # shadow dictionaries), so the schema of a constructed time series reads
    # back as T through the one registry function.
    value_type = _hgraph.ts_value_vt(TS[python_type].handle)
    assert _hgraph.python_type_for_value(value_type) == python_type


@pytest.mark.parametrize("python_type", [int, float, str, bool, date, datetime, SweepRow, SweepColour],
                         ids=lambda t: t.__name__)
def test_native_reverse_binding_rebuilds_atomics_and_nominal_types(python_type):
    assert _hgraph.python_type_for_value(_value_type(python_type)) is python_type


@pytest.mark.parametrize(
    "python_type", [tuple[int, ...], frozenset[int], Mapping[str, int]],
    ids=["tuple", "frozenset", "Mapping"],
)
def test_native_reverse_binding_rebuilds_parameterised_generics(python_type):
    # Flipped in PR C (RFC 0033): the registry hands back the parameterised
    # annotation the DSL wrote, which the opaque/native/bundle/enum lookups
    # could not rebuild; before, the shadow dictionaries were the only path.
    assert _hgraph.python_type_for_value(_value_type(python_type)) == python_type


def test_reverse_binding_hands_back_the_most_recent_spelling():
    # One interned schema, many spellings: the registry keeps the spelling
    # written most recently, as the shadow dictionaries did, so the site that
    # resolved reads back what it wrote.
    class SweepAliasRow(CompoundScalar):
        x: int

    first = _value_type(Mapping[str, SweepAliasRow])
    assert _hgraph.python_type_for_value(first) == Mapping[str, SweepAliasRow]
    second = _value_type(dict[str, SweepAliasRow])
    assert first == second
    assert _hgraph.python_type_for_value(second) == dict[str, SweepAliasRow]


def test_reverse_binding_rebuilds_a_structural_schema_produced_by_resolution():
    # A schema no Python annotation produced (a bound variable inside
    # tuple[K, ...]) reads back as the canonical spelling built from its
    # elements, the spellings the full-value projection uses.
    class SweepResolvedRow(CompoundScalar):
        x: int

    element = _value_type(SweepResolvedRow)
    assert _hgraph.python_type_for_value(_hgraph.tuple_vt(element)) == tuple[SweepResolvedRow, ...]
    assert _hgraph.python_type_for_value(_hgraph.set_vt(element)) == frozenset[SweepResolvedRow]
    assert _hgraph.python_type_for_value(_hgraph.map_vt(_value_type(str), element)) == dict[str, SweepResolvedRow]
    assert _hgraph.python_type_for_value(_hgraph.fixed_tuple_vt([_value_type(str), element])) == tuple[str, SweepResolvedRow]


def test_scope_materialises_a_deferred_type_argument_in_every_form():
    # ResolutionScope.materialise (RFC 0033, PR C): a deferred default's
    # pattern resolved in the scope, projected as the type it carries; None
    # while a variable it needs is unbound.
    from hgraph._types import _scalar_pattern

    class SweepMaterialisedRow(CompoundScalar):
        x: int

    scope = _hgraph.ResolutionScope()
    assert scope.materialise(_scalar_pattern(tuple[K, ...])) is None
    assert scope.materialise(_hgraph.size_pattern_var("N")) is None
    scope.bind_scalar("K", _value_type(SweepMaterialisedRow))
    scope.bind_size("N", 3)
    # scalar form, structural: the annotation is rebuilt from the bound element
    assert scope.materialise(_scalar_pattern(tuple[K, ...])) == tuple[SweepMaterialisedRow, ...]
    assert scope.materialise(_scalar_pattern(K)) is SweepMaterialisedRow
    # size form
    assert scope.materialise(_hgraph.size_pattern_var("N")) == 3
    assert scope.materialise(_hgraph.size_pattern_value(2)) == 2
    # time-series form
    assert scope.materialise(TS[K].pattern) == TS[SweepMaterialisedRow].handle
    # and the matcher accepts a size pattern with a size value
    other = _hgraph.ResolutionScope()
    assert other.match_carrier(_hgraph.size_pattern_var("M"), 4)
    assert other.find_size("M") == 4


def test_reverse_binding_dies_with_the_metadata_on_reset():
    # The shadow dictionaries were keyed by native handles and never cleared,
    # so a handle recycled after reset_registries() could alias a new schema
    # to an old annotation. The registry is cleared with the metadata: after
    # a reset a fresh schema reads back as what was written for it.
    import os
    import subprocess
    import sys
    import textwrap

    script = textwrap.dedent(
        """
        import os
        import _hgraph
        from hgraph import TS  # noqa: F401 (package import initialises the bridge)
        from hgraph._types import _value_type

        before = _value_type(tuple[int, ...])
        assert _hgraph.python_type_for_value(before) == tuple[int, ...]
        _hgraph.reset_registries()
        after = _value_type(frozenset[int])
        assert _hgraph.python_type_for_value(after) == frozenset[int], \
            _hgraph.python_type_for_value(after)
        # Linux: reset + ordinary interpreter exit dies in the final GC.
        os._exit(0)
        """
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


# --------------------------------------------------------------------------
# E. Bare-subscript pinning order per decorator kind (blueprint risk 1)
# --------------------------------------------------------------------------


class TestBareSubscriptOrder:
    def test_graph_prefers_the_default_carrier_over_an_auto_resolve_carrier(self):
        # blueprint risk 1: graphs fill DEFAULT carriers first.
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

    def test_graph_fills_default_carriers_then_auto_carriers_in_declaration_order(self):
        # blueprint risk 1: two bare items on a graph.
        observed = []

        @graph
        def both(
            inferred: type[SCALAR_1] = AUTO_RESOLVE,
            schema: type[SCALAR] = DEFAULT[SCALAR],
        ) -> TS[str]:
            observed.append((inferred, schema))
            return const(f"{schema.__name__}/{inferred.__name__}")

        @graph
        def app() -> TS[str]:
            return both[int, str]()

        assert eval_node(app) == ["int/str"]
        assert observed == [(str, int)]

    def test_node_bare_item_binds_the_default_variable_among_several(self):
        # blueprint risk 1: nodes use sole-remaining-or-DEFAULT.
        @compute_node
        def pick(ts: TS[SCALAR], count: int) -> DEFAULT[OUT]:
            return str(ts.value) * count

        @graph
        def g(ts: TS[int]) -> TS[str]:
            return pick[TS[str]](ts, 2)

        assert eval_node(g, [7]) == ["77"]

    def test_node_enumerated_items_bind_in_declaration_order(self):
        from frozendict import frozendict as fd

        @compute_node
        def probe(values: TSD[K, TS[V]]) -> TS[str]:
            return ",".join(f"{k}={v}" for k, v in sorted(values.value.items()))

        assert eval_node(probe[str, int], [fd(a=1, b=2)]) == ["a=1,b=2"]

    def test_operator_bare_item_binds_the_sole_variable(self):
        @operator
        def scale(value: TS[SCALAR]) -> TS[SCALAR]: ...

        @compute_node(overloads=scale)
        def scale_int(value: TS[int]) -> TS[int]:
            return value.value * 2

        @compute_node(overloads=scale)
        def scale_str(value: TS[str]) -> TS[str]:
            return value.value * 2

        assert eval_node(scale[int], [3]) == [6]
        assert eval_node(scale[SCALAR: str], ["a"]) == ["aa"]

    def test_operator_bare_item_with_two_variables_and_no_default_is_accepted(self):
        # blueprint risk 1 (per-kind inconsistency): a node refuses this with
        # "K, V ... DEFAULT" at subscript time, an operator accepts it and
        # defers the meaning of the bare item to the registry's output rule.
        @operator
        def two(lhs: TS[K], rhs: TS[V]) -> TS[bool]: ...

        assert two[str] is not None


# --------------------------------------------------------------------------
# F. Name-keyed carrier table and const inference (blueprint risk 2)
# --------------------------------------------------------------------------


class TestNameKeyedCarriers:
    def test_const_positional_ts_type_selects_the_output_type(self):
        @graph
        def g() -> TS[float]:
            return const(1, TS[float])

        assert eval_node(g) == [1.0]

    def test_const_positional_type_then_positional_delay(self):
        # 0.5 signature: const(value, tp=AUTO_RESOLVE, delay=MIN_TD). With the
        # native family declaring ``tp`` (RFC 0033, PR B) the positional type
        # stays in place and a positional delay after it lands on ``delay``.
        from hgraph import MIN_TD

        @graph
        def g() -> TS[int]:
            return const(7, TS[int], MIN_TD * 2)

        assert eval_node(g) == [None, None, 7]

    def test_const_keyword_type_then_keyword_delay(self):
        from hgraph import MIN_TD

        @graph
        def g() -> TS[int]:
            return const(7, tp=TS[int], delay=MIN_TD)

        assert eval_node(g) == [None, 7]

    def test_nothing_positional_ts_type_selects_the_output_type(self):
        @graph
        def g(value: TS[int]) -> TS[int]:
            from hgraph import default

            return default(nothing(TS[int]), value)

        assert eval_node(g, [1, 2]) == [1, 2]

    def test_const_of_a_compound_scalar_infers_its_nominal_type(self):
        @graph
        def g() -> TS[SweepRow]:
            return const(SweepRow(3))

        assert eval_node(g) == [SweepRow(3)]

    def test_const_of_an_unregistered_class_registers_it_first(self):
        # blueprint risk 2: the const branch pre-registers an arbitrary class so
        # native inference sees a nominal schema.
        class SweepOpaque:
            pass

        value = SweepOpaque()

        @graph
        def g() -> TS[SweepOpaque]:
            return const(value)

        result = eval_node(g)
        assert result == [value]
        assert result[0] is value
