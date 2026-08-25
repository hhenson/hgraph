import json
from collections.abc import Mapping
from dataclasses import InitVar, dataclass, field
from typing import ClassVar, Generic, Optional, TypeVar

import pytest

from hgraph import (
    MIN_TD,
    SCALAR,
    TS,
    TSB,
    TSD,
    TSS,
    TimeSeriesSchema,
    combine,
    compute_node,
    const,
    convert,
    dispatch_,
    drop_dups,
    eq_,
    from_json_builder,
    generator,
    getattr_,
    graph,
    operator,
    register_python_object_type,
    sink_node,
    to_json_builder,
)
from hgraph._types import _value_type
from hgraph.reflection import fields, is_compound_scalar, scalar_type
from hgraph.test import eval_node


def test_plain_dataclass_is_a_nominal_python_owned_bundle():
    @dataclass(frozen=True)
    class Quote:
        instrument: str
        bid: float
        ask: float

    meta = _value_type(Quote)
    quote = Quote("ABC", 100.0, 101.0)

    @compute_node
    def identity(value: TS[Quote]) -> TS[Quote]:
        return value.value

    @graph
    def bid(value: TS[Quote]) -> TS[float]:
        return value.bid

    result = eval_node(identity, [quote])
    assert result[0] is quote
    assert eval_node(bid, [quote]) == [100.0]
    assert meta.name.endswith("::Quote")
    assert fields(Quote) == {
        "instrument": str,
        "bid": float,
        "ask": float,
    }
    assert fields(TS[Quote]) == fields(Quote)
    assert scalar_type(TS[Quote]) is Quote
    assert is_compound_scalar(TS[Quote])


def test_python_owned_assignment_is_lenient_until_a_field_is_extracted():
    @dataclass(frozen=True)
    class Descriptive:
        number: int

    value = Descriptive("accepted by Python")

    @compute_node
    def identity(source: TS[Descriptive]) -> TS[Descriptive]:
        return source.value

    @graph
    def number(source: TS[Descriptive]) -> TS[int]:
        return source.number

    assert eval_node(identity, [value])[0] is value
    with pytest.raises(Exception, match="int"):
        eval_node(number, [value])


def test_explicit_accessor_type_can_read_a_pre_inflation_field_value():
    @dataclass(frozen=True)
    class Spec:
        name: str

    @dataclass(frozen=True)
    class Series:
        spec: Spec

    @graph
    def spec_symbol(series: TS[Series]) -> TS[str]:
        return getattr_[SCALAR:str](series, "spec")

    assert eval_node(spec_symbol, [Series(spec="ICE_H")]) == ["ICE_H"]


def test_const_map_retains_pre_inflation_python_owned_values():
    @dataclass(frozen=True)
    class Spec:
        name: str

    @dataclass(frozen=True)
    class DerivedSpec(Spec):
        venue: str

    @dataclass(frozen=True)
    class Series:
        spec: Spec

    @dataclass(frozen=True, kw_only=True)
    class SeriesMixin:
        months_ahead: int

    @dataclass(frozen=True, kw_only=True)
    class DerivedSeries(SeriesMixin, Series):
        spec: DerivedSpec

    _value_type(DerivedSeries)
    value = DerivedSeries(spec="ICE_H", months_ahead=12)

    captured = []

    @sink_node
    def capture(items: TSD[str, TS[Series]]):
        captured.append(items.value)

    @graph
    def app():
        capture(const[TSD[str, TS[Series]]]({"item": value}))

    eval_node(app)
    assert captured == [{"item": value}]


def test_explicit_registration_supports_annotated_application_classes():
    @register_python_object_type
    class LegacyQuote:
        instrument: str
        price: float

        def __init__(self, price, instrument="ABC"):
            self.instrument = instrument
            self.price = price

    value = LegacyQuote(42.0)

    @compute_node
    def identity(source: TS[LegacyQuote]) -> TS[LegacyQuote]:
        return source.value

    @graph
    def from_bundle(price: TS[float]) -> TS[LegacyQuote]:
        return combine[TSB[LegacyQuote]](price=price).as_scalar_ts()

    assert register_python_object_type(LegacyQuote) is LegacyQuote
    assert fields(LegacyQuote) == {"instrument": str, "price": float}
    assert eval_node(identity, [value])[0] is value
    reconstructed = eval_node(from_bundle, [42.0])[0]
    assert reconstructed.instrument == "ABC"
    assert reconstructed.price == 42.0


def test_explicit_registration_accepts_an_ordered_field_mapping():
    class Legacy:
        def __init__(self, left, right):
            self.left = left
            self.right = right

    register_python_object_type(
        Legacy, fields={"left": int, "right": str})

    assert fields(Legacy) == {"left": int, "right": str}
    with pytest.raises(TypeError, match="different field schema"):
        register_python_object_type(
            Legacy, fields={"left": str, "right": str})


def test_explicit_class_without_a_field_constructor_is_not_silently_rebuilt():
    @register_python_object_type
    class SchemaOnly:
        value: int

    source = SchemaOnly()
    source.value = 3

    @compute_node
    def identity(value: TS[SchemaOnly]) -> TS[SchemaOnly]:
        return value.value

    assert eval_node(identity, [source])[0] is source
    encoded = to_json_builder(SchemaOnly)(source)
    with pytest.raises(Exception, match="argument|keyword|constructor"):
        from_json_builder(SchemaOnly)(encoded)


def test_parameterized_dataclasses_resolve_invariant_field_schemas():
    value_type = TypeVar("value_type")

    @dataclass(frozen=True)
    class Box(Generic[value_type]):
        value: value_type

    integer_box = _value_type(Box[int])
    string_box = _value_type(Box[str])
    value = Box[int](7)

    @compute_node
    def identity(source: TS[Box[int]]) -> TS[Box[int]]:
        return source.value

    @graph
    def from_bundle(value: TS[int]) -> TS[Box[int]]:
        return combine[TSB[Box[int]]](value=value).as_scalar_ts()

    assert integer_box != string_box
    assert integer_box.local_name == "Box[int]"
    assert string_box.local_name == "Box[str]"
    assert fields(Box[int]) == {"value": int}
    assert fields(TS[Box[str]]) == {"value": str}
    assert scalar_type(TS[Box[int]]) == Box[int]
    assert eval_node(identity, [value])[0] is value
    assert eval_node(from_bundle, [7]) == [Box[int](7)]


def test_parameterized_dataclass_inheritance_preserves_the_active_child():
    value_type = TypeVar("value_type")

    @dataclass(frozen=True)
    class Base(Generic[value_type]):
        value: value_type

    @dataclass(frozen=True)
    class IntegerValue(Base[int]):
        label: str

    @compute_node
    def upcast(value: TS[IntegerValue]) -> TS[Base[int]]:
        return value.value

    @compute_node
    def is_child(value: TS[Base[int]]) -> TS[bool]:
        return isinstance(value.value, IntegerValue)

    @graph
    def app(value: TS[IntegerValue]) -> TS[bool]:
        return is_child(upcast(value))

    assert fields(TS[Base[int]]) == {"value": int}
    assert eval_node(
        app, [IntegerValue(value=1, label="one")]) == [True]


def test_python_owned_property_access_resolves_its_return_type():
    @dataclass(frozen=True)
    class Value:
        number: int

        @property
        def doubled(self) -> int:
            return self.number * 2

    @graph
    def app(value: TS[Value]) -> TS[int]:
        return value.doubled

    assert eval_node(app, [Value(3)]) == [6]


def test_tsb_round_trip_preserves_a_python_owned_polymorphic_field():
    @dataclass(frozen=True)
    class Instrument:
        symbol: str

    @dataclass(frozen=True)
    class Future(Instrument):
        expiry: str

    @dataclass(frozen=True)
    class Cash(Instrument):
        currency: str

    @dataclass(frozen=True)
    class Envelope:
        instrument: Instrument

    @graph
    def round_trip(instrument: TS[Instrument]) -> TS[Envelope]:
        return combine[TSB[Envelope]](
            instrument=instrument).as_scalar_ts()

    future = Future("ES", "2026-09")
    result = eval_node(round_trip, [future])
    assert result == [Envelope(future)]
    assert type(result[0].instrument) is Future

    cash = Cash("USD", "USD")
    assert eval_node(round_trip, [future, cash, future]) == [
        Envelope(future),
        Envelope(cash),
        Envelope(future),
    ]


def test_covariant_python_owned_field_reuses_inherited_native_schema():
    @dataclass(frozen=True)
    class Instrument:
        symbol: str

    @dataclass(frozen=True)
    class Future(Instrument):
        expiry: str

    @dataclass(frozen=True)
    class Envelope:
        instrument: Instrument

    @dataclass(frozen=True)
    class FutureEnvelope(Envelope):
        instrument: Future

    native_fields = dict(_value_type(FutureEnvelope).fields)
    assert native_fields["instrument"] == _value_type(Instrument)
    assert fields(FutureEnvelope)["instrument"] is Future

    @compute_node
    def is_future(value: TS[FutureEnvelope]) -> TS[bool]:
        return isinstance(value.value.instrument, Future)

    future = Future("ES", "2026-09")
    assert eval_node(is_future, [FutureEnvelope(future)]) == [True]


def test_tsd_of_generic_python_dataclass_bundles_tears_down_cleanly():
    T = TypeVar("T")

    @dataclass(frozen=True)
    class Unit:
        name: str

    @dataclass(frozen=True)
    class PrimaryUnit(Unit):
        dimension: str

    @dataclass(frozen=True)
    class Price(Generic[T]):
        qty: T
        currency_unit: Unit
        unit: Unit

    @dataclass(frozen=True)
    class Submission:
        instrument: str
        price: Price[float]

    @graph
    def ignore(values: TSD[str, TSB[Price[float]]]) -> TS[int]:
        return const(1)

    @graph
    def ignore_nested(values: TSD[int, TSB[Submission]]) -> TS[int]:
        return const(1)

    gbp_usd = Price(
        1.25,
        PrimaryUnit("USD", "currency"),
        PrimaryUnit("GBP", "currency"),
    )
    usd_gbp = Price(
        0.8,
        PrimaryUnit("GBP", "currency"),
        PrimaryUnit("USD", "currency"),
    )
    assert eval_node(
        ignore,
        [
            None,
            {
                "GBPUSD": gbp_usd,
                "USDGBP": usd_gbp,
            },
        ],
    ) == [1, None]
    assert eval_node(
        ignore_nested,
        [None, {1: Submission("GBPUSD", gbp_usd)}],
    ) == [1, None]


def test_combine_and_tsb_round_trip_use_the_python_constructor():
    initialized = []

    @dataclass(frozen=True, kw_only=True)
    class Quote:
        instrument: str
        bid: float
        ask: float = 0.0
        tags: tuple[str, ...] = field(default_factory=tuple)
        spread: float = field(init=False)

        def __post_init__(self):
            object.__setattr__(self, "spread", self.ask - self.bid)
            initialized.append(self.instrument)

    @graph
    def build(bid: TS[float]) -> TS[Quote]:
        return combine[TS[Quote]](instrument="ABC", bid=bid)

    @graph
    def from_bundle(bid: TS[float], ask: TS[float]) -> TS[Quote]:
        bundle = combine[TSB[Quote]](
            instrument="ABC", bid=bid, ask=ask)
        return bundle.as_scalar_ts()

    assert eval_node(build, [100.0]) == [
        Quote(instrument="ABC", bid=100.0)]
    assert eval_node(from_bundle, [100.0], [101.5]) == [
        Quote(instrument="ABC", bid=100.0, ask=101.5)]
    assert eval_node(from_bundle, [None, 100.0], [101.5, None]) == [
        None, Quote(instrument="ABC", bid=100.0, ask=101.5)]

    @graph
    def missing_required(bid: TS[float]) -> TS[Quote]:
        return combine[TS[Quote]](bid=bid)

    @graph
    def unknown_field(bid: TS[float]) -> TS[Quote]:
        return combine[TS[Quote]](instrument="ABC", bid=bid, typo=1)

    with pytest.raises(TypeError, match="instrument"):
        eval_node(missing_required, [100.0])
    with pytest.raises(TypeError, match="unknown field"):
        eval_node(unknown_field, [100.0])
    assert initialized.count("ABC") >= 4


def test_declared_and_computed_attribute_projection_uses_python_lookup():
    @dataclass(frozen=True)
    class Quote:
        bid: float
        ask: float

        @property
        def mid(self):
            return (self.bid + self.ask) / 2.0

    @graph
    def declared(value: TS[Quote]) -> TS[float]:
        return value.ask

    @graph
    def computed(value: TS[Quote]) -> TS[float]:
        return getattr_[SCALAR: float](value, "mid")

    values = [Quote(100.0, 102.0)]
    assert eval_node(declared, values) == [102.0]
    assert eval_node(computed, values) == [101.0]


def test_python_owned_hierarchy_uses_native_bundle_dispatch():
    @dataclass(frozen=True)
    class Animal:
        name: str

    @dataclass(frozen=True)
    class Dog(Animal):
        volume: int

    @dataclass(frozen=True)
    class Puppy(Dog):
        cute: bool = True

    @operator
    def sound(animal: TS[Animal]) -> TS[str]: ...

    @compute_node(overloads=sound)
    def dog_sound(animal: TS[Dog]) -> TS[str]:
        return type(animal.value).__name__

    @graph
    def app(animal: TS[Animal]) -> TS[str]:
        return dispatch_(sound, animal)

    assert eval_node(
        app,
        [Dog("dog", 1), Puppy("puppy", 2)],
    ) == ["Dog", "Puppy"]


def test_dispatch_distinguishes_python_generic_specializations():
    value_type = TypeVar("value_type")

    @dataclass(frozen=True)
    class Value:
        name: str

    @dataclass(frozen=True)
    class Box(Value, Generic[value_type]):
        item: value_type

    @operator
    def describe(value: TS[Value]) -> TS[str]: ...

    @compute_node(overloads=describe)
    def describe_int(value: TS[Box[int]]) -> TS[str]:
        return f"int:{value.value.item}"

    @compute_node(overloads=describe)
    def describe_str(value: TS[Box[str]]) -> TS[str]:
        return f"str:{value.value.item}"

    @graph
    def app(value: TS[Value]) -> TS[str]:
        return dispatch_(describe, value)

    assert eval_node(
        app,
        [Box[int]("one", 1), Box[str]("two", "two")],
    ) == ["int:1", "str:two"]


def test_python_equality_hashing_and_deduplication_are_preserved():
    @dataclass(frozen=True, eq=False)
    class Key:
        value: int

        def __eq__(self, other):
            return isinstance(other, Key) and self.value % 2 == other.value % 2

        def __hash__(self):
            return hash(self.value % 2)

    @graph
    def same(lhs: TS[Key], rhs: TS[Key]) -> TS[bool]:
        return eq_(lhs, rhs)

    first = Key(1)
    equal = Key(3)
    different = Key(2)
    assert eval_node(same, [first, first], [equal, different]) == [
        True, False]
    assert eval_node(drop_dups, [first, equal, different],
                     resolution_dict={"ts": TS[Key]}) == [
        first, None, different]
    assert TSS[Key].handle.is_tss
    assert TSD[Key, TS[int]].handle.is_tsd

    @dataclass
    class Unhashable:
        value: int

    with pytest.raises(TypeError, match="hashable"):
        TSS[Unhashable]
    with pytest.raises(TypeError, match="hashable"):
        TSD[Unhashable, TS[int]]


def test_const_map_uses_identity_when_a_python_owned_value_hash_fails():
    @dataclass(frozen=True)
    class Value:
        number: int

        def __hash__(self):
            raise RuntimeError("value hash must not be required")

    value = Value(7)

    @graph
    def app() -> TSD[str, TS[Value]]:
        return const[TSD[str, TS[Value]]]({"item": value})

    assert eval_node(app) == [{"item": value}]


def test_python_equality_exceptions_cross_the_bridge():
    @dataclass(frozen=True, eq=False)
    class Explosive:
        value: int

        def __eq__(self, other):
            raise ValueError("equality exploded")

    @graph
    def same(lhs: TS[Explosive], rhs: TS[Explosive]) -> TS[bool]:
        return eq_(lhs, rhs)

    with pytest.raises(Exception, match="equality exploded"):
        eval_node(same, [Explosive(1)], [Explosive(1)])


def test_recursive_python_owned_dataclass_preserves_nested_objects():
    @dataclass(frozen=True)
    class Recursive:
        value: int
        next: Optional["Recursive"] = None

    @compute_node
    def child_value(value: TS[Recursive]) -> TS[int]:
        child = value.value.next
        return -1 if child is None else child.value

    source = Recursive(1, Recursive(2))
    assert eval_node(child_value, [source, Recursive(3)]) == [2, -1]


def test_indirect_recursive_closed_hierarchy_realizes_from_leaf():
    @dataclass(frozen=True)
    class Instrument:
        symbol: str

    @dataclass(frozen=True)
    class ContractSpec:
        underlying: Instrument

    @dataclass(frozen=True)
    class ContractSeries:
        spec: ContractSpec

    @dataclass(frozen=True)
    class Future(Instrument):
        series: ContractSeries

    @compute_node
    def symbol(value: TS[Future]) -> TS[str]:
        return value.value.symbol

    future = Future("FUT", ContractSeries(ContractSpec(Instrument("SPOT"))))
    assert eval_node(symbol, [future]) == ["FUT"]


def test_time_series_schema_can_be_lifted_from_python_owned_dataclass():
    @dataclass(frozen=True)
    class Quote:
        bid: float
        ask: float

    schema = TimeSeriesSchema.from_scalar_schema(Quote)

    assert schema.scalar_type() is Quote
    assert schema.__annotations__ == {"bid": TS[float], "ask": TS[float]}
    assert TSB[schema].handle.is_tsb
    assert TSB[schema].handle == TSB[Quote].handle


def test_dataclass_bundle_derivation_is_conservative_and_interned():
    @dataclass(frozen=True)
    class Child:
        value: int

    @dataclass(frozen=True)
    class Model:
        number: int
        values: tuple[int, ...]
        lookup: Mapping[str, float]
        child: Child
        constructor_only: InitVar[str] = "ignored"
        class_value: ClassVar[int] = 1

        @property
        def computed(self):
            return self.number * 2

    first = TSB[Model]
    second = TSB[Model]
    explicit = TSB[TimeSeriesSchema.from_scalar_schema(Model)]

    assert first.handle == second.handle == explicit.handle
    assert fields(first) == {
        "number": TS[int],
        "values": TS[tuple[int, ...]],
        "lookup": TS[Mapping[str, float]],
        "child": TS[Child],
    }


def test_dataclass_bundle_rejects_non_scalar_and_unresolved_fields():
    @dataclass(frozen=True)
    class TimeSeriesField:
        value: TS[int]

    with pytest.raises(
        TypeError,
        match="field 'value' must resolve to a supported scalar type",
    ):
        TSB[TimeSeriesField]

    @dataclass(frozen=True)
    class UnresolvedField:
        value: "MissingBundleField"

    with pytest.raises(
        TypeError,
        match="field 'value' must resolve to a supported scalar type",
    ):
        TSB[UnresolvedField]


def test_generic_typevar_resolution_and_nested_container_substitution():
    value_type = TypeVar("value_type")

    @dataclass(frozen=True)
    class Box(Generic[value_type]):
        value: value_type

    @dataclass(frozen=True)
    class Batch(Generic[value_type]):
        values: tuple[Box[value_type], ...]

    @compute_node
    def unbox(value: TS[Box[value_type]]) -> TS[value_type]:
        return value.value.value

    assert fields(Batch[int]) == {"values": tuple[Box[int], ...]}
    assert eval_node(
        unbox,
        [Box[int](1), Box[int](2)],
        resolution_dict={"value": TS[Box[int]]},
    ) == [1, 2]


def test_bundle_conversion_and_json_codec_reconstruct_the_python_class():
    @dataclass(frozen=True)
    class Quote:
        bid: float
        ask: float

    @graph
    def as_bundle(value: TS[Quote]) -> TSB[Quote]:
        return convert[TSB](value)

    quote = Quote(100.0, 101.0)
    assert eval_node(as_bundle, [quote]) == [
        {"bid": 100.0, "ask": 101.0}]

    encoded = to_json_builder(Quote)(quote)
    decoded = from_json_builder(Quote)(encoded)
    assert json.loads(encoded) == {"bid": 100.0, "ask": 101.0}
    assert decoded == quote
    assert type(decoded) is Quote


def test_none_missing_and_descriptor_errors_follow_python_attribute_semantics():
    @dataclass(frozen=True)
    class Maybe:
        value: int

    @graph
    def value_or_default(source: TS[Maybe]) -> TS[int]:
        return getattr_(source, "value", 7)

    assert eval_node(value_or_default, [Maybe(None), Maybe(2)]) == [7, 2]

    @register_python_object_type
    class Explosive:
        value: int

        def __init__(self, value=0):
            self._value = value

        @property
        def value(self):
            raise ValueError("descriptor exploded")

    @graph
    def projected(source: TS[Explosive]) -> TS[int]:
        return source.value

    with pytest.raises(
        Exception,
        match=r"Explosive.*field 'value'|field 'value'.*Explosive",
    ):
        eval_node(projected, [Explosive()])


def test_mutually_recursive_python_owned_classes_are_schema_only_recursion():
    @dataclass(frozen=True)
    class Left:
        value: int
        right: Optional["Right"] = None

    @dataclass(frozen=True)
    class Right:
        value: int
        left: Optional[Left] = None

    @compute_node
    def nested_value(value: TS[Left]) -> TS[int]:
        return value.value.right.left.value

    source = Left(1, Right(2, Left(3)))
    assert eval_node(nested_value, [source]) == [3]


def test_indirect_recursive_union_realization_is_entry_order_independent():
    @dataclass(frozen=True)
    class Instrument:
        identifier: int

    @dataclass(frozen=True)
    class Specification:
        underlying: Instrument

    @dataclass(frozen=True)
    class Series:
        specification: Specification

    @dataclass(frozen=True)
    class Future(Instrument):
        series: Series

    @compute_node
    def underlying_identifier(value: TS[Series]) -> TS[int]:
        return value.value.specification.underlying.identifier

    value = Series(Specification(Instrument(7)))
    assert eval_node(underlying_identifier, [value]) == [7]


def test_in_place_mutation_does_not_create_a_new_value_tick():
    @dataclass
    class Mutable:
        number: int

    @generator
    def values() -> TS[Mutable]:
        value = Mutable(1)
        yield MIN_TD, value
        value.number = 2
        yield MIN_TD, value
        yield MIN_TD, Mutable(3)

    @graph
    def app() -> TS[int]:
        return drop_dups(values()).number

    # The generator resumes and mutates the retained object before the
    # downstream projection, so the first read may already observe 2. The
    # repeated object itself creates no second tick; the new object does.
    assert eval_node(app) == [None, 2, None, 3]


def test_multiple_inheritance_dispatch_reports_ambiguity():
    @dataclass(frozen=True)
    class Root:
        value: int

    @dataclass(frozen=True)
    class Left(Root):
        left: int

    @dataclass(frozen=True)
    class Right(Root):
        right: int

    @dataclass(frozen=True)
    class Hybrid(Left, Right):
        hybrid: int

    @operator
    def describe(value: TS[Root]) -> TS[str]: ...

    @graph(overloads=describe)
    def describe_left(value: TS[Left]) -> TS[str]:
        return "left"

    @graph(overloads=describe)
    def describe_right(value: TS[Right]) -> TS[str]:
        return "right"

    @graph
    def app(value: TS[Root]) -> TS[str]:
        return dispatch_(describe, value)

    with pytest.raises(RuntimeError, match="Ambiguous dispatch"):
        eval_node(app, [Hybrid(value=1, left=2, right=3, hybrid=4)])


def test_descriptor_backed_initvar_remains_a_field_through_multiple_inheritance():
    class Doubled:
        def __get__(self, instance, owner=None):
            return self if instance is None else instance.number * 2

        def __set__(self, instance, value):
            pass

    doubled_descriptor = Doubled()

    @dataclass(frozen=True, kw_only=True)
    class StoredBase:
        doubled: int

    @dataclass(frozen=True, kw_only=True)
    class ExpressionMixin:
        number: int
        doubled: InitVar[int] = doubled_descriptor

    @dataclass(frozen=True, kw_only=True)
    class Value(ExpressionMixin, StoredBase):
        pass

    assert fields(Value) == {"number": int, "doubled": int}
    assert tuple(name for name, _ in _value_type(Value).fields) == (
        "number", "doubled")
    assert Value(number=3).doubled == 6


def test_python_owned_combine_does_not_lift_descriptor_defaults():
    class Doubled:
        def __get__(self, instance, owner=None):
            return self if instance is None else instance.number * 2

        def __set__(self, instance, value):
            if value is not self:
                raise AttributeError("doubled is computed")

    doubled = Doubled()

    @dataclass(frozen=True, kw_only=True)
    class Value:
        number: int
        result: int = doubled

    @graph
    def app(number: TS[int]) -> TS[Value]:
        return combine[TS[Value]](number=number)

    assert eval_node(app, [3]) == [Value(number=3)]
