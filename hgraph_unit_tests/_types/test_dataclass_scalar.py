from dataclasses import InitVar, dataclass, field
import json
from collections.abc import Mapping
from typing import ClassVar, Generic, TypeVar

import pytest

from hgraph import (
    HgCompoundScalarType,
    HgTypeMetaData,
    NodeException,
    ParseError,
    TS,
    TSB,
    TimeSeriesSchema,
    combine,
    compute_node,
    convert,
    dispatch_,
    from_json_builder,
    graph,
    operator,
    to_json_builder,
)
from hgraph.reflection import fields, is_compound_scalar, scalar_type
from hgraph.test import eval_node


@dataclass(frozen=True)
class Quote:
    instrument: str
    bid: float
    ask: float = 0.0
    tags: tuple[str, ...] = field(default_factory=tuple)


VALUE = TypeVar("VALUE")
OTHER = TypeVar("OTHER")


@dataclass(frozen=True)
class Box(Generic[VALUE]):
    value: VALUE


@dataclass(frozen=True)
class LabelledBox(Box[int]):
    label: str


@dataclass(frozen=True)
class Animal:
    name: str


@dataclass(frozen=True)
class Dog(Animal):
    volume: int


@dataclass(frozen=True)
class Computed:
    value: int
    doubled: int = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "doubled", self.value * 2)


@dataclass(frozen=True)
class ConstructorDefaults:
    value: int
    marker: object = field(default_factory=object, kw_only=True)


@dataclass(frozen=True)
class Recursive:
    child: "Recursive"


@dataclass(frozen=True)
class RequiredPair:
    left: int
    right: int


@dataclass(frozen=True)
class BundleChild:
    value: int


@dataclass(frozen=True)
class ConservativeBundleShape:
    number: int
    values: tuple[int, ...]
    lookup: Mapping[str, float]
    child: BundleChild
    constructor_only: InitVar[str] = "ignored"
    class_value: ClassVar[int] = 1

    @property
    def computed(self) -> int:
        return self.number * 2


@dataclass(frozen=True)
class MaybeValue:
    label: str | None
    count: int


@dataclass(frozen=True)
class DispatchRoot:
    pass


@dataclass(frozen=True)
class DispatchLeft(DispatchRoot):
    pass


@dataclass(frozen=True)
class DispatchRight(DispatchRoot):
    pass


@dataclass(frozen=True)
class DispatchBoth(DispatchLeft, DispatchRight):
    pass


def test_dataclass_is_a_nominal_compound_scalar():
    metadata = HgTypeMetaData.parse_type(TS[Quote]).value_scalar_tp

    assert isinstance(metadata, HgCompoundScalarType)
    assert metadata.py_type is Quote
    assert fields(Quote) == {
        "instrument": str,
        "bid": float,
        "ask": float,
        "tags": tuple[str, ...],
    }
    assert fields(TS[Quote]) == fields(Quote)
    assert scalar_type(TS[Quote]) is Quote
    assert is_compound_scalar(Quote)
    assert is_compound_scalar(TS[Quote])
    assert "__serialise_discriminator_field__" not in vars(Quote)
    assert "__serialise_children__" not in vars(Quote)
    assert "__serialise_base__" not in vars(Quote)
    assert "__cpp_native__" not in vars(Quote)


def test_dataclass_value_and_field_projection_preserve_the_object():
    quote = Quote("ABC", 100.0, 101.0)

    @compute_node
    def identity(value: TS[Quote]) -> TS[Quote]:
        return value.value

    @graph
    def bid(value: TS[Quote]) -> TS[float]:
        return value.bid

    assert eval_node(identity, [quote])[0] is quote
    assert eval_node(bid, [quote]) == [100.0]


def test_combine_constructs_dataclass_and_honours_defaults():
    @graph
    def make_quote(bid: TS[float]) -> TS[Quote]:
        return combine[TS[Quote]](instrument="ABC", bid=bid)

    assert eval_node(make_quote, [100.0]) == [Quote("ABC", 100.0)]


def test_combine_calls_default_factories_and_accepts_keyword_only_fields():
    @graph
    def make(value: TS[int]) -> TS[ConstructorDefaults]:
        return combine[TS[ConstructorDefaults]](value=value)

    first, second = eval_node(make, [1, 2])

    assert first.value == 1
    assert second.value == 2
    assert first.marker is not second.marker


def test_dataclass_converts_to_and_from_tsb():
    @graph
    def as_bundle(value: TS[Quote]) -> TSB[Quote]:
        return convert[TSB](value)

    @graph
    def from_bundle(instrument: TS[str], bid: TS[float]) -> TS[Quote]:
        return convert[TS[Quote]](combine[TSB[Quote]](instrument=instrument, bid=bid))

    quote = Quote("ABC", 100.0, 101.0)
    assert eval_node(as_bundle, [quote]) == [{"instrument": "ABC", "bid": 100.0, "ask": 101.0, "tags": ()}]
    assert eval_node(from_bundle, ["ABC"], [100.0]) == [Quote("ABC", 100.0)]


def test_dataclass_bundle_derivation_is_conservative_and_cached():
    assert fields(TSB[ConservativeBundleShape]) == {
        "number": TS[int],
        "values": TS[tuple[int, ...]],
        "lookup": TS[Mapping[str, float]],
        "child": TS[BundleChild],
    }

    first_schema = TimeSeriesSchema.from_scalar_schema(ConservativeBundleShape)
    second_schema = TimeSeriesSchema.from_scalar_schema(ConservativeBundleShape)
    direct_meta = HgTypeMetaData.parse_type(TSB[ConservativeBundleShape])
    explicit_meta = HgTypeMetaData.parse_type(TSB[first_schema])

    assert first_schema is second_schema
    assert direct_meta == explicit_meta
    assert direct_meta.bundle_schema_tp.py_type is first_schema
    assert first_schema.scalar_type() is ConservativeBundleShape


def test_unparameterised_generic_dataclass_derives_a_generic_bundle():
    # TSB[Box] leaves VALUE unbound, so the derived bundle keeps a type
    # variable in its field annotations and has to be generic over it too.
    # Otherwise schema construction rejects it for holding unresolved types
    # while not being a generic class.
    schema = TimeSeriesSchema.from_scalar_schema(Box)

    assert schema.__parameters__ == (VALUE,)
    assert fields(TSB[Box]) == {"value": TS[VALUE]}

    # Binding the variable still resolves to the concrete bundle.
    assert fields(TSB[Box[int]]) == {"value": TS[int]}


def test_unparameterised_generic_dataclass_bundle_resolves_from_its_inputs():
    # A node declared over the unparameterised bundle resolves VALUE from the
    # concrete bundle it is wired to. This is the shape that regressed:
    # TSB[Box] in a signature could not be constructed at all.
    @compute_node
    def unwrap(value: TSB[Box]) -> TS[VALUE]:
        return value.value.value

    @graph
    def g(value: TS[int]) -> TS[int]:
        return unwrap(combine[TSB[Box[int]]](value=value))

    assert eval_node(g, [1, 2]) == [1, 2]


def test_partial_specialisation_repeating_a_type_var_stays_resolvable():
    # Pair[T, T] has __args__ (T, T) but only one parameter. Deriving the bundle
    # from the origin with those raw args would record __parameters__ as (T, T),
    # after which resolving with [int] takes the partial-resolution path and
    # leaves the fields unresolved.
    @dataclass(frozen=True)
    class Pair(Generic[VALUE, OTHER]):
        left: VALUE
        right: OTHER

    schema = TimeSeriesSchema.from_scalar_schema(Pair[VALUE, VALUE])

    assert schema.__parameters__ == (VALUE,)
    assert fields(TSB[schema[int]]) == {"left": TS[int], "right": TS[int]}

    # A fully bound specialisation still binds each position independently.
    assert fields(TSB[Pair[int, str]]) == {"left": TS[int], "right": TS[str]}


def test_dataclass_bundle_rejects_non_scalar_and_unresolved_fields():
    @dataclass(frozen=True)
    class TimeSeriesField:
        value: TS[int]

    with pytest.raises(ParseError, match="field 'value' must be a scalar type"):
        TSB[TimeSeriesField]

    @dataclass(frozen=True)
    class UnresolvedField:
        value: "MissingBundleField"

    with pytest.raises(ParseError, match="field 'value' has unresolved annotation"):
        TSB[UnresolvedField]


def test_dataclass_reconstruction_honours_init_false_and_post_init():
    @compute_node
    def bundle_value(bundle: TSB[Computed]) -> TS[Computed]:
        return bundle.value

    @graph
    def make(value: TS[int]) -> TS[Computed]:
        return combine[TS[Computed]](value=value)

    @graph
    def from_bundle(value: TS[int]) -> TS[Computed]:
        return combine[TSB[Computed]](value=value).as_scalar_ts()

    @graph
    def read_bundle(value: TS[int]) -> TS[Computed]:
        return bundle_value(combine[TSB[Computed]](value=value))

    assert eval_node(make, [3]) == [Computed(3)]
    assert eval_node(from_bundle, [3]) == [Computed(3)]
    assert eval_node(read_bundle, [3]) == [Computed(3)]


def test_partial_dataclass_bundle_value_supplies_none_for_missing_required_fields():
    @compute_node
    def bundle_value(bundle: TSB[RequiredPair]) -> TS[RequiredPair]:
        return bundle.value

    @graph
    def read_bundle(left: TS[int], right: TS[int]) -> TS[RequiredPair]:
        return bundle_value(combine[TSB[RequiredPair]](left=left, right=right))

    assert eval_node(read_bundle, [1], [None]) == [RequiredPair(1, None)]


def test_nonstrict_dataclass_conversion_supplies_none_for_missing_required_fields():
    @graph
    def from_partial_bundle(right: TS[int]) -> TS[RequiredPair]:
        bundle = combine[TSB[RequiredPair]](right=right)
        return convert[TS[RequiredPair]](bundle, __strict__=False)

    assert eval_node(from_partial_bundle, [2]) == [RequiredPair(None, 2)]


def test_dataclass_merge_preserves_required_none_values():
    @graph
    def merge_values(orig: TS[MaybeValue], delta: TS[MaybeValue]) -> TS[MaybeValue]:
        return combine(orig, delta)

    @graph
    def update_count(orig: TS[MaybeValue], count: TS[int]) -> TS[MaybeValue]:
        return combine[TS[MaybeValue]](orig, count=count)

    original = MaybeValue(None, 1)
    assert eval_node(merge_values, [original], [MaybeValue(None, 2)]) == [MaybeValue(None, 2)]
    assert eval_node(update_count, [original], [3]) == [MaybeValue(None, 3)]


def test_generic_dataclass_specializations_are_distinct_and_resolve_fields():
    integer_box = HgTypeMetaData.parse_type(TS[Box[int]]).value_scalar_tp
    string_box = HgTypeMetaData.parse_type(TS[Box[str]]).value_scalar_tp
    labelled_box = HgTypeMetaData.parse_type(TS[LabelledBox]).value_scalar_tp

    assert integer_box != string_box
    assert fields(Box[int]) == {"value": int}
    assert fields(Box[str]) == {"value": str}
    assert integer_box.matches(labelled_box)
    assert not integer_box.matches(string_box)

    @graph
    def unbox(value: TS[Box[int]]) -> TS[int]:
        return value.value

    assert eval_node(unbox, [Box[int](1), Box[int](2)]) == [1, 2]

    @compute_node
    def generic_unbox(value: TS[Box[VALUE]]) -> TS[VALUE]:
        return value.value.value

    assert eval_node(
        generic_unbox,
        [Box[int](1), Box[int](2)],
        resolution_dict={"value": TS[Box[int]]},
    ) == [1, 2]

    @graph
    def round_trip(value: TS[Box[int]]) -> TS[Box[int]]:
        return convert[TS[Box[int]]](convert[TSB](value))

    assert eval_node(round_trip, [Box[int](1), Box[int](2)]) == [Box[int](1), Box[int](2)]

    encoded = to_json_builder(Box[int])(Box[int](3))
    assert from_json_builder(Box[int])(json.loads(encoded)) == Box[int](3)


def test_parameterized_dataclass_can_be_assigned_to_bundle_output():
    @compute_node
    def echo(box: TSB[Box[int]]) -> TSB[Box[int]]:
        return box.value

    assert eval_node(echo, [Box(1), Box(2)]) == [{"value": 1}, {"value": 2}]


def test_recursive_dataclass_schema_is_lazy():
    assert fields(Recursive) == {"child": Recursive}


def test_dataclass_hierarchy_supports_runtime_dispatch():
    @operator
    def sound(animal: TS[Animal]) -> TS[str]: ...

    @compute_node(overloads=sound)
    def dog_sound(animal: TS[Dog]) -> TS[str]:
        return f"{animal.value.name}:{animal.value.volume}"

    @graph
    def app(animal: TS[Animal]) -> TS[str]:
        return dispatch_(sound, animal)

    assert eval_node(app, [Dog("Fido", 3)]) == ["Fido:3"]


def test_dataclass_multiple_inheritance_dispatch_reports_ambiguity():
    @operator
    def choose(value: TS[DispatchRoot]) -> TS[str]: ...

    @compute_node(overloads=choose)
    def choose_left(value: TS[DispatchLeft]) -> TS[str]:
        return "left"

    @compute_node(overloads=choose)
    def choose_right(value: TS[DispatchRight]) -> TS[str]:
        return "right"

    @graph
    def app(value: TS[DispatchRoot]) -> TS[str]:
        return dispatch_(choose, value)

    with pytest.raises(NodeException, match="Ambiguous dispatch"):
        eval_node(app, [DispatchBoth()])


def test_dataclass_reserved_metadata_is_not_silently_overwritten():
    @dataclass(frozen=True)
    class ReservedMetadata:
        value: int

        __hgraph_bundle_constructor_fields__ = ("different",)

    with pytest.raises(ParseError, match="reserved hgraph attribute"):
        HgTypeMetaData.parse_type(TS[ReservedMetadata]).value_scalar_tp.meta_data_schema


def test_dataclass_json_round_trip_reconstructs_the_original_class():
    quote = Quote("ABC", 100.0, 101.0, ("firm",))

    encoded = to_json_builder(Quote)(quote)
    decoded = from_json_builder(Quote)(json.loads(encoded))

    assert decoded == quote
    assert type(decoded) is Quote
