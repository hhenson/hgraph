from dataclasses import dataclass, field
import json
from typing import Generic, TypeVar

from hgraph import (
    HgCompoundScalarType,
    HgTypeMetaData,
    TS,
    TSB,
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


def test_dataclass_json_round_trip_reconstructs_the_original_class():
    quote = Quote("ABC", 100.0, 101.0, ("firm",))

    encoded = to_json_builder(Quote)(quote)
    decoded = from_json_builder(Quote)(json.loads(encoded))

    assert decoded == quote
    assert type(decoded) is Quote
