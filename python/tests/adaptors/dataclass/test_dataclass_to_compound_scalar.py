from dataclasses import dataclass
from typing import Optional

import pytest

from hgraph import CompoundScalar, TS, combine, eval_node, graph
from hgraph.adaptors.dataclass import CS


def test_cs_converts_nested_dataclasses_and_defaults():
    @dataclass
    class Address:
        city: str

    @dataclass
    class Person:
        name: str
        address: Address
        description: Optional[str] = None

    person_type = CS[Person]
    assert issubclass(person_type, CompoundScalar)
    assert person_type.__annotations__["address"] is CS[Address]
    assert CS[Person] is person_type

    @graph
    def build_person(name: TS[str], city: TS[str]) -> TS[person_type]:
        address = combine[TS[CS[Address]]](city=city)
        return combine[TS[person_type]](name=name, address=address)

    assert eval_node(build_person, ["Alex"], ["London"]) == [
        person_type(name="Alex", address=CS[Address](city="London"))
    ]


def test_cs_rejects_non_model_types():
    with pytest.raises(TypeError, match="requires a class"):
        CS[42]
    with pytest.raises(TypeError, match="user-defined class"):
        CS[int]


def test_cs_preserves_nominal_namespace_for_same_named_models():
    first = dataclass(type("Payload", (), {"__annotations__": {"value": int}}))
    first.__module__ = "tests.first"
    second = dataclass(type("Payload", (), {"__annotations__": {"value": int}}))
    second.__module__ = "tests.second"

    assert CS[first].__compound_namespace__ == "tests.first"
    assert CS[second].__compound_namespace__ == "tests.second"
