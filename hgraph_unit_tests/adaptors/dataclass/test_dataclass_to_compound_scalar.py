from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple, get_origin, get_args

import pytest

from hgraph import CompoundScalar, graph, TS, combine, reduce, and_, TSL, valid, not_
from hgraph.adaptors.dataclass._dataclass_to_compound_scalar import CS
from hgraph.test import eval_node


def test_cs_basic():
    @dataclass
    class SimpleModel:
        id: int
        name: str

    assert issubclass(CS[SimpleModel], CompoundScalar)
    obj = CS[SimpleModel](id=1, name="test")
    assert obj.id == 1
    assert obj.name == "test"

    @graph
    def graph_test() -> TS[bool]:
        model1 = combine[TS[CS[SimpleModel]]](id=1, name="test")
        assertions = [
            model1.id == 1,
            model1.name == "test"
        ]
        return reduce(and_, TSL.from_ts(*assertions), zero=False)

    result = eval_node(graph_test)
    assert result[-1] == True


def test_cs_with_defaults():
    @dataclass
    class ModelWithDefaults:
        id: int
        name: str
        value: float = 0.0
        active: bool = True

    obj = CS[ModelWithDefaults](id=1, name="test")
    assert obj.id == 1
    assert obj.name == "test"
    assert obj.value == 0.0
    assert obj.active == True

    @graph
    def graph_test() -> TS[bool]:
        model1 = combine[TS[CS[ModelWithDefaults]]](id=1, name="test")
        assertions = [
            model1.id == 1,
            model1.name == "test",
            model1.value == 0.0,
            model1.active == True
        ]
        return reduce(and_, TSL.from_ts(*assertions), zero=False)

    result = eval_node(graph_test)
    assert result[-1] == True


def test_cs_with_optional():
    @dataclass
    class OptionalModel:
        id: int
        name: str
        description: Optional[str] = None

    obj = CS[OptionalModel](id=1, name="test")
    assert obj.id == 1
    assert obj.name == "test"
    assert obj.description is None

    @graph
    def graph_test() -> TS[bool]:
        model1 = combine[TS[CS[OptionalModel]]](id=1, name="test")
        model2 = combine[TS[CS[OptionalModel]]](id=1, name="test", description="test")
        assertions = [
            model1.id == 1,
            model1.name == "test",
            not_(valid(model1.description)),
            model2.id == 1,
            model2.name == "test",
            model2.description == "test",
        ]
        return reduce(and_, TSL.from_ts(*assertions), zero=False)

    result = eval_node(graph_test)
    assert result[-1] == True


def test_cs_with_nested_dataclass():
    @dataclass
    class Address:
        street: str
        city: str

    @dataclass
    class Person:
        id: int
        name: str
        address: Address

    PersonCS = CS[Person]
    AddressCS = CS[Address]

    assert PersonCS.__annotations__['address'] is AddressCS

    addr = AddressCS(street="123 Main St", city="NYC")
    person = PersonCS(id=1, name="Alice", address=addr)
    assert person.id == 1
    assert person.name == "Alice"
    assert person.address.street == "123 Main St"
    assert person.address.city == "NYC"

    @graph
    def graph_test() -> TS[bool]:
        addr = combine[TS[CS[Address]]](street="123 Main St", city="NYC")
        person = combine[TS[CS[Person]]](id=1, name="Alice", address=addr)
        assertions = [
            person.id == 1,
            person.name == "Alice",
            person.address.street == "123 Main St",
            person.address.city == "NYC"
        ]
        return reduce(and_, TSL.from_ts(*assertions), zero=False)

    result = eval_node(graph_test)
    assert result[-1] == True


def test_cs_with_list_of_dataclass():
    @dataclass
    class Item:
        name: str
        value: int

    @dataclass
    class Container:
        id: int
        items: Tuple[Item, Item]

    ContainerCS = CS[Container]
    ItemCS = CS[Item]

    items_type = ContainerCS.__annotations__['items']
    assert get_origin(items_type) is tuple
    assert get_args(items_type)[0] is ItemCS

    items = [ItemCS(name="a", value=1), ItemCS(name="b", value=2)]
    container = ContainerCS(id=1, items=items)
    assert container.id == 1
    assert len(container.items) == 2
    assert container.items[0].name == "a"
    assert container.items[1].value == 2

    @graph
    def graph_test() -> TS[bool]:
        item1 = ItemCS(name="a", value=1)
        item2 = ItemCS(name="b", value=2)
        container = combine[TS[ContainerCS]](id=1, items=(item1, item2))
        assertions = [
            container.id == 1,
            container.items[0].name == "a",
            container.items[1].name == "b",
        ]
        return reduce(and_, TSL.from_ts(*assertions), zero=False)

    result = eval_node(graph_test)
    assert result[-1] == True


def test_cs_caching():
    @dataclass
    class CachedModel:
        id: int
        name: str

    CS1 = CS[CachedModel]
    CS2 = CS[CachedModel]

    assert CS1 is CS2


def test_cs_plain_class():
    class PlainClass:
        id: int
        name: str

        def __init__(self, id: int, name: str = "default"):
            self.id = id
            self.name = name

    PlainCS = CS[PlainClass]
    assert issubclass(PlainCS, CompoundScalar)

    obj = PlainCS(id=1, name="test")
    assert obj.id == 1
    assert obj.name == "test"

    @graph
    def graph_test() -> TS[bool]:
        model1 = combine[TS[PlainCS]](id=1, name="test")
        assertions = [
            model1.id == 1,
            model1.name == "test"
        ]
        return reduce(and_, TSL.from_ts(*assertions), zero=False)

    result = eval_node(graph_test)
    assert result[-1] == True


def test_cs_pydantic():
    try:
        from pydantic import BaseModel
    except ImportError:
        pytest.skip("Pydantic not installed")

    class PydanticModel(BaseModel):
        id: int
        name: str
        value: float = 1.0

    PydanticCS = CS[PydanticModel]
    assert issubclass(PydanticCS, CompoundScalar)

    obj = PydanticCS(id=1, name="test", value=2.5)
    assert obj.id == 1
    assert obj.name == "test"
    assert obj.value == 2.5

    @graph
    def graph_test() -> TS[bool]:
        model1 = combine[TS[PydanticCS]](id=1, name="test", value=2.5)
        assertions = [
            model1.id == 1,
            model1.name == "test",
            model1.value == 2.5
        ]
        return reduce(and_, TSL.from_ts(*assertions), zero=False)

    result = eval_node(graph_test)
    assert result[-1] == True


def test_cs_error_invalid_type():
    class NoAnnotations:
        pass

    with pytest.raises(TypeError, match="CS\\[...\\] requires a user-defined class"):
        CS[NoAnnotations]

    with pytest.raises(TypeError, match="CS\\[...\\] requires a user-defined class"):
        CS[int]

    with pytest.raises(TypeError, match="CS\\[...\\] requires a class"):
        CS[123]
