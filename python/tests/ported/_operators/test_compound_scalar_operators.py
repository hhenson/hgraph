from dataclasses import dataclass
from typing import Tuple

import pytest

from hgraph import (
    CompoundScalar,
    SCALAR,
    TS,
    combine,
    downcast_ref,
    eq_,
    getattr_,
    graph,
    str_,
    type_,
)
from hgraph.test import eval_node

@dataclass
class _TestCS(CompoundScalar):
    a: int
    b: str = ""


def test_getatttr_cs():
    @graph
    def g(cs: TS[_TestCS]) -> TS[int]:
        return cs.a

    assert eval_node(g, [_TestCS(a=1), _TestCS(a=2)]) == [1, 2]


def test_eq_cs():
    @graph
    def app(lhs: TS[_TestCS], rhs: TS[_TestCS]) -> TS[bool]:
        return eq_(lhs, rhs)

    assert eval_node(app, lhs=[_TestCS(a=1), _TestCS(a=2)], rhs=[_TestCS(a=1), _TestCS(a=3)]) == [True, False]


def test_ne_cs():
    @graph
    def g(lhs: TS[_TestCS], rhs: TS[_TestCS]) -> TS[bool]:
        return lhs != rhs

    assert eval_node(g, lhs=[_TestCS(a=1), _TestCS(a=2)], rhs=[_TestCS(a=1), _TestCS(a=3)]) == [False, True]


def test_getattr_cs():
    @graph
    def g(ts: TS[_TestCS]) -> TS[int]:
        return ts.a

    assert eval_node(g, [_TestCS(a=1)]) == [1]


def test_getattr_computed_compound_scalar_descriptor():
    @dataclass(frozen=True)
    class Computed(CompoundScalar):
        value: int

        @property
        def doubled(self) -> int:
            return self.value * 2

    @graph
    def g(ts: TS[Computed]) -> TS[int]:
        return getattr_[SCALAR: int](ts, "doubled")

    assert eval_node(g, [Computed(3), Computed(5)]) == [6, 10]


def test_getattr_computed_descriptor_without_default_can_return_none():
    @dataclass(frozen=True)
    class Computed(CompoundScalar):
        value: int

        @property
        def optional_value(self) -> int:
            return self.value if self.value > 0 else None

    @graph
    def g(ts: TS[Computed]) -> TS[int]:
        return getattr_[SCALAR: int](ts, "optional_value")

    assert eval_node(g, [Computed(0), Computed(2)]) == [None, 2]


def test_getattr_computed_descriptor_uses_default_only_for_none():
    @dataclass(frozen=True)
    class Computed(CompoundScalar):
        value: int

        @property
        def optional_value(self) -> int:
            return self.value if self.value > 0 else None

    @graph
    def g(ts: TS[Computed], default_value: TS[int]) -> TS[int]:
        return getattr_[SCALAR: int](ts, "optional_value", default_value)

    assert eval_node(g, [Computed(0), Computed(2)], [7, 8]) == [7, 2]


def test_getattr_computed_descriptor_on_closed_union_leaf(monkeypatch):
    @dataclass(frozen=True)
    class ComputedBase(CompoundScalar, abstract=True):
        value: int

    @dataclass(frozen=True)
    class ComputedLeaf(ComputedBase):
        @property
        def doubled(self) -> int:
            return self.value * 2

    @graph
    def g(ts: TS[ComputedBase]) -> TS[int]:
        return getattr_[SCALAR: int](ts, "doubled")

    import hgraph._wiring._core as wiring_core

    original_wire = wiring_core.wire

    def reject_descriptor_dispatch(name, *args, **kwargs):
        if name == "getattr_":
            raise AssertionError("pinned computed descriptor used registry dispatch")
        return original_wire(name, *args, **kwargs)

    monkeypatch.setattr(wiring_core, "wire", reject_descriptor_dispatch)

    assert eval_node(g, [ComputedLeaf(3), ComputedLeaf(5)]) == [6, 10]


def test_combine_compound_scalar_dereferences_reference_fields():
    @dataclass(frozen=True)
    class Animal(CompoundScalar, abstract=True):
        name: str

    @dataclass(frozen=True)
    class Dog(Animal):
        pass

    @dataclass(frozen=True)
    class Kennel(CompoundScalar):
        dog: Dog

    @graph
    def g(animal: TS[Animal]) -> TS[Kennel]:
        return combine[TS[Kennel]](dog=downcast_ref(Dog, animal))

    dog = Dog("Fido")
    assert eval_node(g, [dog]) == [Kennel(dog)]


def test_combine_compound_scalar_accepts_covariant_variadic_tuple_fields():
    @dataclass(frozen=True)
    class Animal(CompoundScalar, abstract=True):
        name: str

    @dataclass(frozen=True)
    class Dog(Animal):
        pass

    @dataclass(frozen=True)
    class Pack(CompoundScalar):
        animals: tuple[Animal, ...]

    @graph
    def upcast(dogs: TS[tuple[Dog, ...]]) -> TS[tuple[Animal, ...]]:
        return dogs

    @graph
    def combine_pack(dogs: TS[tuple[Dog, ...]]) -> TS[Pack]:
        return combine[TS[Pack]](animals=dogs)

    dogs = (Dog("Fido"), Dog("Rex"))
    assert eval_node(upcast, [dogs]) == [dogs]
    assert eval_node(combine_pack, [dogs]) == [Pack(dogs)]


def test_getattr_cs_default():
    @dataclass
    class Test(CompoundScalar):
        b: str = None

    @graph
    def g(ts: TS[Test]) -> TS[str]:
        return getattr_(ts, "b", "DEFAULT")

    assert eval_node(g, [Test()]) == ["DEFAULT"]
    assert eval_node(g, [Test(b="")]) == [""]
    assert eval_node(g, [Test(b=None)]) == ["DEFAULT"]


def test_getattr_tuple_of_cs():
    @graph
    def g(ts: TS[Tuple[_TestCS, ...]]) -> TS[Tuple[int, ...]]:
        return ts.a

    assert eval_node(g, [(_TestCS(a=1), _TestCS(a=2, b="x"))]) == [(1, 2)]


def test_getattr_tuple_of_cs_default():
    @dataclass
    class Test(CompoundScalar):
        b: str = None

    @graph
    def g(ts: TS[Tuple[Test, ...]]) -> TS[Tuple[str, ...]]:
        return getattr_(ts, "b", "DEFAULT")

    assert eval_node(g, [tuple(), (Test(), Test(b=""), Test(b=None))]) == [tuple(), ("DEFAULT", "", "DEFAULT")]


def test_type_cs():
    @graph
    def g(ts: TS[_TestCS]) -> TS[type]:
        return type_(ts)

    assert eval_node(g, [_TestCS(a=1)]) == [_TestCS]


def test_type_comparison_with_class_literal():
    # Registering a parameterized type annotation must not change subsequent
    # schema-free inference for Python class literals.
    assert TS[type[_TestCS]] == TS[object]

    @graph
    def g(ts: TS[int]) -> TS[bool]:
        return type_(ts) == int

    assert eval_node(g, [1, 2]) == [True, True]


def test_polymorphic_compound_scalar_type_comparison():
    @dataclass(frozen=True)
    class Base(CompoundScalar, abstract=True):
        value: int

    @dataclass(frozen=True)
    class First(Base):
        pass

    @dataclass(frozen=True)
    class Second(Base):
        pass

    @graph
    def g(ts: TS[Base]) -> TS[bool]:
        return type_(ts) == First

    assert eval_node(g, [First(1), Second(2)]) == [True, False]


def test_getattr_type():
    @dataclass
    class Test(CompoundScalar):
        b: str = None

    @graph
    def g(ts: TS[Test]) -> TS[str]:
        return getattr_(type_(ts), "name")

    assert eval_node(g, [Test()]) == ["Test"]


@pytest.mark.skip(reason="deviation: CompoundScalar strings use the C++ bundle representation")
def test_str_cs():
    @graph
    def g(ts: TS[_TestCS]) -> TS[str]:
        return str_(ts)

    assert eval_node(g, [_TestCS(a=1)]) == ["_TestCS(a=1, b='')"]
