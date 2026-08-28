from dataclasses import dataclass

from hgraph import CompoundScalar, TS, combine, dispatch, graph
from hgraph.test import eval_node


def test_dispatch_adapts_covariant_branch_outputs_to_the_declared_base_type():
    @dataclass(frozen=True)
    class Animal(CompoundScalar, abstract=True):
        name: str

    @dataclass(frozen=True)
    class Cat(Animal):
        pass

    @dataclass(frozen=True)
    class Dog(Animal):
        pass

    @dataclass(frozen=True)
    class Instrument(CompoundScalar, abstract=True):
        symbol: str

    @dataclass(frozen=True)
    class Future(Instrument):
        expiry: int

    @dataclass(frozen=True)
    class Option(Instrument):
        strike: float

    @dispatch
    def instrument(animal: TS[Animal]) -> TS[Instrument]:
        return combine[TS[Future]](symbol="FUT", expiry=202612)

    @graph(overloads=instrument)
    def instrument_for_dog(animal: TS[Dog]) -> TS[Instrument]:
        return combine[TS[Option]](symbol="OPT", strike=42.0)

    @graph
    def app(animal: TS[Animal]) -> TS[Instrument]:
        return instrument(animal)

    assert eval_node(app, [Cat(name="cat"), Dog(name="dog")]) == [
        Future(symbol="FUT", expiry=202612),
        Option(symbol="OPT", strike=42.0),
    ]
