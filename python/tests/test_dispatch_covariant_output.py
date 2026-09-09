from dataclasses import dataclass
from typing import TypeVar

from hgraph import CompoundScalar, TS, combine, compute_node, dispatch, graph, operator
from hgraph.reflection import operator_overloads, resolved_type
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


def test_recreated_dispatch_filters_branches_by_resolved_output_requirements():
    @dataclass(frozen=True)
    class Request(CompoundScalar):
        symbol: str

    @dataclass(frozen=True)
    class Model(CompoundScalar):
        name: str

    OUT = TypeVar("OUT", TS[int], TS[str])

    @operator
    def price(request: TS[Request], model: TS[Model]) -> OUT: ...

    @compute_node(overloads=price, requires=lambda m: resolved_type(m[OUT]) == TS[int])
    def live_price(request: TS[Request], model: TS[Model]) -> TS[int]:
        return 1

    @compute_node(overloads=price, requires=lambda m: resolved_type(m[OUT]) == TS[str])
    def historical_price(request: TS[Request], model: TS[Model]) -> TS[int]:
        return 2

    def extracted_price(output_type):
        @operator
        def extracted(request: TS[Request], model: TS[Model]) -> OUT: ...

        recreated = dispatch(extracted)
        for overload in operator_overloads(price):
            recreated.overload(overload)
        return recreated[output_type]

    @graph
    def app(request: TS[Request]) -> TS[int]:
        return extracted_price(TS[int])(request, Model(name="test"))

    assert eval_node(app, [Request(symbol="ES")]) == [1]
