from typing import Type

import hgraph
from hgraph import compute_node, TS, NUMBER, graph, WiringNodeClass, SCALAR, REF
from hgraph._runtime._operators import min_

__all__ = ("add_", "sub_", "mult_", "div_")


@compute_node(overloads=hgraph.add_)
def add_(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[NUMBER]:
    """ Adds two time-series values of numbers together"""
    return lhs.value + rhs.value


@compute_node(overloads=hgraph.sub_)
def sub_(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[NUMBER]:
    """ Subtracts two time-series values of numbers together"""
    return lhs.value - rhs.value


@compute_node(overloads=hgraph.mul_)
def mult_(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[NUMBER]:
    """ Multiplies two time-series values of numbers together"""
    return lhs.value * rhs.value


@compute_node(overloads=hgraph.div_)
def div_(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[float]:
    """ Divides two time-series values of numbers together"""
    # TODO: Provide options for improved handling of different scenarios,
    # e.g. divide by zero can be handled in different ways.
    # Add support for integer division
    return lhs.value / rhs.value


@compute_node(overloads=hgraph.lt_)
def lt_(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[bool]:
    return bool(lhs.value < rhs.value)


@graph(overloads=hgraph.zero)
def zero_int(tp: Type[TS[int]], op: WiringNodeClass) -> TS[int]:
    mapping = {
        'add_': 0,
        'mul_': 1
    }
    return mapping[op.signature.name]


@graph(overloads=hgraph.zero)
def zero_float(tp: Type[TS[float]], op: WiringNodeClass) -> TS[float]:
    mapping = {
        'add_': 0.,
        'mul_': 1.
    }
    return mapping[op.signature.name]


@graph(overloads=min_)
def min_of_one(ts: TS[SCALAR]) -> TS[SCALAR]:
    return ts


@graph(overloads=min_)
def min_of_two(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[NUMBER]:
    return min(lhs.value, rhs.value)

