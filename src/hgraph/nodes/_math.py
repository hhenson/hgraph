import hgraph
from hgraph import compute_node, TS, NUMBER, lt_

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


@compute_node(overloads=lt_)
def lt_(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[bool]:
    return lhs.value < rhs.value

