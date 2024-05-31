from typing import Type

from hgraph import compute_node, TS, NUMBER, graph, WiringNodeClass, add_, sub_, mul_, div_, lt_, gt_, le_, zero, ge_

__all__ = ("add_ts", "sub_ts", "mult_ts", "div_ts", "lt_ts", "zero_int", "zero_float")


@compute_node(overloads=add_)
def add_ts(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[NUMBER]:
    """ Adds two time-series values of numbers together"""
    return lhs.value + rhs.value


@compute_node(overloads=sub_)
def sub_ts(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[NUMBER]:
    """ Subtracts two time-series values of numbers together"""
    return lhs.value - rhs.value


@compute_node(overloads=mul_)
def mult_ts(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[NUMBER]:
    """ Multiplies two time-series values of numbers together"""
    return lhs.value * rhs.value


@compute_node(overloads=div_)
def div_ts(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[float]:
    """ Divides two time-series values of numbers together"""
    # TODO: Provide options for improved handling of different scenarios,
    # e.g. divide by zero can be handled in different ways.
    # Add support for integer division
    return lhs.value / rhs.value


@compute_node(overloads=lt_)
def lt_ts(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[bool]:
    return bool(lhs.value < rhs.value)


@compute_node(overloads=gt_)
def gt_ts(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[bool]:
    return bool(lhs.value > rhs.value)


@compute_node(overloads=le_)
def le_ts(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[bool]:
    return bool(lhs.value <= rhs.value)


@compute_node(overloads=ge_)
def ge_ts(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[bool]:
    return bool(lhs.value >= rhs.value)


@graph(overloads=zero)
def zero_int(tp: Type[TS[int]], op: WiringNodeClass) -> TS[int]:
    mapping = {
        'add_': 0,
        'mul_': 1
    }
    return mapping[op.signature.name]


@graph(overloads=zero)
def zero_float(tp: Type[TS[float]], op: WiringNodeClass) -> TS[float]:
    mapping = {
        'add_': 0.,
        'mul_': 1.
    }
    return mapping[op.signature.name]


