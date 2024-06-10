from enum import Enum
from typing import TypeVar

from hgraph import compute_node, min_, TS, lt_, gt_, le_, ge_, TS_OUT, eq_, TSL, SIZE, graph, max_

__all__ = ('ENUM',)


ENUM = TypeVar('ENUM', bound=Enum)


@graph(overloads=min_)
def min_enum(*ts: TSL[TS[ENUM], SIZE], default_value: TS[ENUM] = None) -> TS[ENUM]:
    if len(ts) == 1:
        return min_enum_unary(ts[0])
    elif len(ts) == 2:
        return min_enum_binary(ts[0], ts[1])
    else:
        return min_enum_multi(*ts, default_value=default_value)


@compute_node
def min_enum_unary(ts: TS[ENUM], _output: TS_OUT[ENUM] = None) -> TS[ENUM]:
    """
    Unary min() - running min
    """
    if not _output.valid:
        return ts.value
    elif ts.value.value < _output.value.value:
        return ts.value


@compute_node
def min_enum_binary(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[ENUM]:
    lhs = lhs.value
    rhs = rhs.value
    return lhs if lhs.value <= rhs.value else rhs


@compute_node
def min_enum_multi(*ts: TSL[TS[ENUM], SIZE], default_value: TS[ENUM] = None) -> TS[ENUM]:
    """
    Multi-arg min()
    """
    return min((arg.value for arg in ts), key=lambda enum: enum.value, default=default_value.value)


@graph(overloads=max_)
def max_enum(*ts: TSL[TS[ENUM], SIZE], default_value: TS[ENUM] = None) -> TS[ENUM]:
    if len(ts) == 1:
        return max_enum_unary(ts[0])
    elif len(ts) == 2:
        return max_enum_binary(ts[0], ts[1])
    else:
        return max_enum_multi(*ts, default_value=default_value)


@compute_node
def max_enum_unary(ts: TS[ENUM], _output: TS_OUT[ENUM] = None) -> TS[ENUM]:
    """
    Unary max() - running max
    """
    if not _output.valid:
        return ts.value
    elif ts.value.value > _output.value.value:
        return ts.value


@compute_node
def max_enum_binary(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[ENUM]:
    lhs = lhs.value
    rhs = rhs.value
    return lhs if lhs.value >= rhs.value else rhs


@compute_node
def max_enum_multi(*ts: TSL[TS[ENUM], SIZE], default_value: TS[ENUM] = None) -> TS[ENUM]:
    """
    Multi-arg enum value max()
    """
    return max((arg.value for arg in ts), key=lambda enum: enum.value, default=default_value.value)


@compute_node(overloads=eq_)
def eq_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return lhs.value is rhs.value


@compute_node(overloads=lt_)
def lt_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return bool(lhs.value.value < rhs.value.value)


@compute_node(overloads=gt_)
def gt_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return bool(lhs.value.value > rhs.value.value)


@compute_node(overloads=le_)
def le_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return bool(lhs.value.value <= rhs.value.value)


@compute_node(overloads=ge_)
def ge_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return bool(lhs.value.value >= rhs.value.value)
