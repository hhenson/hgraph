from enum import Enum
from typing import TypeVar, Type

from hgraph import compute_node, min_, TS, AUTO_RESOLVE, max_, lt_, gt_, le_, ge_, TS_OUT, eq_

__all__ = ('ENUM',)


ENUM = TypeVar('ENUM', bound=Enum)


@compute_node(overloads=min_)
def min_enum(lhs: TS[ENUM], rhs: TS[ENUM], e_type: Type[ENUM] = AUTO_RESOLVE) -> TS[ENUM]:
    return e_type(min(lhs.value, rhs.value))


@compute_node(overloads=max_)
def max_enum(lhs: TS[ENUM], rhs: TS[ENUM], e_type: Type[ENUM] = AUTO_RESOLVE) -> TS[ENUM]:
    return e_type(max(lhs.value.value, rhs.value.value))


@compute_node(overloads=eq_)
def eq_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return lhs.value is rhs.value


@compute_node(overloads=lt_)
def lt_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return lt_(lhs.value.value, rhs.value.value)


@compute_node(overloads=gt_)
def gt_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return gt_(lhs.value.value, rhs.value.value)


@compute_node(overloads=le_)
def le_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return le_(lhs.value.value, rhs.value.value)


@compute_node(overloads=ge_)
def ge_enum(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
    return ge_(lhs.value.value, rhs.value.value)


@compute_node(overloads=min_)
def min_enum_unary(ts: TS[ENUM], _output: TS_OUT[ENUM] = None) -> TS[ENUM]:
    """
    Unary min() - running min
    """
    if not _output.valid:
        return ts.value
    elif ts.value.value < _output.value.value:
        return ts.value


@compute_node(overloads=min_)
def max_enum_unary(ts: TS[ENUM], _output: TS_OUT[ENUM] = None) -> TS[ENUM]:
    """
    Unary max() - running max
    """
    if not _output.valid:
        return ts.value
    elif ts.value.value > _output.value.value:
        return ts.value
