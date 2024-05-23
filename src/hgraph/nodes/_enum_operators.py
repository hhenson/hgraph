from enum import Enum
from typing import TypeVar, Type

from hgraph import compute_node, min_, TS, AUTO_RESOLVE, max_

__all__ = ('ENUM', 'min_enum', 'max_enum')


ENUM = TypeVar('ENUM', bound=Enum)


@compute_node(overloads=min_)
def min_enum(lhs: TS[ENUM], rhs: TS[ENUM], e_type: Type[ENUM] = AUTO_RESOLVE) -> TS[ENUM]:
    return e_type(min(lhs.value, rhs.value))


@compute_node(overloads=max_)
def max_enum(lhs: TS[ENUM], rhs: TS[ENUM], e_type: Type[ENUM] = AUTO_RESOLVE) -> TS[ENUM]:
    return e_type(max(lhs.value.value, rhs.value.value))
