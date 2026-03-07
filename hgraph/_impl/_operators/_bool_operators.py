from hgraph._operators import add_, sub_, mul_, pow_, and_, or_, not_
from hgraph._types import TS
from hgraph._wiring import WiringError, graph, compute_node

__all__ = ()


@graph(overloads=add_)
def add_booleans(lhs: TS[bool], rhs: TS[bool]) -> TS[bool]:
    raise WiringError("Cannot add two booleans")


@graph(overloads=sub_)
def sub_booleans(lhs: TS[bool], rhs: TS[bool]) -> TS[bool]:
    raise WiringError("Cannot subtract two booleans")


@graph(overloads=mul_)
def mul_booleans(lhs: TS[bool], rhs: TS[bool]) -> TS[bool]:
    raise WiringError("Cannot multiply two booleans")


@graph(overloads=pow_)
def pow_booleans(lhs: TS[bool], rhs: TS[bool]) -> TS[bool]:
    raise WiringError("Booleans do not support the power operator")


@compute_node(overloads=and_)
def and_booleans(lhs: TS[bool], rhs: TS[bool]) -> TS[bool]:
    return lhs.value and rhs.value


@compute_node(overloads=or_)
def or_booleans(lhs: TS[bool], rhs: TS[bool]) -> TS[bool]:
    return lhs.value or rhs.value


@compute_node(overloads=not_)
def not_boolean(ts: TS[bool]) -> TS[bool]:
    return not ts.value
