from hgraph import add_, TS, WiringError, graph, sub_, mul_, pow_

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
