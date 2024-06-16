from typing import Type

from hgraph import compute_node, COMPOUND_SCALAR, TS, SCALAR, CompoundScalar, getattr_, type_, COMPOUND_SCALAR_1

__all__ = tuple()


@compute_node(overloads=type_, requires=lambda m, s: COMPOUND_SCALAR not in m)
def type_cs_schema(ts: TS[COMPOUND_SCALAR_1]) -> TS[Type[CompoundScalar]]:
    return ts.value.__class__


@compute_node(overloads=type_)
def type_cs_typevar(ts: TS[COMPOUND_SCALAR_1]) -> TS[Type[COMPOUND_SCALAR]]:
    return ts.value.__class__
