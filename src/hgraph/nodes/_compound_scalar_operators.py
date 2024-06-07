from typing import Type

from hgraph import compute_node, COMPOUND_SCALAR, TS, SCALAR, CompoundScalar, getattr_, type_, COMPOUND_SCALAR_1

__all__ = ("getattr_cs",)


@compute_node(overloads=getattr_, resolvers={SCALAR: lambda mapping, scalars: mapping[COMPOUND_SCALAR].meta_data_schema[scalars['attr']].py_type})
def getattr_cs(ts: TS[COMPOUND_SCALAR], attr: str) -> TS[SCALAR]:
    return getattr(ts.value, attr, None)


@compute_node(overloads=type_, requires=lambda m, s: COMPOUND_SCALAR not in m)
def cs_type_(ts: TS[COMPOUND_SCALAR_1]) -> TS[Type[CompoundScalar]]:
    return type(ts.value)


@compute_node(overloads=type_)
def cs_type_(ts: TS[COMPOUND_SCALAR_1]) -> TS[Type[COMPOUND_SCALAR]]:
    return type(ts.value)
