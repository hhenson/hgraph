from typing import Type

from hgraph import compute_node, COMPOUND_SCALAR, TS, SCALAR, CompoundScalar, getattr_, type_, COMPOUND_SCALAR_1

__all__ = ("getattr_cs",)


@compute_node(
    overloads=getattr_,
    resolvers={SCALAR: lambda mapping, scalars: mapping[COMPOUND_SCALAR].meta_data_schema[scalars['attr']].py_type})
def getattr_cs(ts: TS[COMPOUND_SCALAR], attr: str, default_value: TS[SCALAR] = None) -> TS[SCALAR]:
    attr_value = getattr(ts.value, attr, default_value.value)
    return default_value.value if attr_value is None else attr_value


@compute_node(overloads=type_, requires=lambda m, s: COMPOUND_SCALAR not in m)
def type_cs_schema(ts: TS[COMPOUND_SCALAR_1]) -> TS[Type[CompoundScalar]]:
    return ts.value.__class__


@compute_node(overloads=type_)
def type_cs_typevar(ts: TS[COMPOUND_SCALAR_1]) -> TS[Type[COMPOUND_SCALAR]]:
    return ts.value.__class__
