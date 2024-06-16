from hgraph import compute_node, COMPOUND_SCALAR, TS, SCALAR, getattr_

__all__ = tuple()


@compute_node(
    overloads=getattr_,
    resolvers={SCALAR: lambda mapping, scalars: mapping[COMPOUND_SCALAR].meta_data_schema[scalars["attr"]].py_type},
)
def getattr_cs(ts: TS[COMPOUND_SCALAR], attr: str, default_value: TS[SCALAR] = None) -> TS[SCALAR]:
    attr_value = getattr(ts.value, attr, default_value.value)
    return default_value.value if attr_value is None else attr_value