from typing import Type

from hgraph import compute_node, COMPOUND_SCALAR, TS, SCALAR, HgTypeMetaData, IncorrectTypeBinding, with_signature, \
    TimeSeries, CustomMessageWiringError, TSB, TS_SCHEMA, CompoundScalar
from hgraph._runtime._operators import getattr_, type_

__all__ = ("getattr_cs", "cs_from_ts", "cs_from_tsb")

from hgraph._types._scalar_types import COMPOUND_SCALAR_1


@compute_node(overloads=getattr_, resolvers={SCALAR: lambda mapping, scalars: mapping[COMPOUND_SCALAR].meta_data_schema[scalars['attr']].py_type})
def getattr_cs(ts: TS[COMPOUND_SCALAR], attr: str) -> TS[SCALAR]:
    return getattr(ts.value, attr, None)


def cs_from_ts(cls: Type[COMPOUND_SCALAR], **kwargs) -> TS[SCALAR]:
    scalar_schema = cls.__meta_data_schema__
    kwargs_schema = {k: HgTypeMetaData.parse_value(v).dereference() for k, v in kwargs.items()}

    for k, t in scalar_schema.items():
        if (kt := kwargs_schema.get(k)) is None:
            if getattr(cls, k, None) is None:
                raise CustomMessageWiringError(f"Missing input: {k}")
        elif not t.matches(kt if kt.is_scalar else kt.scalar_type()):
            raise IncorrectTypeBinding(t, kwargs_schema[k])

    @compute_node
    @with_signature(kwargs=kwargs_schema, return_annotation=TS[cls])
    def from_ts_node(**kwargs):
        return cls(**{k: v if not isinstance(v, TimeSeries) else v.value for k, v in kwargs.items()})

    return from_ts_node(**kwargs)


@compute_node(resolvers={COMPOUND_SCALAR: lambda mapping, tsb: mapping[TS_SCHEMA].py_type.scalar_type()})
def cs_from_tsb(tsb: TSB[TS_SCHEMA]) -> TS[COMPOUND_SCALAR]:
    return tsb.value


@compute_node(overloads=type_, requires=lambda m, s: COMPOUND_SCALAR not in m)
def cs_type_(ts: TS[COMPOUND_SCALAR_1]) -> TS[Type[CompoundScalar]]:
    return type(ts.value)


@compute_node(overloads=type_)
def cs_type_(ts: TS[COMPOUND_SCALAR_1]) -> TS[Type[COMPOUND_SCALAR]]:
    return type(ts.value)
