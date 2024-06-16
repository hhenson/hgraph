from typing import Type

from hgraph import (
    COMPOUND_SCALAR,
    TS,
    SCALAR,
    HgTypeMetaData,
    CustomMessageWiringError,
    IncorrectTypeBinding,
    compute_node,
    with_signature,
    TimeSeries,
    TS_SCHEMA,
    TSB,
    DEFAULT,
    OUT,
    combine,
    convert,
    CompoundScalar,
)

__all__ = ()


def _check_schema(scalar, bundle):
    if bundle.meta_data_schema.keys() - scalar.meta_data_schema.keys():
        return f"Extra fields: {bundle.meta_data_schema.keys() - scalar.meta_data_schema.keys()}"
    for k, t in scalar.meta_data_schema.items():
        if (kt := bundle.meta_data_schema.get(k)) is None:
            if getattr(scalar.py_type, k, None) is None:
                return f"Missing input: {k}"
        elif not t.matches(kt if kt.is_scalar else kt.scalar_type()):
            return f"field {k} of type {t} does not accept {kt}"
    return True


@compute_node(
    overloads=combine,
    requires=lambda m, s: _check_schema(m[COMPOUND_SCALAR], m[TS_SCHEMA]),
    all_valid=lambda m, s: ("bundle",) if s["__strict__"] else None,
)
def combine_cs(
    tp_out_: Type[TS[COMPOUND_SCALAR]] = DEFAULT[OUT],
    tp_: Type[COMPOUND_SCALAR] = COMPOUND_SCALAR,
    __strict__: bool = True,
    **bundle: TSB[TS_SCHEMA],
) -> TS[COMPOUND_SCALAR]:
    return tp_(**{k: v.value for k, v in bundle.items()})


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type == TS[CompoundScalar],
    resolvers={COMPOUND_SCALAR: lambda m, s: m[TS_SCHEMA].py_type.scalar_type()},
    all_valid=lambda m, s: ("bundle",) if s["__strict__"] else None,
)
def convert_cs_from_tsb(bundle: TSB[TS_SCHEMA], __strict__: bool = True) -> TS[COMPOUND_SCALAR]:
    return bundle.value


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type != TS[CompoundScalar],
    all_valid=lambda m, s: ("bundle",) if s["__strict__"] else None,
)
def convert_cs_from_tsb_typed(
    bundle: TSB[TS_SCHEMA], __strict__: bool = True, tp_: Type[TS[COMPOUND_SCALAR]] = DEFAULT[OUT]
) -> TS[COMPOUND_SCALAR]:
    return bundle.value
