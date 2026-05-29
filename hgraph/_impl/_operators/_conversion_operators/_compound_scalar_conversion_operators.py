from typing import Type

from hgraph import TimeSeriesSchema
from hgraph._operators import combine, convert
from hgraph._types import TS, TS_SCHEMA, TSB, DEFAULT, OUT, AUTO_RESOLVE, COMPOUND_SCALAR, CompoundScalar
from hgraph._wiring import compute_node

__all__ = ()


MISSING = object()


def _check_schema(scalar, bundle):
    if bundle.meta_data_schema.keys() - scalar.meta_data_schema.keys():
        return f"Extra fields: {bundle.meta_data_schema.keys() - scalar.meta_data_schema.keys()}"
    for k, t in scalar.meta_data_schema.items():
        if (kt := bundle.meta_data_schema.get(k)) is None:
            if getattr(scalar.py_type, k, MISSING) is MISSING:
                return f"Missing input: {k}"
        elif not t.matches(kt if kt.is_scalar else kt.scalar_type()):
            return f"field {k} of type {t} does not accept {kt}"
    return True


def _check_schema_nonstrict(scalar, bundle):
    if bundle.meta_data_schema.keys() - scalar.meta_data_schema.keys():
        return f"Extra fields: {bundle.meta_data_schema.keys() - scalar.meta_data_schema.keys()}"
    for k, t in scalar.meta_data_schema.items():
        if (kt := bundle.meta_data_schema.get(k)) is None:
            continue
        elif not t.matches(kt if kt.is_scalar else kt.scalar_type()):
            return f"field {k} of type {t} does not accept {kt}"
    return True


@compute_node(
    overloads=combine,
    requires=lambda m: _check_schema(m[COMPOUND_SCALAR], m[TS_SCHEMA]),
    all_valid=lambda m, __strict__: ("bundle",) if __strict__ else None,
)
def combine_cs(
    tp_out_: Type[TS[COMPOUND_SCALAR]] = DEFAULT[OUT],
    tp_: Type[COMPOUND_SCALAR] = COMPOUND_SCALAR,
    __strict__: bool = True,
    **bundle: TSB[TS_SCHEMA],
) -> TS[COMPOUND_SCALAR]:
    return tp_(**{k: v.value for k, v in bundle.items()})


@compute_node(overloads=combine, valid=("orig",))
def combine_compound_scalars(orig: TS[COMPOUND_SCALAR], delta: TS[COMPOUND_SCALAR]) -> TS[COMPOUND_SCALAR]:
    """
    Combines two compound scalars. This assumes that the merge is right applied to left with the left value considered
    as the original and the right the change to apply.
    """
    if not delta.valid:
        return orig.value
    original_values = (o_v := orig.value).to_dict()
    items = [(key, value, original_values) for key, value in delta.value.to_dict().items()]
    while items:
        key, value, orig_values = items.pop()
        if isinstance(value, dict):
            values = orig_values.get(key, {})
            items.extend([(k, v, values) for k, v in value.items()])
        else:
            orig_values[key] = value
    return type(o_v).from_dict(original_values)


@compute_node(
    overloads=combine, valid=("orig",),
    requires=lambda m: _check_schema_nonstrict(m[COMPOUND_SCALAR], m[TS_SCHEMA]),
    all_valid=lambda m, __strict__: ("bundle",) if __strict__ else None,
    )
def combine_cs_with(
    orig: TS[COMPOUND_SCALAR], 
    tp_out_: Type[TS[COMPOUND_SCALAR]] = DEFAULT[OUT],
    tp_: Type[COMPOUND_SCALAR] = COMPOUND_SCALAR,
    __strict__: bool = True,
    **bundle: TSB[TS_SCHEMA],
) -> TS[COMPOUND_SCALAR]:
    """
    Combines two compound scalars. This assumes that the merge is right applied to left with the left value considered
    as the original and the right the change to apply.
    """
    if not bundle.valid:
        return orig.value
    original_values = (o_v := orig.value).to_dict()
    items = [(key, value.value, original_values) for key, value in bundle.items()]
    while items:
        key, value, orig_values = items.pop()
        if isinstance(value, dict):
            values = orig_values.get(key, {})
            items.extend([(k, v, values) for k, v in value.items()])
        else:
            orig_values[key] = value
    return type(o_v).from_dict(original_values)


@compute_node(
    overloads=convert,
    requires=lambda m: m[OUT].py_type == TS[CompoundScalar],
    resolvers={COMPOUND_SCALAR: lambda m: m[TS_SCHEMA].py_type.scalar_type()},
    all_valid=lambda m, __strict__: ("bundle",) if __strict__ else None,
)
def convert_cs_from_tsb(bundle: TSB[TS_SCHEMA], __strict__: bool = True) -> TS[COMPOUND_SCALAR]:
    return bundle.value


@compute_node(
    overloads=convert,
    requires=lambda m: m[OUT].py_type != TS[CompoundScalar],
    all_valid=lambda m, __strict__: ("bundle",) if __strict__ else None,
)
def convert_cs_from_tsb_typed(
    bundle: TSB[TS_SCHEMA],
    __strict__: bool = True,
    tp_: Type[TS[COMPOUND_SCALAR]] = DEFAULT[OUT],
    scalar_tp_: Type[COMPOUND_SCALAR] = AUTO_RESOLVE,
) -> TS[COMPOUND_SCALAR]:
    return scalar_tp_(
        **{k: v.value if v.valid else None for k, v in bundle.items() if k in scalar_tp_.__meta_data_schema__}
    )


@compute_node(
    overloads=convert,
    requires=lambda m: m[OUT].py_type is TSB,
    resolvers={TS_SCHEMA: lambda m: TimeSeriesSchema.from_scalar_schema(m[COMPOUND_SCALAR].py_type)},
)
def convert_tsb_from_cs(
    ts: TS[COMPOUND_SCALAR],
    to: type[OUT] = DEFAULT[OUT],
    tp_: type[TS_SCHEMA] = AUTO_RESOLVE,
) -> TSB[TS_SCHEMA]:
    value = ts.value
    as_dict = value.to_dict()
    return {
        k: getattr(value, k)
        for k, v in as_dict.items()
        if k in tp_.__meta_data_schema__
    }