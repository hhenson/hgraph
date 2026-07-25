from dataclasses import MISSING as DATACLASS_MISSING, fields, is_dataclass
from typing import Type

from hgraph import TimeSeriesSchema
from hgraph._operators import combine, convert
from hgraph._types import (
    TS,
    TS_SCHEMA,
    TSB,
    DEFAULT,
    OUT,
    AUTO_RESOLVE,
    COMPOUND_SCALAR,
    CompoundScalar,
    HgCompoundScalarType,
    HgScalarTypeMetaData,
)
from hgraph._wiring import compute_node

__all__ = ()


MISSING = object()


def _dataclass_fields_by_name(tp):
    origin = getattr(tp, "__origin__", None) or tp
    return (
        {field.name: field for field in fields(origin)}
        if is_dataclass(origin) and not issubclass(origin, CompoundScalar)
        else None
    )


def _field_has_default(tp, name):
    dataclass_fields = _dataclass_fields_by_name(tp)
    if dataclass_fields is None:
        return getattr(tp, name, MISSING) is not MISSING
    field = dataclass_fields[name]
    return not field.init or field.default is not DATACLASS_MISSING or field.default_factory is not DATACLASS_MISSING


def _constructor_values(tp, values):
    dataclass_fields = _dataclass_fields_by_name(tp)
    if dataclass_fields is None:
        return values
    return {name: value for name, value in values.items() if dataclass_fields[name].init}


def _required_constructor_fields(tp):
    dataclass_fields = _dataclass_fields_by_name(tp)
    if dataclass_fields is None:
        return ()
    return tuple(
        name
        for name, field in dataclass_fields.items()
        if field.init and field.default is DATACLASS_MISSING and field.default_factory is DATACLASS_MISSING
    )


def _is_python_dataclass_meta(meta):
    return _dataclass_fields_by_name(meta.py_type) is not None


def _construct_dataclass_from_bundle(tp, bundle, strict):
    dataclass_fields = _dataclass_fields_by_name(tp)
    required = _required_constructor_fields(tp)
    if strict and any(not bundle[name].valid for name in required):
        return None
    values = {
        name: value.value
        for name, value in bundle.items()
        if value.valid and name in dataclass_fields and dataclass_fields[name].init
    }
    if not strict:
        values.update({name: None for name in required if name not in values})
    return tp(**values)


def _structured_scalar_to_dict(value, *, include_none=False):
    if isinstance(value, CompoundScalar):
        return value.to_dict()
    scalar = HgScalarTypeMetaData.parse_type(type(value))
    if not isinstance(scalar, HgCompoundScalarType):
        raise TypeError(f"{type(value)} is not a structured scalar")
    return {
        name: getattr(value, name, None)
        for name in scalar.meta_data_schema
        if include_none or getattr(value, name, None) is not None
    }


def _structured_scalar_from_dict(tp, values):
    if isinstance(tp, type) and issubclass(tp, CompoundScalar):
        return tp.from_dict(values)
    return tp(**_constructor_values(tp, values))


def _check_schema(scalar, bundle):
    if bundle.meta_data_schema.keys() - scalar.meta_data_schema.keys():
        return f"Extra fields: {bundle.meta_data_schema.keys() - scalar.meta_data_schema.keys()}"
    for k, t in scalar.meta_data_schema.items():
        if (kt := bundle.meta_data_schema.get(k)) is None:
            if not _field_has_default(scalar.py_type, k):
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
    return tp_(**_constructor_values(tp_, {k: v.value for k, v in bundle.items()}))


@compute_node(overloads=combine, valid=("orig",))
def combine_compound_scalars(orig: TS[COMPOUND_SCALAR], delta: TS[COMPOUND_SCALAR]) -> TS[COMPOUND_SCALAR]:
    """
    Combines two compound scalars. This assumes that the merge is right applied to left with the left value considered
    as the original and the right the change to apply.
    """
    if not delta.valid:
        return orig.value
    original_values = _structured_scalar_to_dict(o_v := orig.value, include_none=True)
    items = [(key, value, original_values) for key, value in _structured_scalar_to_dict(delta.value).items()]
    while items:
        key, value, orig_values = items.pop()
        if isinstance(value, dict):
            values = orig_values.get(key, {})
            items.extend([(k, v, values) for k, v in value.items()])
        else:
            orig_values[key] = value
    return _structured_scalar_from_dict(type(o_v), original_values)


@compute_node(
    overloads=combine,
    valid=("orig",),
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
    original_values = _structured_scalar_to_dict(o_v := orig.value, include_none=True)
    items = [(key, value.value, original_values) for key, value in bundle.items()]
    while items:
        key, value, orig_values = items.pop()
        if isinstance(value, dict):
            values = orig_values.get(key, {})
            items.extend([(k, v, values) for k, v in value.items()])
        else:
            orig_values[key] = value
    return _structured_scalar_from_dict(type(o_v), original_values)


@compute_node(
    overloads=convert,
    requires=lambda m: m[OUT].py_type == TS[CompoundScalar],
    resolvers={COMPOUND_SCALAR: lambda m: m[TS_SCHEMA].py_type.scalar_type()},
    all_valid=lambda m, __strict__: (
        ("bundle",) if __strict__ and not _is_python_dataclass_meta(m[COMPOUND_SCALAR]) else None
    ),
)
def convert_cs_from_tsb(
    bundle: TSB[TS_SCHEMA],
    __strict__: bool = True,
    scalar_tp_: Type[COMPOUND_SCALAR] = AUTO_RESOLVE,
) -> TS[COMPOUND_SCALAR]:
    return (
        _construct_dataclass_from_bundle(scalar_tp_, bundle, __strict__)
        if _dataclass_fields_by_name(scalar_tp_) is not None
        else bundle.value
    )


@compute_node(
    overloads=convert,
    requires=lambda m: m[OUT].py_type != TS[CompoundScalar],
    all_valid=lambda m, __strict__: (
        ("bundle",) if __strict__ and not _is_python_dataclass_meta(m[COMPOUND_SCALAR]) else None
    ),
)
def convert_cs_from_tsb_typed(
    bundle: TSB[TS_SCHEMA],
    __strict__: bool = True,
    tp_: Type[TS[COMPOUND_SCALAR]] = DEFAULT[OUT],
    scalar_tp_: Type[COMPOUND_SCALAR] = AUTO_RESOLVE,
) -> TS[COMPOUND_SCALAR]:
    scalar = HgScalarTypeMetaData.parse_type(scalar_tp_)
    if _dataclass_fields_by_name(scalar_tp_) is not None:
        return _construct_dataclass_from_bundle(scalar_tp_, bundle, __strict__)
    values = {k: v.value for k, v in bundle.items() if k in scalar.meta_data_schema}
    return scalar_tp_(**_constructor_values(scalar_tp_, values))


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
    as_dict = _structured_scalar_to_dict(value)
    return {k: getattr(value, k) for k, v in as_dict.items() if k in tp_.__meta_data_schema__}
