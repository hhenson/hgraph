import functools
from dataclasses import dataclass
from functools import singledispatch
from typing import Callable, Iterable

from frozendict import frozendict as fd

from hgraph import (
    HgTypeMetaData,
    TABLE,
    TIME_SERIES_TYPE,
    HgTSTypeMetaData,
    HgCompoundScalarType,
    HgTSBTypeMetaData,
    HgTSSTypeMetaData,
)


@dataclass(frozen=True)
class PartialSchema:
    tp: type
    keys: tuple[str, ...]
    types: tuple[type, ...]
    partition_keys: tuple[str, ...]
    to_table: Callable[[TIME_SERIES_TYPE], TABLE]
    from_table: Callable[[Iterable], TIME_SERIES_TYPE]


@functools.cache
def extract_table_schema_raw_type(tp: type[TIME_SERIES_TYPE]) -> PartialSchema:
    return extract_table_schema(HgTypeMetaData.parse_type(tp))


@singledispatch
def extract_table_schema(tp: HgTypeMetaData) -> PartialSchema:
    """Virtual function to extract the table schema from a HgTypeMetaData"""
    raise RuntimeError(f"Cannot extract table schema from '{tp}'")


@extract_table_schema.register(HgCompoundScalarType)
def _(tp: HgCompoundScalarType) -> PartialSchema:
    keys = []
    types = []
    from_table = []
    to_table = []
    for k, v in tp.meta_data_schema.items():
        if type(v) is HgCompoundScalarType:
            schema = extract_table_schema(v)
            keys.extend(f"{k}.{k_}" for k_ in schema.keys)
            types.extend(schema.types)
            to_table.append(lambda value, k=k: schema.to_table(getattr(value, k)))
            from_table.append(schema.from_table)
        else:
            keys.append(k)
            types.append(v.py_type)
            to_table.append(lambda value, k=k: (getattr(value, k),))
            from_table.append(lambda it: next(it))
    return PartialSchema(
        tp,
        keys=tuple(keys),
        types=tuple(types),
        partition_keys=tuple(),
        to_table=lambda v: tuple(i for fn in to_table for i in fn(v)),
        from_table=lambda it: tp.py_type(**{k: v(it) for k, v in zip(tp.meta_data_schema.keys(), from_table)}),
    )


@extract_table_schema.register(HgTSTypeMetaData)
def _(tp: HgTSTypeMetaData) -> PartialSchema:
    item_tp = tp.value_scalar_tp
    if type(item_tp) is HgCompoundScalarType:
        schema = extract_table_schema(item_tp)
        return PartialSchema(
            tp.py_type,
            keys=tuple(schema.keys),
            types=tuple(schema.types),
            partition_keys=tuple(schema.partition_keys),
            to_table=lambda ts: schema.to_table(ts.value),
            from_table=schema.from_table,
        )
    else:
        return PartialSchema(
            tp.py_type,
            keys=("value",),
            types=(item_tp.py_type,),
            partition_keys=tuple(),
            to_table=lambda v: (v.value if v.modified else None,),
            from_table=lambda iter: next(iter),
        )


@extract_table_schema.register(HgTSBTypeMetaData)
def _(tp: HgTSBTypeMetaData) -> PartialSchema:
    schema = tp.bundle_schema_tp.meta_data_schema
    schema_keys = schema.keys()
    keys = []
    types = []
    from_table = []
    to_table = []
    for k, v in schema.items():
        tp_ = type(v)
        if tp_ is HgTSBTypeMetaData:
            schema = extract_table_schema(v)
            keys.extend(f"{k}.{k_}" for k_ in schema.keys)
            types.extend(schema.types)
            to_table.append(lambda value, k=k, schema=schema: schema.to_table(getattr(value, k)))
            from_table.append(schema.from_table)
        elif tp_ in (HgTSTypeMetaData, HgTSSTypeMetaData):
            schema = extract_table_schema(v)
            keys.append(k)
            types.append(v.scalar_type().py_type)
            to_table.append(lambda value, k=k, schema=schema: schema.to_table(getattr(value, k)))
            from_table.append(schema.from_table)
        else:
            raise RuntimeError(f"Cannot extract table schema from '{tp}' as {k}: {v} is not convertable")
    return PartialSchema(
        tp,
        keys=tuple(keys),
        types=tuple(types),
        partition_keys=tuple(),
        to_table=lambda v: tuple(i for fn in to_table for i in fn(v)),
        from_table=lambda it, key_s=schema_keys, f_t=from_table,: fd(
            **{k: v_ for k, v in zip(key_s, f_t) if (v_ := v(it)) is not None}
        ),
    )
