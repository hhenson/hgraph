import functools
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import singledispatch
from typing import Callable, Iterable, Any

from frozendict import frozendict as fd

from hgraph import (
    HgTypeMetaData,
    TABLE,
    TIME_SERIES_TYPE,
    HgTSTypeMetaData,
    HgCompoundScalarType,
    HgTSBTypeMetaData,
    HgTSSTypeMetaData,
    HgTSDTypeMetaData,
    TSD,
    K,
    V,
    REMOVE_IF_EXISTS,
)


@dataclass(frozen=True)
class PartialSchema:
    tp: type
    keys: tuple[str, ...]
    types: tuple[type, ...]
    partition_keys: tuple[str, ...]
    remove_partition_keys: tuple[str, ...]
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
        remove_partition_keys=tuple(),
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
            partition_keys=tuple(),
            remove_partition_keys=tuple(),
            to_table=lambda ts, schema=schema: schema.to_table(ts.value) if ts.modified else (None,) * len(schema.keys),
            from_table=schema.from_table,
        )
    else:
        return PartialSchema(
            tp.py_type,
            keys=("value",),
            types=(item_tp.py_type,),
            partition_keys=tuple(),
            remove_partition_keys=tuple(),
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
            if len(schema.keys) > 1:  # If the type is a CompoundScalar
                keys.extend(f"{k}.{k_}" for k_ in schema.keys)
                types.extend(schema.types)
            else:
                keys.append(k)
                types.append(schema.types[0])
            to_table.append(lambda value, k=k, schema=schema: schema.to_table(getattr(value, k)))
            from_table.append(schema.from_table)
        else:
            raise RuntimeError(f"Cannot extract table schema from '{tp}' as {k}: {v} is not convertable")
    return PartialSchema(
        tp,
        keys=tuple(keys),
        types=tuple(types),
        partition_keys=tuple(),
        remove_partition_keys=tuple(),
        to_table=lambda v: tuple(i for fn in to_table for i in fn(v)),
        from_table=lambda it, key_s=schema_keys, f_t=from_table,: fd(
            **{k: v_ for k, v in zip(key_s, f_t) if (v_ := v(it)) is not None}
        ),
    )


class PartitionKeyCounter:
    count: int = 0

    def __enter__(self):
        PartitionKeyCounter.count += 1

    def __exit__(self, exc_type, exc_val, exc_tb):
        PartitionKeyCounter.count -= 1


@extract_table_schema.register(HgTSDTypeMetaData)
def _(tp: HgTSDTypeMetaData) -> PartialSchema:
    with PartitionKeyCounter():
        key_name = f"__key_{PartitionKeyCounter.count}__"
        removed_name = f"__key_{PartitionKeyCounter.count}_removed__"
        key_type = tp.key_tp.py_type
        if key_type not in (bool, int, str, date, datetime, time, timedelta):
            raise ValueError(
                f"Cannot extract table schema from '{tp}' as {key_type} is not supported as a keyable column"
            )
        schema = extract_table_schema(tp.value_tp)
    return PartialSchema(
        tp,
        keys=(
            removed_name,
            key_name,
        )
        + schema.keys,
        types=(
            bool,
            key_type,
        )
        + schema.types,
        partition_keys=(key_name,) + schema.partition_keys,
        remove_partition_keys=(removed_name,) + schema.remove_partition_keys,
        to_table=lambda tsd, k=key_name, schema=schema: _tsd_to_table(tsd, schema),
        from_table=lambda it, schema=schema: _tsd_from_table(it, schema),
    )


def _tsd_to_table(tsd: TSD[K, V], schema: PartialSchema) -> TABLE:
    if schema.partition_keys:
        # If there are partial keys in the value, then we will potentially get multiple rows
        out = []
        for k, v in tsd.modified_items():
            for row in schema.to_table(v):
                out.append(
                    (
                        False,
                        k,
                    )
                    + row
                )
        for k in tsd.removed_keys():
            out.append((True, k) + (None,) * len(schema.keys))
        return tuple(out)
    else:
        return tuple(
            (
                (
                    False,
                    k,
                )
                + schema.to_table(v)
            )
            for k, v in tsd.modified_items()
        ) + tuple(((True, k) + (None,) * len(schema.keys)) for k in tsd.removed_keys())


def _tsd_from_table(it, schema: PartialSchema) -> fd:
    if schema.partition_keys:
        old_k = None
        out = {}
        values = []
        for r in it:
            removed = r[0]
            key = r[1]
            if old_k is None:
                old_k = key
            elif old_k != key:
                out[old_k] = schema.from_table(iter(values))
                old_k = key
                values = []
            if removed:
                out[key] = REMOVE_IF_EXISTS
                continue
            values.append(r[2:])
        if values:
            out[key] = schema.from_table(iter(values))
    else:
        out = {}
        for r in it:
            removed = r[0]
            key = r[1]
            if removed:
                out[key] = REMOVE_IF_EXISTS
            else:
                out[key] = schema.from_table(iter(r[2:]))
    return fd(out)
