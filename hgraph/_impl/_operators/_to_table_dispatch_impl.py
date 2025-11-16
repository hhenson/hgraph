import functools
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import singledispatch
from typing import Callable, Iterable

from frozendict import frozendict as fd

from hgraph._operators._to_table import TABLE
from hgraph._types import (
    HgTypeMetaData,
    TIME_SERIES_TYPE,
    HgTSTypeMetaData,
    HgTSWTypeMetaData,
    HgCompoundScalarType,
    HgTSBTypeMetaData,
    HgTSDTypeMetaData,
    TSD,
    K,
    V,
    REMOVE_IF_EXISTS,
    HgREFTypeMetaData,
    STATE,
    HgDataFrameScalarTypeMetaData,
    CompoundScalar,
)
from hgraph._types._scalar_type_meta_data import HgTupleFixedScalarType

__all__ = ("PartialSchema", "extract_table_schema", "extract_table_schema_raw_type")


@dataclass(frozen=True)
class PartialSchema:
    tp: type
    keys: tuple[str, ...]
    types: tuple[type, ...]
    partition_keys: tuple[str, ...]
    remove_partition_keys: tuple[str, ...]
    to_table: Callable[[TIME_SERIES_TYPE], TABLE]
    to_table_sample: Callable[[TIME_SERIES_TYPE], TABLE]
    to_table_snap: Callable[[TIME_SERIES_TYPE], TABLE]
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
        if type(v) in (HgCompoundScalarType, HgTupleFixedScalarType):
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
        to_table_sample=lambda v: tuple(i for fn in to_table for i in fn(v)),
        to_table_snap=lambda v: tuple(i for fn in to_table for i in fn(v)),
        from_table=lambda it: tp.py_type(**{k: v(it) for k, v in zip(tp.meta_data_schema.keys(), from_table)}),
    )


@extract_table_schema.register(HgTupleFixedScalarType)
def _(tp: HgTupleFixedScalarType) -> PartialSchema:
    keys = []
    types = []
    from_table = []
    to_table = []
    for k, v in enumerate(tp.element_types):
        if type(v) in (HgCompoundScalarType, HgTupleFixedScalarType):
            schema = extract_table_schema(v)
            keys.extend(f"{k}.{k_}" for k_ in schema.keys)
            types.extend(schema.types)
            to_table.append(lambda value, k=k: schema.to_table(value[k]))
            from_table.append(schema.from_table)
        else:
            keys.append(k)
            types.append(v.py_type)
            to_table.append(lambda value, k=k: (value[k],))
            from_table.append(lambda it: next(it))
    return PartialSchema(
        tp,
        keys=tuple(keys),
        types=tuple(types),
        partition_keys=tuple(),
        remove_partition_keys=tuple(),
        to_table=lambda v: tuple(i for fn in to_table for i in fn(v)),
        to_table_sample=lambda v: tuple(i for fn in to_table for i in fn(v)),
        to_table_snap=lambda v: tuple(i for fn in to_table for i in fn(v)),
        from_table=lambda it: tp.py_type(*[v(it) for v in from_table]),
    )


@extract_table_schema.register(HgTSTypeMetaData)
def _(tp: HgTSTypeMetaData) -> PartialSchema:
    item_tp = tp.value_scalar_tp
    if type(item_tp) in (HgCompoundScalarType, HgDataFrameScalarTypeMetaData):
        schema = extract_table_schema(item_tp)
        return PartialSchema(
            tp,
            keys=tuple(schema.keys),
            types=tuple(schema.types),
            partition_keys=tuple(),
            remove_partition_keys=tuple(),
            to_table=lambda ts, schema=schema: (
                schema.to_table(ts.delta_value) if ts.modified else (None,) * len(schema.keys)
            ),
            to_table_sample=lambda ts, schema=schema: (
                schema.to_table_sample(ts.value) if ts.valid else (None,) * len(schema.keys)
            ),
            to_table_snap=lambda ts, schema=schema: (
                schema.to_table_snap(ts.value) if ts.valid else (None,) * len(schema.keys)
            ),
            from_table=schema.from_table,
        )
    else:
        return PartialSchema(
            tp,
            keys=("value",),
            types=(item_tp.py_type,),
            partition_keys=tuple(),
            remove_partition_keys=tuple(),
            to_table=lambda v: (v.delta_value if v.modified else None,),
            to_table_sample=lambda v: (v.value,),
            to_table_snap=lambda v: (v.value,),
            from_table=lambda iter: next(iter),
        )


@extract_table_schema.register(HgTSWTypeMetaData)
def _(tp: HgTSWTypeMetaData) -> PartialSchema:
    schema = extract_table_schema(HgTSTypeMetaData(tp.value_scalar_tp))
    # TODO: ensure the from_table loads historical data
    return schema


@extract_table_schema.register(HgREFTypeMetaData)
def _(tp: HgREFTypeMetaData) -> PartialSchema:
    item_tp = tp.value_tp
    schema = extract_table_schema(item_tp)
    return ref_schema_from_schema(tp, schema)

    
def ref_schema_from_schema(tp, schema: PartialSchema) -> PartialSchema:
    return PartialSchema(
        tp,
        keys=schema.keys,
        types=schema.types,
        partition_keys=schema.partition_keys,
        remove_partition_keys=schema.remove_partition_keys,
        to_table=lambda v, schema=schema: schema.to_table(
            v.value.output
            if v.value.has_output
            else (
                STATE(
                    **{k: i.output for k, i in zip(schema.tp.bundle_schema_tp.meta_data_schema.keys(), v.value.items)}
                )
                if v.value.is_valid
                else (None,) * len(schema.keys)
            )
        ),
        to_table_sample=lambda v, schema=schema: schema.to_table_sample(
            v.value.output
            if v.value.has_output
            else (
                STATE(
                    **{k: i.output for k, i in zip(schema.tp.bundle_schema_tp.meta_data_schema.keys(), v.value.items)}
                )
                if v.value.is_valid
                else (None,) * len(schema.keys)
            )
        ),
        to_table_snap=lambda v, schema=schema: schema.to_table_snap(
            v.value.output
            if v.value.has_output
            else (
                STATE(
                    **{k: i.output for k, i in zip(schema.tp.bundle_schema_tp.meta_data_schema.keys(), v.value.items)}
                )
                if v.value.is_valid
                else (None,) * len(schema.keys)
            )
        ),
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
    to_table_sample = []
    to_table_snap = []
    for k, v in schema.items():
        schema = extract_table_schema(v)
        if len(schema.keys) > 1:  # If the type is a CompoundScalar
            keys.extend(f"{k}.{k_}" for k_ in schema.keys)
            types.extend(schema.types)
        else:
            keys.append(k)
            types.append(schema.types[0])
        to_table.append(lambda value, k=k, schema=schema: schema.to_table(getattr(value, k)))
        to_table_sample.append(lambda value, k=k, schema=schema: schema.to_table_sample(getattr(value, k)))
        to_table_snap.append(lambda value, k=k, schema=schema: schema.to_table_snap(getattr(value, k)))
        from_table.append(schema.from_table)
    return PartialSchema(
        tp,
        keys=tuple(keys),
        types=tuple(types),
        partition_keys=tuple(),
        remove_partition_keys=tuple(),
        to_table=lambda v: tuple(i for fn in to_table for i in fn(v)),
        to_table_sample=lambda v: tuple(i for fn in to_table_sample for i in fn(v)),
        to_table_snap=lambda v: tuple(i for fn in to_table_snap for i in fn(v)),
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
        key_type = tp.key_tp.py_type
        if key_type in (bool, int, str, date, datetime, time, timedelta):
            key_names = (f"__key_{PartitionKeyCounter.count}__",)
            key_schema = PartialSchema(
                tp.key_tp,
                keys=('',),
                types=(key_type,),
                partition_keys=tuple(),
                remove_partition_keys=tuple(),
                to_table=lambda v, k=key_names[0]: (v,),
                to_table_sample=lambda v, k=key_names[0]: (v,),
                to_table_snap=lambda v, k=key_names[0]: (v,),
                from_table=lambda it: next(it),
            )
        else:
            key_schema = extract_table_schema(tp.key_tp)
            key_names = tuple(f"__key_{PartitionKeyCounter.count}_{k}__" for k in key_schema.keys)

        removed_name = f"__key_{PartitionKeyCounter.count}_removed__"
        schema = extract_table_schema(tp.value_tp)
    return PartialSchema(
        tp,
        keys=(removed_name,) + key_names + schema.keys,
        types=(bool,) + key_schema.types + schema.types,
        partition_keys=key_names + schema.partition_keys,
        remove_partition_keys=(removed_name,) + schema.remove_partition_keys,
        to_table=lambda tsd, k=key_names, key_schema=key_schema, schema=schema: _tsd_to_table(tsd, key_schema, schema),
        to_table_sample=lambda tsd, k=key_names, key_schema=key_schema, schema=schema: _tsd_to_table(
            tsd, key_schema, schema, False, True
        ),
        to_table_snap=lambda tsd, k=key_names, key_schema=key_schema, schema=schema: _tsd_to_table(
            tsd, key_schema, schema, True
        ),
        from_table=lambda it, key_schema=key_schema, schema=schema: _tsd_from_table(it, key_schema, schema),
    )


def _tsd_to_table(tsd: TSD[K, V], key_schema, schema: PartialSchema, snap=False, sample=False) -> TABLE:
    # the below stoppred working with the C++ engine as the types are not available on the API anymore
    # leave the code commented for future reference hoping to find a better way to do this check

    # if tsd.__value_tp__ != schema.tp:
    #     # this can happen when we have a REF[TSD] and the inner TSD type is different from the outer TSD type in references
    #     # like TSD[int, REF[TS[int]]] vs TSD[int, TS[int]]
    #     if isinstance(tsd.__value_tp__, HgREFTypeMetaData) and tsd.__value_tp__.value_tp == schema.tp:
    #         schema = ref_schema_from_schema(tsd.__value_tp__, schema)
    #     else:
    #         raise RuntimeError(f"TSD value type '{tsd.__value_tp__}' does not match schema type '{schema.tp}'")
        
    if schema.partition_keys:
        # If there are partial keys in the value, then we will potentially get multiple rows
        out = []
        for k, v in tsd.modified_items() if not snap else tsd.valid_items():
            keys = key_schema.to_table(k)
            if snap:
                rows = schema.to_table_snap(v)
            elif sample:
                rows = schema.to_table_sample(v)
            else:
                rows = schema.to_table(v)

            for row in rows:
                out.append((False,) + keys + row)
        if not snap:
            for k in tsd.removed_keys():
                keys = key_schema.to_table(k)
                out.append((True,) + keys + (None,) * len(schema.keys))
        return tuple(out)
    else:
        if snap:
            fn = schema.to_table_snap
        elif sample:
            fn = schema.to_table_sample
        else:
            fn = schema.to_table

        return tuple(
            ((False,) + key_schema.to_table(k) + fn(v))
            for k, v in (tsd.modified_items() if not snap else tsd.valid_items())
        ) + tuple(
            ((True,) + key_schema.to_table(k) + (None,) * len(schema.keys))
            for k in (tsd.removed_keys() if not snap else ())
        )


def _tsd_from_table(it, key_schema: PartialSchema, schema: PartialSchema) -> fd:
    if schema.partition_keys:
        old_k = None
        out = {}
        values = []
        for r in it:
            removed = r[0]
            row_values = iter(r[1:])
            key = key_schema.from_table(row_values)
            if old_k is None:
                old_k = key
            elif old_k != key:
                out[old_k] = schema.from_table(iter(values))
                old_k = key
                values = []
            if removed:
                out[key] = REMOVE_IF_EXISTS
                continue
            values.append(tuple(row_values))
        if values:
            out[key] = schema.from_table(iter(values))
    else:
        out = {}
        for r in it:
            removed = r[0]
            values = iter(r[1:])
            key = key_schema.from_table(values)
            if removed:
                out[key] = REMOVE_IF_EXISTS
            else:
                out[key] = schema.from_table(values)
    return fd(out)


@extract_table_schema.register(HgDataFrameScalarTypeMetaData)
def _(tp: HgDataFrameScalarTypeMetaData) -> PartialSchema:
    import polars as pl

    if type(tp.schema) is HgCompoundScalarType:
        schema = extract_table_schema(tp.schema)
        return PartialSchema(
            tp,
            keys=schema.keys,
            types=schema.types,
            partition_keys=tuple(),
            remove_partition_keys=tuple(),
            to_table=lambda v: tuple(schema.to_table(tp.schema.py_type(**i)) for i in v.rows(named=True)),
            to_table_sample=lambda v: tuple(schema.to_table(tp.schema.py_type(**i)) for i in v.rows(named=True)),
            to_table_snap=lambda v: tuple(schema.to_table(tp.schema.py_type(**i)) for i in v.rows(named=True)),
            from_table=lambda it: pl.DataFrame(tuple(schema.from_table(iter(i)) for i in it)),
        )
    else:
        return PartialSchema(
            tp,
            keys=tuple(),
            types=tuple(),
            partition_keys=tuple(),
            remove_partition_keys=tuple(),
            to_table=lambda v: (v.value,) if v.modified else (None,),
            to_table_sample=lambda v: (v.value,) if v.modified else (None,),
            to_table_snap=lambda v: (v.value,) if v.modified else (None,),
            from_table=lambda iter: next(iter),
        )
