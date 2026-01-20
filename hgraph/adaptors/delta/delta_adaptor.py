from datetime import datetime
from typing import Tuple, Type, cast

from polars import DataFrame

from hgraph.adaptors.delta.delta_adaptor_raw import DeltaSchemaMode, DeltaWriteMode, delta_query_adaptor_raw, \
    delta_read_adaptor_raw, delta_write_adaptor_raw
from hgraph.adaptors.data_catalogue.catalogue import DataEnvironment
from hgraph import (
    service_adaptor,
    TS,
    TSB,
    SCHEMA,
    service_adaptor_impl,
    TSD,
    graph,
    convert,
    Frame,
    AUTO_RESOLVE,
    map_,
    DEFAULT,
    exception_time_series,
    if_then_else,
    feedback,
    TSS,
    valid,
)
from hgraph.stream.stream import Stream, StreamStatus, Data


__all__ = ['delta_read_adaptor', 'delta_read_adaptor_impl', 'delta_write_adaptor', 'delta_write_adaptor_impl', 'delta_query_adaptor', 'delta_query_adaptor_impl']


@graph
def raw_data_to_schema(
    data: TSB[Stream[Data[DataFrame]]], _schema: Type[SCHEMA] = AUTO_RESOLVE
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    converted_data = convert[TS[Frame[_schema]]](data.values)
    error = exception_time_series(converted_data)
    return cast(
        TSB,
        {
            "status": if_then_else(valid(error), StreamStatus.ERROR, data.status),
            "status_msg": if_then_else(valid(error), error.error_msg, data.status_msg),
            "timestamp": data.timestamp,
            "values": converted_data,
        },
    )


@service_adaptor
def delta_read_adaptor(
        path: str,
        table: TS[str],
        filters: TS[tuple[tuple[str, str, object], ...]] = None,
        sort: TS[tuple[tuple[str, bool]]] = None,
        _schema: Type[SCHEMA] = DEFAULT[SCHEMA]
) -> TSB[Stream[Data[Frame[SCHEMA]]]]: ...


@service_adaptor_impl(interfaces=delta_read_adaptor)
def delta_read_adaptor_impl(
    path: str,
    table: TSD[int, TS[str]],
    filters: TSD[int, TS[tuple[tuple[str, str, object], ...]]] = None,
    sort: TSD[int, TS[tuple[tuple[str, bool]]]] = None,
    _schema: Type[SCHEMA] = AUTO_RESOLVE
) -> TSD[int, TSB[Stream[Data[Frame[SCHEMA]]]]]:
    de = DataEnvironment.current()
    if not de:
        raise RuntimeError(f"No DataEnvironment set up for {path}")
    connection_string = de.get_entry(path).environment_path

    columns = tuple(_schema.__meta_data_schema__.keys())

    map_(lambda key, t, c, s, f: delta_read_adaptor_raw.from_graph(path=connection_string, columns=c, table=t, filters=f, sort=s, __request_id__=key), 
         t=table, c=columns, f=filters, s=sort)

    fb_keys = feedback(TSS[int])(table.key_set)

    return map_(
        lambda key: raw_data_to_schema(
            delta_read_adaptor_raw.to_graph(path=connection_string, __request_id__=key, __no_ts_inputs__=True), _schema=_schema
        ),
        __keys__=fb_keys(),
    )


@service_adaptor
def delta_query_adaptor(
        path: str,
        tables: TS[set[str]],
        query: TS[str],
        _schema: Type[SCHEMA] = DEFAULT[SCHEMA]
) -> TSB[Stream[Data[Frame[SCHEMA]]]]: ...


@service_adaptor_impl(interfaces=delta_query_adaptor)
def delta_query_adaptor_impl(
    path: str,
    tables: TSD[int, TS[set[str]]],
    query: TSD[int, TS[str]],
    _schema: Type[SCHEMA] = AUTO_RESOLVE
) -> TSD[int, TSB[Stream[Data[Frame[SCHEMA]]]]]:
    de = DataEnvironment.current()
    if not de:
        raise RuntimeError(f"No DataEnvironment set up for {path}")
    connection_string = de.get_entry(path).environment_path

    map_(lambda key, t, q: delta_query_adaptor_raw.from_graph(path=connection_string, tables=t, query=q, __request_id__=key), t=tables, q=query)

    fb_keys = feedback(TSS[int])(tables.key_set)

    return map_(
        lambda key: raw_data_to_schema(
            delta_query_adaptor_raw.to_graph(path=connection_string, __request_id__=key, __no_ts_inputs__=True), _schema=_schema
        ),
        __keys__=fb_keys(),
    )


@service_adaptor
def delta_write_adaptor(
    path: str,
    table: TS[str],
    data: TS[DataFrame],
    write_mode: TS[DeltaWriteMode],
    schema_mode: TS[DeltaSchemaMode],
    keys: TS[Tuple[str, ...]],
    partition: TS[Tuple[str, ...]],
) -> TSB[Stream[Data[datetime]]]: ...


@service_adaptor_impl(interfaces=delta_write_adaptor)
def delta_write_adaptor_impl(
    path: str,
    table: TSD[int, TS[str]],
    data: TSD[int, TS[DataFrame]],
    write_mode: TSD[int, TS[DeltaWriteMode]],
    schema_mode: TSD[int, TS[DeltaSchemaMode]],
    keys: TSD[int, TS[Tuple[str, ...]]],
    partition: TSD[int, TS[Tuple[str, ...]]],
) -> TSD[int, TSB[Stream[Data[datetime]]]]:
    de = DataEnvironment.current()
    if not de:
        raise RuntimeError(f"No DataEnvironment set up for {path}")
    connection_string = de.get_entry(path).environment_path

    map_(
        lambda key, t, d, wm, sm, k, p: delta_write_adaptor_raw.from_graph(
            path=connection_string, table=t, data=d, write_mode=wm, schema_mode=sm, keys=k, partition=p, __request_id__=key
        ),
        t=table,
        d=data,
        wm=write_mode,
        sm=schema_mode,
        k=keys,
        p=partition,
    )

    fb_keys = feedback(TSS[int])(table.key_set)

    return map_(
        lambda key: delta_write_adaptor_raw.to_graph(path=connection_string, __request_id__=key, __no_ts_inputs__=True),
        __keys__=fb_keys(),
    )
