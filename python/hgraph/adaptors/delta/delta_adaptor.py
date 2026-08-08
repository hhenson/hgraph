from datetime import datetime
from typing import cast

from hgraph import (
    AUTO_RESOLVE, DEFAULT, SCHEMA, Frame, TS, TSB, TSD, TSS,
    convert, exception_time_series, feedback, graph, if_then_else, map_,
    null_sink, service_adaptor, service_adaptor_impl, valid,
)
from hgraph.adaptors.data_catalogue.catalogue import DataEnvironment
from hgraph.reflection import fields
from hgraph.stream import Data, Stream, StreamStatus

from .delta_adaptor_raw import (
    DeltaSchemaMode, DeltaWriteMode, _RAW_FRAME_STREAM, _TIME_STREAM,
    delta_query_adaptor_raw, delta_read_adaptor_raw, delta_write_adaptor_raw,
)

__all__ = (
    "delta_read_adaptor", "delta_read_adaptor_impl",
    "delta_query_adaptor", "delta_query_adaptor_impl",
    "delta_write_adaptor", "delta_write_adaptor_impl",
)


def _environment_path(path):
    environment = DataEnvironment.current()
    if environment is None:
        raise RuntimeError(f"No DataEnvironment set up for {path}")
    return environment.get_entry(path).environment_path


@graph
def raw_data_to_schema(raw: _RAW_FRAME_STREAM, _schema: type[SCHEMA] = AUTO_RESOLVE):
    converted = convert[TS[Frame[_schema]]](raw.values)
    error = exception_time_series(converted)
    output_type = TSB[Stream[Data[Frame[_schema]]]]
    return cast(TSB, output_type.from_ts(
        status=if_then_else(valid(error), StreamStatus.ERROR, raw.status),
        status_msg=if_then_else(valid(error), error.error_msg, raw.status_msg),
        timestamp=raw.timestamp, values=converted))


@service_adaptor
def delta_read_adaptor(
    path: str, table: TS[str],
    filters: TS[tuple[tuple[str, str, object], ...]] = (),
    sort: TS[tuple[tuple[str, bool], ...]] = (),
    _schema: type[SCHEMA] = DEFAULT[SCHEMA],
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    ...


@service_adaptor_impl(interfaces=delta_read_adaptor)
def delta_read_adaptor_impl(
    path: str, table: TSD[int, TS[str]],
    filters: TSD[int, TS[tuple[tuple[str, str, object], ...]]] = None,
    sort: TSD[int, TS[tuple[tuple[str, bool], ...]]] = None,
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, TSB[Stream[Data[Frame[SCHEMA]]]]]:
    connection_string = _environment_path(path)
    columns = tuple(fields(_schema))

    @graph
    def send(
        key: TS[int], table: TS[str],
        filters: TS[tuple[tuple[str, str, object], ...]],
        sort: TS[tuple[tuple[str, bool], ...]],
    ) -> TS[int]:
        return delta_read_adaptor_raw.from_graph(
            path=connection_string, table=table, columns=columns,
            filters=filters, sort=sort, __request_id__=key)

    null_sink(map_(send, table=table, filters=filters, sort=sort))
    keys = feedback(TSS[int])(table.key_set)

    @graph
    def receive(key: TS[int]):
        return raw_data_to_schema(
            delta_read_adaptor_raw.to_graph(
                path=connection_string, __request_id__=key,
                __no_ts_inputs__=True), _schema=_schema)

    return map_(receive, __keys__=keys())


@service_adaptor
def delta_query_adaptor(
    path: str, tables: TS[set[str]], query: TS[str],
    _schema: type[SCHEMA] = DEFAULT[SCHEMA],
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    ...


@service_adaptor_impl(interfaces=delta_query_adaptor)
def delta_query_adaptor_impl(
    path: str, tables: TSD[int, TS[set[str]]], query: TSD[int, TS[str]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, TSB[Stream[Data[Frame[SCHEMA]]]]]:
    connection_string = _environment_path(path)

    @graph
    def send(key: TS[int], tables: TS[set[str]], query: TS[str]) -> TS[int]:
        return delta_query_adaptor_raw.from_graph(
            path=connection_string, tables=tables, query=query,
            __request_id__=key)

    null_sink(map_(send, tables=tables, query=query))
    keys = feedback(TSS[int])(tables.key_set)

    @graph
    def receive(key: TS[int]):
        return raw_data_to_schema(
            delta_query_adaptor_raw.to_graph(
                path=connection_string, __request_id__=key,
                __no_ts_inputs__=True), _schema=_schema)

    return map_(receive, __keys__=keys())


@service_adaptor
def delta_write_adaptor(
    path: str, table: TS[str], data: TS[Frame[SCHEMA]],
    write_mode: TS[DeltaWriteMode], schema_mode: TS[DeltaSchemaMode],
    keys: TS[tuple[str, ...]], partition: TS[tuple[str, ...]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> _TIME_STREAM:
    ...


@service_adaptor_impl(interfaces=delta_write_adaptor)
def delta_write_adaptor_impl(
    path: str, table: TSD[int, TS[str]], data: TSD[int, TS[Frame[SCHEMA]]],
    write_mode: TSD[int, TS[DeltaWriteMode]],
    schema_mode: TSD[int, TS[DeltaSchemaMode]],
    keys: TSD[int, TS[tuple[str, ...]]],
    partition: TSD[int, TS[tuple[str, ...]]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, _TIME_STREAM]:
    connection_string = _environment_path(path)

    @graph
    def send(
        key: TS[int], table: TS[str], data: TS[Frame[_schema]],
        write_mode: TS[DeltaWriteMode], schema_mode: TS[DeltaSchemaMode],
        keys: TS[tuple[str, ...]], partition: TS[tuple[str, ...]],
    ) -> TS[int]:
        return delta_write_adaptor_raw.from_graph(
            path=connection_string, table=table, data=data,
            write_mode=write_mode, schema_mode=schema_mode, keys=keys,
            partition=partition, __request_id__=key)

    null_sink(map_(
        send, table=table, data=data, write_mode=write_mode,
        schema_mode=schema_mode, keys=keys, partition=partition))
    request_keys = feedback(TSS[int])(table.key_set)

    @graph
    def receive(key: TS[int]):
        return delta_write_adaptor_raw[SCHEMA:_schema].to_graph(
            path=connection_string, __request_id__=key,
            __no_ts_inputs__=True)

    return map_(receive, __keys__=request_keys())
