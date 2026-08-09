from datetime import datetime
from typing import cast

from hgraph import (
    AUTO_RESOLVE,
    DEFAULT,
    MIN_DT,
    SCHEMA,
    Frame,
    TS,
    TSB,
    TSD,
    TSS,
    compute_node,
    convert,
    default,
    exception_time_series,
    feedback,
    graph,
    if_then_else,
    last_modified_time,
    map_,
    null_sink,
    sample,
    service_adaptor,
    service_adaptor_impl,
)
from hgraph.adaptors.data_catalogue.catalogue import DataEnvironment
from hgraph.stream import Data, Stream, StreamStatus

from .sql_adaptor_raw import (
    SQLWriteMode,
    _RAW_FRAME_STREAM,
    _TIME_STREAM,
    sql_execute_adaptor_raw,
    sql_read_adaptor_raw,
    sql_write_adaptor_raw,
)

__all__ = (
    "sql_read_adaptor",
    "sql_read_adaptor_impl",
    "sql_write_adaptor",
    "sql_write_adaptor_impl",
    "sql_execute_adaptor",
    "sql_execute_adaptor_impl",
)


def _environment_path(path: str) -> str:
    environment = DataEnvironment.current()
    if environment is None:
        raise RuntimeError(f"No DataEnvironment set up for {path}")
    return environment.get_entry(path).environment_path


@compute_node
def _to_naive(timestamp: TS[datetime]) -> TS[datetime]:
    value = timestamp.value
    return value.replace(tzinfo=None) if value.tzinfo is not None else value


@service_adaptor
def sql_read_adaptor(
    path: str, query: TS[str], _schema: type[SCHEMA] = DEFAULT[SCHEMA],
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    ...


@service_adaptor_impl(interfaces=sql_read_adaptor)
def sql_read_adaptor_impl(
    path: str, query: TSD[int, TS[str]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, TSB[Stream[Data[Frame[SCHEMA]]]]]:
    connection_string = _environment_path(path)

    @graph
    def send(key: TS[int], statement: TS[str]) -> TS[int]:
        return sql_read_adaptor_raw.from_graph(
            path=connection_string, query=statement, __request_id__=key)

    null_sink(map_(send, statement=query))
    keys = feedback(TSS[int])(query.key_set)

    @graph
    def raw_data_to_schema(raw: _RAW_FRAME_STREAM):
        converted = convert[TS[Frame[_schema]]](raw.values)
        error = exception_time_series(converted)
        output_type = TSB[Stream[Data[Frame[_schema]]]]
        return cast(TSB, output_type.from_ts(
            status=if_then_else(
                last_modified_time(raw.status)
                == default(last_modified_time(error), MIN_DT),
                sample(error, StreamStatus.ERROR), raw.status),
            status_msg=if_then_else(
                last_modified_time(raw.status)
                == default(last_modified_time(error), MIN_DT),
                error.error_msg, raw.status_msg),
            timestamp=_to_naive(raw.timestamp),
            values=converted,
        ))

    @graph
    def receive(key: TS[int]):
        return raw_data_to_schema(sql_read_adaptor_raw.to_graph(
            path=connection_string, __request_id__=key,
            __no_ts_inputs__=True))

    return map_(receive, __keys__=keys())


@service_adaptor
def sql_write_adaptor(
    path: str, table: TS[str], data: TS[Frame[SCHEMA]], mode: TS[SQLWriteMode],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> _TIME_STREAM:
    ...


@service_adaptor_impl(interfaces=sql_write_adaptor)
def sql_write_adaptor_impl(
    path: str, table: TSD[int, TS[str]], data: TSD[int, TS[Frame[SCHEMA]]],
    mode: TSD[int, TS[SQLWriteMode]], _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, _TIME_STREAM]:
    connection_string = _environment_path(path)

    @graph
    def send(
        key: TS[int], table_name: TS[str], frame: TS[Frame[_schema]],
        write_mode: TS[SQLWriteMode],
    ) -> TS[int]:
        return sql_write_adaptor_raw.from_graph(
            path=connection_string, table=table_name, data=frame,
            mode=write_mode, __request_id__=key)

    null_sink(map_(
        send, table_name=table, frame=data, write_mode=mode))
    keys = feedback(TSS[int])(table.key_set)

    @graph
    def receive(key: TS[int]):
        return sql_write_adaptor_raw[SCHEMA:_schema].to_graph(
            path=connection_string, __request_id__=key,
            __no_ts_inputs__=True)

    return map_(receive, __keys__=keys())


@service_adaptor
def sql_execute_adaptor(path: str, query: TS[str]) -> _TIME_STREAM:
    ...


@service_adaptor_impl(interfaces=sql_execute_adaptor)
def sql_execute_adaptor_impl(
    path: str, query: TSD[int, TS[str]],
) -> TSD[int, _TIME_STREAM]:
    connection_string = _environment_path(path)

    @graph
    def send(key: TS[int], statement: TS[str]) -> TS[int]:
        return sql_execute_adaptor_raw.from_graph(
            path=connection_string,
            query=statement + "; select getutcdate()",
            __request_id__=key)

    null_sink(map_(send, statement=query))
    keys = feedback(TSS[int])(query.key_set)

    @graph
    def receive(key: TS[int]):
        return sql_execute_adaptor_raw.to_graph(
            path=connection_string, __request_id__=key,
            __no_ts_inputs__=True)

    return map_(receive, __keys__=keys())
