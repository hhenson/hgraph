import logging
import time
from concurrent.futures import Executor
from datetime import datetime, timezone
from enum import Enum

import pyarrow as pa
import pyarrow.compute as pc

from hgraph import (
    AUTO_RESOLVE,
    SCHEMA,
    STATE,
    Frame,
    TS,
    TSB,
    TSD,
    map_,
    push_queue,
    service_adaptor,
    service_adaptor_impl,
    sink_node,
)
from hgraph.adaptors.executor import adaptor_executor
from hgraph.stream import Data, Stream, StreamStatus

from .sql_connection import (
    SqlAdaptorConnection,
    _require_polars,
    start_sql_adaptor,
)

logger = logging.getLogger(__name__)

__all__ = (
    "SQLWriteMode",
    "sql_read_adaptor_raw",
    "sql_read_adaptor_raw_impl",
    "sql_write_adaptor_raw",
    "sql_write_adaptor_raw_impl",
    "sql_execute_adaptor_raw",
    "sql_execute_adaptor_raw_impl",
)


_RAW_FRAME_STREAM = TSB[Stream[Data[Frame]]]
_TIME_STREAM = TSB[Stream[Data[datetime]]]


class SQLWriteMode(Enum):
    APPEND = "append"
    OVERWRITE = "replace"
    FAIL = "fail"


def _utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _normalise_frame(frame: pa.Table) -> pa.Table:
    columns = []
    fields = []
    for field, column in zip(frame.schema, frame.columns):
        value = column
        data_type = field.type
        if pa.types.is_timestamp(data_type):
            if data_type.tz is not None:
                value = pc.cast(value, pa.timestamp(data_type.unit, tz="UTC"))
            value = pc.cast(value, pa.timestamp("us"))
            data_type = pa.timestamp("us")
        elif pa.types.is_decimal(data_type):
            data_type = pa.int64() if data_type.scale == 0 else pa.float64()
            value = pc.cast(value, data_type)
        columns.append(value)
        fields.append(pa.field(field.name, data_type, nullable=field.nullable))
    return pa.Table.from_arrays(columns, schema=pa.schema(fields))


@service_adaptor
def sql_read_adaptor_raw(
    path: str, query: TS[str],
) -> _RAW_FRAME_STREAM:
    ...


@service_adaptor_impl(interfaces=sql_read_adaptor_raw)
def sql_read_adaptor_raw_impl(
    path: str, query: TSD[int, TS[str]],
) -> TSD[int, _RAW_FRAME_STREAM]:
    sender_ref = {}

    @push_queue(TSD[int, _RAW_FRAME_STREAM])
    def sql_to_graph(sender):
        sender_ref["sender"] = sender

    def run_query(connection, request_id, statement, sender, previous):
        if previous is not None:
            previous.result()
        start = time.perf_counter_ns()
        try:
            logger.info("Running query %s on %s:\n%s", request_id, path, statement)
            result = _normalise_frame(connection.read_database(statement))
            elapsed = (time.perf_counter_ns() - start) / 1_000_000_000
            logger.info(
                "Finished query %s in %ss with %s rows",
                request_id, elapsed, result.num_rows)
            sender({request_id: {
                "status": StreamStatus.OK,
                "status_msg": "",
                "values": result,
                "timestamp": _utc_now(),
            }})
        except Exception as error:
            elapsed = (time.perf_counter_ns() - start) / 1_000_000_000
            logger.exception(
                "Query %s on %s failed after %ss", request_id, path, elapsed)
            sender({request_id: {
                "status": StreamStatus.ERROR,
                "status_msg": str(error),
            }})

    @sink_node
    def send_query(
        request_id: TS[int], statement: TS[str],
        connection: TS[SqlAdaptorConnection], executor: TS[Executor],
        _state: STATE = None,
    ):
        previous = getattr(_state, "future", None)
        _state.future = executor.value.submit(
            run_query, connection.value, request_id.value, statement.value,
            sender_ref["sender"], previous)

    connection = start_sql_adaptor(path=path)
    executor = adaptor_executor()
    map_(
        lambda key, statement, connection, executor: send_query(
            request_id=key, statement=statement,
            connection=connection, executor=executor),
        statement=query, connection=connection, executor=executor,
    )
    return sql_to_graph()


@service_adaptor
def sql_write_adaptor_raw(
    path: str, table: TS[str], data: TS[Frame[SCHEMA]], mode: TS[SQLWriteMode],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> _TIME_STREAM:
    ...


@service_adaptor_impl(interfaces=sql_write_adaptor_raw)
def sql_write_adaptor_raw_impl(
    path: str, table: TSD[int, TS[str]], data: TSD[int, TS[Frame[SCHEMA]]],
    mode: TSD[int, TS[SQLWriteMode]], _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, _TIME_STREAM]:
    sender_ref = {}

    @push_queue(TSD[int, _TIME_STREAM])
    def sql_write_to_graph(sender):
        sender_ref["sender"] = sender

    def write_data(
        connection, request_id, table_name, write_mode, frame, sender, previous,
    ):
        if previous is not None:
            previous.result()
        start = time.perf_counter_ns()
        try:
            pl = _require_polars()
            with connection.connection.connect() as query_connection:
                autocommit = query_connection.execution_options(autocommit=True)
                data_frame = pl.from_arrow(frame)
                logger.info(
                    "Query %s writing %s rows to table %s on %s",
                    request_id, data_frame.height, table_name,
                    connection.connection.url)
                rows = data_frame.write_database(
                    table_name, connection=autocommit,
                    if_table_exists=write_mode.value)
                elapsed = (time.perf_counter_ns() - start) / 1_000_000_000
                logger.info(
                    "Query %s finished writing %s rows in %ss",
                    request_id, rows, elapsed)
                now = _utc_now()
                sender({request_id: {
                    "status": StreamStatus.OK,
                    "status_msg": "",
                    "values": now,
                    "timestamp": now,
                }})
        except Exception as error:
            elapsed = (time.perf_counter_ns() - start) / 1_000_000_000
            logger.exception(
                "Query %s writing to table %s failed after %ss",
                request_id, table_name, elapsed)
            sender({request_id: {
                "status": StreamStatus.ERROR,
                "status_msg": str(error),
            }})

    @sink_node
    def send_data(
        request_id: TS[int], table: TS[str], data: TS[Frame[_schema]],
        mode: TS[SQLWriteMode], connection: TS[SqlAdaptorConnection],
        executor: TS[Executor], _state: STATE = None,
    ):
        previous = getattr(_state, "future", None)
        _state.future = executor.value.submit(
            write_data, connection.value, request_id.value, table.value,
            mode.value, data.value, sender_ref["sender"], previous)

    connection = start_sql_adaptor(path=path)
    executor = adaptor_executor()
    map_(
        lambda key, table, data, connection, executor, mode: send_data(
            request_id=key, table=table, data=data, mode=mode,
            connection=connection, executor=executor),
        table=table, data=data, connection=connection,
        executor=executor, mode=mode,
    )
    return sql_write_to_graph()


@service_adaptor
def sql_execute_adaptor_raw(path: str, query: TS[str]) -> _TIME_STREAM:
    ...


@service_adaptor_impl(interfaces=sql_execute_adaptor_raw)
def sql_execute_adaptor_raw_impl(
    path: str, query: TSD[int, TS[str]],
) -> TSD[int, _TIME_STREAM]:
    sender_ref = {}

    @push_queue(TSD[int, _TIME_STREAM])
    def sql_to_graph(sender):
        sender_ref["sender"] = sender

    def run_query(connection, request_id, statement, sender, previous):
        if previous is not None:
            previous.result()
        start = time.perf_counter_ns()
        try:
            logger.info("Executing query %s on %s:\n%s", request_id, path, statement)
            connection.read_database(statement)
            elapsed = (time.perf_counter_ns() - start) / 1_000_000_000
            logger.info("Finished executing query %s in %ss", request_id, elapsed)
            sender({request_id: {
                "status": StreamStatus.OK,
                "status_msg": "",
                "timestamp": _utc_now(),
            }})
        except Exception as error:
            elapsed = (time.perf_counter_ns() - start) / 1_000_000_000
            logger.exception(
                "Query %s on %s failed after %ss", request_id, path, elapsed)
            sender({request_id: {
                "status": StreamStatus.ERROR,
                "status_msg": str(error),
            }})

    @sink_node
    def send_query(
        request_id: TS[int], statement: TS[str],
        connection: TS[SqlAdaptorConnection], executor: TS[Executor],
        _state: STATE = None,
    ):
        previous = getattr(_state, "future", None)
        _state.future = executor.value.submit(
            run_query, connection.value, request_id.value, statement.value,
            sender_ref["sender"], previous)

    connection = start_sql_adaptor(path=path)
    executor = adaptor_executor()
    map_(
        lambda key, statement, connection, executor: send_query(
            request_id=key, statement=statement,
            connection=connection, executor=executor),
        statement=query, connection=connection, executor=executor,
    )
    return sql_to_graph()
