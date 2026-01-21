import logging
import time
from concurrent.futures import Executor
from datetime import timedelta, datetime, UTC
from enum import Enum
from typing import Callable

import polars as pl
from polars import DataFrame

from hgraph.adaptors.executor.executor import adaptor_executor
from hgraph.adaptors.sql.sql_connection import SqlAdaptorConnection, start_sql_adaptor
from hgraph import (
    TSB,
    TS,
    TSD,
    service_adaptor,
    service_adaptor_impl,
    push_queue,
    GlobalState,
    generator,
    sink_node,
    map_,
    graph,
    register_adaptor,
    run_graph,
    debug_print,
    EvaluationMode,
)
from hgraph.stream.stream import Stream, StreamStatus, Data

logger = logging.getLogger(__name__)


__all__ = ['SQLWriteMode', 'sql_read_adaptor_raw', 'sql_read_adaptor_raw_impl', 'sql_write_adaptor_raw', 'sql_write_adaptor_raw_impl', 'sql_execute_adaptor_raw', 'sql_execute_adaptor_raw_impl']


class SQLWriteMode(Enum):
    APPEND = "append"
    OVERWRITE = "replace"
    FAIL = "fail"


@service_adaptor
def sql_read_adaptor_raw(
    path: str,
    query: TS[str]
) -> TSB[Stream[Data[DataFrame]]]:
    ...


@service_adaptor_impl(interfaces=sql_read_adaptor_raw)
def sql_read_adaptor_raw_impl(
    path: str,
    query: TSD[int, TS[str]]
) -> TSD[int, TSB[Stream[Data[DataFrame]]]]:

    @push_queue(TSD[int, TSB[Stream[Data[DataFrame]]]])
    def sql_to_graph(sender: Callable, path: str) -> TSD[int, TSB[Stream[Data[DataFrame]]]]:
        GlobalState.instance()[f"sql_read_adaptor_raw://{path}/queue"] = sender

    def run_query(connection: SqlAdaptorConnection, id: int, query: str, queue):
        try:
            logger.info(f"Running query {id} on {path}:\n{query}")
            start = time.perf_counter_ns()
            r = connection.read_database(query)
            
            r = r.with_columns(
                **{col: r[col].dt.convert_time_zone('UTC') for col in r.columns if r[col].dtype == pl.Datetime and r.schema[col].time_zone is not None}
            )            
            r = r.with_columns(
                **{col: r[col].dt.replace_time_zone(None) for col in r.columns if r[col].dtype == pl.Datetime and r.schema[col].time_zone == 'UTC'}
            )            
            r = r.with_columns(
                **{col: r[col].dt.cast_time_unit("us") for col in r.columns if r[col].dtype == pl.Datetime}
            )
            r = r.with_columns(
                **{col: r[col].cast(pl.Int64) for col in r.columns if r[col].dtype == pl.Decimal and r.schema[col].scale == 0}
            )            
            r = r.with_columns(
                **{col: r[col].cast(pl.Float64) for col in r.columns if r[col].dtype == pl.Decimal and r.schema[col].scale > 0}
            )
            
            time_taken = (time.perf_counter_ns() - start) / 1_000_000_000
            logger.info(f"Finished query {id} in {time_taken}s with {len(r)} rows")
            tick = {id: {"status": StreamStatus.OK, "status_msg": "", "values": r, "timestamp": datetime.utcnow()}}
            queue(tick)
        except Exception as e:
            logger.error(f"Query {id} on {path} failed: {query}")
            time_taken = time_str((time.perf_counter_ns() - start) / 1_000_000_000)
            logger.exception(f"Query failed after {time_taken}")
            error = {id: {"status": StreamStatus.ERROR, "status_msg": str(e)}}
            queue(error)

    @sink_node
    def send_query(
        id: TS[int],
        query: TS[str],
        path: str,
        connection: TS[SqlAdaptorConnection],
        executor: TS[Executor]
    ):
        queue = GlobalState.instance()[f"sql_read_adaptor_raw://{path}/queue"]
        executor.value.submit(run_query, connection=connection.value, id=id.value, query=query.value, queue=queue)

    connection = start_sql_adaptor(path=path)

    executor = adaptor_executor()
    map_(
        lambda key, q, c, e: send_query(id=key, query=q, path=path, connection=c, executor=e),
        q=query,
        c=connection,
        e=executor,
    )
    return sql_to_graph(path)


@service_adaptor
def sql_write_adaptor_raw(
    path: str,
    table: TS[str],
    data: TS[DataFrame],
    mode: TS[SQLWriteMode],
) -> TSB[Stream[Data[datetime]]]:
    ...


@service_adaptor_impl(interfaces=sql_write_adaptor_raw)
def sql_write_adaptor_raw_impl(
    path: str,
    table: TSD[int, TS[str]],
    data: TSD[int, TS[DataFrame]],
    mode: TSD[int, TS[SQLWriteMode]],
) -> TSD[int, TSB[Stream[Data[datetime]]]]:

    @push_queue(TSD[int, TSB[Stream[Data[datetime]]]])
    def sql_write_to_graph(sender: Callable, path: str) -> TSD[int, TSB[Stream[Data[datetime]]]]:
        GlobalState.instance()[f"sql_write_adaptor_raw://{path}/queue"] = sender

    def write_data(connection: SqlAdaptorConnection, id: int, table: str, mode: SQLWriteMode, data: DataFrame, queue):
        try:
            start = None

            # TODO - support Snowflake writing
            with connection.connection.connect() as query_connection:
                autocommit_connection = query_connection.execution_options(autocommit=True)
                logger.info(f"writing {data.height} rows to table {table} on {connection.connection.url}")

                start = time.perf_counter_ns()
                rows = data.write_database(table, connection=autocommit_connection, if_table_exists=mode.value)
                period = (time.perf_counter_ns() - start) / 1_000_000_000
                logger.info(f"finished writing {rows} rows to table {table} in {period}s")
                tick = {
                    id: {
                        "status": StreamStatus.OK,
                        "status_msg": "",
                        "values": datetime.utcnow(),
                        "timestamp": datetime.utcnow(),
                    }
                }
                queue(tick)
        except Exception as e:
            error = {id: {"status": StreamStatus.ERROR, "status_msg": str(e)}}
            period = ((time.perf_counter_ns() - start) / 1_000_000_000) if start else "0"
            logger.exception(f"writing to table {table} failed after {period}s with {str(e)}")
            queue(error)

    @sink_node
    def send_data(
        id: TS[int],
        table: TS[str],
        data: TS[DataFrame],
        mode: TS[SQLWriteMode],
        path: str,
        connection: TS[SqlAdaptorConnection],
        executor: TS[Executor],
    ):
        queue = GlobalState.instance()[f"sql_write_adaptor_raw://{path}/queue"]
        executor.value.submit(
            write_data,
            connection=connection.value,
            id=id.value,
            table=table.value,
            data=data.value,
            queue=queue,
            mode=mode.value,
        )

    connection = start_sql_adaptor(path=path)
    executor = adaptor_executor()
    map_(
        lambda key, t, d, c, e, m: send_data(id=key, table=t, data=d, path=path, connection=c, executor=e, mode=m),
        t=table,
        d=data,
        c=connection,
        e=executor,
        m=mode,
    )
    return sql_write_to_graph(path)


@service_adaptor
def sql_execute_adaptor_raw(path: str, query: TS[str],) -> TSB[Stream[Data[datetime]]]:
    ...


@service_adaptor_impl(interfaces=sql_execute_adaptor_raw)
def sql_execute_adaptor_raw_impl(path: str, query: TSD[int, TS[str]]) -> TSD[int, TSB[Stream[Data[datetime]]]]:

    @push_queue(TSD[int, TSB[Stream[Data[datetime]]]])
    def sql_to_graph(sender: Callable, path: str) -> TSD[int, TSB[Stream[Data[datetime]]]]:
        GlobalState.instance()[f"sql_execute_adaptor_raw://{path}/queue"] = sender

    def run_query(connection: SqlAdaptorConnection, id: int, query: str, queue):
        try:
            logger.info(f"Executing query {id} on {path}:\n{query}")
            start = time.perf_counter_ns()
            r = connection.read_database(query)
            time_taken = time_str((time.perf_counter_ns() - start) / 1_000_000_000)
            logger.info(f"Finished executing query {id} in {time_taken}")
            tick = {id: {"status": StreamStatus.OK, "status_msg": "", "timestamp": datetime.now(UTC)}}
            queue(tick)
        except Exception as e:
            logger.error(f"Query {id} on {path} failed: {query}")
            time_taken = time_str((time.perf_counter_ns() - start) / 1_000_000_000)
            logger.exception(f"Query failed after {time_taken}")
            error = {id: {"status": StreamStatus.ERROR, "status_msg": str(e)}}
            queue(error)

    @sink_node
    def send_query(
        id: TS[int],
        query: TS[str],
        path: str,
        connection: TS[SqlAdaptorConnection],
        executor: TS[Executor]
    ):
        queue = GlobalState.instance()[f"sql_execute_adaptor_raw://{path}/queue"]
        executor.value.submit(run_query, connection=connection.value, id=id.value, query=query.value, queue=queue)

    connection = start_sql_adaptor(path=path)

    executor = adaptor_executor()
    map_(
        lambda key, q, c, e: send_query(id=key, query=q, path=path, connection=c, executor=e),
        q=query,
        c=connection,
        e=executor,
    )
    return sql_to_graph(path)


if __name__ == "__main__":
    import sqlite3
    from hgraph import if_true, valid
    from hgraph import stop_engine

    conn = sqlite3.connect("prices.db")
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS reuters_close")
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS reuters_close (
        val REAL,
        business_date INTEGER,
        ticker TEXT
    )
    """
    )

    df = pl.DataFrame(
        (
            {"val": 100.0, "business_date": 20240102, "ticker": ".SPX"},
            {"val": 1001.0, "business_date": 20240102, "ticker": ".FTSE"},
            {"val": 101.0, "business_date": 20240103, "ticker": ".SPX"},
            {"val": 1011.0, "business_date": 20240103, "ticker": ".FTSE"},
            {"val": 102.0, "business_date": 20240104, "ticker": ".SPX"},
            {"val": 1022.0, "business_date": 20240104, "ticker": ".FTSE"},
            {"val": 103.0, "business_date": 20240105, "ticker": ".SPX"},
            {"val": 1031.0, "business_date": 20240105, "ticker": ".FTSE"},
            {"val": 104.0, "business_date": 20240106, "ticker": ".SPX"},
            {"val": 1014.0, "business_date": 20240106, "ticker": ".FTSE"},
            {"val": 105.0, "business_date": 20240107, "ticker": ".SPX"},
            {"val": 1025.0, "business_date": 20240107, "ticker": ".FTSE"},
        )
    )

    @graph
    def g():
        register_adaptor("sqlite:///c:\\Temp\\prices.db", sql_read_adaptor_raw_impl)
        register_adaptor("sqlite:///c:\\Temp\\prices.db", sql_write_adaptor_raw_impl)

        @generator
        def gen_data() -> TS[DataFrame]:
            yield timedelta(), df

        written = sql_write_adaptor_raw(
            path="sqlite:///c:\\Temp\\prices.db", table="reuters_close", data=gen_data(), mode=SQLWriteMode.OVERWRITE
        )
        debug_print("written", written)
        res1 = sql_read_adaptor_raw(
            "sqlite:///c:\\Temp\\prices.db",
            "SELECT ticker, business_date, val from reuters_close where business_date > 20240101 and ticker = '.SPX'",
        )
        res2 = sql_read_adaptor_raw(
            "sqlite:///c:\\Temp\\prices.db",
            "SELECT ticker, business_date, val FROM reuters_close WHERE business_date > 20240103 AND ticker = '.FTSE'",
        )

        debug_print("result1", res1)
        debug_print("result", res2)
        stop_engine(if_true(valid(res1) & valid(res2)))

    run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=100))
