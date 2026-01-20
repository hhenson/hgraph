from datetime import datetime
from typing import Type, cast

from polars import DataFrame

from hgraph.adaptors.sql.sql_adaptor_raw import (
    sql_read_adaptor_raw,
    sql_read_adaptor_raw_impl,
    sql_write_adaptor_raw,
    SQLWriteMode,
    sql_write_adaptor_raw_impl, sql_execute_adaptor_raw,
)
from hgraph.adaptors.data_catalogue.catalogue import DataEnvironment
from hgraph import (
    MIN_DT,
    MIN_TD,
    default,
    last_modified_time,
    modified,
    sample,
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
    compute_node,
)
from hgraph.stream.stream import Stream, StreamStatus, Data


__all__ = ['sql_read_adaptor', 'sql_read_adaptor_impl', 'sql_write_adaptor', 'sql_write_adaptor_impl', 'sql_execute_adaptor', 'sql_execute_adaptor_impl']

@service_adaptor
def sql_read_adaptor(
    query: TS[str],
    path: str,
    _schema: Type[SCHEMA] = DEFAULT[SCHEMA]
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    ...


@service_adaptor_impl(interfaces=sql_read_adaptor)
def sql_read_adaptor_impl(
    query: TSD[int, TS[str]],
    path: str,
    _schema: Type[SCHEMA] = AUTO_RESOLVE
) -> TSD[int, TSB[Stream[Data[Frame[SCHEMA]]]]]:
    @graph
    def raw_data_to_schema(
        data: TSB[Stream[Data[DataFrame]]], _schema: Type[SCHEMA] = AUTO_RESOLVE
    ) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
        converted_data = convert[TS[Frame[_schema]]](data.values)
        error = exception_time_series(converted_data)
        return cast(
            TSB,
            {
                "status": if_then_else(last_modified_time(data.status) == default(last_modified_time(error), MIN_DT), sample(error, StreamStatus.ERROR), data.status),
                "status_msg": if_then_else(last_modified_time(data.status) == default(last_modified_time(error), MIN_DT), error.error_msg, data.status_msg),
                "timestamp": to_naive(data.timestamp),
                "values": converted_data,
            },
        )

    de = DataEnvironment.current()
    if not de:
        raise RuntimeError(f"No DataEnvironment set up for {path}")
    connection_string = de.get_entry(path).environment_path

    map_(
        lambda key, q: sql_read_adaptor_raw.from_graph(
            path=connection_string, query=q, __request_id__=key
        ),
         q=query,
    )
    fb_keys = feedback(TSS[int])(query.key_set)

    return map_(
        lambda key: raw_data_to_schema(
            sql_read_adaptor_raw.to_graph(
                path=connection_string,
                __request_id__=key,
                __no_ts_inputs__=True
            ),
            _schema=_schema,
        ),
        __keys__=fb_keys(),
    )


@compute_node
def to_naive(timestamp: TS[datetime]) -> TS[datetime]:
    return timestamp.value.replace(tzinfo=None)


@service_adaptor
def sql_write_adaptor(
    path: str, table: TS[str], data: TS[DataFrame], mode: TS[SQLWriteMode]
) -> TSB[Stream[Data[datetime]]]: ...


@service_adaptor_impl(interfaces=sql_write_adaptor)
def sql_write_adaptor_impl(
    path: str,
    table: TSD[int, TS[str]],
    data: TSD[int, TS[DataFrame]],
    mode: TSD[int, TS[SQLWriteMode]],
) -> TSD[int, TSB[Stream[Data[datetime]]]]:
    de = DataEnvironment.current()
    if not de:
        raise RuntimeError(f"No DataEnvironment set up for {path}")
    connection_string = de.get_entry(path).environment_path

    map_(
        lambda key, t, d, m: sql_write_adaptor_raw.from_graph(
            path=connection_string, table=t, data=d, mode=m, __request_id__=key
        ),
        t=table,
        d=data,
        m=mode,
    )

    fb_keys = feedback(TSS[int])(table.key_set)

    return map_(
        lambda key: sql_write_adaptor_raw.to_graph(path=connection_string, __request_id__=key, __no_ts_inputs__=True),
        __keys__=fb_keys(),
    )


@service_adaptor
def sql_execute_adaptor(query: TS[str], path: str) -> TSB[Stream[Data[datetime]]]:
    ...


@service_adaptor_impl(interfaces=sql_execute_adaptor)
def sql_execute_adaptor_impl(query: TSD[int, TS[str]], path: str,) -> TSD[int, TSB[Stream[Data[datetime]]]]:

    de = DataEnvironment.current()
    if not de:
        raise RuntimeError(f"No DataEnvironment set up for {path}")
    connection_string = de.get_entry(path).environment_path

    map_(
        lambda key, q: sql_execute_adaptor_raw.from_graph(
            path=connection_string,
            query=q + "; select getutcdate()",
            __request_id__=key
        ),
         q=query,
    )
    fb_keys = feedback(TSS[int])(query.key_set)

    return map_(
        lambda key: sql_execute_adaptor_raw.to_graph(path=connection_string, __request_id__=key, __no_ts_inputs__=True),
        __keys__=fb_keys(),
    )


if __name__ == "__main__":
    import sqlite3
    import polars as pl
    from dataclasses import dataclass
    from hgraph.adaptors.data_catalogue.catalogue import DataEnvironmentEntry
    from hgraph import debug_print, if_true, register_adaptor, CompoundScalar, run_graph, EvaluationMode, generator
    from hgraph import stop_engine
    from datetime import timedelta

    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect("prices.db")
    cursor = conn.cursor()

    # Create table
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
            {"val": 100.0, "timestamp": 20240102, "symbol": ".SPX"},
            {"val": 101.0, "timestamp": 20240103, "symbol": ".SPX"},
            {"val": 102.0, "timestamp": 20240104, "symbol": ".SPX"},
            {"val": 103.0, "timestamp": 20240105, "symbol": ".SPX"},
            {"val": 104.0, "timestamp": 20240108, "symbol": ".SPX"},
            {"val": 105.0, "timestamp": 20240109, "symbol": ".SPX"},
        )
    )

    data_env = DataEnvironment()
    data_env.add_entry(
        DataEnvironmentEntry(source_path="close_prices", environment_path="sqlite:///c:\\Temp\\prices.db")
    )

    DataEnvironment.set_current(data_env)

    @dataclass
    class HistoricalPrice(CompoundScalar):
        val: float
        timestamp: int
        symbol: str

    @graph
    def g():
        register_adaptor("close_prices", sql_read_adaptor_impl)
        register_adaptor("close_prices", sql_write_adaptor_impl)
        register_adaptor(None, sql_read_adaptor_raw_impl)
        register_adaptor(None, sql_write_adaptor_raw_impl)

        @generator
        def gen_data() -> TS[Frame[HistoricalPrice]]:
            yield timedelta(), df

        written = sql_write_adaptor(
            path="close_prices", table="reuters_close", data=gen_data(), mode=SQLWriteMode.OVERWRITE
        )

        debug_print("written", written)

        res1 = sql_read_adaptor[HistoricalPrice](
            path="close_prices",
            query=(
                "SELECT val, business_date AS TIMESTAMP FROM reuters_close "
                "WHERE business_date >= 20240101 AND business_date <= 20240131"
            ),
        )

        debug_print("result1", res1)
        stop_engine(if_true(valid(res1)))

    run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=100))
