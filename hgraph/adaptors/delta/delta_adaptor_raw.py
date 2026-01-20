import logging
import os
from concurrent.futures import Executor
from dataclasses import dataclass
from datetime import timedelta, datetime
from enum import Enum
from typing import Callable, Tuple

import boto3
import polars as pl
from deltalake import DeltaTable, QueryBuilder, write_deltalake
from polars import DataFrame

from hgraph.adaptors.executor.executor import adaptor_executor
from hgraph import (
    REMOVE,
    TSB,
    TS,
    TSD,
    service_adaptor,
    service_adaptor_impl,
    push_queue,
    GlobalState,
    generator,
    STATE,
    sink_node,
    map_,
    graph,
    register_adaptor,
    run_graph,
    debug_print,
    EvaluationMode,
    CompoundScalar,
    valid,
    if_true,
    Frame,
    sample,
    const,
    nothing,
    schedule,
    MIN_DT,
)
from hgraph import stop_engine
from hgraph.stream.stream import Stream, StreamStatus, Data

logger = logging.getLogger(__name__)


__all__ = ['delta_read_adaptor_raw', 'delta_read_adaptor_raw_impl', 'delta_write_adaptor_raw', 'delta_write_adaptor_raw_impl', 'delta_query_adaptor_raw', 'delta_query_adaptor_raw_impl']

@service_adaptor
def delta_read_adaptor_raw(
    path: str, 
    table: TS[str], 
    columns: TS[tuple[str, ...]] = None,
    filters: TS[tuple[tuple[str, str, object], ...]] = None, 
    sort: TS[tuple[tuple[str, bool]]] = None
) -> TSB[Stream[Data[DataFrame]]]: 
    """
    Read data from a Delta table.

    :param path: The base path to the Delta table.
    :param table: The name of the Delta table to read.
    :param filters: Optional filters to apply to the data.
                    Each filter is a tuple containing a column name, an operator (e.g., ">", "<", "=", etc.) and a value
    :param sort: Optional sorting criteria for the data, where each tuple contains a column name and a boolean
                 indicating ascending (True) or descending (False) order.
    :return: A Stream of DataFrame containing the queried data; this stream will tick once.
    """


@service_adaptor_impl(interfaces=delta_read_adaptor_raw)
def delta_read_adaptor_raw_impl(
    path: str, 
    table: TSD[int, TS[str]], 
    columns: TSD[int, TS[tuple[str, ...]]] = None,
    filters: TSD[int, TS[tuple[tuple[str, str, object], ...]]] = None,
    sort: TSD[int, TS[tuple[tuple[str, bool], ...]]] = None
) -> TSD[int, TSB[Stream[Data[DataFrame]]]]:
    if not path.endswith(os.path.sep):
        path += os.path.sep

    @push_queue(TSD[int, TSB[Stream[Data[DataFrame]]]])
    def delta_to_graph(sender: Callable, path: str) -> TSD[int, TSB[Stream[Data[DataFrame]]]]:
        GlobalState.instance()[f"delta_read_adaptor_raw://{path}/queue"] = sender
        return None

    def run_query(
        credentials: dict,
        id: int,
        table: str,
        columns: tuple[str, ...],
        filters: tuple[tuple[str, str, object], ...],
        sort: tuple[tuple[str, bool]],
        queue
    ):
        try:
            logger.info(f"will read from delta table {table} with filters {filters}")
            t1 = datetime.now()
            dt = DeltaTable(table, storage_options=credentials or None)

            pa = dt.to_pyarrow_table(filters=list(filters), columns=list(columns) if columns else None)
            r = pl.DataFrame(pa)
            logger.info(f"successfully read from delta table {table} with filters {filters}: {r.height} rows")

            if sort:
                r = r.sort(by=[col[0] for col in sort], descending=[not col[1] for col in sort])
            t2 = datetime.now()
            logger.info(
                f"successfully read from delta table {table} with filters {filters}: "
                f"{len(r)} rows in {t2 - t1}"
            )
            tick = {id: {"status": StreamStatus.OK, "status_msg": "", "values": r, "timestamp": datetime.now()}}
            queue(tick)
        except Exception as e:
            logger.error(f"error reading from delta table {table} with filters {filters}:\n{type(e)}: {str(e)}")
            error = {id: {"status": StreamStatus.ERROR, "status_msg": str(e)}}
            queue(error)

    @sink_node(valid=("id", "table", "filters", "credentials", "executor"))
    def send_query(
        id: TS[int],
        table: TS[str],
        columns: TS[tuple[str, ...]],
        filters: TS[tuple[tuple[str, str, object], ...]],
        sort: TS[tuple[tuple[str, bool], ...]],
        path: str,
        credentials: TS[object],
        executor: TS[Executor],
    ):
        queue = GlobalState.instance()[f"delta_read_adaptor_raw://{path}/queue"]
        creds = dict(credentials.value) if credentials.valid else {}
        path = creds.pop("path", path)
        executor.value.submit(
            run_query,
            credentials=creds,
            id=id.value,
            table=path + table.value,
            columns=columns.value,
            filters=filters.value,
            sort=sort.value,
            queue=queue
        )

    @send_query.stop
    def stop_send_query(id: TS[int], path: str):
        queue = GlobalState.instance()[f"delta_read_adaptor_raw://{path}/queue"]
        queue({id.value: REMOVE})

    credentials = delta_storage_options(path)
    executor = adaptor_executor()
    map_(
        lambda key, t, k, f, s, c, e: send_query(
            id=key,
            table=t,
            columns=k,
            filters=f,
            sort=s,
            path=path,
            credentials=c,
            executor=e
        ),
        t=table,
        k=columns,
        f=filters,
        s=sort,
        c=credentials,
        e=executor,
    )

    return delta_to_graph(path)


@service_adaptor
def delta_query_adaptor_raw(
    path: str, tables: TS[set[str]], query: TS[str]
) -> TSB[Stream[Data[DataFrame]]]: 
    """
    Query data from a Delta table or Delta tables.

    :param path: The base path to the Delta table.
    :param tables: The names of the Delta tables to be used in the query.
    :param query: SQL query for Apache DataFusion engine
    :return: A Stream of DataFrame containing the queried data, this stream will tick once.
    """


@service_adaptor_impl(interfaces=delta_query_adaptor_raw)
def delta_query_adaptor_raw_impl(
    path: str, 
    tables: TSD[int, TS[set[str]]], 
    query: TSD[int, TS[str]]
) -> TSD[int, TSB[Stream[Data[DataFrame]]]]:
    if not path.endswith(os.path.sep):
        path += os.path.sep

    @push_queue(TSD[int, TSB[Stream[Data[DataFrame]]]])
    def delta_to_graph(sender: Callable, path: str) -> TSD[int, TSB[Stream[Data[DataFrame]]]]:
        GlobalState.instance()[f"delta_query_adaptor_raw://{path}/queue"] = sender
        return None

    def run_query(credentials: dict, id: int, path: str, tables: set[str], query: str, queue):
        import pyarrow as pa
        try:
            logger.info(f"will query from delta tables {tables} with SQL '{query}'")

            qb = QueryBuilder()
            for t in tables:
                qb.register(t, DeltaTable(path + t, storage_options=credentials or None))

            rbr = qb.execute(query)
            if rbr:
                rdr = pa.RecordBatchReader.from_batches(rbr[0].schema, rbr)
                r = pl.from_arrow(rdr.read_all())
            else:
                r = pl.DataFrame()

            logger.info(f"successfully queried from delta tables {tables} with SQL '{query}': {len(r)} rows")
            tick = {id: {"status": StreamStatus.OK, "status_msg": "", "values": r, "timestamp": datetime.now()}}
            queue(tick)
        except Exception as e:
            logger.error(f"error querying from delta tables {tables} with SQL `{query}`:\n{type(e)}: {str(e)}")
            error = {id: {"status": StreamStatus.ERROR, "status_msg": str(e)}}
            queue(error)

    @sink_node(valid=("id", "tables", "query", "credentials", "executor"))
    def send_query(
        id: TS[int],
        tables: TS[set[str]],
        query: TS[str],
        path: str,
        credentials: TS[object],
        executor: TS[Executor],
    ):
        queue = GlobalState.instance()[f"delta_query_adaptor_raw://{path}/queue"]
        creds = dict(credentials.value) if credentials.valid else {}
        path = creds.pop("path", path)
        executor.value.submit(
            run_query, credentials=creds, id=id.value, path=path, tables=tables.value, query=query.value, queue=queue
        )

    credentials = delta_storage_options(path)
    executor = adaptor_executor()
    map_(
        lambda key, t, q, c, e: send_query(id=key, tables=t, query=q, path=path, credentials=c, executor=e),
        t=tables,
        q=query,
        c=credentials,
        e=executor,
    )

    return delta_to_graph(path)


class DeltaWriteMode(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    ERROR = "error"
    IGNORE = "ignore"


class DeltaSchemaMode(Enum):
    OVERWRITE = "overwrite"
    MERGE = "merge"


@service_adaptor
def delta_write_adaptor_raw(
    path: str,
    table: TS[str],
    data: TS[DataFrame],
    write_mode: TS[DeltaWriteMode],
    schema_mode: TS[DeltaSchemaMode],
    keys: TS[Tuple[str, ...]],
    partition: TS[Tuple[str, ...]],
) -> TSB[Stream[Data[datetime]]]: ...


@service_adaptor_impl(interfaces=delta_write_adaptor_raw)
def delta_write_adaptor_raw_impl(
    path: str,
    table: TSD[int, TS[str]],
    data: TSD[int, TS[DataFrame]],
    write_mode: TSD[int, TS[DeltaWriteMode]],
    schema_mode: TSD[int, TS[DeltaSchemaMode]],
    keys: TSD[int, TS[Tuple[str, ...]]],
    partition: TSD[int, TS[Tuple[str, ...]]],
) -> TSD[int, TSB[Stream[Data[datetime]]]]:
    if not path.endswith(os.path.sep):
        path += os.path.sep

    @push_queue(TSD[int, TSB[Stream[Data[datetime]]]])
    def delta_to_graph(sender: Callable, path: str) -> TSD[int, TSB[Stream[Data[datetime]]]]:
        GlobalState.instance()[f"delta_write_adaptor_raw://{path}/queue"] = sender
        return None

    def run_query(
        credentials: dict,
        id: int,
        table: str,
        mode: DeltaWriteMode,
        schema_mode: DeltaSchemaMode,
        keys: tuple[str, ...],
        partition: tuple[str, ...],
        data: pl.DataFrame,
        ts: datetime,
        queue,
    ):
        try:
            logger.info(
                f"will write {len(data)} rows to delta table {table} with mode {mode.value}, "
                f"schema mode {schema_mode.value} and keys {keys}"
            )
            configuration = {
                "delta.deletedFileRetentionDuration": "interval 1 days",
                "delta.logRetentionDuration": "interval 2 days",
            }

            predicate = None
            if keys:
                if len(keys) > 1:
                    data = data.with_columns(__index__=
                        pl.concat_str(*[pl.col(k).cast(pl.String) for k in keys], separator=",")
                    )
                if mode == DeltaWriteMode.OVERWRITE:
                    if len(keys) == 1:
                        if data[keys[0]].dtype != pl.String:
                            predicate = f"{keys[0]} in ({', '.join(data[keys[0]].unique().cast(pl.String).to_list())})"
                        else:
                            predicate = f"{keys[0]} in ('{"', '".join([v.replace("'", "''") for v in data[keys[0]].unique().to_list()])}')"
                    else:
                        predicate = f"__index__ in ('{"', '".join(data['__index__'].unique().to_list())}')"

            write_deltalake(
                table,
                data.to_arrow(),
                mode=mode.value,
                storage_options=credentials or None,
                configuration=configuration,
                schema_mode=schema_mode.value,
                partition_by=list(partition) if partition else None,
                predicate=predicate,
            )

            tick = {id: {"status": StreamStatus.OK, "status_msg": "", "timestamp": ts}}
            queue(tick)
        except Exception as e:
            logger.error(
                f"Error writing to delta table {table} with mode {mode.value} and keys {keys}:\n{type(e)} {str(e)}"
            )
            error = {id: {"status": StreamStatus.ERROR, "status_msg": str(e)}}
            queue(error)

    @sink_node(valid=("id", "table", "data", "mode", "credentials", "executor"))
    def send_query(
        id: TS[int],
        table: TS[str],
        data: TS[DataFrame],
        mode: TS[DeltaWriteMode],
        schema_mode: TS[DeltaSchemaMode],
        keys: TS[tuple[str, ...]],
        partition: TS[tuple[str, ...]],
        path: str,
        credentials: TS[object],
        executor: TS[Executor],
    ):
        queue = GlobalState.instance()[f"delta_write_adaptor_raw://{path}/queue"]
        creds = dict(credentials.value) if credentials.valid else {}
        path = creds.pop("path", path)
        executor.value.submit(
            run_query,
            credentials=creds,
            id=id.value,
            table=path + table.value,
            data=data.value,
            mode=mode.value,
            keys=keys.value,
            partition=partition.value,
            ts=data.last_modified_time,
            queue=queue,
            schema_mode=schema_mode.value,
        )

    credentials = delta_storage_options(path)
    executor = adaptor_executor()
    map_(
        send_query,
        __key_arg__="id",
        table=table,
        mode=write_mode,
        keys=keys,
        partition=partition,
        data=data,
        credentials=credentials,
        executor=executor,
        path=path,
        schema_mode=schema_mode
    )

    return delta_to_graph(path)


@graph
def delta_table_maintenance(path: str, table: str, periodic: timedelta, start: datetime = MIN_DT):
    if not path.endswith(os.path.sep):
        path += os.path.sep

    trigger = schedule(periodic, start=start)

    @sink_node
    def maintenance(path: str, table: str, trigger: TS[bool], executor: TS[Executor], credentials: TS[object]):
        creds = dict(credentials.value) if credentials.valid else {}
        path = creds.pop("path", path) if credentials.valid else path

        def _do_maintenance(credentials):
            table_path = path + table

            try:
                logger.info(f"Doing maintenance for delta table {table_path}")
                dt = DeltaTable(table_path, storage_options=credentials)
                compact_result = dt.optimize.compact()
                logger.info(f"Compaction for delta table {table_path}: {compact_result}")
                vacuum_result = dt.vacuum(retention_hours=1, enforce_retention_duration=False, dry_run=False)
                logger.info(f"Vacuum for delta table {table_path}: {vacuum_result}")
            except Exception as e:
                logger.error(f"Error doing maintenance for delta table {table_path}:\n{type(e)} {str(e)}")
                return

        if trigger.value:
            executor.value.submit(_do_maintenance, creds)

    maintenance(path, table, trigger, adaptor_executor(), delta_storage_options(path))


@generator
def delta_storage_options(path: str, _state: STATE = None) -> TS[object]:
    if path.startswith("s3://"):
        credentials = boto3.Session().get_credentials()  ## this is thread safe ## maybe do "credentials.client(s3)"???
        _state.storage_options = {
            "aws_access_key_id": credentials.access_key,
            "aws_secret_access_key": credentials.secret_key,
            "aws_session_token": credentials.token,
        }
    elif path.startswith("efs://"):
        fs_path = path.split("://")[1]
        _state.storage_options = {"path": fs_path}
    else:
        _state.storage_options = {}

    yield timedelta(), _state.storage_options


if __name__ == "__main__":

    @graph
    def g():
        import tempfile
        tempfile.gettempdir()

        register_adaptor(tempfile.tempdir + "/delta_test", delta_write_adaptor_raw_impl)
        register_adaptor(tempfile.tempdir + "/delta_test", delta_read_adaptor_raw_impl)
        register_adaptor(tempfile.tempdir + "/delta_test", delta_query_adaptor_raw_impl)

        @dataclass
        class ABStruct(CompoundScalar):
            a: int
            b: int

        @generator
        def gen_df() -> TS[Frame[ABStruct]]:
            yield timedelta(), pl.DataFrame({"a": [1, 2, 3, 3], "b": [4, 5, 6, 7]})

        written = delta_write_adaptor_raw(
            path=tempfile.tempdir + "/delta_test",
            table="test_table",
            data=gen_df(),
            write_mode=DeltaWriteMode.OVERWRITE,
            schema_mode=DeltaSchemaMode.MERGE,
            keys=nothing(TS[tuple[str, ...]]),
            partition=nothing(TS[tuple[str, ...]]),
        )

        res = delta_read_adaptor_raw(
            path=tempfile.tempdir + "/delta_test",
            table=sample(if_true(written.status == StreamStatus.OK), "test_table"),
            filters=const((("a", ">", 2),), TS[tuple[tuple[str, str, str], ...]]),
        )

        q = delta_query_adaptor_raw(
            path=tempfile.tempdir + "/delta_test",
            tables=sample(if_true(written.status == StreamStatus.OK), const({"test_table"}, TS[set[str]])),
            query=const("SELECT a, last_value(b*2) as b FROM test_table WHERE a > 1 GROUP BY a", TS[str]),
        )

        debug_print("written", written)
        debug_print("result", res)
        debug_print("query", q)
        stop_engine(if_true(valid(written) & valid(res) & valid(q)))

    run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=100), __trace__=False)
