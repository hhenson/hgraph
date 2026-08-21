import logging
from concurrent.futures import Executor
from datetime import datetime, timedelta
from enum import Enum

import pyarrow as pa

from hgraph import (
    AUTO_RESOLVE, MIN_DT, SCHEMA, STATE, Frame, TS, TSB, TSD,
    generator, graph, map_, push_queue, schedule, service_adaptor,
    service_adaptor_impl, sink_node,
)
from hgraph.adaptors.executor import adaptor_executor
from hgraph.stream import Data, Stream, StreamStatus

logger = logging.getLogger(__name__)

__all__ = (
    "DeltaWriteMode", "DeltaSchemaMode",
    "delta_read_adaptor_raw", "delta_read_adaptor_raw_impl",
    "delta_query_adaptor_raw", "delta_query_adaptor_raw_impl",
    "delta_write_adaptor_raw", "delta_write_adaptor_raw_impl",
    "delta_table_maintenance", "delta_storage_options",
)

_RAW_FRAME_STREAM = TSB[Stream[Data[Frame]]]
_TIME_STREAM = TSB[Stream[Data[datetime]]]


def _require_delta_lake():
    try:
        from deltalake import DeltaTable, QueryBuilder, write_deltalake
    except ModuleNotFoundError as error:
        raise RuntimeError("Delta adaptors require the 'delta' extra") from error
    return DeltaTable, QueryBuilder, write_deltalake


def _require_polars():
    try:
        import polars
    except ModuleNotFoundError as error:
        raise RuntimeError("Delta adaptors require the 'delta' extra") from error
    return polars


def _require_boto3():
    try:
        import boto3
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Delta S3 storage requires the 'delta' extra"
        ) from error
    return boto3


class DeltaWriteMode(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    ERROR = "error"
    IGNORE = "ignore"


class DeltaSchemaMode(Enum):
    OVERWRITE = "overwrite"
    MERGE = "merge"


def _base_path(path):
    # Table locations are URI-style (s3://…, memory, plain directories);
    # deltalake accepts "/" on every platform while "\" corrupts URIs.
    return path if path.endswith("/") else path + "/"


def _credentials(credentials, path):
    result = dict(credentials) if credentials else {}
    return result.pop("path", path), result


def _submit(executor, state, operation, *args):
    previous = getattr(state, "future", None)
    state.future = executor.submit(operation, *args, previous)


@service_adaptor
def delta_read_adaptor_raw(
    path: str, table: TS[str], columns: TS[tuple[str, ...]] = (),
    filters: TS[tuple[tuple[str, str, object], ...]] = (),
    sort: TS[tuple[tuple[str, bool], ...]] = (),
) -> _RAW_FRAME_STREAM:
    ...


@service_adaptor_impl(interfaces=delta_read_adaptor_raw)
def delta_read_adaptor_raw_impl(
    path: str, table: TSD[int, TS[str]],
    columns: TSD[int, TS[tuple[str, ...]]] = None,
    filters: TSD[int, TS[tuple[tuple[str, str, object], ...]]] = None,
    sort: TSD[int, TS[tuple[tuple[str, bool], ...]]] = None,
) -> TSD[int, _RAW_FRAME_STREAM]:
    path = _base_path(path)
    sender_ref = {}

    @push_queue(TSD[int, _RAW_FRAME_STREAM])
    def delta_to_graph(sender):
        sender_ref["sender"] = sender

    def run_query(credentials, request_id, table_path, columns, filters, sort,
                  sender, previous):
        if previous is not None:
            previous.result()
        try:
            DeltaTable, _, _ = _require_delta_lake()
            logger.info("Will read delta table %s with filters %s", table_path, filters)
            delta_table = DeltaTable(table_path, storage_options=credentials or None)
            result = delta_table.to_pyarrow_table(
                filters=list(filters) if filters else None,
                columns=list(columns) if columns else None)
            if sort:
                result = result.sort_by([
                    (name, "ascending" if ascending else "descending")
                    for name, ascending in sort])
            sender({request_id: {
                "status": StreamStatus.OK, "status_msg": "",
                "values": result, "timestamp": datetime.now(),
            }})
        except Exception as error:
            logger.exception("Error reading delta table %s", table_path)
            sender({request_id: {
                "status": StreamStatus.ERROR, "status_msg": str(error)}})

    @sink_node(valid=("request_id", "table", "filters", "credentials", "executor"))
    def send_query(
        request_id: TS[int], table: TS[str], columns: TS[tuple[str, ...]],
        filters: TS[tuple[tuple[str, str, object], ...]],
        sort: TS[tuple[tuple[str, bool], ...]], credentials: TS[object],
        executor: TS[Executor], _state: STATE = None,
    ):
        base, options = _credentials(
            credentials.value if credentials.valid else {}, path)
        _submit(
            executor.value, _state, run_query, options, request_id.value,
            base + table.value, columns.value, filters.value, sort.value,
            sender_ref["sender"])

    @send_query.stop
    def stop_send_query(request_id: TS[int], _state: STATE = None):
        try:
            sender_ref["sender"]({request_id.value: __import__("hgraph").REMOVE})
        except RuntimeError as error:
            # A mapped child can stop after its push source during whole-graph
            # teardown. The removal is then unobservable, and the push-source
            # sender intentionally reports that it no longer accepts values.
            if str(error) != "Push source is not accepting values":
                raise

    map_(
        lambda key, table, columns, filters, sort, credentials, executor:
            send_query(
                request_id=key, table=table, columns=columns, filters=filters,
                sort=sort, credentials=credentials, executor=executor),
        table=table, columns=columns, filters=filters, sort=sort,
        credentials=delta_storage_options(path), executor=adaptor_executor())
    return delta_to_graph()


@service_adaptor
def delta_query_adaptor_raw(
    path: str, tables: TS[set[str]], query: TS[str],
) -> _RAW_FRAME_STREAM:
    ...


@service_adaptor_impl(interfaces=delta_query_adaptor_raw)
def delta_query_adaptor_raw_impl(
    path: str, tables: TSD[int, TS[set[str]]], query: TSD[int, TS[str]],
) -> TSD[int, _RAW_FRAME_STREAM]:
    path = _base_path(path)
    sender_ref = {}

    @push_queue(TSD[int, _RAW_FRAME_STREAM])
    def delta_to_graph(sender):
        sender_ref["sender"] = sender

    def run_query(credentials, request_id, base, tables, query, sender, previous):
        if previous is not None:
            previous.result()
        try:
            DeltaTable, QueryBuilder, _ = _require_delta_lake()
            builder = QueryBuilder()
            for table in tables:
                builder.register(
                    table, DeltaTable(base + table, storage_options=credentials or None))
            batches = builder.execute(query)
            if hasattr(batches, "__arrow_c_stream__"):
                result = pa.RecordBatchReader.from_stream(batches).read_all()
            else:
                if hasattr(batches, "fetchall"):
                    batches = batches.fetchall()
                result = pa.Table.from_batches(batches) if batches else pa.table({})
            sender({request_id: {
                "status": StreamStatus.OK, "status_msg": "",
                "values": result, "timestamp": datetime.now(),
            }})
        except Exception as error:
            logger.exception("Error querying delta tables %s", tables)
            sender({request_id: {
                "status": StreamStatus.ERROR, "status_msg": str(error)}})

    @sink_node(valid=("request_id", "tables", "query", "credentials", "executor"))
    def send_query(
        request_id: TS[int], tables: TS[set[str]], query: TS[str],
        credentials: TS[object], executor: TS[Executor], _state: STATE = None,
    ):
        base, options = _credentials(
            credentials.value if credentials.valid else {}, path)
        _submit(
            executor.value, _state, run_query, options, request_id.value,
            base, tables.value, query.value, sender_ref["sender"])

    map_(
        lambda key, tables, query, credentials, executor: send_query(
            request_id=key, tables=tables, query=query,
            credentials=credentials, executor=executor),
        tables=tables, query=query, credentials=delta_storage_options(path),
        executor=adaptor_executor())
    return delta_to_graph()


@service_adaptor
def delta_write_adaptor_raw(
    path: str, table: TS[str], data: TS[Frame[SCHEMA]],
    write_mode: TS[DeltaWriteMode], schema_mode: TS[DeltaSchemaMode],
    keys: TS[tuple[str, ...]], partition: TS[tuple[str, ...]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> _TIME_STREAM:
    ...


@service_adaptor_impl(interfaces=delta_write_adaptor_raw)
def delta_write_adaptor_raw_impl(
    path: str, table: TSD[int, TS[str]], data: TSD[int, TS[Frame[SCHEMA]]],
    write_mode: TSD[int, TS[DeltaWriteMode]],
    schema_mode: TSD[int, TS[DeltaSchemaMode]],
    keys: TSD[int, TS[tuple[str, ...]]],
    partition: TSD[int, TS[tuple[str, ...]]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, _TIME_STREAM]:
    path = _base_path(path)
    sender_ref = {}

    @push_queue(TSD[int, _TIME_STREAM])
    def delta_to_graph(sender):
        sender_ref["sender"] = sender

    def run_query(credentials, request_id, table_path, mode, schema_mode,
                  keys, partition, data, timestamp, sender, previous):
        if previous is not None:
            previous.result()
        try:
            pl = _require_polars()
            _, _, write_deltalake = _require_delta_lake()
            frame = pl.from_arrow(data)
            predicate = None
            if keys:
                if len(keys) > 1:
                    frame = frame.with_columns(__index__=pl.concat_str(
                        *[pl.col(key).cast(pl.String) for key in keys], separator=","))
                if mode is DeltaWriteMode.OVERWRITE:
                    values = frame[keys[0]].unique().to_list() if len(keys) == 1 \
                        else frame["__index__"].unique().to_list()
                    column = keys[0] if len(keys) == 1 else "__index__"
                    rendered = ", ".join(
                        f"'{str(value).replace(chr(39), chr(39) * 2)}'"
                        for value in values)
                    predicate = f"{column} in ({rendered})"
            write_deltalake(
                table_path, frame.to_arrow(), mode=mode.value,
                storage_options=credentials or None,
                configuration={
                    "delta.deletedFileRetentionDuration": "interval 1 days",
                    "delta.logRetentionDuration": "interval 2 days"},
                schema_mode=schema_mode.value,
                partition_by=list(partition) if partition else None,
                predicate=predicate)
            sender({request_id: {
                "status": StreamStatus.OK, "status_msg": "",
                "timestamp": timestamp}})
        except Exception as error:
            logger.exception("Error writing delta table %s", table_path)
            sender({request_id: {
                "status": StreamStatus.ERROR, "status_msg": str(error)}})

    @sink_node(valid=("request_id", "table", "data", "mode", "credentials", "executor"))
    def send_query(
        request_id: TS[int], table: TS[str], data: TS[Frame[_schema]],
        mode: TS[DeltaWriteMode], schema_mode: TS[DeltaSchemaMode],
        keys: TS[tuple[str, ...]], partition: TS[tuple[str, ...]],
        credentials: TS[object], executor: TS[Executor], _state: STATE = None,
    ):
        base, options = _credentials(
            credentials.value if credentials.valid else {}, path)
        _submit(
            executor.value, _state, run_query, options, request_id.value,
            base + table.value, mode.value, schema_mode.value, keys.value,
            partition.value, data.value, data.last_modified_time,
            sender_ref["sender"])

    map_(
        send_query, __key_arg__="request_id", table=table, mode=write_mode,
        keys=keys, partition=partition, data=data,
        credentials=delta_storage_options(path), executor=adaptor_executor(),
        schema_mode=schema_mode)
    return delta_to_graph()


@graph
def delta_table_maintenance(
    path: str, table: str, periodic: timedelta, start: datetime = MIN_DT,
):
    path = _base_path(path)
    trigger = schedule(periodic, start=start)

    @sink_node
    def maintenance(
        trigger: TS[bool], executor: TS[Executor], credentials: TS[object],
    ):
        base, options = _credentials(
            credentials.value if credentials.valid else {}, path)

        def run():
            table_path = base + table
            try:
                DeltaTable, _, _ = _require_delta_lake()
                delta_table = DeltaTable(
                    table_path, storage_options=options or None)
                logger.info("Compaction for %s: %s", table_path,
                            delta_table.optimize.compact())
                logger.info("Vacuum for %s: %s", table_path, delta_table.vacuum(
                    retention_hours=1, enforce_retention_duration=False,
                    dry_run=False))
            except Exception:
                logger.exception("Error maintaining delta table %s", table_path)

        if trigger.value:
            executor.value.submit(run)

    maintenance(trigger, adaptor_executor(), delta_storage_options(path))


@generator
def delta_storage_options(path: str, _state: STATE = None) -> TS[object]:
    if path.startswith("s3://"):
        boto3 = _require_boto3()
        credentials = boto3.Session().get_credentials()
        _state.storage_options = {
            "aws_access_key_id": credentials.access_key,
            "aws_secret_access_key": credentials.secret_key,
            "aws_session_token": credentials.token,
        }
    elif path.startswith("efs://"):
        _state.storage_options = {"path": path.split("://")[1]}
    else:
        _state.storage_options = {}
    yield timedelta(), _state.storage_options
