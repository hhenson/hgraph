import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta, datetime, date
from typing import Type, Generic

import polars as pl

from hgraph.adaptors.delta.delta_adaptor_raw import delta_write_adaptor_raw, DeltaWriteMode, DeltaSchemaMode
from hgraph.adaptors.data_catalogue.catalogue import DataEnvironment
from hgraph import TS, TSD, graph, to_table, Frame, TABLE, SCHEMA, AUTO_RESOLVE, compute_node, STATE, schedule, Base, \
    COMPOUND_SCALAR, SCALAR, operator, TIME_SERIES_TYPE, TSB, TS_SCHEMA, map_, convert, nothing, log_, if_, rekey, str_, \
    LOGGER
from hgraph._impl._operators._to_table_dispatch_impl import extract_table_schema_raw_type
from hgraph.stream.stream import StreamStatus


__all__ = ['publish_tsd_to_delta_table']


@dataclass(frozen=True)
class TimeExtended(Base[COMPOUND_SCALAR], Generic[COMPOUND_SCALAR]):
    __date__: date              # Broken out from the timestamp to allow us to partition by date
    __timestamp__: datetime
    __is_deleted__: bool
    key: SCALAR


DEFAULT_DELTA_PUBLISH_BATCH_SIZE = 100_000
DEFAULT_DELTA_PUBLISH_FLUSH_PERIOD = timedelta(minutes=5)


@operator
def publish_tsd_to_delta_table(
        table_name: str,
        tsd: TSD[SCALAR, TIME_SERIES_TYPE],
        max_rows: TS[int] = DEFAULT_DELTA_PUBLISH_BATCH_SIZE,
        flush_period: TS[timedelta] = DEFAULT_DELTA_PUBLISH_FLUSH_PERIOD):
    ...


@graph(overloads=publish_tsd_to_delta_table)
def publish_tsd_to_delta_table_compound_key(
        table_name: str,
        tsd: TSD[COMPOUND_SCALAR, TIME_SERIES_TYPE],
        max_rows: TS[int] = DEFAULT_DELTA_PUBLISH_BATCH_SIZE,
        flush_period: TS[timedelta] = DEFAULT_DELTA_PUBLISH_FLUSH_PERIOD):
    # For compound keys, convert to string keys before publishing.  This includes items such as Trayport ids
    # which have multiple components to the key.  The repr() of the key should be something intelligible
    scalar_keys = map_(str_, __keys__=tsd.key_set, __key_arg__="ts")
    tsd_scalar = rekey(tsd, scalar_keys)
    publish_tsd_to_delta_table(table_name, tsd_scalar, max_rows, flush_period)


@graph(overloads=publish_tsd_to_delta_table)
def publish_tsd_to_delta_table_scalar_key(
        table_name: str,
        tsd: TSD[SCALAR, TIME_SERIES_TYPE],
        max_rows: TS[int] = DEFAULT_DELTA_PUBLISH_BATCH_SIZE,
        flush_period: TS[timedelta] = DEFAULT_DELTA_PUBLISH_FLUSH_PERIOD):
    frame = tsd_to_frame_batched(tsd, max_rows=max_rows, flush_period=flush_period)
    cache_location = DataEnvironment.current().get_entry("table_history_path").environment_path

    written = delta_write_adaptor_raw(
        path=cache_location,
        table=table_name.replace(" ", "_"),
        data=frame,
        write_mode=DeltaWriteMode.APPEND,
        keys=nothing(TS[tuple[str, ...]]),
        partition=("__date__",),
        schema_mode=DeltaSchemaMode.MERGE)

    log_("Delta table write error for " + table_name + ": {}",
         if_(written.status != StreamStatus.OK, written.status_msg).true,
         level=logging.ERROR)


@operator
def tsd_to_frame_batched(tsd: TSD[SCALAR, TIME_SERIES_TYPE],
                         max_rows: TS[int],
                         flush_period: TS[timedelta],
                         schema_type: Type[SCHEMA] = AUTO_RESOLVE) -> TS[Frame[TimeExtended[SCHEMA]]]:
    ...


@graph(overloads=tsd_to_frame_batched)
def tsd_to_frame_batched_ts(tsd: TSD[SCALAR, TS[SCHEMA]],
                            max_rows: TS[int],
                            flush_period: TS[timedelta],
                            schema_type: Type[SCHEMA] = AUTO_RESOLVE) -> TS[Frame[TimeExtended[SCHEMA]]]:
    table = to_table(tsd)
    flush = schedule(flush_period, initial_delay=True)
    batched_table = batch_table(table, max_rows=max_rows, flush=flush)
    return table_from_simple_tsd_to_frame(batched_table, extract_table_schema_raw_type(schema_type).keys, schema_type)


@graph(overloads=tsd_to_frame_batched, resolvers={SCHEMA: lambda m: m[TS_SCHEMA].py_type.scalar_type()})
def tsd_to_frame_batched_tsb(tsd: TSD[SCALAR, TSB[TS_SCHEMA]],
                             max_rows: TS[int],
                             flush_period: TS[timedelta],
                             schema_type: Type[SCHEMA] = AUTO_RESOLVE) -> TS[Frame[TimeExtended[SCHEMA]]]:
    tsd_scalar = map_(lambda tsb, t: convert[TS[t]](tsb, __strict__=False), tsd, schema_type)
    return tsd_to_frame_batched(tsd_scalar, max_rows, flush_period)


@compute_node(valid=("table", "max_rows"))
def batch_table(table: TS[TABLE], max_rows: TS[int], flush: TS[bool], state: STATE = None) -> TS[TABLE]:
    if table.modified:
        state.table += table.value
        publish = len(state.table) >= max_rows.value
    else:
        publish = False
    if flush.modified and len(state.table) > 0:
        publish = True
    elif max_rows.modified and len(state.table) >= max_rows.value:
        publish = True
    if publish:
        try:
            return state.table
        finally:
            state.table = ()


@batch_table.start
def _(state: STATE = None):
    state.table = ()


@compute_node
def table_from_simple_tsd_to_frame(
    table: TS[TABLE],
    schema_names: tuple[str, ...],
    schema_tp: type[SCHEMA],
    _log: LOGGER = None,
) -> TS[Frame[TimeExtended[SCHEMA]]]:

    try:
        columns = defaultdict(list)
        for row in table.value:
            last_modified = row[0]
            columns["__date__"].append(last_modified.date())
            columns["__timestamp__"].append(last_modified)
            # Not publishing as-of from row[1] at the moment
            columns["__is_deleted__"].append(row[2])
            columns["key"].append(row[3])
            for name, value in zip(schema_names, row[4:]):
                columns[name].append(value)
        return pl.DataFrame(columns)
    except Exception:
        _log.exception(f"Failed to create dataframe from TABLE - schema type {schema_tp}")
