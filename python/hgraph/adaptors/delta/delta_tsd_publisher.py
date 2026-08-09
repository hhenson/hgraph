import logging
from dataclasses import make_dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache

import pyarrow as pa

from hgraph import (
    AUTO_RESOLVE, SCHEMA, STATE, TABLE, CompoundScalar, Frame, TS, TSD,
    compute_node, map_, rekey, schedule, sink_node, str_, table_schema, to_table,
)
from hgraph._types import _TsExpr
from hgraph._wiring import _unwrap
from hgraph.adaptors.data_catalogue import DataEnvironment
from hgraph.stream import StreamStatus

from .delta_adaptor_raw import (
    DeltaSchemaMode, DeltaWriteMode, delta_write_adaptor_raw,
)

__all__ = ("publish_tsd_to_delta_table", "tsd_to_frame_batched")

logger = logging.getLogger(__name__)

DEFAULT_DELTA_PUBLISH_BATCH_SIZE = 100_000
DEFAULT_DELTA_PUBLISH_FLUSH_PERIOD = timedelta(minutes=5)


@lru_cache(None)
def _frame_schema(key_type, names, types):
    fields = [
        ("__date__", date),
        ("__timestamp__", datetime),
        ("__is_deleted__", bool),
        ("key", key_type),
        *zip(names, types),
    ]
    suffix = abs(hash((key_type, names, types)))
    return make_dataclass(
        f"DeltaTsdFrame_{suffix}", fields,
        bases=(CompoundScalar,), frozen=True,
        namespace={"__module__": __name__},
    )


@compute_node(valid=("table", "max_rows"))
def _batch_table(
    table: TS[TABLE], max_rows: TS[int], flush: TS[bool], _state: STATE = None,
) -> TS[TABLE]:
    if table.modified:
        _state.table += table.value
        publish = len(_state.table) >= max_rows.value
    else:
        publish = False
    if flush.modified and _state.table:
        publish = True
    elif max_rows.modified and len(_state.table) >= max_rows.value:
        publish = True
    if publish:
        result, _state.table = _state.table, ()
        return result


@_batch_table.start
def _batch_table_start(_state: STATE = None):
    _state.table = ()


@compute_node
def _table_to_frame(
    table: TS[TABLE], schema_names: tuple[str, ...],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TS[Frame[SCHEMA]]:
    try:
        rows = []
        for row in table.value:
            timestamp = row[0]
            rows.append({
                "__date__": timestamp.date(),
                "__timestamp__": timestamp,
                "__is_deleted__": row[2],
                "key": row[3],
                **dict(zip(schema_names, row[4:])),
            })
        return pa.Table.from_pylist(rows)
    except Exception:
        logger.exception("Failed to create Delta frame from TABLE for %s", _schema)
        raise


def _tsd_to_frame_batched(
    tsd: TSD,
    max_rows: TS[int] = DEFAULT_DELTA_PUBLISH_BATCH_SIZE,
    flush_period: TS[timedelta] = DEFAULT_DELTA_PUBLISH_FLUSH_PERIOD,
):
    """Convert native TSD table deltas to reference-compatible Delta batches."""
    ts_type = _TsExpr(_unwrap(tsd).ts_type, repr(_unwrap(tsd).ts_type))
    schema = table_schema(ts_type).value
    if len(schema.partition_keys) > 1:
        scalar_keys = map_(str_, __keys__=tsd.key_set, __key_arg__="ts")
        return _tsd_to_frame_batched(
            rekey(tsd, scalar_keys), max_rows=max_rows,
            flush_period=flush_period)

    key_type = schema.types[3]
    names = tuple(schema.keys[4:])
    frame_schema = _frame_schema(key_type, names, tuple(schema.types[4:]))
    batched = _batch_table(
        to_table(tsd), max_rows,
        schedule(flush_period, initial_delay=True),
    )
    return (_table_to_frame[SCHEMA:frame_schema](
        batched, schema_names=names), frame_schema)


def tsd_to_frame_batched(
    tsd: TSD,
    max_rows: TS[int] = DEFAULT_DELTA_PUBLISH_BATCH_SIZE,
    flush_period: TS[timedelta] = DEFAULT_DELTA_PUBLISH_FLUSH_PERIOD,
):
    return _tsd_to_frame_batched(
        tsd, max_rows=max_rows, flush_period=flush_period)[0]


def publish_tsd_to_delta_table(
    table_name: str,
    tsd: TSD,
    max_rows: TS[int] = DEFAULT_DELTA_PUBLISH_BATCH_SIZE,
    flush_period: TS[timedelta] = DEFAULT_DELTA_PUBLISH_FLUSH_PERIOD,
):
    frame, frame_schema = _tsd_to_frame_batched(
        tsd, max_rows=max_rows, flush_period=flush_period)
    cache_location = DataEnvironment.current().get_entry(
        "table_history_path").environment_path
    written = delta_write_adaptor_raw[SCHEMA:frame_schema](
        path=cache_location,
        table=table_name.replace(" ", "_"),
        data=frame,
        write_mode=DeltaWriteMode.APPEND,
        keys=(),
        partition=("__date__",),
        schema_mode=DeltaSchemaMode.MERGE,
    )

    @sink_node
    def _log_error(status: TS[StreamStatus], message: TS[str]):
        if status.value is not StreamStatus.OK:
            logger.error("Delta table write error for %s: %s", table_name, message.value)

    _log_error(written.status, written.status_msg)
    return written
