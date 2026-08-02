from collections import OrderedDict
from datetime import date, datetime, time, timedelta

import pyarrow as pa

from hgraph import (
    Array,
    EvaluationEngineApi,
    Frame,
    Size,
    TS,
    TSB,
    TSD,
    TSL,
    compound_scalar,
    convert,
    generator,
    operator_function,
    ts_schema,
)

from ._data_frame_source import DATA_FRAME_SOURCE, DataStore, _as_arrow_table

__all__ = (
    "schema_from_frame",
    "tsb_from_data_source",
    "tsd_k_v_from_data_source",
    "tsd_k_b_from_data_source",
    "tsd_k_tsd_from_data_source",
    "ts_of_array_from_data_source",
    "tsd_k_a_from_data_source",
    "tsl_from_data_source",
    "ts_of_matrix_from_data_source",
)

# hgraph parity: the second Array size variable used in matrix-shaped
# signatures (upstream clones it from SIZE in this module).
import typing as _typing

SIZE_1 = _typing.TypeVar("SIZE_1")

_from_data_frame_batches = operator_function("from_data_frame_batches")


def _python_type(arrow_type: pa.DataType):
    if pa.types.is_boolean(arrow_type):
        return bool
    if pa.types.is_integer(arrow_type):
        return int
    if pa.types.is_floating(arrow_type):
        return float
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return str
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return bytes
    if pa.types.is_date(arrow_type):
        return date
    if pa.types.is_timestamp(arrow_type):
        return datetime
    if pa.types.is_time(arrow_type):
        return time
    if pa.types.is_duration(arrow_type):
        return timedelta
    if pa.types.is_dictionary(arrow_type):
        return _python_type(arrow_type.value_type)
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return tuple[_python_type(arrow_type.value_type), ...]
    raise TypeError(f"unsupported Arrow datatype {arrow_type}")


def schema_from_frame(frame) -> type:
    table = _as_arrow_table(frame)
    return compound_scalar(
        **{field.name: _python_type(field.type) for field in table.schema}
    )


def _source(source_type: type[DATA_FRAME_SOURCE]):
    return DataStore.instance().get_data_source(source_type)


def _schema_table(source_type: type[DATA_FRAME_SOURCE]) -> pa.Table:
    return pa.Table.from_batches([], schema=_source(source_type).schema)


def _stream_batches(
    source_type: type[DATA_FRAME_SOURCE],
    dt_col: str,
    offset: timedelta,
    transform=None,
):
    source = _source(source_type)

    @generator
    def batches(_api: EvaluationEngineApi = None) -> TS[Frame]:
        pending = None
        for value in source.iter_frames(_api.start_time, _api.end_time):
            frame = _normalise_dt(_as_arrow_table(value), dt_col)
            if frame.num_rows == 0:
                continue

            if pending is not None:
                pending_last = pending.column(dt_col)[-1].as_py()
                frame_first = frame.column(dt_col)[0].as_py()
                if frame_first < pending_last:
                    raise ValueError(
                        f"dataframe source batches must be ordered by {dt_col!r}"
                    )
                if frame_first == pending_last:
                    pending = pa.concat_tables(
                        (pending, frame), promote_options="default"
                    )
                    continue
                ready = transform(pending) if transform is not None else pending
                when = ready.column(dt_col)[0].as_py() + offset
                if when < _api.start_time:
                    when = _api.start_time
                yield when, ready
            pending = frame

        if pending is not None:
            ready = transform(pending) if transform is not None else pending
            when = ready.column(dt_col)[0].as_py() + offset
            if when < _api.start_time:
                when = _api.start_time
            yield when, ready

    return batches()


def _normalise_dt(table: pa.Table, dt_col: str) -> pa.Table:
    field = table.schema.field(dt_col)
    if pa.types.is_date(field.type):
        values = [
            datetime.combine(value, time()) if value is not None else None
            for value in table.column(dt_col).to_pylist()
        ]
        table = table.set_column(
            table.schema.get_field_index(dt_col), dt_col, pa.array(values)
        )
    elif not pa.types.is_timestamp(field.type):
        raise TypeError(
            f"datetime column {dt_col!r} must be an Arrow date or timestamp, got {field.type}"
        )
    values = table.column(dt_col).to_pylist()
    if any(left > right for left, right in zip(values, values[1:])):
        raise ValueError(f"dataframe source must be ordered by {dt_col!r}")
    return table


def _schema_without(table: pa.Table, *names: str):
    excluded = set(names)
    return OrderedDict(
        (field.name, _python_type(field.type))
        for field in table.schema
        if field.name not in excluded
    )


def ts_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str = "date",
    value_col: str = "value",
    offset: timedelta = timedelta(),
):
    schema = _source(dfs).schema
    return _from_data_frame_batches[
        TS[_python_type(schema.field(value_col).type)]
    ](
        _stream_batches(dfs, dt_col, offset), dt_col=dt_col,
        value_col=value_col, offset=offset
    )


def tsb_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    offset: timedelta = timedelta(),
):
    frame = _schema_table(dfs)
    schema = ts_schema(**{name: TS[tp] for name, tp in _schema_without(frame, dt_col).items()})
    return _from_data_frame_batches[TSB[schema]](
        _stream_batches(dfs, dt_col, offset), dt_col=dt_col, offset=offset
    )


def tsd_k_v_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    key_col: str,
    offset: timedelta = timedelta(),
):
    frame = _schema_table(dfs)
    values = _schema_without(frame, dt_col, key_col)
    if len(values) != 1:
        raise TypeError("tsd_k_v_from_data_source requires exactly one value column")
    key_type = _python_type(frame.schema.field(key_col).type)
    value_col, value_type = next(iter(values.items()))
    return _from_data_frame_batches[TSD[key_type, TS[value_type]]](
        _stream_batches(dfs, dt_col, offset),
        dt_col=dt_col,
        key_col=key_col,
        value_col=value_col,
        offset=offset,
    )


def tsd_k_b_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    key_col: str,
    offset: timedelta = timedelta(),
):
    frame = _schema_table(dfs)
    key_type = _python_type(frame.schema.field(key_col).type)
    schema = ts_schema(
        **{
            name: TS[tp]
            for name, tp in _schema_without(frame, dt_col, key_col).items()
        }
    )
    return _from_data_frame_batches[TSD[key_type, TSB[schema]]](
        _stream_batches(dfs, dt_col, offset), dt_col=dt_col,
        key_col=key_col, offset=offset
    )


def _rows_by_time(frame: pa.Table, dt_col: str):
    groups = OrderedDict()
    for row in frame.to_pylist():
        groups.setdefault(row[dt_col], []).append(row)
    return groups


def _source_row_groups(source, dt_col, start_time, end_time):
    """Yield globally ordered timestamp groups while retaining one group.

    A timestamp may straddle adjacent provider batches. Keeping that boundary
    group avoids duplicate generator events at one engine time without eager
    concatenation of the source.
    """
    pending_when = None
    pending_rows = []
    for value in source.iter_frames(start_time, end_time):
        frame = _normalise_dt(_as_arrow_table(value), dt_col)
        for row in frame.to_pylist():
            when = row[dt_col]
            if pending_when is not None and when < pending_when:
                raise ValueError(
                    f"dataframe source batches must be ordered by {dt_col!r}"
                )
            if pending_when is not None and when != pending_when:
                yield pending_when, pending_rows
                pending_rows = []
            pending_when = when
            pending_rows.append(row)
    if pending_when is not None:
        yield pending_when, pending_rows


def _value_frame(frame: pa.Table, dt_col: str, values, value_type):
    return pa.table(
        {
            dt_col: pa.array([when for when, _ in values]),
            "value": pa.array([value for _, value in values], type=value_type),
        }
    )


def ts_of_array_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    offset: timedelta = timedelta(),
):
    return _ts_of_array_from_source(dfs, dt_col, offset)


def _ts_of_array_from_source(dfs, dt_col: str, offset: timedelta):
    frame = _schema_table(dfs)
    columns = _schema_without(frame, dt_col)
    types = set(columns.values())
    if len(types) != 1:
        raise TypeError("array source columns must have one scalar type")
    value_type = next(iter(types))

    def pack(batch):
        values = [
            (row[dt_col], tuple(row[name] for name in columns))
            for row in batch.to_pylist()
        ]
        return _value_frame(
            batch, dt_col, values,
            pa.list_(batch.schema.field(next(iter(columns))).type),
        )

    return _from_data_frame_batches[
        TS[Array[value_type, Size[len(columns)]]]
    ](
        _stream_batches(dfs, dt_col, offset, pack), dt_col=dt_col,
        value_col="value", offset=offset
    )


def tsl_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    offset: timedelta = timedelta(),
):
    frame = _schema_table(dfs)
    columns = _schema_without(frame, dt_col)
    value_type = next(iter(columns.values()))
    series = _ts_of_array_from_source(dfs, dt_col, offset)
    return convert[TSL[TS[value_type], Size[len(columns)]]](series)


def tsd_k_a_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    key_col: str,
    offset: timedelta = timedelta(),
):
    frame = _schema_table(dfs)
    columns = _schema_without(frame, dt_col, key_col)
    types = set(columns.values())
    if len(types) != 1:
        raise TypeError("array source columns must have one scalar type")
    value_type = next(iter(types))

    def pack(batch):
        return pa.Table.from_pylist(
            [
                {
                    dt_col: row[dt_col],
                    key_col: row[key_col],
                    "value": tuple(row[name] for name in columns),
                }
                for row in batch.to_pylist()
            ]
        )

    key_type = _python_type(frame.schema.field(key_col).type)
    return _from_data_frame_batches[
        TSD[key_type, TS[Array[value_type, Size[len(columns)]]]]
    ](
        _stream_batches(dfs, dt_col, offset, pack),
        dt_col=dt_col,
        key_col=key_col,
        value_col="value",
        offset=offset,
    )


def ts_of_matrix_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    offset: timedelta = timedelta(),
):
    frame = _schema_table(dfs)
    columns = _schema_without(frame, dt_col)
    types = set(columns.values())
    if len(types) != 1:
        raise TypeError("matrix source columns must have one scalar type")
    value_type = next(iter(types))

    def pack(batch):
        values = [
            (
                when,
                tuple(tuple(row[name] for name in columns) for row in rows),
            )
            for when, rows in _rows_by_time(batch, dt_col).items()
        ]
        return pa.Table.from_pylist(
            [{dt_col: when, "value": value} for when, value in values]
        )

    return _from_data_frame_batches[
        TS[Array[value_type, Size[-1], Size[len(columns)]]]
    ](
        _stream_batches(dfs, dt_col, offset, pack), dt_col=dt_col,
        value_col="value", offset=offset
    )


def ts_of_frames_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    offset: timedelta = timedelta(),
    remove_dt_col: bool = True,
):
    source_instance = _source(dfs)
    schema = source_instance.schema
    names = [name for name in schema.names if not remove_dt_col or name != dt_col]
    row_schema = compound_scalar(
        **{name: _python_type(schema.field(name).type) for name in names}
    )

    # Arrow cannot nest a Table in a cell. A Python generator is retained for
    # this one batching adaptor; the produced Frame values and graph runtime
    # remain native Arrow/C++ values.
    @generator
    def source(_api: EvaluationEngineApi = None) -> TS[Frame[row_schema]]:
        for when, rows in _source_row_groups(
            source_instance, dt_col, _api.start_time, _api.end_time
        ):
            scheduled = when + offset
            if scheduled < _api.start_time:
                continue
            yield scheduled, pa.Table.from_pylist(
                [{name: row[name] for name in names} for row in rows],
                schema=pa.schema([schema.field(name) for name in names]),
            )

    return source()


def tsd_k_tsd_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    key_col: str,
    pivot_col: str,
    offset: timedelta = timedelta(),
):
    source_instance = _source(dfs)
    frame = pa.Table.from_batches([], schema=source_instance.schema)
    values = _schema_without(frame, dt_col, key_col, pivot_col)
    if len(values) != 1:
        raise TypeError("pivot sources require exactly one value column")
    value_col, value_type = next(iter(values.items()))
    key_type = _python_type(frame.schema.field(key_col).type)
    pivot_type = _python_type(frame.schema.field(pivot_col).type)
    # yield the nested dict as a TSD DELTA (additive per tick — absent keys
    # keep their state, matching upstream's pivot semantics); routing through
    # convert would state-sync and emit REMOVEs for absent keys.
    @generator
    def source(
        _api: EvaluationEngineApi = None,
    ) -> TSD[key_type, TSD[pivot_type, TS[value_type]]]:
        for when, rows in _source_row_groups(
            source_instance, dt_col, _api.start_time, _api.end_time
        ):
            scheduled = when + offset
            if scheduled < _api.start_time:
                continue
            nested = OrderedDict()
            for row in rows:
                nested.setdefault(row[key_col], {})[row[pivot_col]] = row[value_col]
            yield scheduled, dict(nested)

    return source()
