from collections import OrderedDict
from datetime import date, datetime, time, timedelta

import pyarrow as pa

from hgraph import (
    Frame,
    Size,
    TS,
    TSB,
    TSD,
    TSL,
    compound_scalar,
    convert,
    from_data_frame,
    graph,
    map_,
    ts_schema,
)

from ._data_frame_source import DATA_FRAME_SOURCE, DataStore, _as_arrow_table

__all__ = (
    "SIZE_1",
    "schema_from_frame",
    "ts_from_data_source",
    "tsb_from_data_source",
    "tsd_k_v_from_data_source",
    "tsd_k_b_from_data_source",
    "tsd_k_tsd_from_data_source",
    "ts_of_array_from_data_source",
    "tsd_k_a_from_data_source",
    "tsl_from_data_source",
    "ts_of_matrix_from_data_source",
    "ts_of_frames_from_data_source",
)

# hgraph parity: the second Array size variable used in matrix-shaped
# signatures (upstream clones it from SIZE in this module).
import typing as _typing

SIZE_1 = _typing.TypeVar("SIZE_1")


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


def _frames(source_type: type[DATA_FRAME_SOURCE]) -> pa.Table:
    frames = [_as_arrow_table(frame) for frame in _source(source_type).iter_frames()]
    if not frames:
        return pa.table({})
    if len(frames) == 1:
        return frames[0]
    return pa.concat_tables(frames, promote_options="default")


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
    frame = _normalise_dt(_frames(dfs), dt_col)
    return from_data_frame[TS[_python_type(frame.schema.field(value_col).type)]](
        frame, dt_col=dt_col, value_col=value_col, offset=offset
    )


def tsb_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    offset: timedelta = timedelta(),
):
    frame = _normalise_dt(_frames(dfs), dt_col)
    schema = ts_schema(**{name: TS[tp] for name, tp in _schema_without(frame, dt_col).items()})
    return from_data_frame[TSB[schema]](frame, dt_col=dt_col, offset=offset)


def tsd_k_v_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    key_col: str,
    offset: timedelta = timedelta(),
):
    frame = _normalise_dt(_frames(dfs), dt_col)
    values = _schema_without(frame, dt_col, key_col)
    if len(values) != 1:
        raise TypeError("tsd_k_v_from_data_source requires exactly one value column")
    key_type = _python_type(frame.schema.field(key_col).type)
    value_col, value_type = next(iter(values.items()))
    return from_data_frame[TSD[key_type, TS[value_type]]](
        frame,
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
    frame = _normalise_dt(_frames(dfs), dt_col)
    key_type = _python_type(frame.schema.field(key_col).type)
    schema = ts_schema(
        **{
            name: TS[tp]
            for name, tp in _schema_without(frame, dt_col, key_col).items()
        }
    )
    return from_data_frame[TSD[key_type, TSB[schema]]](
        frame, dt_col=dt_col, key_col=key_col, offset=offset
    )


def _rows_by_time(frame: pa.Table, dt_col: str):
    groups = OrderedDict()
    for row in frame.to_pylist():
        groups.setdefault(row[dt_col], []).append(row)
    return groups


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
    frame = _normalise_dt(_frames(dfs), dt_col)
    columns = _schema_without(frame, dt_col)
    types = set(columns.values())
    if len(types) != 1:
        raise TypeError("array source columns must have one scalar type")
    value_type = next(iter(types))
    values = [
        (row[dt_col], tuple(row[name] for name in columns))
        for row in frame.to_pylist()
    ]
    packed = _value_frame(
        frame, dt_col, values, pa.list_(frame.schema.field(next(iter(columns))).type)
    )
    return from_data_frame[TS[tuple[value_type, ...]]](
        packed, dt_col=dt_col, value_col="value", offset=offset
    )


def tsl_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    offset: timedelta = timedelta(),
):
    frame = _normalise_dt(_frames(dfs), dt_col)
    columns = _schema_without(frame, dt_col)
    value_type = next(iter(columns.values()))
    series = ts_of_array_from_data_source(dfs, dt_col, offset)
    return convert[TSL[TS[value_type], Size[len(columns)]]](series)


def tsd_k_a_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    key_col: str,
    offset: timedelta = timedelta(),
):
    frame = _normalise_dt(_frames(dfs), dt_col)
    columns = _schema_without(frame, dt_col, key_col)
    types = set(columns.values())
    if len(types) != 1:
        raise TypeError("array source columns must have one scalar type")
    value_type = next(iter(types))
    rows = [
        {
            dt_col: row[dt_col],
            key_col: row[key_col],
            "value": tuple(row[name] for name in columns),
        }
        for row in frame.to_pylist()
    ]
    packed = pa.Table.from_pylist(rows)
    key_type = _python_type(frame.schema.field(key_col).type)
    return from_data_frame[TSD[key_type, TS[tuple[value_type, ...]]]](
        packed,
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
    frame = _normalise_dt(_frames(dfs), dt_col)
    columns = _schema_without(frame, dt_col)
    types = set(columns.values())
    if len(types) != 1:
        raise TypeError("matrix source columns must have one scalar type")
    value_type = next(iter(types))
    values = [
        (
            when,
            tuple(tuple(row[name] for name in columns) for row in rows),
        )
        for when, rows in _rows_by_time(frame, dt_col).items()
    ]
    packed = pa.Table.from_pylist(
        [{dt_col: when, "value": value} for when, value in values]
    )
    return from_data_frame[TS[tuple[tuple[value_type, ...], ...]]](
        packed, dt_col=dt_col, value_col="value", offset=offset
    )


def ts_of_frames_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    offset: timedelta = timedelta(),
    remove_dt_col: bool = True,
):
    frame = _normalise_dt(_frames(dfs), dt_col)
    names = [name for name in frame.column_names if not remove_dt_col or name != dt_col]
    row_schema = compound_scalar(
        **{name: _python_type(frame.schema.field(name).type) for name in names}
    )
    values = []
    for when, rows in _rows_by_time(frame, dt_col).items():
        values.append(
            {
                dt_col: when,
                "value": pa.Table.from_pylist(
                    [{name: row[name] for name in names} for row in rows]
                ),
            }
        )

    # Arrow cannot nest a Table in a cell. A Python generator is retained for
    # this one batching adaptor; the produced Frame values and graph runtime
    # remain native Arrow/C++ values.
    from hgraph import generator

    @generator
    def source() -> TS[Frame[row_schema]]:
        for value in values:
            yield value[dt_col] + offset, value["value"]

    return source()


def tsd_k_tsd_from_data_source(
    dfs: type[DATA_FRAME_SOURCE],
    dt_col: str,
    key_col: str,
    pivot_col: str,
    offset: timedelta = timedelta(),
):
    frame = _normalise_dt(_frames(dfs), dt_col)
    values = _schema_without(frame, dt_col, key_col, pivot_col)
    if len(values) != 1:
        raise TypeError("pivot sources require exactly one value column")
    value_col, value_type = next(iter(values.items()))
    key_type = _python_type(frame.schema.field(key_col).type)
    pivot_type = _python_type(frame.schema.field(pivot_col).type)
    packed_rows = []
    for when, rows in _rows_by_time(frame, dt_col).items():
        nested = OrderedDict()
        for row in rows:
            nested.setdefault(row[key_col], {})[row[pivot_col]] = row[value_col]
        packed_rows.append({dt_col: when, "value": dict(nested)})
    from hgraph import generator

    # yield the nested dict as a TSD DELTA (additive per tick — absent keys
    # keep their state, matching upstream's pivot semantics); routing through
    # convert would state-sync and emit REMOVEs for absent keys.
    @generator
    def source() -> TSD[key_type, TSD[pivot_type, TS[value_type]]]:
        for row in packed_rows:
            yield row[dt_col] + offset, row["value"]

    return source()
