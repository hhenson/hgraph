from datetime import timedelta, date, datetime, time
from typing import Callable, OrderedDict

import numpy as np
import polars as pl

from hgraph import generator, TS_SCHEMA, TSB, TSD, SCALAR, SCALAR_1, TS, ts_schema, Array, \
    SIZE, TSL, clone_typevar, Size, SCALAR_2, HgScalarTypeMetaData, HgTSTypeMetaData
from hgraph.adaptors.data_frame._data_frame_source import DATA_FRAME_SOURCE, DataStore

__all__ = (
    "tsb_from_data_source", "tsd_k_a_from_data_source", "ts_of_matrix_from_data_source", "tsd_k_v_from_data_source",
    "tsd_k_b_from_data_source", "tsd_k_tsd_from_data_source", "ts_of_array_from_data_source", 'tsl_from_data_source'
)


def _schema_and_dt_col(mapping, scalars) -> tuple[
    OrderedDict[str, HgScalarTypeMetaData], tuple[str, HgScalarTypeMetaData]]:
    dfs: type[DATA_FRAME_SOURCE] = mapping[DATA_FRAME_SOURCE].py_type
    dfs_instance = DataStore.instance().get_data_source(dfs)
    schema = dfs_instance.schema
    dt_col = scalars['dt_col']
    return {k: _convert_type(v) for k, v in schema.items() if k != dt_col}, (dt_col, _convert_type(schema[dt_col]))


def _extract_schema(mapping, scalars) -> TS_SCHEMA:
    """Extract the schema from the mapping"""
    schema, _ = _schema_and_dt_col(mapping, scalars)
    return ts_schema(**{k: HgTSTypeMetaData(v) for k, v in schema.items()})


@generator(resolvers={TS_SCHEMA: _extract_schema})
def tsb_from_data_source(
        dfs: type[DATA_FRAME_SOURCE], dt_col: str, offset: timedelta = timedelta()
) -> TSB[TS_SCHEMA]:
    """
    Iterates over the data_frame, returning an instance of TS_SCHEMA for each row in the table.
    null values are not ticked.
    """
    df: pl.DataFrame
    dfs_instance = DataStore.instance().get_data_source(dfs)
    dt_converter = _dt_converter(dfs_instance.schema[dt_col])
    for df in dfs_instance.iter_frames():
        for value in df.iter_rows(named=True):
            dt = dt_converter(value.pop(dt_col))
            yield dt + offset, value


def _extract_tsd_key_scalar(mapping, scalars) -> SCALAR:
    schema, _ = _schema_and_dt_col(mapping, scalars)
    return schema[scalars['key_col']].py_type


def _extract_tsd_key_value_scalar(mapping, scalars) -> SCALAR_1:
    schema, _ = _schema_and_dt_col(mapping, scalars)
    schema.pop(scalars['key_col'])
    assert len(schema) == 1
    return next(iter(schema.values())).py_type


@generator(resolvers={SCALAR: _extract_tsd_key_scalar, SCALAR_1: _extract_tsd_key_value_scalar})
def tsd_k_v_from_data_source(
        dfs: type[DATA_FRAME_SOURCE], dt_col: str, key_col: str, offset: timedelta = timedelta()
) -> TSD[SCALAR, TS[SCALAR_1]]:
    """
    Extract a TSD instance from the data frame source. This will extract the key_col column and will
    set the value to be the remaining column.
    The requirement is that the results are ordered by date at a minimum. It is useful to order by key_col as well.
    The expected shape of the data frame would be:
        +------+---------+-------+
        | date | key_col | value |
        +------+---------+-------+
        |  ...                   |
    """
    df: pl.DataFrame
    dfs_instance = DataStore.instance().get_data_source(dfs)
    schema = tuple(dfs_instance.schema.keys())
    dt_ndx = schema.index(dt_col)
    key_ndx = schema.index(key_col)
    value_ndx = next(iter({0, 1, 2} - {dt_ndx, key_ndx}))
    dt_converter = _dt_converter(dfs_instance.schema[dt_col])
    values: dict[SCALAR, SCALAR_1] = {}
    last_dt: datetime | None = None
    for df in dfs_instance.iter_frames():
        for value in df.iter_rows(named=False):
            dt = dt_converter(value[dt_ndx])
            if last_dt != dt:
                if last_dt is not None:
                    yield last_dt + offset, values
                values = {value[key_ndx]: value[value_ndx]}
                last_dt = dt
            else:
                key = value[key_ndx]
                values[key] = value[value_ndx]
    if last_dt is not None:
        yield last_dt + offset, values


def _extract_tsd_pivot_key_value_scalar(mapping, scalars) -> SCALAR_1:
    schema, _ = _schema_and_dt_col(mapping, scalars)
    return schema[scalars['pivot_col']].py_type


def _extract_tsd_pivot_value_value_scalar(mapping, scalars) -> SCALAR_1:
    schema, _ = _schema_and_dt_col(mapping, scalars)
    schema.pop(scalars['key_col'])
    schema.pop(scalars['pivot_col'])
    assert len(schema) == 1
    return next(iter(schema.values())).py_type


@generator(resolvers={SCALAR: _extract_tsd_key_scalar, SCALAR_1: _extract_tsd_pivot_key_value_scalar,
                      SCALAR_2: _extract_tsd_pivot_value_value_scalar})
def tsd_k_tsd_from_data_source(
        dfs: type[DATA_FRAME_SOURCE], dt_col: str, key_col: str, pivot_col: str, offset: timedelta = timedelta()
) -> TSD[SCALAR, TSD[SCALAR_1, TS[SCALAR_2]]]:
    """
    Extract a TSD instance from the data frame source. This uses key_col for the first dimension and pivot_col for the
    second dimension.
    The requirement is that the results are ordered by date and key_col at a minimum. It is useful to order by pivot_col
    as well.
    The expected shape of the data frame would be:
        +------+---------+------------+-------+
        | date | key_col |  pivot_col | value |
        +------+---------+------------+-------+
        |  ...                                |
    """
    df: pl.DataFrame
    dfs_instance = DataStore.instance().get_data_source(dfs)
    schema = tuple(dfs_instance.schema.keys())
    dt_ndx = schema.index(dt_col)
    key_ndx = schema.index(key_col)
    pivot_ndx = schema.index(pivot_col)
    value_ndx = next(iter({0, 1, 2, 3} - {dt_ndx, key_ndx, pivot_ndx}))
    dt_converter = _dt_converter(dfs_instance.schema[dt_col])
    outer_dict: dict[SCALAR, SCALAR_1] = {}
    inner_dict: dict[SCALAR_1, SCALAR_2] = {}
    last_dt: datetime | None = None
    last_key: SCALAR_1 | None = None
    for df in dfs_instance.iter_frames():
        for value in df.iter_rows(named=False):
            dt = dt_converter(value[dt_ndx])
            if last_dt != dt:
                if last_dt is not None:
                    yield last_dt + offset, outer_dict
                last_dt = dt
                last_key = value[key_ndx]
                inner_dict = {value[pivot_ndx]: value[value_ndx]}
                outer_dict = {value[key_ndx]: inner_dict}
            elif last_key != value[key_ndx]:
                last_key = value[key_ndx]
                outer_dict[last_key] = inner_dict = {value[pivot_ndx]: value[value_ndx]}
            else:
                inner_dict[value[pivot_ndx]] = value[value_ndx]
    if last_dt is not None:
        yield last_dt + offset, outer_dict


def _extract_tsd_key_value_bundle(mapping, scalars) -> TS_SCHEMA:
    schema, _ = _schema_and_dt_col(mapping, scalars)
    schema.pop(scalars['key_col'])
    return ts_schema(**{k: HgTSTypeMetaData(v) for k, v in schema.items()})


@generator(resolvers={SCALAR: _extract_tsd_key_scalar, TS_SCHEMA: _extract_tsd_key_value_bundle})
def tsd_k_b_from_data_source(
        dfs: type[DATA_FRAME_SOURCE], dt_col: str, key_col: str, offset: timedelta = timedelta()
) -> TSD[SCALAR, TSB[TS_SCHEMA]]:
    """
    Extract a TSD instance from the data frame source. This will extract the key_col column and will
    set the value to be the remainder of the data frames columns.
    The requirement is that the results are ordered by date at a minimum. It is useful to order by key_col as well.
    The expected shape of the data frame would be:
        +------+---------+----+-----+----+
        | date | key_col | p1 | ... | pn |
        +------+---------+----+-----+----+
        |          ...                   |
    """
    df: pl.DataFrame
    dfs_instance = DataStore.instance().get_data_source(dfs)
    dt_converter = _dt_converter(dfs_instance.schema[dt_col])
    values: dict[SCALAR, dict] = {}
    last_dt: datetime | None = None
    for df in dfs_instance.iter_frames():
        for value in df.iter_rows(named=True):
            dt = dt_converter(value.pop(dt_col))
            if last_dt != dt:
                if last_dt is not None:
                    yield last_dt + offset, values
                values = {value.pop(key_col): value}
                last_dt = dt
            else:
                values[value.pop(key_col)] = value
    if last_dt is not None:
        yield last_dt + offset, values


def _extract_tsd_array_value(mapping, scalars) -> SCALAR_1:
    schema, _ = _schema_and_dt_col(mapping, scalars)
    schema.pop(scalars['key_col'])
    tp = next(iter(schema.values())).py_type
    assert all(tp == v.py_type for v in schema.values()), f"All columns must be of same type ({tp}): {schema}"
    return tp


def _extract_tsd_array_size(mapping, scalars) -> SIZE:
    schema, _ = _schema_and_dt_col(mapping, scalars)
    schema.pop(scalars['key_col'])
    return Size[len(schema)]


@generator(
    resolvers={SCALAR: _extract_tsd_key_scalar, SCALAR_1: _extract_tsd_array_value, SIZE: _extract_tsd_array_size})
def tsd_k_a_from_data_source(
        dfs: type[DATA_FRAME_SOURCE], dt_col: str, key_col: str, offset: timedelta = timedelta()
) -> TSD[SCALAR, TS[Array[SCALAR_1, SIZE]]]:
    """
    Extract out a TSD with value type of Array. This requires the value columns to be of the same type.
    This is best used when the data type is bool, int, float, date or datetime.
    """
    df: pl.DataFrame
    dfs_instance = DataStore.instance().get_data_source(dfs)
    dt_converter = _dt_converter(dfs_instance.schema[dt_col])
    values: dict[SCALAR, Array[SCALAR_1, SIZE]] = {}
    last_dt: datetime | None = None
    for df in dfs_instance.iter_frames():
        df_dt = df[dt_col]
        df_key = df[key_col]
        df_value = df.select(*(k for k in df.schema.keys() if k not in (dt_col, key_col)))
        for dt, key, value in zip(df_dt, df_key, df_value.iter_rows(named=False)):
            value = np.array(value)
            dt = dt_converter(dt)
            if last_dt != dt:
                if last_dt is not None:
                    yield last_dt + offset, values
                last_dt = dt
                values = {key: value}
            else:
                values[key] = value
    if last_dt is not None:
        yield last_dt + offset, values


def _extract_array_value(mapping, scalars) -> SCALAR:
    schema, _ = _schema_and_dt_col(mapping, scalars)
    tp = next(iter(schema.values())).py_type
    assert all(tp == v.py_type for v in schema.values()), f"All columns must be of same type ({tp}): {schema}"
    return tp


def _extract_array_size(mapping, scalars) -> SIZE:
    schema, _ = _schema_and_dt_col(mapping, scalars)
    return Size[len(schema)]


@generator(resolvers={SCALAR: _extract_array_value, SIZE: _extract_array_size})
def ts_of_array_from_data_source(
        dfs: type[DATA_FRAME_SOURCE], dt_col: str, offset: timedelta = timedelta()
) -> TS[Array[SCALAR, SIZE]]:
    """
    Extract out a TS of Array values.
    """
    df: pl.DataFrame
    dfs_instance = DataStore.instance().get_data_source(dfs)
    dt_converter = _dt_converter(dfs_instance.schema[dt_col])
    for df in dfs_instance.iter_frames():
        df_dt = df[dt_col]
        df_values = df.select(*(k for k in df.schema.keys() if k != dt_col))
        for dt, values in zip(df_dt, df_values.iter_rows(named=False)):
            dt = dt_converter(dt)
            yield dt + offset, np.array(values)


@generator(resolvers={SCALAR: _extract_array_value, SIZE: _extract_array_size})
def tsl_from_data_source(
        dfs: type[DATA_FRAME_SOURCE], dt_col: str, offset: timedelta = timedelta()
) -> TSL[TS[SCALAR], SIZE]:
    """
    Extract a TSL from a data frame.
    """
    df: pl.DataFrame
    dfs_instance = DataStore.instance().get_data_source(dfs)
    dt_converter = _dt_converter(dfs_instance.schema[dt_col])
    for df in dfs_instance.iter_frames():
        df_dt = df[dt_col]
        df_values = df.select(*(k for k in df.schema.keys() if k != dt_col))
        for dt, values in zip(df_dt, df_values.iter_rows(named=False)):
            dt = dt_converter(dt)
            yield dt + offset, values


SIZE_1 = clone_typevar(SIZE, "SIZE_1")


@generator(resolvers={SCALAR: _extract_array_value, SIZE: _extract_array_size, SIZE_1: lambda m, s: Size[-1]})
def ts_of_matrix_from_data_source(
        dfs: type[DATA_FRAME_SOURCE], dt_col: str, offset: timedelta = timedelta()
) -> TS[Array[SCALAR, SIZE, SIZE_1]]:
    """
    Extract out a TS of a matrix Array. The size of the second value is the size of the matrix is the columns
    (other than the dt_col one). The size of the second index is the number of columns with the same date / datetime
    value.
    By default, the second size is set to be variable, the second size could be set if known.
    """
    df: pl.DataFrame
    dfs_instance = DataStore.instance().get_data_source(dfs)
    dt_converter = _dt_converter(dfs_instance.schema[dt_col])
    values: list[list[SCALAR]] = []
    last_dt: datetime | None = None
    for df in dfs_instance.iter_frames():
        df_dt = df[dt_col]
        df_value = df.select(*(k for k in df.schema.keys() if k != dt_col))
        for dt, value in zip(df_dt, df_value.iter_rows(named=False)):
            dt = dt_converter(dt)
            if last_dt != dt:
                if last_dt is not None:
                    yield last_dt + offset, np.array(values)
                last_dt = dt
                values = [value]
            else:
                values.append(value)
    if last_dt is not None:
        yield last_dt + offset, np.array(values)


def _convert_type(pl_type: pl.DataType) -> HgScalarTypeMetaData:
    from polars import String, Boolean, Date, Datetime, Time, Duration, Categorical, List, Array, Object
    from polars.datatypes import IntegerType
    from polars.datatypes.classes import FloatType
    if isinstance(pl_type, IntegerType):
        return HgScalarTypeMetaData.parse_type(int)
    if isinstance(pl_type, FloatType):
        return HgScalarTypeMetaData.parse_type(float)
    if isinstance(pl_type, String):
        return HgScalarTypeMetaData.parse_type(str)
    if isinstance(pl_type, Boolean):
        return HgScalarTypeMetaData.parse_type(bool)
    if isinstance(pl_type, Date):
        return HgScalarTypeMetaData.parse_type(date)
    if isinstance(pl_type, Datetime):
        return HgScalarTypeMetaData.parse_type(datetime)
    if isinstance(pl_type, Time):
        return HgScalarTypeMetaData.parse_type(time)
    if isinstance(pl_type, Duration):
        return HgScalarTypeMetaData.parse_type(timedelta)
    if isinstance(pl_type, Categorical):
        return HgScalarTypeMetaData.parse_type(str)
    if isinstance(pl_type, (List, Array)):
        tp: List = pl_type
        return HgScalarTypeMetaData.parse_type(_convert_type(tp.inner).py_type)
    if isinstance(pl_type, Object):
        return HgScalarTypeMetaData.parse_type(object)
    # Do Struct, still

    raise ValueError(f"Unable to convert {pl_type} to HgTimeSeriesTypeMetaData")


def _dt_converter(dt_tp: pl.DataType) -> Callable[[date | datetime], datetime]:
    if isinstance(dt_tp, pl.datatypes.Date):
        return lambda dt: datetime.combine(dt, time())
    if isinstance(dt_tp, pl.datatypes.Datetime):
        return lambda dt: dt
    raise RuntimeError(f"Unable to convert {dt_tp} to a date or datetime")
