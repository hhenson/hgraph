from datetime import timedelta, date, datetime, time
from typing import Callable, OrderedDict

import polars as pl

from hgraph import generator, TS_SCHEMA, TSB, TSD, SCALAR, SCALAR_1, TS, ts_schema, HgTimeSeriesTypeMetaData, Array, \
    SIZE, TSL, clone_typevar, Size, SCALAR_2
from hgraph.adaptors.data_frame.data_frame_source import DATA_FRAME_SOURCE, DataStore

__all__ = ("tsb_from_data_source",)


def _schema_and_dt_col(mapping, scalars) -> tuple[OrderedDict[str, pl.DataType], tuple[str, pl.DataType]]:
    dfs: type[DATA_FRAME_SOURCE] = mapping[DATA_FRAME_SOURCE].py_type
    dfs_instance = DataStore.instance().get_data_source(dfs)
    schema = dfs_instance.schema
    dt_col = scalars['dt_col']
    return {k: _convert_type(v) for k, v in schema.items() if k != dt_col}, (dt_col, schema[dt_col])


def _extract_schema(mapping, scalars) -> TS_SCHEMA:
    """Extract the schema from the mapping"""
    schema, _ = _schema_and_dt_col(mapping, scalars)
    return ts_schema(**schema)


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
    schema, (dt_col, dt_tp) = _schema_and_dt_col(mapping, scalars)
    return schema[scalars['key_col']].py_type


def _extract_tsd_key_value_scalar(mapping, scalars) -> SCALAR_1:
    schema, (dt_col, dt_tp) = _schema_and_dt_col(mapping, scalars)
    schema.pop(scalars['key_col'])
    assert len(schema) == 1
    return next(schema.values().py_type)


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
    ...


@generator
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


@generator
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
    ...


@generator
def tsd_k_a_from_data_source(
        dfs: type[DATA_FRAME_SOURCE], dt_col: str, key_col: str, offset: timedelta = timedelta()
) -> TSD[SCALAR, TS[Array[SCALAR_1, SIZE]]]:
    """
    Extract out a TSD with value type of Array. This requires the value columns to be of the same type.
    This is best used when the data type is bool, int, float, date or datetime.
    """
    ...


@generator
def ts_of_array_from_data_source(
        dfs: type[DATA_FRAME_SOURCE], dt_col: str, offset: timedelta = timedelta()
) -> TS[Array[SCALAR_1, SIZE]]:
    """
    Extract out a TS of Array values.
    """


@generator
def tsl_from_data_source(
        dfs: type[DATA_FRAME_SOURCE], dt_col: str, offset: timedelta = timedelta()
) -> TSL[TS[SCALAR], SIZE]:
    """
    Extract a TSL from a data frame.
    """


SIZE_1 = clone_typevar(SIZE, "SIZE_1")


@generator
def ts_of_array_from_data_source(
        dfs: type[DATA_FRAME_SOURCE], dt_col: str, offset: timedelta = timedelta(),
        sz_2: type[SIZE_1] = Size[-1]
) -> TS[Array[SCALAR_1, SIZE, SIZE_1]]:
    """
    Extract out a TS of a matrix Array. The size of the second value is the size of the matrix is the columns
    (other than the dt_col one). The size of the second index is the number of columns with the same date / datetime
    value.
    By default, the second size is set to be variable, the second size could be set if known.
    """


def _convert_type(pl_type: pl.DataType) -> HgTimeSeriesTypeMetaData:
    from polars import String, Boolean, Date, Datetime, Time, Duration, Categorical, List, Array, Object
    from polars.datatypes import IntegerType
    from polars.datatypes.classes import FloatType
    if isinstance(pl_type, IntegerType):
        return HgTimeSeriesTypeMetaData.parse_type(TS[int])
    if isinstance(pl_type, FloatType):
        return HgTimeSeriesTypeMetaData.parse_type(TS[float])
    if isinstance(pl_type, String):
        return HgTimeSeriesTypeMetaData.parse_type(TS[str])
    if isinstance(pl_type, Boolean):
        return HgTimeSeriesTypeMetaData.parse_type(TS[bool])
    if isinstance(pl_type, Date):
        return HgTimeSeriesTypeMetaData.parse_type(TS[date])
    if isinstance(pl_type, Datetime):
        return HgTimeSeriesTypeMetaData.parse_type(TS[datetime])
    if isinstance(pl_type, Time):
        return HgTimeSeriesTypeMetaData.parse_type(TS[time])
    if isinstance(pl_type, Duration):
        return HgTimeSeriesTypeMetaData.parse_type(TS[timedelta])
    if isinstance(pl_type, Categorical):
        return HgTimeSeriesTypeMetaData.parse_type(TS[str])
    if isinstance(pl_type, (List, Array)):
        tp: List = pl_type
        return HgTimeSeriesTypeMetaData.parse_type(TS[_convert_type(tp.inner).py_type])
    if isinstance(pl_type, Object):
        return HgTimeSeriesTypeMetaData.parse_type(TS[object])
    # Do Struct, still

    raise ValueError(f"Unable to convert {pl_type} to HgTimeSeriesTypeMetaData")


def _dt_converter(dt_tp: pl.DataType) -> Callable[[date | datetime], datetime]:
    if isinstance(dt_tp, pl.datatypes.Date):
        return lambda dt: datetime.combine(dt, time())
    if isinstance(dt_tp, pl.datatypes.Datetime):
        return lambda dt: dt
    raise RuntimeError(f"Unable to convert {dt_tp} to a date or datetime")
