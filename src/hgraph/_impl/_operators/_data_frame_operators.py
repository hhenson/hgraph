from datetime import timedelta, date, datetime, time
from typing import Callable, OrderedDict

import polars as pl

from hgraph import (
    generator,
    TS_SCHEMA,
    TSB,
    TSD,
    SCALAR,
    SCALAR_1,
    TS,
    ts_schema,
    HgScalarTypeMetaData,
    HgTSTypeMetaData,
    Frame,
    COMPOUND_SCALAR,
    compound_scalar,
    EvaluationEngineApi,
    from_data_frame,
    OUT,
    AUTO_RESOLVE,
    HgTSBTypeMetaData, HgTSDTypeMetaData, compute_node, to_data_frame, TIME_SERIES_TYPE,
)

__all__ = []


def schema_from_frame(frame: pl.DataFrame) -> COMPOUND_SCALAR:
    """Converts a polars data frame to a COMPOUND_SCALAR."""
    schema = frame.schema
    out = compound_scalar(**{name: _convert_type(dtype).py_type for name, dtype in schema.items()})
    return out


def _schema_and_dt_col(
        mapping, scalars
) -> tuple[OrderedDict[str, HgScalarTypeMetaData], tuple[str, HgScalarTypeMetaData]]:
    df: pl.DataFrame = scalars["df"]
    schema = df.schema
    dt_col = scalars["dt_col"]
    return {k: _convert_type(v) for k, v in schema.items() if k != dt_col}, (dt_col, _convert_type(schema[dt_col]))


def _cs_from_frame(mapping, scalar) -> COMPOUND_SCALAR:
    df: pl.DataFrame = scalar["df"]
    schema = df.schema
    return compound_scalar(**{k: _convert_type(v) for k, v in schema.items()})


def _extract_schema(mapping, scalars) -> TS_SCHEMA:
    """Extract the schema from the mapping"""
    schema, _ = _schema_and_dt_col(mapping, scalars)
    return ts_schema(**{k: HgTSTypeMetaData(v) for k, v in schema.items()})


def _extract_scalar(mapping, scalars) -> SCALAR:
    df: pl.DataFrame = scalars["df"]
    schema = df.schema
    k = scalars["value_col"]
    return _convert_type(schema[k])


def _validate_ts_schema(mapping, scalars) -> str:
    cs = mapping[COMPOUND_SCALAR]
    dt_col = scalars["dt_col"]
    value_col = scalars["value_col"]
    if dt_col not in cs.meta_data_schema:
        return f"dt_col '{dt_col}' not found in schema: {cs.meta_data_schema}"
    if value_col not in cs.meta_data_schema:
        return f"value_col '{value_col}' not found in schema: {cs.meta_data_schema}"
    return True


@generator(
    overloads=from_data_frame,
    resolvers={SCALAR: _extract_scalar, COMPOUND_SCALAR: _cs_from_frame},
    requires=_validate_ts_schema,
)
def from_data_frame_ts(
        df: Frame[COMPOUND_SCALAR],
        dt_col: str = "date",
        value_col: str = "value",
        offset: timedelta = timedelta(),
        _df_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
        _out_tp: type[OUT] = AUTO_RESOLVE,
        _api: EvaluationEngineApi = None,
) -> TS[SCALAR]:
    dt_converter = _dt_converter(_df_tp.__meta_data_schema__[dt_col].py_type)
    if not df.is_empty():
        for dt, value in (
                df.filter(pl.col(dt_col).is_between(_api.start_time, _api.end_time))
                        .select([dt_col, value_col])
                        .iter_rows(named=False)
        ):
            dt = dt_converter(dt)
            yield dt + offset, value


def _validate_tsb_schema(mapping, scalars) -> str:
    out = mapping[OUT]
    if type(out) is not HgTSBTypeMetaData:
        return f"Expected OUT to be TSB, got: {out}"
    cs = mapping[COMPOUND_SCALAR]
    dt_col = scalars["dt_col"]
    if dt_col not in cs.meta_data_schema:
        return f"dt_col '{dt_col}' not found in schema: {cs.meta_data_schema}"
    return True


@generator(
    overloads=from_data_frame,
    resolvers={TS_SCHEMA: _extract_schema, COMPOUND_SCALAR: _cs_from_frame},
    requires=_validate_tsb_schema,
)
def from_data_frame_tsb(
        df: Frame[COMPOUND_SCALAR],
        dt_col: str = "date",
        offset: timedelta = timedelta(),
        _df_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
        _out_tp: type[OUT] = AUTO_RESOLVE,
        _api: EvaluationEngineApi = None,
) -> TSB[TS_SCHEMA]:
    """
    Iterates over the data_frame, returning an instance of TS_SCHEMA for each row in the table.
    null values are not ticked.
    """
    dt_converter = _dt_converter(_df_tp.__meta_data_schema__[dt_col].py_type)
    for value in df.filter(pl.col(dt_col).is_between(_api.start_time, _api.end_time)).iter_rows(named=True):
        dt = dt_converter(value.pop(dt_col))
        yield dt + offset, value


def _extract_tsd_key_scalar(mapping, scalars) -> SCALAR:
    schema, _ = _schema_and_dt_col(mapping, scalars)
    return schema[scalars["key_col"]].py_type


def _extract_tsd_key_value_scalar(mapping, scalars) -> SCALAR_1:
    schema, _ = _schema_and_dt_col(mapping, scalars)
    schema.pop(scalars["key_col"])
    assert len(schema) == 1
    return next(iter(schema.values())).py_type


def _validate_tsd_k_v_schema(mapping, scalars) -> str:
    out = mapping[OUT]
    if type(out) is not HgTSDTypeMetaData:
        return f"Expected OUT to be TSD, got: {out}"
    cs = mapping[COMPOUND_SCALAR]
    dt_col = scalars["dt_col"]
    if dt_col not in cs.meta_data_schema:
        return f"dt_col '{dt_col}' not found in schema: {cs.meta_data_schema}"
    k_col = scalars["key_col"]
    if k_col not in cs.meta_data_schema:
        return f"key_col '{k_col}' not found in schema: {cs.meta_data_schema}"
    if len(cs.meta_data_schema) != 3:
        return f"Expected 3 columns, got: {cs.meta_data_schema}"
    return True


@generator(
    overloads=from_data_frame,
    resolvers={SCALAR: _extract_tsd_key_scalar, SCALAR_1: _extract_tsd_key_value_scalar,
               COMPOUND_SCALAR: _cs_from_frame},
    requires=_validate_tsd_k_v_schema,
)
def from_data_frame_tsd_k_v(
        df: Frame[COMPOUND_SCALAR],
        dt_col: str = "date",
        key_col: str = "key",
        offset: timedelta = timedelta(),
        _df_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
        _out_tp: type[OUT] = AUTO_RESOLVE,
        _api: EvaluationEngineApi = None,
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
    value_col = next((k for k in _df_tp.__meta_data_schema__.keys() if k not in (key_col, dt_col)))
    dt_converter = _dt_converter(_df_tp.__meta_data_schema__[dt_col].py_type)
    for (dt,), df_ in df.filter(pl.col(dt_col).is_between(_api.start_time, _api.end_time)).group_by(dt_col,
                                                                                                    maintain_order=True):
        dt = dt_converter(dt)
        yield dt + offset, {k: v for k, v in df_.select(key_col, value_col).iter_rows()}


# def _extract_tsd_pivot_key_value_scalar(mapping, scalars) -> SCALAR_1:
#     schema, _ = _schema_and_dt_col(mapping, scalars)
#     return schema[scalars["pivot_col"]].py_type
#
#
# def _extract_tsd_pivot_value_value_scalar(mapping, scalars) -> SCALAR_1:
#     schema, _ = _schema_and_dt_col(mapping, scalars)
#     schema.pop(scalars["key_col"])
#     schema.pop(scalars["pivot_col"])
#     assert len(schema) == 1
#     return next(iter(schema.values())).py_type
#
#
# @generator(
#     resolvers={
#         SCALAR: _extract_tsd_key_scalar,
#         SCALAR_1: _extract_tsd_pivot_key_value_scalar,
#         SCALAR_2: _extract_tsd_pivot_value_value_scalar,
#     }
# )
# def tsd_k_tsd_from_data_source(
#     dfs: type[DATA_FRAME_SOURCE],
#     dt_col: str,
#     key_col: str,
#     pivot_col: str,
#     offset: timedelta = timedelta(),
#     _api: EvaluationEngineApi = None,
# ) -> TSD[SCALAR, TSD[SCALAR_1, TS[SCALAR_2]]]:
#     """
#     Extract a TSD instance from the data frame source. This uses key_col for the first dimension and pivot_col for the
#     second dimension.
#     The requirement is that the results are ordered by date and key_col at a minimum. It is useful to order by pivot_col
#     as well.
#     The expected shape of the data frame would be:
#         +------+---------+------------+-------+
#         | date | key_col |  pivot_col | value |
#         +------+---------+------------+-------+
#         |  ...                                |
#     """
#     df: pl.DataFrame
#     dfs_instance = DataStore.instance().get_data_source(dfs)
#     value_col = next((k for k in dfs_instance.schema.keys() if k not in (key_col, dt_col, pivot_col)))
#     dt_converter = _dt_converter(dfs_instance.schema[dt_col])
#     for df_all in dfs_instance.iter_frames(start_time=_api.start_time, end_time=_api.end_time):
#         if df_all.is_empty():
#             continue
#         for (dt,), df_1 in df_all.group_by(dt_col, maintain_order=True):
#             dt = dt_converter(dt)
#             out = {}
#             for (key,), df in df_1.group_by(key_col, maintain_order=True):
#                 out[key] = {k: v for k, v in df.select(pivot_col, value_col).iter_rows()}
#             yield dt + offset, out
#
#
# def _extract_tsd_key_value_bundle(mapping, scalars) -> TS_SCHEMA:
#     schema, _ = _schema_and_dt_col(mapping, scalars)
#     schema.pop(scalars["key_col"])
#     return ts_schema(**{k: HgTSTypeMetaData(v) for k, v in schema.items()})
#
#
# @generator(resolvers={SCALAR: _extract_tsd_key_scalar, TS_SCHEMA: _extract_tsd_key_value_bundle})
# def tsd_k_b_from_data_source(
#     dfs: type[DATA_FRAME_SOURCE],
#     dt_col: str,
#     key_col: str,
#     offset: timedelta = timedelta(),
#     _api: EvaluationEngineApi = None,
# ) -> TSD[SCALAR, TSB[TS_SCHEMA]]:
#     """
#     Extract a TSD instance from the data frame source. This will extract the key_col column and will
#     set the value to be the remainder of the data frames columns.
#     The requirement is that the results are ordered by date at a minimum. It is useful to order by key_col as well.
#     The expected shape of the data frame would be:
#         +------+---------+----+-----+----+
#         | date | key_col | p1 | ... | pn |
#         +------+---------+----+-----+----+
#         |          ...                   |
#     """
#     df: pl.DataFrame
#     dfs_instance = DataStore.instance().get_data_source(dfs)
#     dt_converter = _dt_converter(dfs_instance.schema[dt_col])
#     value_keys = tuple(k for k in dfs_instance.schema.keys() if k not in (key_col, dt_col))
#     for df_all in dfs_instance.iter_frames(start_time=_api.start_time, end_time=_api.end_time):
#         if df_all.is_empty():
#             continue
#         for (dt,), df in df_all.group_by(dt_col, maintain_order=True):
#             dt = dt_converter(dt)
#             key_df = df[key_col]
#             value_df = df.select(*value_keys)
#             yield dt + offset, {k: v for k, v in zip(key_df, value_df.iter_rows(named=True))}
#
#
# def _extract_tsd_array_value(mapping, scalars) -> SCALAR_1:
#     schema, _ = _schema_and_dt_col(mapping, scalars)
#     schema.pop(scalars["key_col"])
#     tp = next(iter(schema.values())).py_type
#     assert all(tp == v.py_type for v in schema.values()), f"All columns must be of same type ({tp}): {schema}"
#     return tp
#
#
# def _extract_tsd_array_size(mapping, scalars) -> SIZE:
#     schema, _ = _schema_and_dt_col(mapping, scalars)
#     schema.pop(scalars["key_col"])
#     return Size[len(schema)]
#
#
# @generator(
#     resolvers={SCALAR: _extract_tsd_key_scalar, SCALAR_1: _extract_tsd_array_value, SIZE: _extract_tsd_array_size}
# )
# def tsd_k_a_from_data_source(
#     dfs: type[DATA_FRAME_SOURCE],
#     dt_col: str,
#     key_col: str,
#     offset: timedelta = timedelta(),
#     _api: EvaluationEngineApi = None,
# ) -> TSD[SCALAR, TS[Array[SCALAR_1, SIZE]]]:
#     """
#     Extract out a TSD with value type of Array. This requires the value columns to be of the same type.
#     This is best used when the data type is bool, int, float, date or datetime.
#     """
#     df: pl.DataFrame
#     dfs_instance = DataStore.instance().get_data_source(dfs)
#     dt_converter = _dt_converter(dfs_instance.schema[dt_col])
#     value_keys = tuple(k for k in dfs_instance.schema.keys() if k not in (key_col, dt_col))
#     for df_all in dfs_instance.iter_frames(start_time=_api.start_time, end_time=_api.end_time):
#         if df_all.is_empty():
#             continue
#         for (dt,), df in df_all.group_by(dt_col, maintain_order=True):
#             dt = dt_converter(dt)
#             out = {k: np.array(v) for k, v in zip(df[key_col], df.select(*value_keys).iter_rows())}
#             yield dt + offset, out
#
#
# def _extract_array_value(mapping, scalars) -> SCALAR:
#     schema, _ = _schema_and_dt_col(mapping, scalars)
#     tp = next(iter(schema.values())).py_type
#     assert all(tp == v.py_type for v in schema.values()), f"All columns must be of same type ({tp}): {schema}"
#     return tp
#
#
# def _extract_array_size(mapping, scalars) -> SIZE:
#     schema, _ = _schema_and_dt_col(mapping, scalars)
#     return Size[len(schema)]
#
#
# @generator(resolvers={SCALAR: _extract_array_value, SIZE: _extract_array_size})
# def ts_of_array_from_data_source(
#     dfs: type[DATA_FRAME_SOURCE], dt_col: str, offset: timedelta = timedelta(), _api: EvaluationEngineApi = None
# ) -> TS[Array[SCALAR, SIZE]]:
#     """
#     Extract out a TS of Array values.
#     """
#     df: pl.DataFrame
#     dfs_instance = DataStore.instance().get_data_source(dfs)
#     dt_converter = _dt_converter(dfs_instance.schema[dt_col])
#     value_keys = tuple(k for k in dfs_instance.schema.keys() if k != dt_col)
#     for df in dfs_instance.iter_frames(start_time=_api.start_time, end_time=_api.end_time):
#         if df.is_empty():
#             continue
#         for dt, values in zip(df[dt_col], df.select(*value_keys).iter_rows()):
#             dt = dt_converter(dt)
#             yield dt + offset, np.array(values)
#
#
# @generator(resolvers={SCALAR: _extract_array_value, SIZE: _extract_array_size})
# def tsl_from_data_source(
#     dfs: type[DATA_FRAME_SOURCE], dt_col: str, offset: timedelta = timedelta(), _api: EvaluationEngineApi = None
# ) -> TSL[TS[SCALAR], SIZE]:
#     """
#     Extract a TSL from a data frame.
#     """
#     df: pl.DataFrame
#     dfs_instance = DataStore.instance().get_data_source(dfs)
#     dt_converter = _dt_converter(dfs_instance.schema[dt_col])
#     value_keys = tuple(k for k in df.schema.keys() if k != dt_col)
#     for df in dfs_instance.iter_frames(start_time=_api.start_time, end_time=_api.end_time):
#         if df.is_empty():
#             continue
#         df_dt = df[dt_col]
#         df_values = df.select(*value_keys)
#         for dt, values in zip(df_dt, df_values.iter_rows(named=False)):
#             dt = dt_converter(dt)
#             yield dt + offset, values
#
#
# SIZE_1 = clone_type_var(SIZE, "SIZE_1")
#
#
# @generator(resolvers={SCALAR: _extract_array_value, SIZE: _extract_array_size, SIZE_1: lambda m, s: Size[-1]})
# def ts_of_matrix_from_data_source(
#     dfs: type[DATA_FRAME_SOURCE], dt_col: str, offset: timedelta = timedelta(), _api: EvaluationEngineApi = None
# ) -> TS[Array[SCALAR, SIZE, SIZE_1]]:
#     """
#     Extract out a TS of a matrix Array. The size of the second value is the size of the matrix is the columns
#     (other than the dt_col one). The size of the second index is the number of columns with the same date / datetime
#     value.
#     By default, the second size is set to be variable, the second size could be set if known.
#     """
#     df: pl.DataFrame
#     dfs_instance = DataStore.instance().get_data_source(dfs)
#     dt_converter = _dt_converter(dfs_instance.schema[dt_col])
#     value_keys = tuple(k for k in dfs_instance.schema.keys() if k != dt_col)
#     for df_all in dfs_instance.iter_frames(start_time=_api.start_time, end_time=_api.end_time):
#         if df_all.is_empty():
#             continue
#         for (dt,), df in df_all.group_by(dt_col, maintain_order=True):
#             dt = dt_converter(dt)
#             df_values = df.select(*value_keys)
#             yield dt + offset, df_values.to_numpy()
#
#
# def _extract_frame_schema(mapping, scalars) -> COMPOUND_SCALAR:
#     schema, dt_col = _schema_and_dt_col(mapping, scalars)
#     remove_dt_col = scalars["remove_dt_col"]
#     if not remove_dt_col:
#         schema = {dt_col[0]: dt_col[1]} | schema
#     cs = compound_scalar(**{k: v.py_type for k, v in schema.items()})
#     return cs
#
#
# @generator(resolvers={COMPOUND_SCALAR: _extract_frame_schema})
# def ts_of_frames_from_data_source(
#     dfs: type[DATA_FRAME_SOURCE],
#     dt_col: str,
#     offset: timedelta = timedelta(),
#     remove_dt_col: bool = True,
#     _api: EvaluationEngineApi = None,
# ) -> TS[Frame[COMPOUND_SCALAR]]:
#     """
#     Iterates over the data frame/s grouping by date. The resultant data frame is returned, by default with the
#     date column remove, though this can be included by adjusting the value of remove_dt_col.
#     """
#     df: pl.DataFrame
#     dfs_instance = DataStore.instance().get_data_source(dfs)
#     dt_converter = _dt_converter(dfs_instance.schema[dt_col])
#     value_keys = tuple(k for k in dfs_instance.schema.keys() if not remove_dt_col or k != dt_col)
#     for df_all in dfs_instance.iter_frames(start_time=_api.start_time, end_time=_api.end_time):
#         if df_all.is_empty():
#             continue
#         for (dt,), df in df_all.group_by(dt_col, maintain_order=True):
#             dt = dt_converter(dt)
#             df_values = df.select(*value_keys)
#             yield dt + offset, df_values


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
        return HgScalarTypeMetaData.parse_type(tuple[_convert_type(tp.inner).py_type, ...])
    if isinstance(pl_type, Object):
        return HgScalarTypeMetaData.parse_type(object)
    # Do Struct, still

    raise ValueError(f"Unable to convert {pl_type} to HgScalarTypeMetaData")


def _dt_converter(dt_tp: pl.DataType) -> Callable[[date | datetime], datetime]:
    if issubclass(dt_tp, datetime):
        return lambda dt: dt
    if issubclass(dt_tp, date):
        return lambda dt: datetime.combine(dt, time())
    raise RuntimeError(f"Attempting to convert: {dt_tp} to a datetime but it is neither a date or a datime as requried")


def _base_schema(m, s):
    include_date = s["include_date"]
    schema = {}
    if include_date:
        dt_col = s["dt_col"]
        as_date = s["as_date"]
        schema[dt_col] = date if as_date else datetime
    return schema


def _resolve_ts(m, s):
    schema = _base_schema(m, s)
    value_col = s["value_col"]
    scalar_tp = m[SCALAR].py_type
    schema[value_col] = scalar_tp
    return compound_scalar(**schema)


@compute_node(overloads=to_data_frame, resolvers={COMPOUND_SCALAR: _resolve_ts})
def to_data_frame_ts(ts: TS[SCALAR], dt_col: str = "date", value_col: str = "value", as_date: bool = False,
                     include_date: bool = True) -> TS[Frame[COMPOUND_SCALAR]]:
    if include_date:
        data = {dt_col: [ts.last_modified_time.date() if as_date else ts.last_modified_time]}
    else:
        data = {}
    data[value_col] = ts.value
    return pl.DataFrame(data)


def _resolve_tsd_k_v(m, s):
    schema = _base_schema(m, s)
    key_col = s["key_col"]
    schema[key_col] = m[SCALAR].py_type
    value_col = s["value_col"]
    scalar_tp = m[TIME_SERIES_TYPE].value_scalar_tp.py_type
    schema[value_col] = scalar_tp
    return compound_scalar(**schema)


@compute_node(overloads=to_data_frame, resolvers={COMPOUND_SCALAR: _resolve_tsd_k_v})
def to_data_frame_tsd_k_v(ts: TSD[SCALAR, TIME_SERIES_TYPE], dt_col: str = "date", key_col: str = "key",
                     value_col: str = "value", as_date: bool = False,
                     include_date: bool = True) -> TS[Frame[COMPOUND_SCALAR]]:
    value = ts.value
    if include_date:
        data = {
            dt_col: [ts.last_modified_time.date() if as_date else ts.last_modified_time] * len(value),
            key_col: [],
            value_col: [],
        }
    else:
        data = {
            key_col: [],
            value_col: [],
        }
    for k, v in value.items():
        data[key_col].append(k)
        data[value_col].append(v)
    return pl.DataFrame(data)
