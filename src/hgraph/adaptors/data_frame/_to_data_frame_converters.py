"""
Tooling to convert to a dataframe from variable time-series types.
"""

from datetime import date, datetime

import polars as pl
from frozendict import frozendict

from hgraph import (
    graph,
    TIME_SERIES_TYPE,
    TS,
    Frame,
    COMPOUND_SCALAR,
    compute_node,
    SCALAR,
    STATE,
    AUTO_RESOLVE,
    TSB,
    TS_SCHEMA,
    SCALAR_1,
    TSD,
    compound_scalar,
    operator,
)


@operator
def to_frame(ts: TIME_SERIES_TYPE) -> TS[Frame[COMPOUND_SCALAR]]:
    """
    Converts the time-series into a dataframe.
    For this to work the schema of the frame needs to be provided.
    If it is possible, that is all that is required, otherwise each shape of time-series has a dedicated converter
    that will provide the ability to be passed rules for the conversion.
    For example:
    ```python
    my_time_series = const(3.0)
    SchemaDef = compound_scalar(dt: datetime, value: float)
    frame = to_frame[COMPOUND_SCALAR: SchemaDef](my_time_series)
    ```
    In the example above, the first column is a datetime element indicating that the frame should include the dt
    when constructed, the remaining property ('value') is then populated with the value of the time-series.
    """


def _ts_frame_cs_resolver(mapping, scalars):
    value_col = scalars["value_col"]
    if value_col is None:
        raise ValueError("value_col cannot be None")
    schema = {value_col: mapping[SCALAR].py_type}
    dt_col = scalars["dt_col"]
    if dt_col is not None:
        schema = {dt_col: date if scalars["dt_is_date"] else datetime} | schema
    return compound_scalar(**schema)


@compute_node(overloads=to_frame, resolvers={COMPOUND_SCALAR: _ts_frame_cs_resolver})
def to_frame_ts(
    ts: TS[SCALAR],
    value_col: str = None,
    dt_col: str = None,
    dt_is_date: bool = False,
    _state: STATE = None,
    _tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
    _s_tp: type[SCALAR] = AUTO_RESOLVE,
) -> TS[Frame[COMPOUND_SCALAR]]:
    return _state.to_frame(ts)


@to_frame_ts.start
def _start_to_frame_ts(value_col: str, dt_col: str, _state: STATE, _tp: type[COMPOUND_SCALAR], _s_tp: type[SCALAR]):
    schema = _tp.__meta_data_schema__

    if len(schema) > 2:
        raise RuntimeError(f"to_frame_ts cannot have more than 2 columns in the schema definition, got: {schema}")
    if len(schema) < 1:
        raise RuntimeError(f"to_frame_ts cannot have less than 1 column in the schema definition, got: {schema}")
    if len(schema) == 1 and dt_col is not None:
        raise RuntimeError(f"to_frame_ts cannot have only one column with dt_col('{dt_col}') being defined")

    if dt_col is None and len(schema) == 2:
        dt_col = next(iter(schema.keys()))

    if value_col is None:
        v = next(i := iter(schema.keys()))
        if len(schema) == 2:
            v = next(i)
        value_col = v

    value_col_tp = schema[value_col].py_type
    if not issubclass(value_col_tp, _s_tp):
        raise RuntimeError(f"to_frame_ts(ts: TS[{_s_tp}]) value_col('{value_col}') is not a subclass of {_s_tp}")

    if dt_col:
        dt_col_tp = schema[dt_col].py_type
        if dt_col_tp not in (date, datetime):
            raise RuntimeError(
                f"to_frame_ts type of dt_col('{dt_col}') is {dt_col_tp}, which is not date or datetime as required"
            )

        if dt_col_tp is datetime:
            _state.to_frame = lambda ts: pl.DataFrame({dt_col: [ts.last_modified_time], value_col: [ts.value]})
        else:
            _state.to_frame = lambda ts: pl.DataFrame({dt_col: [ts.last_modified_time.date()], value_col: [ts.value]})
    else:
        _state.to_frame = lambda ts: pl.DataFrame({value_col: [ts.value]})


def _extract_tsb_schema(mapping, scalars):
    _tsb_tp = mapping[TS_SCHEMA]
    tsb_schema = {k: v.scalar_type().py_type for k, v in _tsb_tp.py_type.__meta_data_schema__.items()}
    map_ = scalars["map_"]
    if map_:
        tsb_schema = {map_.get(k, k): v for k, v in tsb_schema.items()}
    dt_col = scalars["dt_col"]
    if dt_col:
        tsb_schema = {dt_col: date if scalars["dt_is_date"] else datetime} | tsb_schema
    return compound_scalar(**tsb_schema)


@compute_node(overloads=to_frame, resolvers={COMPOUND_SCALAR: _extract_tsb_schema})
def to_frame_tsb(
    ts: TSB[TS_SCHEMA],
    dt_col: str = None,
    map_: frozendict[str, str] = None,
    dt_is_date: bool = False,
    _state: STATE = None,
    _tsb_tp: type[TS_SCHEMA] = AUTO_RESOLVE,
    _frame_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
) -> TS[Frame[COMPOUND_SCALAR]]:
    return _state.to_frame(ts)


@to_frame_tsb.start
def _start_to_frame_tsb(
    dt_col: str, map_: frozendict[str, str], _state: STATE, _tsb_tp: type[TS_SCHEMA], _frame_tp: type[COMPOUND_SCALAR]
):
    tsb_schema = {k: v.scalar_type().py_type for k, v in _tsb_tp.__meta_data_schema__.items()}
    frame_schema = {k: v.py_type for k, v in _frame_tp.__meta_data_schema__.items()}
    df_schema = dict(frame_schema)
    is_dt = False
    if len(tsb_schema) + 1 == len(df_schema):
        # Implies we are expecting date / dt col.
        if dt_col is None:
            dt_col = next(iter(df_schema.keys()))
        if df_schema[dt_col] not in (date, datetime):
            raise RuntimeError(f"to_frame the dt_col('{dt_col}') is not a date or datetime as required")
        is_dt = df_schema.pop(dt_col) is datetime
    if len(tsb_schema) == len(df_schema):
        if tsb_schema == df_schema:
            if dt_col is None:
                _state.to_frame = lambda ts: pl.DataFrame(
                    {k: [t.value if (t := ts[k]).valid else None] for k in tsb_schema.keys()}, schema=frame_schema
                )
            else:
                _state.to_frame = lambda ts: pl.DataFrame(
                    {k: [t.value if (t := ts[k]).valid else None] for k in tsb_schema.keys()}
                    | {dt_col: [ts.last_modified_time if is_dt else ts.last_modified_time.date()]},
                    schema=frame_schema,
                )
        elif map_:
            if {map_.get(k, k): v for k, v in tsb_schema.items()} == df_schema:
                if dt_col is None:
                    _state.to_frame = lambda ts: pl.DataFrame(
                        {map_.get(k, k): t.value if (t := ts[k]).valid else None for k in tsb_schema.keys()},
                        schema=frame_schema,
                    )
                else:
                    _state.to_frame = lambda ts: pl.DataFrame(
                        {map_.get(k, k): t.value if (t := ts[k]).valid else None for k in tsb_schema.keys()}
                        | {dt_col: [ts.last_modified_time if is_dt else ts.last_modified_time.date()]},
                        schema=frame_schema,
                    )
        else:
            raise RuntimeError(f"Unable to map from {tsb_schema} to {frame_schema} using the mapping {map_}")
    else:
        raise RuntimeError(
            f"to_frame unable to map from {tsb_schema} to {frame_schema} {'using the mapping ' if map_ else ''}{map_ if map_ else ''}"
        )


# @compute_node(overloads=to_frame)
# def to_frame_tsd(ts: TSD[SCALAR, TS[SCALAR_1]],
#                  dt_col: str = None, key_col_: str = None, value_col_: str = None, dt_is_date: bool = False,
#                  _state: STATE = None,
#                  _key_tp: type[SCALAR] = AUTO_RESOLVE,
#                  _value_tp: type[SCALAR_1] = AUTO_RESOLVE,
#                  _frame_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE) -> TS[Frame[COMPOUND_SCALAR]]:
#     return _state.to_frame(ts)
#
#
# @to_frame_tsd.start
# def _start_to_frame_tsd(dt_col: str, key_col_: str, value_col_: str,
#                         _state: STATE,
#                         _key_tp: type[SCALAR],
#                         _value_tp: type[SCALAR_1],
#                         _frame_tp: type[COMPOUND_SCALAR]):
#     ...
