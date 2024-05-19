"""
Tooling to convert to a dataframe from variable time-series types.
"""
from datetime import date, datetime
import polars as pl

from hgraph import graph, TIME_SERIES_TYPE, TS, Frame, COMPOUND_SCALAR, compute_node, SCALAR, STATE, AUTO_RESOLVE


@graph
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
    raise NotImplemented(f"to_frame is not yet implemented for type: {ts.output_type}")


@compute_node(overloads=to_frame)
def to_frame_ts(ts: TS[SCALAR], value_col: str = None, dt_col: str = None,
                _state: STATE = None,
                _tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
                _s_tp: type[SCALAR] = AUTO_RESOLVE) -> TS[Frame[COMPOUND_SCALAR]]:
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
                f"to_frame_ts type of dt_col('{dt_col}') is {dt_col_tp}, which is not date or datetime as required")

        if dt_col_tp is datetime:
            _state.to_frame = lambda ts: pl.DataFrame({dt_col: [ts.last_modified_time], value_col: [ts.value]})
        else:
            _state.to_frame = lambda ts: pl.DataFrame({dt_col: [ts.last_modified_time.date()], value_col: [ts.value]})
    else:
        _state.to_frame = lambda ts: pl.DataFrame({value_col: [ts.value]})