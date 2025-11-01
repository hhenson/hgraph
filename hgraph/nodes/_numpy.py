from dataclasses import dataclass
from datetime import datetime
from typing import Generic

import numpy as np

from hgraph import (
    compute_node,
    TS,
    TSB,
    SCALAR,
    TimeSeriesSchema,
    Array,
    SIZE,
    STATE,
    AUTO_RESOLVE,
    MIN_DT,
    NUMBER,
    COMPOUND_SCALAR,
    Frame,
    Size,
    TSW,
    WINDOW_SIZE_MIN,
    WINDOW_SIZE,
)

__all__ = ("NpRollingWindowResult", "NpRollingWindowState", "np_rolling_window", "np_quantile", "np_std")


@dataclass
class NpRollingWindowResult(TimeSeriesSchema, Generic[SCALAR, SIZE]):
    buffer: TS[Array[SCALAR, SIZE]]
    index: TS[Array[datetime, SIZE]]


@dataclass
class NpRollingWindowState:
    capacity: int = None
    buffer: Array[SCALAR] = None
    index: Array[datetime] = None
    start: int = 0
    length: int = 0


@compute_node
def np_rolling_window(
    ts: TS[SCALAR],
    period: SIZE,
    min_window_period: int = None,
    _sz: type[SIZE] = AUTO_RESOLVE,
    _scalar: type[SCALAR] = AUTO_RESOLVE,
    _state: STATE[NpRollingWindowState] = None,
) -> TSB[NpRollingWindowResult]:
    buffer: Array[SCALAR] = _state.buffer
    index: Array[datetime] = _state.index
    capacity: int = _state.capacity
    start: int = _state.start
    length: int = _state.length
    length += 1
    if length > capacity:
        start += 1
        start %= capacity
        _state.start = start
        length = capacity
    _state.length = length
    pos = (start + length - 1) % capacity
    buffer[pos] = ts.value
    index[pos] = ts.last_modified_time
    if length == capacity or (min_window_period is not None and length >= min_window_period):
        b = np.roll(buffer, -start)
        i = np.roll(index, -start)
        if length != capacity:
            b = b[:length]
            i = i[:length]
        return {"buffer": b, "index": i}


@np_rolling_window.start
def np_rolling_window_start(_sz: type[SIZE], _scalar: type[SCALAR], _state: STATE[NpRollingWindowState]):
    _state.capacity = _sz.SIZE
    if _state.capacity < 1:
        raise RuntimeError("Period must be at least 1")
    _state.buffer = np.array([_scalar()] * _sz.SIZE)
    _state.index = np.array([MIN_DT] * _sz.SIZE)


@compute_node
def np_quantile(ts: TS[Array[NUMBER, SIZE]], q: float, method: str = "linear") -> TS[float]:
    """The np.quantile function, limited to a single axis"""
    return np.quantile(ts.value, q, method=method)


@compute_node
def np_std(ts: TS[Array[NUMBER, SIZE]], ddof: int = None) -> TS[float]:
    """Return the standard deviation of the values of the ts"""
    return np.std(ts.value, ddof=ddof)


def _compute_size(compound_type: COMPOUND_SCALAR) -> type:
    schema = compound_type.__meta_data_schema__
    values = iter(schema.values())
    v = next(values)
    if not all(v != v_ for v_ in values):
        raise ValueError("Not all values of the frame are the same")
    return Size[len(schema.values())]


def _compute_data_tp(compound_type: COMPOUND_SCALAR) -> type:
    schema = compound_type.__meta_data_schema__
    values = iter(schema.values())
    v = next(values)
    return v.py_type


@compute_node(
    resolvers={
        SIZE: lambda mapping, scalars: _compute_size(mapping[COMPOUND_SCALAR]),
        NUMBER: lambda mapping, scalars: _compute_data_tp(mapping[COMPOUND_SCALAR]),
    }
)
def frame_to_1d_array(
    frame: TS[Frame[COMPOUND_SCALAR]], _sz: type[SIZE] = Size, _tp: SCALAR = float
) -> TS[Array[SCALAR, SIZE]]: ...
