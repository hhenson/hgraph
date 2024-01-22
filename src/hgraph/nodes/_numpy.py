from datetime import datetime

from hgraph import compute_node, TS, TSB, SCALAR, TimeSeriesSchema, Array, SIZE, STATE, AUTO_RESOLVE, MIN_DT, NUMBER
import numpy as np


class NpRollingWindowResult(TimeSeriesSchema):
    buffer: TS[Array[SCALAR, SIZE]]
    index: TS[Array[datetime, SIZE]]


@compute_node
def np_rolling_window(ts: TS[SCALAR], period: SIZE, min_window_period: int = None, _sz: type[SIZE] = AUTO_RESOLVE,
                      _scalar: type[SCALAR] = AUTO_RESOLVE, _state: STATE = None) \
        -> TSB[NpRollingWindowResult]:
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
    pos = (start+length-1) % capacity
    buffer[pos] = ts.value
    index[pos] = ts.last_modified_time
    if length == capacity or (min_window_period is not None and length >= min_window_period):
        b = np.roll(buffer, -start)
        i = np.roll(index, -start)
        if length != capacity:
            b = b[:length]
            i = i[:length]
        return {'buffer': b, 'index': i}


@np_rolling_window.start
def np_rolling_window_start(_sz: type[SIZE], _scalar: type[SCALAR], _state: STATE):
    _state.capacity = _sz.SIZE
    if _state.capacity < 1:
        raise RuntimeError('Period must be at least 1')
    _state.buffer = np.array([_scalar()] * _sz.SIZE)
    _state.index = np.array([MIN_DT] * _sz.SIZE)
    _state.start = 0
    _state.length = 0


@compute_node
def np_quantile(ts: TS[Array[NUMBER]], q: float, method: str = 'linear') -> TS[float]:
    """The np.quantile function, limited to a single axis"""
    return np.quantile(ts.value, q, method=method)
