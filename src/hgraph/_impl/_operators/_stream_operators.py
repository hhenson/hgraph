import sys
from collections import deque
from dataclasses import dataclass
from datetime import timedelta, datetime

from hgraph import (
    compute_node,
    TIME_SERIES_TYPE,
    STATE,
    SCHEDULER,
    SIGNAL,
    generator,
    TS,
    schedule,
    sample,
    MIN_TD,
    graph,
    resample,
    dedup,
    filter_,
    throttle,
    SCALAR,
    take,
    CompoundScalar,
    REF,
    drop,
    window,
    WindowResult,
    TSB,
    gate,
    batch,
    step,
    slice_,
    lag,
    TSL,
    SIZE,
    INT_OR_TIME_DELTA,
)

__all__ = ()


@compute_node(overloads=sample, active=("signal",))
def sample_default(signal: SIGNAL, ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    Samples the value from ts whenever the signal ticks
    """
    return ts.value


@graph(overloads=lag)
def lag_tsl(ts: TSL[TIME_SERIES_TYPE, SIZE], period: INT_OR_TIME_DELTA) -> TSL[TIME_SERIES_TYPE, SIZE]:
    return TSL.from_ts(lag(ts_, period) for ts_ in ts.values())


@dataclass
class LagState:
    buffer: deque = None


@compute_node(overloads=lag)
def lag_tick(ts: TS[SCALAR], period: int, _state: STATE[LagState] = None) -> TS[SCALAR]:
    buffer: deque[SCALAR] = _state.buffer
    try:
        if len(buffer) == period:
            return buffer.popleft()
    finally:
        buffer.append(ts.value)


@lag_tick.start
def lag_tick_start(period: int, _state: STATE[LagState]):
    _state.buffer = deque[SCALAR](maxlen=period)


@compute_node(overloads=lag)
def lag_timedelta(
    ts: TS[SCALAR], period: timedelta, _scheduler: SCHEDULER = None, _state: STATE[LagState] = None
) -> TS[SCALAR]:
    # Uses the scheduler to keep track of when to deliver the values recorded in the buffer.
    buffer: deque[SCALAR] = _state.buffer
    if ts.modified:
        buffer.append(ts.value)
        _scheduler.schedule(ts.last_modified_time + period)

    if _scheduler.is_scheduled_now:
        return buffer.popleft()


@lag_timedelta.start
def lag_timedelta_start(_state: STATE[LagState]):
    _state.buffer = deque[SCALAR]()


@generator(overloads=schedule)
def schedule(delay: timedelta, initial_delay: bool = True, max_ticks: int = sys.maxsize) -> TS[bool]:
    initial_timedelta = delay if initial_delay else timedelta()
    yield (initial_timedelta, True)
    for _ in range(max_ticks - 1):
        yield (delay, True)


@graph(overloads=resample)
def resample(ts: TIME_SERIES_TYPE, delay: timedelta) -> TIME_SERIES_TYPE:
    return sample(schedule(delay), ts)


@compute_node(overloads=dedup)
def drop_dups_default(ts: TIME_SERIES_TYPE, _output: TIME_SERIES_TYPE = None) -> TIME_SERIES_TYPE:
    """
    Drops duplicate values from a time-series.
    """
    if _output.valid:
        if ts.value != _output.value:
            return ts.delta_value
    else:
        return ts.value


@compute_node(overloads=dedup)
def drop_dups_float(ts: TS[float], abs_tol: float = 1e-15, _output: TS[float] = None) -> TS[float]:
    """
    Drops 'duplicate' float values from a time-series which are almost equal
    """
    if _output.valid:
        if not (-abs_tol < ts.value - _output.value < abs_tol):
            return ts.delta_value
    else:
        return ts.value


@compute_node(overloads=filter_)
def filter_(condition: TS[bool], ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    if condition.value:
        return ts.value if condition.modified else ts.delta_value


@compute_node(overloads=throttle)
def throttle(ts: TIME_SERIES_TYPE, period: timedelta, sched: SCHEDULER = None, state: STATE = None) -> TIME_SERIES_TYPE:
    if ts.modified:
        state.queue.append(ts.value)
    if sched.is_scheduled_now:
        sched.schedule(period)
        if state.queue:
            return state.queue.popleft()


@throttle.start
def throttle_start(state: STATE, sched: SCHEDULER):
    state.queue = deque(maxlen=1)
    sched.schedule(MIN_TD)


@dataclass
class CounterState(CompoundScalar):
    count: int = 0


@compute_node(overloads=take)
def take(ts: TIME_SERIES_TYPE, count: int = 1, state: STATE[CounterState] = None) -> TIME_SERIES_TYPE:
    if count == 0:
        ts.make_passive
    else:
        state.count += 1
        c = state.count
        if c == count:
            ts.make_passive()
        return ts.delta_value


@graph(overloads=drop)
def drop(ts: TIME_SERIES_TYPE, count: int = 1) -> TIME_SERIES_TYPE:
    """
    Drops the first `count` ticks and then returns the remainder of the ticks
    """
    return _drop(ts, ts, count)


@compute_node(active=("ts_counter",))
def _drop(
    ts: REF[TIME_SERIES_TYPE], ts_counter: SIGNAL, count: int = 1, state: STATE[CounterState] = None
) -> REF[TIME_SERIES_TYPE]:
    state.count += 1
    if state.count > count:
        ts_counter.make_passive()
        return ts.value


@compute_node(overloads=window)
def window_cyclic_buffer(
    ts: TS[SCALAR], period: int, min_window_period: int = None, _state: STATE = None
) -> TSB[WindowResult]:
    buffer: deque[SCALAR] = _state.buffer
    index: deque[datetime] = _state.index
    buffer.append(ts.value)
    index.append(ts.last_modified_time)
    l = len(buffer)
    if l == period or (min_window_period is not None and l >= min_window_period):
        return {"buffer": tuple(buffer), "index": tuple(index)}


@window_cyclic_buffer.start
def window_cyclic_buffer_start(period: int, _state: STATE):
    _state.buffer = deque[SCALAR](maxlen=period)
    _state.index = deque[datetime](maxlen=period)


@compute_node(overloads=window)
def window_timedelta(
    ts: TS[SCALAR], period: timedelta, min_window_period: timedelta = None, _state: STATE = None
) -> TSB[WindowResult]:
    buffer: deque[SCALAR] = _state.buffer
    index: deque[datetime] = _state.index
    buffer.append(ts.value)
    index.append(ts.last_modified_time)
    delta = index[-1] - index[0]
    is_full = delta >= period
    while delta > period:
        buffer.popleft()
        index.popleft()
        delta = index[-1] - index[0]
    if is_full or (min_window_period is not None and delta >= min_window_period):
        return {"buffer": tuple(buffer), "index": tuple(index)}


@window_timedelta.start
def window_timedelta_start(_state: STATE):
    _state.buffer = deque[SCALAR]()
    _state.index = deque[datetime]()


@compute_node(overloads=gate)
def gate(
    condition: TS[bool],
    ts: TIME_SERIES_TYPE,
    delay: timedelta = MIN_TD,
    buffer_length: int = sys.maxsize,
    _state: STATE = None,
    _sched: SCHEDULER = None,
) -> TIME_SERIES_TYPE:
    if ts.modified:
        if len(_state.buffer) < buffer_length:
            _state.buffer.append(ts.delta_value)
        else:
            raise RuntimeError(f"Buffer overflow when adding {ts.delta_value} to gate buffer")

    if (condition.modified or _sched.is_scheduled_now) and condition.value and _state.buffer:
        out = _state.buffer.popleft()
        if _state.buffer:
            _sched.schedule(delay)
        return out


@gate.start
def gate_start(_state: STATE):
    _state.buffer = deque()


@compute_node(overloads=batch)
def batch(
    condition: TS[bool],
    ts: TS[SCALAR],
    delay: timedelta = MIN_TD,
    buffer_length: int = sys.maxsize,
    _state: STATE = None,
    _sched: SCHEDULER = None,
) -> TS[tuple[SCALAR, ...]]:
    if ts.modified:
        if len(_state.buffer) < buffer_length:
            _state.buffer.append(ts.delta_value)
        else:
            raise RuntimeError(f"Buffer overflow when adding {ts.delta_value} to batch buffer")
    if (condition.modified or _sched.is_scheduled_now) and condition.value and _state.buffer:
        _sched.schedule(delay)
        out = tuple(_state.buffer)
        _state.buffer.clear()
        return out


@batch.start
def batch_start(_state: STATE):
    _state.buffer = []  #


@compute_node(overloads=step)
def step(ts: TIME_SERIES_TYPE, step_size: int = 1, _state: STATE[CounterState] = None) -> TIME_SERIES_TYPE:
    out = None
    if _state.count % step_size == 0:
        out = ts.delta_value
    _state.count += 1
    return out


@graph(overloads=slice_)
def slice_tick(ts: TIME_SERIES_TYPE, start: int = 0, stop: int = 1, step_size: int = 1) -> TIME_SERIES_TYPE:
    if start == -1:
        start = sys.maxsize
    return step(drop(take(ts, stop), start), step_size)
