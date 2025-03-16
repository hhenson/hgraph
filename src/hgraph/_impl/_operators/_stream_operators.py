import sys
from collections import deque
from dataclasses import dataclass, field
from datetime import timedelta, datetime
from typing import Mapping, Generic

from hgraph import (
    TSW,
    WINDOW_SIZE,
    WINDOW_SIZE_MIN,
    CompoundScalar,
    INT_OR_TIME_DELTA,
    MIN_TD,
    REF,
    REMOVE_IF_EXISTS,
    SCALAR,
    SCHEDULER,
    SIGNAL,
    SIZE,
    STATE,
    TIME_SERIES_TYPE,
    TS,
    TSB,
    TSL,
    WindowResult,
    batch,
    to_window,
    compute_node,
    dedup,
    drop,
    filter_,
    gate,
    generator,
    graph,
    lag,
    resample,
    sample,
    schedule,
    slice_,
    step,
    take,
    throttle,
    window,
    WindowSize,
    EvaluationClock,
    MIN_DT,
    count,
    MAX_DT,
    TSD,
    map_,
    TS_SCHEMA,
    AUTO_RESOLVE,
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
def lag_tick(ts: TIME_SERIES_TYPE, period: int, _state: STATE[LagState] = None) -> TIME_SERIES_TYPE:
    buffer: deque[SCALAR] = _state.buffer
    try:
        if len(buffer) == period:
            return buffer.popleft()
    finally:
        buffer.append(ts.delta_value)


@lag_tick.start
def lag_tick_start(period: int, _state: STATE[LagState]):
    _state.buffer = deque[SCALAR](maxlen=period)


@compute_node(overloads=lag)
def lag_timedelta(
    ts: TIME_SERIES_TYPE, period: timedelta, _scheduler: SCHEDULER = None, _state: STATE[LagState] = None
) -> TIME_SERIES_TYPE:
    # Uses the scheduler to keep track of when to deliver the values recorded in the buffer.
    buffer: deque[SCALAR] = _state.buffer
    if ts.modified:
        buffer.append(ts.delta_value)
        _scheduler.schedule(ts.last_modified_time + period)

    if _scheduler.is_scheduled_now:
        return buffer.popleft()


@lag_timedelta.start
def lag_timedelta_start(_state: STATE[LagState]):
    _state.buffer = deque[SCALAR]()


@generator(overloads=schedule)
def schedule_scalar(delay: timedelta, initial_delay: bool = True, max_ticks: int = sys.maxsize) -> TS[bool]:
    initial_timedelta = delay if initial_delay else timedelta()
    yield (initial_timedelta, True)
    for _ in range(max_ticks - 1):
        yield (delay, True)


@dataclass
class TickCount(CompoundScalar):
    count: int = 0


@compute_node(overloads=schedule)
def schedule_ts(
    delay: TS[timedelta],
    *,
    start: TS[datetime] = None,
    initial_delay: bool = True,
    use_wall_clock: bool = False,
    max_ticks: int = sys.maxsize,
    _scheduler: SCHEDULER = None,
    _state: STATE[TickCount] = None,
    _clock: EvaluationClock = None,
) -> TS[bool]:
    if _state.count == max_ticks and not start.modified:
        return

    scheduled = _scheduler.is_scheduled_now

    if use_wall_clock:
        now = _clock.now
    else:
        now = _clock.evaluation_time

    if start.valid:
        if now < start.value and not initial_delay:
            _scheduler.schedule(start.value, "_", on_wall_clock=use_wall_clock)
        else:
            next = (1 + (max(now, start.value) - start.value) // delay.value) * delay.value + start.value
            _scheduler.schedule(next, "_", on_wall_clock=use_wall_clock)

        if start.modified:
            _state.count = 0  # Reset the count if the start time changes
    else:
        _scheduler.schedule(delay.value, "_", on_wall_clock=use_wall_clock)

    if (delay.modified and not initial_delay) or (scheduled and not delay.modified):
        if _state.count < max_ticks:
            _state.count += 1
            return True


@graph(overloads=resample)
def resample_default(ts: TIME_SERIES_TYPE, period: timedelta) -> TIME_SERIES_TYPE:
    return sample(schedule(period), ts)


@compute_node(overloads=dedup)
def dedup_default(ts: TIME_SERIES_TYPE, _output: TIME_SERIES_TYPE = None) -> TIME_SERIES_TYPE:
    """
    Drops duplicate values from a time-series.
    """
    from multimethod import multimethod
    from hgraph import PythonTimeSeriesValueInput
    from hgraph import PythonTimeSeriesDictInput

    @multimethod
    def dedup_item(input, output):
        return {
            k_new: v_new
            for k_new, v_new in ((k, dedup_item(v, output[k])) for k, v in input.modified_items())
            if v_new is not None
        }

    @dedup_item.register
    def dedup_dicts(input: PythonTimeSeriesDictInput, output):
        out = {
            k_new: v_new
            for k_new, v_new in ((k, dedup_item(v, output.get_or_create(k))) for k, v in input.modified_items())
            if v_new is not None
        }
        return out | {k: REMOVE_IF_EXISTS for k in input.removed_keys()}

    @dedup_item.register
    def dedup_value(input: PythonTimeSeriesValueInput, output):
        if output.valid:
            if input.value != output.value:
                return input.delta_value
        else:
            return input.value

    return dedup_item(ts, _output)


@compute_node(overloads=dedup)
def dedup_float(ts: TS[float], abs_tol: TS[float] = 1e-15, _output: TS[float] = None) -> TS[float]:
    """
    Drops 'duplicate' float values from a time-series which are almost equal
    """
    if _output.valid:
        abs_tol = abs_tol.value
        if not (-abs_tol < ts.value - _output.value < abs_tol):
            return ts.delta_value
    else:
        return ts.value


@compute_node(overloads=filter_)
def filter_default(condition: TS[bool], ts: TIME_SERIES_TYPE, _output: TIME_SERIES_TYPE = None) -> TIME_SERIES_TYPE:
    if condition.value:
        if condition.modified:
            if _output.last_modified_time < ts.last_modified_time:
                _output.copy_from_input(ts)
                return None

        if ts.modified:
            return ts.delta_value


@dataclass
class _ThrottleState(CompoundScalar):
    tick: dict = field(default_factory=dict)


@compute_node(overloads=throttle)
def throttle_default(
    ts: TIME_SERIES_TYPE,
    period: TS[timedelta],
    delay_first_tick: bool = False,
    use_wall_clock: bool = False,
    _sched: SCHEDULER = None,
    _state: STATE[_ThrottleState] = None,
) -> TIME_SERIES_TYPE:
    from multimethod import multimethod
    from hgraph import PythonTimeSeriesValueInput
    from hgraph import PythonTimeSeriesDictInput

    @multimethod
    def collect_tick(input, out):
        return {
            k_new: v_new
            for k_new, v_new in ((k, collect_tick(v, out.setdefault(k, dict()))) for k, v in input.modified_items())
            if v_new is not None
        }

    @collect_tick.register
    def collect_dict(input: PythonTimeSeriesDictInput, out):
        out |= {
            k_new: v_new
            for k_new, v_new in ((k, collect_tick(v, out.setdefault(k, dict()))) for k, v in input.modified_items())
            if v_new is not None
        }
        return out | {k: REMOVE_IF_EXISTS for k in input.removed_keys()}

    @collect_tick.register
    def collect_value(input: PythonTimeSeriesValueInput, out):
        return input.value

    if ts.modified:
        if _sched.is_scheduled:
            _state.tick = collect_tick(ts, _state.tick)
        elif delay_first_tick:
            _state.tick = collect_tick(ts, _state.tick)
            _sched.schedule(period.value)
        else:
            _state.tick = {}
            _sched.schedule(period.value)
            return ts.delta_value

    if _sched.is_scheduled_now:
        if tick := _state.tick:
            _state.tick = {}
            _sched.schedule(period.value, on_wall_clock=use_wall_clock)
            return tick


@dataclass
class CounterState(CompoundScalar):
    count: int = 0


@compute_node(overloads=take)
def take_by_count(ts: TIME_SERIES_TYPE, count: int = 1, state: STATE[CounterState] = None) -> TIME_SERIES_TYPE:
    if count == 0:
        ts.make_passive
    else:
        state.count += 1
        c = state.count
        if c == count:
            ts.make_passive()
        return ts.delta_value


@dataclass
class TimeState(CompoundScalar):
    time: datetime = MIN_DT


@compute_node(overloads=take)
def take_by_time(ts: TIME_SERIES_TYPE, count: timedelta = MIN_TD, state: STATE[TimeState] = None) -> TIME_SERIES_TYPE:
    if state.time == MIN_DT:  # First tick
        state.time = ts.last_modified_time

    if ts.last_modified_time - state.time > count:
        ts.make_passive()
    else:
        return ts.delta_value


@graph(overloads=drop)
def drop_default(ts: TIME_SERIES_TYPE, count: int = 1) -> TIME_SERIES_TYPE:
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


@compute_node(
    overloads=to_window,
    resolvers={
        WINDOW_SIZE: lambda m, s: WindowSize[s["period"]],
        WINDOW_SIZE_MIN: lambda m, s: WindowSize[
            s["min_window_period"] if s["min_window_period"] is not None else s["period"]
        ],
    },
)
def to_window_impl(
    ts: TS[SCALAR], period: INT_OR_TIME_DELTA, min_window_period: INT_OR_TIME_DELTA = None
) -> TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN]:
    """
    Basic implementation of the `to_window` operator.
    """
    return ts.value


@compute_node(overloads=gate, valid=("ts",))
def gate_default(
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


@gate_default.start
def gate_default_start(_state: STATE):
    _state.buffer = deque()


@compute_node(overloads=batch)
def batch_default(
    condition: TS[bool],
    ts: TS[SCALAR],
    delay: timedelta,
    buffer_length: int = sys.maxsize,
    _state: STATE = None,
    _sched: SCHEDULER = None,
) -> TS[tuple[SCALAR, ...]]:
    if ts.modified:
        if len(_state.buffer) < buffer_length:
            _state.buffer.append(ts.delta_value)
        else:
            raise RuntimeError(f"Buffer overflow when adding {ts.delta_value} to batch buffer")

    if condition.value is not True:
        return

    if not _sched.is_scheduled and not condition.modified:  # only schedule on data ticks
        _sched.schedule(delay)

    if (_sched.is_scheduled_now or condition.modified) and _state.buffer:
        out = tuple(_state.buffer)
        _state.buffer.clear()
        return out


@batch_default.start
def batch_default_start(_state: STATE):
    _state.buffer = []  #


@compute_node(overloads=step)
def step_default(ts: TIME_SERIES_TYPE, step_size: int = 1, _state: STATE[CounterState] = None) -> TIME_SERIES_TYPE:
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


@dataclass
class _LagProxyState(CompoundScalar, Generic[SCALAR]):
    cache: Mapping[int, SCALAR] = field(default_factory=dict)


@compute_node(active=("ts",), valid=("ts", "c"))
def _lag_proxy(ts: TS[SCALAR], c: TS[int], lag_c: TS[int], _state: STATE[_LagProxyState[SCALAR]] = None) -> TS[SCALAR]:
    cache = _state.cache
    if ts.modified:
        lag_c.make_active()
        cache[c.value] = ts.value

    if lag_c.modified:
        out = cache.pop(lag_c.value, None)
        if len(cache) == 0:
            lag_c.make_passive()
        return out


@graph(overloads=lag)
def lag_proxy(ts: TS[SCALAR], period: int, proxy: SIGNAL) -> TS[SCALAR]:
    """
    Lag the value of ts to be delayed by the number of periods defined by the proxy ticking.
    If the proxy has not ticked, we are not lagging.
    If the ts value ticks multiple times for a single tick of the proxy, we return the last value
    only.
    """
    c = count(proxy)
    lag_c = lag(c, period)
    return _lag_proxy(ts, c, lag_c)


@graph(overloads=lag)
def lag_proxy_tsd(ts: TSD[SCALAR, TIME_SERIES_TYPE], period: int, proxy: SIGNAL) -> TSD[SCALAR, TIME_SERIES_TYPE]:
    """
    Lag the value of ts to be delayed by the number of periods defined by the proxy ticking.
    If the proxy has not ticked, we are not lagging.
    If the ts value ticks multiple times for a single tick of the proxy, we return the last value
    only.
    """
    return map_(lag_proxy, ts, period, proxy)


@graph(overloads=lag)
def lag_proxy_tsb(
    ts: TSB[TS_SCHEMA], period: int, proxy: SIGNAL, _tp: type[TS_SCHEMA] = AUTO_RESOLVE
) -> TSB[TS_SCHEMA]:
    """
    Lag the value of ts to be delayed by the number of periods defined by the proxy ticking.
    If the proxy has not ticked, we are not lagging.
    If the ts value ticks multiple times for a single tick of the proxy, we return the last value
    only.
    """
    return TSB[_tp].from_ts(**{k: lag(v, period, proxy) for k, v in ts.as_dict().items()})
