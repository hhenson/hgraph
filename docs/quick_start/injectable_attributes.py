## STATE

from hgraph import compute_node, TS, STATE, MIN_TD, sink_node, TIME_SERIES_TYPE, TS_OUT
from hgraph.test import eval_node


@compute_node
def window(ts: TS[int], size: int, _state: STATE = None) -> TS[tuple[int, ...]]:
    window = _state.window
    window.append(ts.value)
    if len(window) == size:
        return tuple(window)


@window.start
def window_start(size: int, _state: STATE) -> STATE[int]:
    from collections import deque
    _state.window = deque(maxlen=size)


print(eval_node(window, ts=[1, 2, 3, 4, 5], size=3))


## SCHEDULER

from hgraph import compute_node, TS, SCHEDULER


@compute_node
def lag(ts: TS[int], _scheduler: SCHEDULER = None, _state: STATE = None) -> TS[int]:
    """Lag the input by one time step."""
    out = None
    if _scheduler.is_scheduled:
        out = _state.last_value

    if ts.modified:
        _scheduler.schedule(ts.last_modified_time + MIN_TD)
        _state.last_value = ts.value

    return out


@lag.start
def lag_start(_state: STATE):
    _state.last_value = None


print("SCHEDULE", eval_node(lag, ts=[1, 2, 3, 4, 5]))


## EvaluationClock

from hgraph import compute_node, TS, EvaluationClock


@sink_node
def print_tick_time(ts: TIME_SERIES_TYPE, _clock: EvaluationClock = None):
    print("Tick time: ", _clock.evaluation_time)
    print("Now: ", _clock.now)
    print("Cycle Time: ", _clock.cycle_time)


eval_node(print_tick_time, ts=[1, 2, 3, 4, 5])


## EvaluationEngineApi

from hgraph import compute_node, TS, EvaluationEngineApi


@compute_node
def register_hooks(ts: TS[int], _engine: EvaluationEngineApi = None) -> TS[int]:
    _engine.add_after_evaluation_notification(lambda: print(f"After evaluation [{ts.value}]"))
    _engine.add_before_evaluation_notification(lambda: print(f"Before evaluation [{ts.value}]"))
    return ts.value


eval_node(register_hooks, ts=[1, 2, 3])


## _output


@compute_node
def sum_(ts: TS[int], _output: TS_OUT[int] = None) -> TS[int]:
    return _output.value + ts.value if _output.valid else ts.value


print(eval_node(sum_, ts=[1, 2, 3, 4, 5]))


## LOGGER

from hgraph import LOGGER

@sink_node
def log(ts: TS[int], _logger: LOGGER = None):
    _logger.info(f"Logging: {ts.value}")

eval_node(log, ts=[1, 2, 3, 4, 5])
