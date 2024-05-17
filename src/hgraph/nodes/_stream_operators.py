import sys
from collections import deque
from datetime import timedelta

from hgraph import compute_node, TIME_SERIES_TYPE, STATE, SCHEDULER, SIGNAL, EvaluationClock, generator, TS, graph

__all__ = ("sample", "delay", "signal")


@compute_node(active=('signal',))
def sample(signal: SIGNAL, ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """Samples the value from ts whenever the signal ticks."""
    return ts.value


@compute_node
def delay(ts: TIME_SERIES_TYPE, delay: timedelta, sched: SCHEDULER = None, ec: EvaluationClock = None, state: STATE = None) -> TIME_SERIES_TYPE:
    """Delays the time-series by the given delay."""
    if ts.modified:
        state.queue.append(ts.delta_value)
        sched.schedule(delay, tag=ec.evaluation_time.isoformat())

    if sched.is_scheduled_now:
        return state.queue.popleft()


@delay.start
def delay_start(state: STATE):
    state.queue = deque()


@generator
def signal(delay: timedelta, initial_delay: bool = True, max_ticks: int = sys.maxsize) -> TS[bool]:
    initial_timedelta = delay if initial_delay else timedelta()
    yield(initial_timedelta, True)
    for _ in range(max_ticks - 1):
        yield (delay, True)
