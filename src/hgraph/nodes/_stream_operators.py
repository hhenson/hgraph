from collections import deque
from datetime import timedelta

from hgraph import compute_node, TIME_SERIES_TYPE, MIN_TD, STATE, SCHEDULER


__all__ = ("lag_ts",)


@compute_node
def lag_ts(ts: TIME_SERIES_TYPE, delay: timedelta = MIN_TD, scheduler: SCHEDULER = None, state: STATE = None) -> TIME_SERIES_TYPE:
    """
    Delays a time-series by the specified amount of time.
    """
    if ts.modified:
        # Add next scheduled time and value to the queue
        state.delayed_values.append(ts.delta_value)
        scheduler.schedule(ts.last_modified_time + delay)

    if scheduler.is_scheduled_now:
        value = state.delayed_values.popleft()
        return value


@lag_ts.start
def _delay_ts_start(state: STATE = None):
    """
    Delays a time-series by the specified amount of time.
    """
    state.delayed_values = deque()
