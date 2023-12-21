from collections import deque
from datetime import timedelta

from hgraph import compute_node, TIME_SERIES_TYPE, MIN_TD, STATE, SCHEDULER, SIGNAL


__all__ = ("sample",)


@compute_node(active=('signal',))
def sample(signal: SIGNAL, ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """Samples the value from ts whenever the signal ticks."""
    return ts.value
