from dataclasses import dataclass

from hgraph import (
    NUMBER,
    SCALAR,
    SIGNAL,
    STATE,
    TS,
    TS_OUT,
    CompoundScalar,
    clip,
    compute_node,
    count,
    diff,
    ewma,
    graph,
)

__all__ = tuple()


@graph(overloads=diff)
def diff_scalar(ts: TS[SCALAR]) -> TS[SCALAR]:
    """
    Computes the difference between the current value and the previous value in the time-series.
    """
    from hgraph import lag

    return ts - lag(ts, 1)


@compute_node(overloads=count, active=("ts",), valid=("ts",))
def count_impl(ts: SIGNAL, reset: SIGNAL = None, _output: TS_OUT[int] = None) -> TS[int]:
    """
    Performs a running count of the number of times the time-series has ticked (i.e. emitted a value).
    """
    increment = _output.valid and reset.last_modified_time <= _output.last_modified_time
    return _output.value + 1 if increment else 1


@compute_node(overloads=clip)
def clip_number(ts: TS[NUMBER], min_: TS[NUMBER], max_: TS[NUMBER]) -> TS[NUMBER]:
    min_value = min_.value
    max_value = max_.value
    if (min_.modified or max_.modified) and (min_value > max_value):
        raise RuntimeError(f"clip given min: {min_.value}, max: {max_.value}, but min is not < max")

    v = ts.value
    if v < min_value:
        return min_value
    if v > max_value:
        return max_value
    return v


@dataclass
class EwmaState(CompoundScalar):
    s_prev: float = None
    count: int = 0


@compute_node(overloads=ewma)
def ewma_number(ts: TS[float], alpha: float, min_periods: int = 0, _state: STATE[EwmaState] = None) -> TS[float]:
    """
    Exponential Weighted Moving Average
    """
    x_t = ts.value
    s_prev = _state.s_prev

    if s_prev is None:
        s = x_t
    else:
        s = s_prev + alpha * (x_t - s_prev)

    _state.s_prev = s
    _state.count += 1

    if _state.count > min_periods:
        return s


@ewma_number.start
def ewma_number_start(alpha: float):
    if not (0.0 <= alpha <= 1.0):
        raise ValueError("EWMA alpha must be between 0 and 1")
