from dataclasses import dataclass

from hgraph import graph, TS, NUMBER, compute_node, SIGNAL, TS_OUT, CompoundScalar, STATE

__all__ = ("diff", "count", "clip", "ewma")


@graph
def diff(ts: TS[NUMBER]) -> TS[NUMBER]:
    """
    Computes the difference between the current value and the previous value in the time-series.
    """
    from hgraph import lag
    return ts - lag(ts, 1)


@compute_node
def count(ts: SIGNAL, _output: TS_OUT[int] = None) -> TS[int]:
    """
    Performs a running count of the number of times the time-series has ticked (i.e. emitted a value).
    """
    return _output.value + 1 if _output.valid else 1


@compute_node
def clip(ts: TS[NUMBER], min_: NUMBER, max_: NUMBER) -> TS[NUMBER]:
    v = ts.value
    if v < min_:
        return min_
    if v > max_:
        return max_
    return v


@clip.start
def clip_start(min_: NUMBER, max_: NUMBER):
    if min_ < max_:
        return
    raise RuntimeError(f"clip given min: {min_}, max: {max_}, but min is not < max")


@dataclass
class EwmaState(CompoundScalar):
    s_prev: float = None
    count: int = 0


@compute_node
def ewma(ts: TS[float], alpha: float, min_periods: int = 0, _state: STATE[EwmaState] = None) -> TS[float]:
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


@ewma.start
def ewma_start(alpha: float):
    if not (0.0 <= alpha <= 1.0):
        raise ValueError("EWMA alpha must be between 0 and 1")
