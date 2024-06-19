from dataclasses import dataclass

from hgraph import graph, TS, NUMBER, compute_node, SIGNAL, TS_OUT, CompoundScalar, STATE, diff, count, clip, ewma

__all__ = tuple()


@graph(overloads=diff)
def diff_number(ts: TS[NUMBER]) -> TS[NUMBER]:
    """
    Computes the difference between the current value and the previous value in the time-series.
    """
    from hgraph import lag

    return ts - lag(ts, 1)


@compute_node(overloads=count)
def count_impl(ts: SIGNAL, _output: TS_OUT[int] = None) -> TS[int]:
    """
    Performs a running count of the number of times the time-series has ticked (i.e. emitted a value).
    """
    return _output.value + 1 if _output.valid else 1


@compute_node(overloads=clip)
def clip_number(ts: TS[NUMBER], min_: NUMBER, max_: NUMBER) -> TS[NUMBER]:
    v = ts.value
    if v < min_:
        return min_
    if v > max_:
        return max_
    return v


@clip_number.start
def clip_number_start(min_: NUMBER, max_: NUMBER):
    if min_ < max_:
        return
    raise RuntimeError(f"clip given min: {min_}, max: {max_}, but min is not < max")


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
