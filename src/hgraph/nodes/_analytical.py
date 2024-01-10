from hgraph import compute_node, TS, STATE, TIME_SERIES_TYPE, graph, TSL, SIZE, NUMBER, AUTO_RESOLVE, reduce, add_, TSD, \
    SCALAR
from hgraph.nodes._operators import cast_, len_

__all__ = ("ewma", "center_of_mass_to_alpha", "span_to_alpha", "mean", "clip")


@compute_node
def ewma(ts: TS[float], alpha: float, min_periods: int = 0, _state: STATE = None) -> TS[float]:
    """Exponential Weighted Moving Average"""
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
def ewma_start(alpha: float, _state: STATE):
    if 0.0 <= alpha <= 1.0:
        _state.s_prev = None
        _state.count  = 0
    else:
        raise ValueError("alpha must be between 0 and 1")


def center_of_mass_to_alpha(com: float) -> float:
    if com <= 0:
        raise ValueError(f"Center of mass must be positive, got {com}")
    return 1.0 / (com + 1.0)


def span_to_alpha(span: float) -> float:
    if span <= 0:
        raise ValueError(f"Span must be positive, got {span}")
    return 2.0 / (span + 1.0)


@graph
def mean(ts: TIME_SERIES_TYPE) -> TS[float]:
    """
    The mean of the values at point in time.
    For example:

        ``mean(TSL[TS[float], SIZE) ``

    will produce the mean of the values of the time-series list each time the list changes
    """
    raise NotImplementedError(f"No implementation found for {ts.output_type}")


@graph(overloads=mean)
def tsl_mean(ts: TSL[TS[NUMBER], SIZE], _sz: type[SIZE] = AUTO_RESOLVE,
             _num_tp: type[NUMBER] = AUTO_RESOLVE) -> TS[float]:
    numerator = reduce(add_, ts, 0.0 if _num_tp is float else 0)
    if _num_tp is int:
        numerator = cast_(float, numerator)
    return numerator / float(_sz.SIZE)


@graph(overloads=mean)
def tsd_mean(ts: TSD[SCALAR, TS[NUMBER]], _num_tp: type[NUMBER] = AUTO_RESOLVE) -> TS[float]:
    numerator = reduce(add_, ts, 0.0 if _num_tp is float else 0)
    if _num_tp is int:
        numerator = cast_(float, numerator)
    return numerator / cast_(float, len_(ts))


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
