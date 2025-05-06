from datetime import timedelta

from hgraph import (
    TS,
    graph,
    NUMBER,
    AUTO_RESOLVE,
    operator,
    take,
    drop,
    lag,
    INT_OR_TIME_DELTA,
    sample,
    window,
    sum_,
    default,
)

__all__ = ("rolling_window", "rolling_average")

# Prefer window
rolling_window = window


@operator
def rolling_average(
    ts: TS[NUMBER], period: INT_OR_TIME_DELTA, min_window_period: INT_OR_TIME_DELTA = None
) -> TS[float]:
    """
    Computes the rolling average of the time-series.
    This will either average by the number of ticks or by the time-delta.
    """


@graph(overloads=rolling_average)
def rolling_average_p_int(
    ts: TS[NUMBER], period: int, min_window_period: int = None, _tp: type[NUMBER] = AUTO_RESOLVE
) -> TS[float]:
    from hgraph import if_then_else, count, cast_, default

    lagged_ts = lag(ts, period)
    current_value = sum_(ts)
    delayed_value = sum_(lagged_ts)
    denom = float(period) if _tp is float else period

    if min_window_period:
        count_ = drop(count(take(ts, period)), min_window_period - 1)
        period_ = if_then_else(count_ < period, count_, period)
        delayed_value = default(delayed_value, sample(count_, 0.0 if _tp is float else 0))
        denom = cast_(float, period_) if _tp is float else period_

    return (current_value - delayed_value) / denom


@graph(overloads=rolling_average)
def rolling_average_p_time_delta(
    ts: TS[NUMBER], period: timedelta, min_window_period: timedelta = None, _tp: type[NUMBER] = AUTO_RESOLVE
) -> TS[float]:
    from hgraph import if_then_else, count, cast_, const

    lagged_ts = lag(ts, period)
    current_value = sum_(ts)
    delayed_value = sum_(lagged_ts)

    delayed_count = count(lagged_ts)
    if min_window_period:
        delayed_count = default(delayed_count, const(0, period))

    delta_ticks = count(ts) - delayed_count
    delta_ticks = if_then_else(delta_ticks == 0, float("NaN"), cast_(float, delta_ticks))

    delta_value = current_value - delayed_value
    return (delta_value if _tp is float else cast_(float, delta_value)) / delta_ticks
