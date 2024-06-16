from datetime import timedelta
from typing import Type

from hgraph import generator, SCALAR, TIME_SERIES_TYPE, TS, compute_node, graph, REF, AUTO_RESOLVE, EvaluationEngineApi

__all__ = ("default", "nothing")


@graph
def default(ts: TIME_SERIES_TYPE, default_value: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    Returns the time-series ts with any missing values replaced with default_value.

    Example:
    ```python

        ts = ...
        out = default(ts, 0)  # Note the ability to pass in a constant value.
    ```

    another example:
    ```python
        ts: TS[int] = ...
        ts_default: TS[int]  = ...
        out = default(ts, ts_default)
    ```

    :param ts: The time-series to replace missing values in.
    :param default_value: The value to replace missing values with.
    :return: The time-series with missing values replaced with default_value.
    """
    return _default(ts, ts, default_value)


@compute_node(valid=tuple())
def _default(
    ts_ref: REF[TIME_SERIES_TYPE], ts: TIME_SERIES_TYPE, default_value: REF[TIME_SERIES_TYPE]
) -> REF[TIME_SERIES_TYPE]:
    if not ts.valid:
        # In case this has become invalid, we need to make sure we detect a tick from the real value.
        ts.make_active()
        return default_value.value
    else:
        ts.make_passive()
        return ts_ref.value


@generator
def nothing(tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE) -> TIME_SERIES_TYPE:
    """
    Produces no ticks ever

    :param tp: Used to resolve the correct type for the output, by default this is TS[SCALAR] where SCALAR is the type
               of the value.
    :return: A time series that will never tick
    """
    yield from ()
