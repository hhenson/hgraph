from hgraph._types._scalar_types import DEFAULT
from hgraph._types._time_series_types import OUT, TIME_SERIES_TYPE
from hgraph._types._type_meta_data import AUTO_RESOLVE
from hgraph._wiring._decorators import operator


__all__ = ("default", "nothing", "null_sink")


@operator
def default(ts: DEFAULT[OUT], default_value: OUT) -> OUT:
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
        ts_default: TS[int] = ...
        out = default(ts, ts_default)
    ```

    :param ts: The time-series to replace missing values in.
    :param default_value: The value to replace missing values with.
    :return: The time-series with missing values replaced with default_value.
    """


@operator
def nothing(tp: type[OUT] = AUTO_RESOLVE) -> OUT:
    """
    Produces no ticks ever. This can be used in one of two ways:

    ```python
    nothing[TS[int]]()
    ```

    or

    ```python
    nothing(TS[int])
    ```

    This is equivalent to None for time-series inputs.
    """


@operator
def null_sink(ts: TIME_SERIES_TYPE):
    """
    A sink node that will consume the time-series and do nothing with it.
    This is useful when you want to consume a time-series but do not want to do anything with it.
    """