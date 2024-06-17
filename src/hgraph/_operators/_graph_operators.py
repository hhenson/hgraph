import logging

from hgraph._types._scalar_types import DEFAULT
from hgraph._types._ts_type import TS
from hgraph._types._tsb_type import TSB, TS_SCHEMA, TS_SCHEMA_1
from hgraph._types._time_series_types import OUT, TIME_SERIES_TYPE
from hgraph._types._type_meta_data import AUTO_RESOLVE
from hgraph._wiring._decorators import operator


__all__ = ("default", "nothing", "null_sink", "debug_print", "print_", "log_")


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


@operator
def debug_print(
    label: str,
    ts: TIME_SERIES_TYPE,
    print_delta: bool = True,
    sample: int = -1
):
    """
    Use this to help debug code, this will print the value of the supplied time-series to the standard out.
    It will include the engine time in the print. Do not leave these lines in production code.

    :param label: The label to print before the value
    :param ts: The time-series to print
    :param print_delta: If true, print the delta value, otherwise print the value
    :param sample: Only print an output for every sample number of ticks.
    """


@operator
def print_(format_str: TS[str], *args: TSB[TS_SCHEMA], __std_out__: bool = True, **kwargs:TSB[TS_SCHEMA_1]):
    """
    A sink node that will write the formatted string to the std out.
    This should be generally be used for debugging purposes and not be present in production code, instead use the
    log nodes for writing in a production context.

    :param format_str: The format string as defined in format
    :param args: The time-series enumerated inputs
    :param kwargs: The named time-series inputs
    """


@operator
def log_(format_str: TS[str], *args: TSB[TS_SCHEMA], level: int = logging.INFO, **kwargs: TSB[TS_SCHEMA_1]):
    """
    A sink node that will log the formatted string to the system logger.

    :param format_str: The format string as defined in format
    :param level: The logging level
    :param args: The time-series enumerated inputs
    :param kwargs: The named time-series inputs
    """