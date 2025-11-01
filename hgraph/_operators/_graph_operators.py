import logging

from hgraph._runtime._evaluation_engine import EvaluationEngineApi
from hgraph._types._scalar_types import DEFAULT, LOGGER
from hgraph._types._ts_type import TS
from hgraph._types._tsb_type import TSB, TS_SCHEMA, TS_SCHEMA_1
from hgraph._types._time_series_types import OUT, TIME_SERIES_TYPE, SIGNAL
from hgraph._types._type_meta_data import AUTO_RESOLVE
from hgraph._wiring._decorators import operator, sink_node, compute_node

__all__ = ("default", "nothing", "null_sink", "print_", "log_", "assert_", "stop_engine", "pass_through_node")


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
def nothing(tp: type[OUT] = AUTO_RESOLVE) -> DEFAULT[OUT]:
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
def print_(format_str: TS[str], *args: TSB[TS_SCHEMA], __std_out__: bool = True, **kwargs: TSB[TS_SCHEMA_1]):
    """
    A sink node that will write the formatted string to the std out.
    This should be generally be used for debugging purposes and not be present in production code, instead use the
    log nodes for writing in a production context.

    :param format_str: The format string as defined in format
    :param args: The time-series enumerated inputs
    :param kwargs: The named time-series inputs
    """


@operator
def log_(
    format_str: TS[str],
    *args: TSB[TS_SCHEMA],
    level: int = logging.INFO,
    sample_count: int = 1,
    **kwargs: TSB[TS_SCHEMA_1],
):
    """
    A sink node that will log the formatted string to the system logger.

    :param format_str: The format string as defined in format
    :param level: The logging level
    :param args: The time-series enumerated inputs
    :param sample_count: The frequency to sample the log values
    :param kwargs: The named time-series inputs
    """


@sink_node
def stop_engine(ts: SIGNAL, msg: str = "Stopping", _engine: EvaluationEngineApi = None, _logger: LOGGER = None):
    """Stops the engine"""
    _logger.info(f"[{_engine.evaluation_clock.now}][{_engine.evaluation_clock.evaluation_time}] stop_engine: {msg}")
    _engine.request_engine_stop()


@operator
def assert_(condition: TS[bool], error_msg: str):
    """
    Asserts that the condition is True, if not raises an AssertionError with the error message.
    """


@compute_node
def pass_through_node(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """Just passes the value through, this is useful for testing and for rank adjustment"""
    return ts.delta_value
