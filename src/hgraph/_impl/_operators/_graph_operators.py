import logging
import sys
from dataclasses import dataclass

from hgraph import if_true
from hgraph._operators._analytical_operators import count
from hgraph._operators._graph_operators import default, nothing, null_sink, debug_print, print_, log_, assert_
from hgraph._operators._string import format_
from hgraph._operators._stream import sample
from hgraph._runtime._evaluation_clock import EvaluationClock
from hgraph._types._ref_type import REF
from hgraph._types._scalar_types import CompoundScalar, STATE, LOGGER, DEFAULT
from hgraph._types._time_series_types import OUT, TIME_SERIES_TYPE
from hgraph._types._ts_type import TS
from hgraph._types._tsb_type import TSB, TS_SCHEMA, TS_SCHEMA_1
from hgraph._types._type_meta_data import AUTO_RESOLVE
from hgraph._wiring._decorators import generator, graph, compute_node, sink_node

__all__ = tuple()


@graph(overloads=default)
def default_impl(ts: OUT, default_value: OUT) -> OUT:
    """
    takes the imports, and converts them to references in the compute_node, this makes the solution more
    efficient.
    """
    return _default(ts, ts, default_value)


@compute_node(valid=tuple())
def _default(ts_ref: REF[OUT], ts: OUT, default_value: REF[OUT]) -> REF[OUT]:
    if not ts.valid:
        # In case this has become invalid, we need to make sure we detect a tick from the real value.
        ts.make_active()
        return default_value.value
    else:
        ts.make_passive()
        return ts_ref.value


@generator(overloads=nothing)
def nothing_impl(tp: type[OUT] = AUTO_RESOLVE) -> DEFAULT[OUT]:
    """
    Produces no ticks ever

    :param tp: Used to resolve the correct type for the output, by default this is TS[SCALAR] where SCALAR is the type
               of the value.
    :return: A time series that will never tick
    """
    yield from ()


@sink_node(overloads=null_sink)
def null_sink_impl(ts: TIME_SERIES_TYPE):
    """
    A sink node that will consume the time-series and do nothing with it.
    This is useful when you want to consume a time-series but do not want to do anything with it.
    """


@dataclass
class _DebugPrintState(CompoundScalar):
    count: int = 0


@sink_node(overloads=debug_print)
def debug_print_impl(
    label: str,
    ts: TIME_SERIES_TYPE,
    print_delta: bool = True,
    sample: int = -1,
    _clock: EvaluationClock = None,
    _state: STATE[_DebugPrintState] = None,
):
    """
    Use this to help debug code, this will print the value of the supplied time-series to the standard out.
    It will include the engine time in the print. Do not leave these lines in production code.

    :param label: The label to print before the value
    :param ts: The time-series to print
    :param print_delta: If true, print the delta value, otherwise print the value
    :param sample: Only print an output for every sample number of ticks.
    :param _clock: The evaluation clock (to be injected)
    """
    _state.count += 1
    if sample < 2 or _state.count % sample == 0:
        if print_delta:
            value = ts.delta_value
        else:
            value = ts.value
        if sample > 1:
            count = f"[{_state.count}]"
        else:
            count = ""
        print(f"[{_clock.now}][{_clock.evaluation_time}]{count} {label}: {value}")


@graph(overloads=print_)
def print_impl(format_str: TS[str], *args: TSB[TS_SCHEMA], __std_out__: bool = True, **kwargs: TSB[TS_SCHEMA_1]):
    """
    A sink node that will write the formatted string to the std out.
    This should be generally be used for debugging purposes and not be present in production code, instead use the
    log nodes for writing in a production context.

    :param format_str: The format string as defined in format
    :param args: The time-series enumerated inputs
    :param kwargs: The named time-series inputs
    """
    if kwargs is not None:
        kwargs = kwargs.as_dict()
    if args is None:
        if kwargs:
            return _print(format_(format_str, **kwargs), std_out=__std_out__)
        else:
            return _print(format_str, std_out=__std_out__)
    else:
        if kwargs:
            return _print(format_(format_str, *args, **kwargs), std_out=__std_out__)
        else:
            return _print(format_(format_str, *args), std_out=__std_out__)


@sink_node
def _print(ts: TS[str], std_out: bool = True):
    """
    A sink node that will write the string time-series to the standard out.

    :param ts: The string to write to the std out.
    :param std_out: If true, print to std out else std err.
    """
    if std_out:
        print(ts.value)
    else:
        print(ts.value, file=sys.stderr)


@graph(overloads=log_)
def log_impl(
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
    :param sample_count: The number of samples to log, a number greater than 1 will cause this to only log at the frequency specified.
    :param kwargs: The named time-series inputs
    """
    if kwargs is not None:
        kwargs = kwargs.as_dict()
    if args is None:
        if kwargs:
            msg = format_(format_str, **kwargs)
        else:
            msg = format_str
    else:
        if kwargs:
            msg = format_(format_str, *args, **kwargs)
        else:
            msg = format_(format_str, *args)

    raw_msg = msg
    if sample_count > 1:
        msg = sample(if_true((count(msg) % sample_count) == 0), msg)

    _log(msg, level, raw_msg, final_value=sample_count > 1)


@sink_node
def _log(ts: TS[str], level: int, raw_ts: REF[TS[str]], final_value: bool = False, logger: LOGGER = None):
    logger.log(level, "[%s] %s", ts.last_modified_time, ts.value)


@_log.stop
def _log_stop(ts: TS[str], level: int, raw_ts: REF[TS[str]], final_value: bool, logger: LOGGER):
    if final_value and raw_ts.valid and raw_ts.value.has_output:
        out = raw_ts.value.output
        logger.log(level, "[%s] Final value: %s", out.last_modified_time, out.value)


@sink_node(overloads=assert_)
def assert_default(condition: TS[bool], error_msg: str):
    if not condition.value:
        raise AssertionError(error_msg)


@sink_node(overloads=assert_)
def assert_default(condition: TS[bool], error_msg: TS[str]):
    if condition.modified and not condition.value:
        raise AssertionError(error_msg.value)


@graph(overloads=assert_)
def assert_format(condition: TS[bool], error_msg: str, *args: TSB[TS_SCHEMA], **kwargs: TSB[TS_SCHEMA_1]):
    from hgraph import sample, if_true

    do_format = if_true(condition == False)
    assert_(condition, format_(error_msg, __pos_args__=sample(do_format, args), __kw_args__=sample(do_format, kwargs)))
