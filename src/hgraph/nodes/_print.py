import sys
import logging
from dataclasses import dataclass

from hgraph import sink_node, TIME_SERIES_TYPE, EvaluationClock, TS, STATE, CompoundScalar, LOGGER
from hgraph.nodes import format_


__all__ = ("debug_print", "print_", "log")


@dataclass
class _DebugPrintState(CompoundScalar):
    count: int = 0


@sink_node
def debug_print(
        label: str,
        ts: TIME_SERIES_TYPE,
        print_delta: bool = True,
        sample: int = -1,
        _clock: EvaluationClock = None,
        _state: STATE[_DebugPrintState] = None
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


def print_(format_str: TS[str] | str, *args, __std_out__: bool =True, **kwargs):
    """
    A sink node that will write the formatted string to the std out.
    This should be generally be used for debugging purposes and not be present in production code, instead use the
    log nodes for writing in a production context.

    :param format_str: The format string as defined in format
    :param args: The time-series enumerated inputs
    :param kwargs: The named time-series inputs
    """
    if len(args) == 0 and len(kwargs) == 0:
        return _print(format_str)
    else:
        return _print(format_(format_str, *args, **kwargs), std_out__=__std_out__)


@sink_node
def _print(ts: TS[str], std_out: bool = True):
    """
    A sink node that will write the string time-series to the standard out.

    :param ts: The string to write to the std out.
    :param std_out: If true, print to std out else std err.
    """
    print(ts.value) if std_out else print(ts.value, file=sys.stderr)


def log(format_str: TS[str] | str, *args, level: int = logging.INFO, **kwargs):
    """
    A sink node that will log the formatted string to the system logger.

    :param format_str: The format string as defined in format
    :param level: The logging level
    :param args: The time-series enumerated inputs
    :param kwargs: The named time-series inputs
    """
    if len(args) == 0 and len(kwargs) == 0:
        return _log(format_str, level)
    else:
        return _log(format_(format_str, *args, **kwargs), level)


@sink_node
def _log(ts: TS[str], level: int, logger: LOGGER = None):
    logger.log(level, ts.value)
