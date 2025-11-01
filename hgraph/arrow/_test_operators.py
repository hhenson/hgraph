import string
from dataclasses import dataclass

from hgraph import CompoundScalar, compute_node, STATE, AUTO_RESOLVE, TS, OUT, EvaluationClock
from hgraph.arrow import arrow
from hgraph.arrow._arrow import A, extract_value, extract_delta_value

__all__ = ("assert_", "debug_", "d")


@dataclass
class _AssertState(CompoundScalar):
    count: int = 0
    failed: bool = False


def assert_(*args, message: str = None):
    """
    Provides a simple assertion operator that can use used for simple tests.
    Provide the sequence of expected values, this acts as a pass through node,
    that will raise an AssertionError if the value does not match the expected value.
    """
    if message is None:
        message = ""
    else:
        message = f": ({message})"

    @compute_node
    def _assert(ts: A, _tp: type[A] = AUTO_RESOLVE, _state: STATE[_AssertState] = None) -> A:
        if (c := _state.count) >= (l := len(args)):
            _state.failed = True
            raise AssertionError(f"Expected {l} ticks, but still getting results: '{ts.value}'{message}")
        expected = args[c]
        _state.count += 1
        value = extract_value(ts, _tp)
        if value != expected:
            _state.failed = True
            raise AssertionError(f"Expected '{expected}' but got '{value}' on tick count: {_state.count}{message}")
        return ts.delta_value

    @_assert.stop
    def _assert_stop(_state: STATE[_AssertState]):
        if not _state.failed and ((l := len(args)) != (c := _state.count)):
            raise AssertionError(f"Expected {l} values but got {c} results{message}")

    return arrow(_assert, __has_side_effects__=True)


@arrow(__has_side_effects__=True)
@compute_node
def debug_(
    msg_: OUT,
    fmt_str: str = None,
    delta_value: bool = False,
    _out_tp: type[OUT] = AUTO_RESOLVE,
    _clock: EvaluationClock = None,
) -> OUT:
    """
    Useful utility for debugging. This will print the value of the input stream and then emit the value out.
    This ensures the debug statement will rank immediately after the input and before any subsequent consumers.
    This will work with a format string, or a prefix (no formatting elements) or no prefix.

    This is a slow implementation, but is only intended for debugging, and mostly
    in unit / integration test scenarios. This is not intended for general purpose use.
    :param msg_: The time-series input to print out.
    :param fmt_str: The format string to use, if {} is included this will use formatter else will produce <fmt_str>: <msg_str>
    :param delta_value: If true this will print the delta value instead of the full value.
    :return: The input is returned to allow this to be inlined and ranked between input and consumers post use.
    """
    msg_str = extract_delta_value(msg_, _out_tp) if delta_value else extract_value(msg_, _out_tp)
    if fmt_str is not None:
        _, parsed, _, _ = next(string.Formatter().parse(fmt_str), (None, None, None, None))
        msg_str = f"{fmt_str}: {msg_str}" if parsed is None else fmt_str.format(msg_str)
    print(f"[DEBUG][{_clock.evaluation_time}] {msg_str}")
    return msg_.delta_value


d = debug_
