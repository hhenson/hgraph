"""Test operators — port of upstream ``hgraph.arrow._test_operators``.

Dialect: STATE here is hg_cpp's attribute-namespace injectable (upstream's
``STATE[Schema]`` subscript form does not exist in this runtime)."""
import string

import hgraph as hg
from hgraph import CLOCK, STATE, TIME_SERIES_TYPE, TS

from .._wiring import compute_node
from ._arrow import _pair_shape, _pair_value_port, _value_to_tuples, arrow

__all__ = ("assert_", "debug_", "d")


def assert_(*args, message: str = None):
    """A pass-through assertion node: checks each tick's (full, tuple-shaped)
    value against the expected sequence; the stop hook checks the count."""
    message = "" if message is None else f": ({message})"
    expected_values = args

    @compute_node
    def _assert(ts: TIME_SERIES_TYPE, _state: STATE = None) -> TS[object]:
        c = getattr(_state, "count", 0)
        if c >= (l := len(expected_values)):
            _state.failed = True
            raise AssertionError(
                f"Expected {l} ticks, but still getting results: '{ts.value}'{message}")
        expected = expected_values[c]
        _state.count = c + 1
        value = _value_to_tuples(ts.value)
        if value != expected:
            _state.failed = True
            raise AssertionError(
                f"Expected '{expected}' but got '{value}' on tick count: {_state.count}{message}")
        return ts.value

    @_assert.stop
    def _assert_stop(_state: STATE = None):
        failed = getattr(_state, "failed", False)
        count = getattr(_state, "count", 0)
        if not failed and (l := len(expected_values)) != count:
            raise AssertionError(f"Expected {l} values but got {count} results{message}")

    def _wrapper(x):
        # Feed the assert from a tuple-shaped TS[object] view of x, but pass
        # the ORIGINAL port through so chains continue with typed values.
        value_port = _pair_value_port(x) if _pair_shape(x) is not None else x
        checked = _assert(value_port)
        hg.null_sink(checked)
        return x

    return arrow(_wrapper, __name__=f"assert_{expected_values}",
                 __has_side_effects__=True)


def debug_(fmt_str: str = None, delta_value: bool = False):
    """Print the (tuple-shaped) value of the stream and pass it through."""

    @compute_node
    def _debug(ts: TIME_SERIES_TYPE, fmt: str, use_delta: bool,
               _clock: CLOCK = None) -> TS[object]:
        msg = _value_to_tuples(ts.delta_value if use_delta else ts.value)
        if fmt:
            _, parsed, _, _ = next(string.Formatter().parse(fmt), (None,) * 4)
            msg = f"{fmt}: {msg}" if parsed is None else fmt.format(msg)
        when = _clock.evaluation_time if _clock is not None else ""
        print(f"[DEBUG][{when}] {msg}")
        return ts.value

    def _wrapper(x):
        value_port = _pair_value_port(x) if _pair_shape(x) is not None else x
        hg.null_sink(_debug(value_port, fmt_str or "", delta_value))
        return x

    return arrow(_wrapper, __name__=f"debug_({fmt_str})",
                 __has_side_effects__=True)


d = debug_
