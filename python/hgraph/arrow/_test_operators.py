"""Test operators — port of upstream ``hgraph.arrow._test_operators``.

Dialect: STATE here is hg_cpp's attribute-namespace injectable (upstream's
``STATE[Schema]`` subscript form does not exist in this runtime)."""
import string

from hgraph import CLOCK, STATE, TIME_SERIES_TYPE

from .._wiring import compute_node
from ._arrow import _value_to_tuples, arrow

__all__ = ("assert_", "debug_", "d")


def assert_(*args, message: str = None):
    """A pass-through assertion node: checks each tick's (full, tuple-shaped)
    value against the expected sequence; the stop hook checks the count."""
    message = "" if message is None else f": ({message})"
    expected_values = args

    @compute_node(valid=())
    def _assert(
        ts: TIME_SERIES_TYPE, _state: STATE = None
    ) -> TIME_SERIES_TYPE:
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
        return ts.delta_value

    @_assert.stop
    def _assert_stop(_state: STATE = None):
        failed = getattr(_state, "failed", False)
        count = getattr(_state, "count", 0)
        if not failed and (l := len(expected_values)) != count:
            raise AssertionError(f"Expected {l} values but got {count} results{message}")

    def _wrapper(x):
        checked = _assert(x)
        return checked

    return arrow(_wrapper, __name__=f"assert_{expected_values}",
                 __has_side_effects__=True)


@compute_node(valid=())
def _debug(
    ts: TIME_SERIES_TYPE,
    fmt: str,
    use_delta: bool,
    _clock: CLOCK = None,
) -> TIME_SERIES_TYPE:
    """Typed runtime half of the Arrow diagnostic pass-through."""
    msg = _value_to_tuples(ts.delta_value if use_delta else ts.value)
    if fmt:
        _, parsed, _, _ = next(string.Formatter().parse(fmt), (None,) * 4)
        msg = f"{fmt}: {msg}" if parsed is None else fmt.format(msg)
    when = _clock.evaluation_time if _clock is not None else ""
    print(f"[DEBUG][{when}] {msg}")
    return ts.delta_value


def _debug_arrow(*args, fmt_str: str = None, delta_value: bool = False):
    """Apply bound diagnostic options to the final Arrow input port."""
    if not args:
        raise TypeError("debug_ requires an Arrow input when it is evaluated")
    ts = args[-1]
    options = args[:-1]
    if len(options) > 2:
        raise TypeError("debug_ accepts at most a format string and delta_value")
    if options:
        if fmt_str is not None:
            raise TypeError("debug_ received fmt_str both positionally and by keyword")
        fmt_str = options[0]
    if len(options) == 2:
        if delta_value is not False:
            raise TypeError("debug_ received delta_value both positionally and by keyword")
        delta_value = options[1]
    return _debug(ts, fmt_str or "", delta_value)


# ``debug_`` is itself an Arrow, as in release/0.5.  It can therefore be
# placed directly in a chain, while calls bind its optional configuration:
# ``... >> debug_ >> ...`` and ``... >> debug_("value {}") >> ...``.
debug_ = arrow(_debug_arrow, __name__="debug_", __has_side_effects__=True)


d = debug_
