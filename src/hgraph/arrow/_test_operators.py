from dataclasses import dataclass

from hgraph import CompoundScalar, compute_node, STATE
from hgraph.arrow import arrow
from hgraph.arrow._arrow import A

__all__ = ("assert_",)

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
    def _assert(ts: A, _state: STATE[_AssertState] = None) -> A:
        if (c := _state.count) >= (l := len(args)):
            _state.failed = True
            raise AssertionError(f"Expected {l} ticks, but still getting results{message}")
        expected = args[c]
        _state.count += 1
        if ts.value != expected:
            _state.failed = True
            raise AssertionError(f"Expected '{expected}' but got '{ts.value}' on tick count: {_state.count}{message}")
        return ts.delta_value

    @_assert.stop
    def _assert_stop(_state: STATE[_AssertState]):
        if not _state.failed and ((l := len(args)) != (c := _state.count)):
            raise AssertionError(f"Expected {l} values but got {c} results{message}")

    return arrow(_assert)
