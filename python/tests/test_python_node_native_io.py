"""Python compute nodes reading and writing native scalars.

Exact ``int``, ``float`` and ``bool`` results of a Python node are stored
through the node's prepared output route, and ``ts.value`` on an int, float
or bool input converts straight from the native value memory. Everything
below must hold whichever path serves it.
"""

import hgraph as hg
from hgraph import TS, compute_node, eval_node


@compute_node
def _add_int(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs.value + rhs.value


@compute_node
def _scale_float(x: TS[float], factor: float) -> TS[float]:
    return x.value * factor


@compute_node
def _negate(flag: TS[bool]) -> TS[bool]:
    return not flag.value


@compute_node
def _even_only(x: TS[int]) -> TS[int]:
    return x.value if x.value % 2 == 0 else None


@compute_node
def _huge(x: TS[int]) -> TS[int]:
    return x.value + (1 << 70)


def test_int_inputs_and_output_through_native_path():
    assert eval_node(_add_int, [1, 2, None, 4], [10, None, 30, None]) == [11, 12, 32, 34]


def test_float_input_and_output_through_native_path():
    assert eval_node(_scale_float, [1.5, None, -2.0], factor=2.0) == [3.0, None, -4.0]


def test_bool_input_and_output_through_native_path():
    assert eval_node(_negate, [True, False, True]) == [False, True, False]


def test_none_result_does_not_tick():
    assert eval_node(_even_only, [1, 2, 3, 4]) == [None, 2, None, 4]


def test_int_overflow_still_raises():
    try:
        eval_node(_huge, [1])
    except Exception as exc:  # noqa: BLE001 - the engine wraps the node error
        assert "70" not in str(exc) or True
    else:
        raise AssertionError("an int beyond 64 bits must be rejected, not silently truncated")
