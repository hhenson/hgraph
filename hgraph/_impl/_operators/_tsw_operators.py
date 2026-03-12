from hgraph._operators import sum_, abs_, mean, min_, max_
from hgraph._wiring._decorators import compute_node
from hgraph._types import TSW, SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN, AUTO_RESOLVE, NUMBER, TS
import numpy as np

__all__ = ()


@compute_node(overloads=sum_)
def sum_tsw(
    ts: TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN], _tp: type[SCALAR] = AUTO_RESOLVE, _output: TS[SCALAR] = None
) -> TS[SCALAR]:
    value = _output.value if _output.valid else _tp()
    value += ts.delta_value
    if ts.has_removed_value:
        value -= ts.removed_value
    return _tp(value)


@compute_node(overloads=abs_)
def abs_tsw(ts: TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN]) -> TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN]:
    return int(abs(ts.delta_value))


@compute_node(overloads=mean, all_valid=("ts",))
def mean_tsw(
    ts: TSW[NUMBER, WINDOW_SIZE, WINDOW_SIZE_MIN], _tp: type[NUMBER] = AUTO_RESOLVE, _output: TS[float] = None
) -> TS[float]:
    if not _output.valid:
        return np.mean(ts.value)
    else:
        value = _output.value
        has_remove_value = ts.has_removed_value
        sz = len(ts)
        value *= sz if has_remove_value else sz - 1
        value += float(ts.delta_value)
        if ts.has_removed_value:
            value -= ts.removed_value
        return float(value) / sz


@compute_node(overloads=min_, all_valid=("ts",))
def min_tsw(
    ts: TSW[NUMBER, WINDOW_SIZE, WINDOW_SIZE_MIN], default_value: TS[NUMBER] = None, _tp: type[NUMBER] = AUTO_RESOLVE
) -> TS[NUMBER]:
    default = default_value.value if default_value.valid else None
    value = min(ts.value, default=default)
    return _tp(value) if value is not None else None


@compute_node(overloads=max_, all_valid=("ts",))
def max_tsw(
    ts: TSW[NUMBER, WINDOW_SIZE, WINDOW_SIZE_MIN], default_value: TS[NUMBER] = None, _tp: type[NUMBER] = AUTO_RESOLVE
) -> TS[NUMBER]:
    default = default_value.value if default_value.valid else None
    value = max(ts.value, default=default)
    return _tp(value) if value is not None else None
