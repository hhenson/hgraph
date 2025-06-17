from hgraph._operators import sum_, abs_, mean
from hgraph._wiring._decorators import compute_node
from hgraph._types import TSW, SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN, AUTO_RESOLVE, NUMBER, TS

__all__ = ("sum_tsw",)


@compute_node(overloads=sum_)
def sum_tsw(ts: TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN], _tp: type[SCALAR] = AUTO_RESOLVE,
            _output: TS[SCALAR] = None) -> TS[SCALAR]:
    value = _output.value if _output.valid else _tp()
    value += ts.delta_value
    if ts.has_removed_value:
        value -= ts.removed_value
    return _tp(value)


@compute_node(overloads=abs_)
def abs_tsw(ts: TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN]) -> TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN]:
    return int(abs(ts.delta_value))


@compute_node(overloads=mean)
def mean_tsw(
        ts: TSW[NUMBER, WINDOW_SIZE, WINDOW_SIZE],
        _tp: type[SCALAR] = AUTO_RESOLVE,
        _output: TS[SCALAR] = None
) -> TS[float]:
    value = _output.value if _output.valid else 0.0
    value *= ts.size
    value += float(ts.delta_value)
    if ts.has_removed_value:
        value -= ts.removed_value
    return float(value)
