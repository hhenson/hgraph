import math

from hgraph import compute_node, TIME_SERIES_TYPE, TS

__all__ = ("drop_dups", "drop_dups_float")


@compute_node
def drop_dups(ts: TIME_SERIES_TYPE, _output: TIME_SERIES_TYPE = None) -> TIME_SERIES_TYPE:
    """
    Drops duplicate values from a time-series.
    """
    if _output.valid:
        if ts.value != _output.value:
            return ts.delta_value
    else:
        return ts.value


@compute_node(overloads=drop_dups)
def drop_dups_float(ts: TS[float], abs_tol: float = 1e-15, _output: TS[float] = None) -> TS[float]:
    """
    Drops 'duplicate' float values from a time-series which are almost equal
    """
    if _output.valid:
        if not (-abs_tol < ts.value - _output.value < abs_tol):
            return ts.delta_value
    else:
        return ts.value
