from typing import Type

from hgraph import compute_node, SCALAR, SCALAR_1, TS, TIME_SERIES_TYPE


__all__ = ("cast_", "len_")


@compute_node
def cast_(tp: Type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
    """
    Casts a time-series to a different type.
    """
    return tp(ts.value)


@compute_node
def len_(ts: TIME_SERIES_TYPE) -> TS[int]:
    """
    Returns the notion of length for the input time-series.
    By default, it is the length of the value of the time-series.
    """
    return len(ts.value)
