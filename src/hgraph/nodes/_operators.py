from typing import Type

from hgraph import compute_node, SCALAR, SCALAR_1, TS


__all__ = ("cast_",)


@compute_node
def cast_(tp: Type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
    """
    Casts a time-series to a different type.
    """
    return tp(ts.value)
