from typing import Type

from hgraph import compute_node, SCALAR, SCALAR_1, TS, REF

__all__ = ("cast_", "downcast_", "downcast_ref")


@compute_node
def cast_(tp: Type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
    """
    Casts a time-series to a different type.
    """
    return tp(ts.value)


@compute_node
def downcast_(tp: Type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
    """
    Downcasts a time-series to the given type.
    """
    assert isinstance(ts.value, tp)
    return ts.value


@compute_node
def downcast_ref(tp: Type[SCALAR], ts: REF[TS[SCALAR_1]]) -> REF[TS[SCALAR]]:
    """
    Downcasts a time-series reference to the given type. This is fast but unsafe as there is no type checking happens here
    """
    return ts.value
