from typing import Type

from hgraph import compute_node, SCALAR, SCALAR_1, TS, REF
from hgraph._operators import cast_, downcast_, downcast_ref

__all__ = tuple()


@compute_node(overloads=cast_)
def cast_impl(tp: Type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
    """
    Casts a time-series to a different type.
    """
    return tp(ts.value)


@compute_node(overloads=downcast_)
def downcast_impl(tp: Type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
    """
    Downcasts a time-series to the given type.
    """
    assert isinstance(ts.value, getattr(tp, '__origin__', tp)), f"During downcast, expected an instance of {tp}, got {type(ts.value)} ({ts.value})"
    return ts.value


@compute_node(overloads=downcast_ref)
def downcast_ref_impl(tp: Type[SCALAR], ts: REF[TS[SCALAR_1]]) -> REF[TS[SCALAR]]:
    """
    Downcasts a time-series reference to the given type.
    This is fast but unsafe as there is no type checking happens here
    """
    return ts.value
