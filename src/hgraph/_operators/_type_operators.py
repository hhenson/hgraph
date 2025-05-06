from hgraph._wiring._decorators import operator
from hgraph._types._scalar_types import SCALAR, SCALAR_1
from hgraph._types._ts_type import TS
from hgraph._types._ref_type import REF

__all__ = ("cast_", "downcast_", "downcast_ref")


@operator
def cast_(tp: type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
    """
    Casts a time-series to a different type.
    """
    return tp(ts.value)


@operator
def downcast_(tp: type[SCALAR], ts: TS[SCALAR_1]) -> TS[SCALAR]:
    """
    Downcasts a time-series to the given type.
    """
    assert isinstance(ts.value, tp)
    return ts.value


@operator
def downcast_ref(tp: type[SCALAR], ts: REF[TS[SCALAR_1]]) -> REF[TS[SCALAR]]:
    """
    Downcasts a time-series reference to the given type. This is fast but unsafe as there is no type checking happens here
    """
