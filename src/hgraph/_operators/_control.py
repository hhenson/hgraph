from hgraph._types._scalar_types import SIZE
from hgraph._types._tsl_type import TSL
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._wiring._decorators import operator


__all__ = ("merge",)


@operator
def merge(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE:
    """
    Selects and returns the first of the values that tick (are modified) in the list provided.
    If more than one input is modified in the engine-cycle, it will return the first one that ticked in order of the
    list.
    """
