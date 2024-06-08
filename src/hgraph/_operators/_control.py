from hgraph._operators._operators import bit_or, bit_and
from hgraph._types._scalar_types import SIZE
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._ts_type import TS
from hgraph._types._tsl_type import TSL
from hgraph._wiring._decorators import operator
from hgraph._wiring._reduce import reduce

__all__ = ("merge", "all_", "any_")


@operator
def merge(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE:
    """
    Selects and returns the first of the values that tick (are modified) in the list provided.
    If more than one input is modified in the engine-cycle, it will return the first one that ticked in order of the
    list.
    """


def all_(*args) -> TS[bool]:
    """
    Graph version of python `all` operator
    """
    return reduce(bit_and, TSL.from_ts(*args), False)


def any_(*args) -> TS[bool]:
    """
    Graph version of python `any` operator
    """
    return reduce(bit_or, TSL.from_ts(*args), False)
