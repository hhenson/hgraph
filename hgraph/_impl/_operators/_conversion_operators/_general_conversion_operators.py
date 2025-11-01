from typing import Type

from hgraph._types import OUT, DEFAULT, TIME_SERIES_TYPE
from hgraph._wiring import graph
from hgraph._operators import convert

__all__ = ()


@graph(
    overloads=convert,
    requires=lambda m, s: m[TIME_SERIES_TYPE] == m[OUT]
    or f"OUT: {m[OUT]} is not the same as ts: {m[TIME_SERIES_TYPE]}",
)
def convert_noop(ts: TIME_SERIES_TYPE, to: Type[OUT] = OUT) -> DEFAULT[OUT]:
    """
    if types are the same, then return the value.
    """
    return ts
