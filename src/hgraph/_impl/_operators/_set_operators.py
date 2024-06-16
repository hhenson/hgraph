from typing import Type

from hgraph import (
    intersection,
    graph,
    TSL,
    TIME_SERIES_TYPE,
    SIZE,
    AUTO_RESOLVE,
    bit_and,
    reduce,
    zero,
    difference,
    sub_,
    WiringError,
    symmetric_difference,
    bit_xor,
    union,
    bit_or,
)

__all__ = ()


@graph(overloads=union)
def union_default(*tsl: TSL[TIME_SERIES_TYPE, SIZE], tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE) -> TIME_SERIES_TYPE:
    """
    Performs a union of the provided time-series values.

    Union is { p | p element of tsl[i] for i in range(len(tsl)) }
    """
    if len(tsl) == 1:
        return tsl[0]
    elif len(tsl) == 2:
        return bit_or(tsl[0], tsl[1])
    else:
        return reduce(bit_or, tsl, zero(tp, bit_or))


@graph(overloads=intersection)
def intersection_default(
    *tsl: TSL[TIME_SERIES_TYPE, SIZE], tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE
) -> TIME_SERIES_TYPE:
    if len(tsl) == 1:
        return tsl[0]
    elif len(tsl) == 2:
        return bit_and(tsl[0], tsl[1])
    else:
        return reduce(bit_and, tsl, zero(tp, bit_and))


@graph(overloads=difference)
def difference_default(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE:
    """
    Performs a difference of the provided time-series values.

    Difference is { p | p element of lhs and p not element of rhs }
    """
    if len(tsl) == 1:
        return tsl[0]
    elif len(tsl) == 2:
        return sub_(tsl[0], tsl[1])
    else:
        raise WiringError("Difference between multiple items is not supported")


@graph(overloads=symmetric_difference)
def symmetric_difference_default(
    *tsl: TSL[TIME_SERIES_TYPE, SIZE], tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE
) -> TIME_SERIES_TYPE:
    """
    Performs the symmetric difference of the provided time-series values.

    Symmetric difference is { p | p element of union(lhs, rhs), but not element of intersection(lhs, rhs) }
    """
    if len(tsl) == 1:
        return tsl[0]
    elif len(tsl) == 2:
        return bit_xor(tsl[0], tsl[1])
    else:
        return reduce(bit_xor, tsl, zero(tp, bit_xor))
