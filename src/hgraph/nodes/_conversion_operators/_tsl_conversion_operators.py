from typing import Type

from hgraph import graph, TSL, TIME_SERIES_TYPE, SIZE, combine, OUT, TIME_SERIES_TYPE_1, DEFAULT


@graph(overloads=combine, requires=lambda m, s: OUT not in m or m[OUT].py_type == TSL)
def combine_tsl(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    return tsl


@graph(overloads=combine)
def combine_tsl(*tsl: TSL[TIME_SERIES_TYPE, SIZE], tp_: Type[TSL[TIME_SERIES_TYPE_1, SIZE]] = DEFAULT[OUT]) \
        -> TSL[TIME_SERIES_TYPE_1, SIZE]:
    return tsl
