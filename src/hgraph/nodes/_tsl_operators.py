from typing import Type

from hgraph import compute_node, TSL, TIME_SERIES_TYPE, SIZE, SCALAR, TS, graph, AUTO_RESOLVE, NUMBER, REF, TSD, \
    PythonTimeSeriesReference, len_, operator
from hgraph.nodes._analytical import sum_
from hgraph.nodes._const import const


__all__ = ("flatten_tsl_values", "merge", "tsl_len", "sum_tsl", "tsl_to_tsd", "tsl_get_item", "tsl_get_item_ts",
           "index_of")


@compute_node
def flatten_tsl_values(tsl: TSL[TIME_SERIES_TYPE, SIZE], all_valid: bool = False) -> TS[tuple[SCALAR, ...]]:
    """
    This will convert the TSL into a time-series of tuples. The value will be the value type of the time-series
    provided to the TSL. If the value has not ticked yet, the value will be None.
    The output type must be defined by the user.

    Usage:
    ```Python
        tsl: TSL[TS[float], Size[3]] = ...
        out = flatten_tsl[SCALAR: float](tsl)
    ```
    """
    return tsl.value if not all_valid or tsl.all_valid else None

@operator
def merge(tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE:
    """
    Selects and returns the first of the values that tick (are modified) in the list provided.
    If more than one input is modified in the engine-cycle, it will return the first one that ticked in order of the
    list.
    """


@compute_node(overloads=merge)
def merge_default(tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE:
    """
    Selects and returns the first of the values that tick (are modified) in the list provided.
    If more than one input is modified in the engine-cycle, it will return the first one that ticked in order of the
    list.
    """
    return next(tsl.modified_values()).delta_value


@graph(overloads=len_)
def tsl_len(ts: TSL[TIME_SERIES_TYPE, SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TS[int]:
    return const(_sz.SIZE)


@compute_node(overloads=sum_)
def sum_tsl(ts: TSL[TS[NUMBER], SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TS[NUMBER]:
    return sum(t.value for t in ts.valid_values())


@compute_node
def tsl_to_tsd(tsl: TSL[REF[TIME_SERIES_TYPE], SIZE], keys: tuple[str, ...]) -> TSD[str, REF[TIME_SERIES_TYPE]]:
    """
    Converts a time series into a time series dictionary with the keys provided.
    """
    return {k: ts.value for k, ts in zip(keys, tsl) if ts.modified}


@operator
def tsl_get_item(tsl: TSL[TIME_SERIES_TYPE, SIZE], index: int) -> TIME_SERIES_TYPE:
    """
    Returns the item from the TSL that matches the index provided
    """


@compute_node(overloads=tsl_get_item, requires=lambda m, s: 0 <= s['index'] < m[SIZE])
def tsl_get_item_default(tsl: REF[TSL[TIME_SERIES_TYPE, SIZE]], index: int) -> REF[TIME_SERIES_TYPE]:
    """
    Return a reference to an item in the TSL referenced
    """
    if tsl.value.valid:
        if tsl.value.has_peer:
            return PythonTimeSeriesReference(tsl.value.output[index])
        else:
            return tsl.value.items[index]
    else:
        return PythonTimeSeriesReference()


@compute_node(overloads=tsl_get_item)
def tsl_get_item_ts(tsl: REF[TSL[TIME_SERIES_TYPE, SIZE]], index: TS[int], size: Type[SIZE] = AUTO_RESOLVE) -> REF[TIME_SERIES_TYPE]:
    """
    Return a reference to an item in the TSL referenced
    """
    if index.value < 0 or index.value >= size.SIZE:
        return PythonTimeSeriesReference()

    if tsl.value.valid:
        if tsl.value.has_peer:
            return PythonTimeSeriesReference(tsl.value.output[index.value])
        else:
            return tsl.value.items[index.value]
    else:
        return PythonTimeSeriesReference()


@compute_node
def index_of(tsl: TSL[TIME_SERIES_TYPE, SIZE], ts: TIME_SERIES_TYPE) -> TS[int]:
    """
    Return the index of the leftmost time-series with the equal value to ts in the TSL
    """
    return next((i for i, t in enumerate(tsl) if t.valid and t.value == ts.value), -1)