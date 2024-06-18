from hgraph import (
    compute_node,
    TSL,
    TIME_SERIES_TYPE,
    SIZE,
    SCALAR,
    TS,
    REF,
    TSD
)

__all__ = ("flatten_tsl_values", "tsl_to_tsd",)


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


@compute_node(deprecated="Use combine(keys, tsl)")
def tsl_to_tsd(tsl: TSL[REF[TIME_SERIES_TYPE], SIZE], keys: tuple[str, ...]) -> TSD[str, REF[TIME_SERIES_TYPE]]:
    """
    Converts a time series into a time series dictionary with the keys provided.
    """
    return {k: ts.value for k, ts in zip(keys, tsl) if ts.modified}
