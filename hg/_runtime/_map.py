from typing import Callable, Optional

__all__ = ("tsd_map", "pass_through", "no_key", "tsl_map", "tsd_reduce", "tsl_reduce")

from hg._types._time_series_types import TIME_SERIES_TYPE
from hg._types._tsd_type import TSD
from hg._types._tsl_type import TSL
from hg._types._ts_type import TS
from hg._types._scalar_types import SIZE, SCALAR_1, SCALAR
from hg._types._tss_type import TSS


class _MappingMarker:

    def __init__(self, value: TSD[SCALAR, TIME_SERIES_TYPE]):
        self.value = value


class _PassthroughMarker:
    ...


class _NoKeyMarker:
    ...


def pass_through(tsd: TSD[SCALAR, TIME_SERIES_TYPE]) -> TSD[SCALAR, TIME_SERIES_TYPE]:
    """
    Marks the TSD input as a pass through value. This will ensure the TSD is not included in the key mapping in the
    tsd_map function. This is useful when the function takes a template type and the TSD has the same SCALAR type as
    the implied keys for the tsd_map function.
    """
    return _PassthroughMarker(tsd)


def no_key(tsd: TSD[SCALAR, TIME_SERIES_TYPE]) -> TSD[SCALAR, TIME_SERIES_TYPE]:
    """
    Marks the TSD input as not contributing to the keys of the tsd_map function.
    This is useful when the input TSD is likely to be larger than the desired keys to process.
    This is only required if no keys are supplied to the tsd_map function.
    """
    return _NoKeyMarker(tsd)


def tsd_map(func: Callable[[...], TIME_SERIES_TYPE], *args, keys: Optional[TSS[SCALAR]] = None, **kwargs) \
        -> TSD[SCALAR, TIME_SERIES_TYPE]:
    """
    The ability to demultiplex a stream of TSD values and process the associated time-series values against the func
    provided. The func can take a first argument of type TS[SCALAR] which will be mapped to the key of the TSD's been
    de-multiplexed. If the first argument is not of type TS[SCALAR] or the name of the first argument matches kwargs
    supplied (or the count of *args + **kwargs is equivalent to th number of arguments of the function) then the key
    will not be mapped into the first argument.

    If the keys kwarg is not supplied:
    #. The keys type is inferred from the types of the TSDs supplied for the function's inputs (that are not marked as
       pass through).
    #. The keys will be taken as the union of the keys of the TSDs supplied, that no not marked as pass through or
       no_key.

    Example:
        lhs: TSD[str, int] = ...
        rhs: TSD[str, int] = ...
        out = tsd_map(add_, lhs, rhs)
    """


def tsl_map(func: Callable[[...], TIME_SERIES_TYPE], *args,
            index: Optional[TSL[TS[bool], SIZE] | TS[tuple[int, ...]]] = None, **kwargs) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    The ability to demultiplex a stream of TSL values and apply the de-multiplexed time-series values against the func
    provided. In this case the inputs of the functions should either be TSL values with the associated time-series
    being the same as the associated input or be the type of the input. This will attempt to determine the expected
    size of the output either using the index kwarg or by using the size of the input TSLs.

    If any of the inputs are fixed sized, then the output will be the smallest fixed size. If the index is fixed size
    then all TSL inputs must be at least that size. If the index is not fixed size, but any of the inputs are fixed,
    the output will still be set to the size of the smallest input.
    """


def tsd_reduce(func: Callable[[TIME_SERIES_TYPE, TIME_SERIES_TYPE], TIME_SERIES_TYPE],
               tsd: TSD[SCALAR, TIME_SERIES_TYPE], zero: SCALAR_1) \
        -> TIME_SERIES_TYPE:
    """
    The ability to reduce a stream of TSD values and process the associated time-series values against the func
    provided. The entries of the tsd as reduced one with another until a single value is produced. If only one
    value is present, the assumption is that the value is the desired output. If no values are present, the zero
    value is used as the return value. The func must be associative and commutative as order of evaluation is
    not assured.

    Example:
        tsd: TSD[str, TS[int]] = ...
        out = tsd_reduce(add_, tsd, 0)
    """


def tsl_reduce(func: Callable[[TIME_SERIES_TYPE, TIME_SERIES_TYPE], TIME_SERIES_TYPE], tsl: TSL[TIME_SERIES_TYPE, SIZE],
               zero: SCALAR_1, is_associated: bool = True) -> TIME_SERIES_TYPE:
    """
    The ability to reduce a stream of TSL values and process the associated time-series values against the func.
    The structure of computation is dependent on the is_associated flag. If False, then the time-series values
    are processed in order. If True, then the time-series values are processed in a tree structure.

    Example:
        tsl: TSL[TS[int], SIZE] = ...
        out = tsl_reduce(add_, tsl, 0)
        >> tsl <- [1, 2, 3, 4, 5]
        >> out <- 15
    """
