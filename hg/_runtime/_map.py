from typing import TypeVar, Callable


__all__ = ("map",)

from hg import TSS, SCALAR

SWITCH_SIGNATURE = TypeVar("SWITCH_SIGNATURE", bound=Callable)


def map(func: SWITCH_SIGNATURE, keys: TSS[SCALAR]):
    """
    A mapping function that applies a function to a time-series of keys.
    Map returns a wiring node that accepts arguments of the form TSD[SCALAR, ...] to be de-multiplexed into
    the func signature. The output is a TSD[SCALAR, OUTPUT_TYPE] where the OUTPUT_TYPE is the output of the
    func signature and the SCALAR is the key for each mapped element.

    For example:

        @graph
        def func(key: SCALAR, ts1: TIME_SERIES_1, ... tsn: TIME_SERIES_N) -> TIME_SERIES_OUT:
            ...

        tss: TSS[SCALAR] = ...
        ts1: TSD[SCALAR, TIME_SERIES_1] = ...
        ...
        tsn: TSD[SCALAR, TIME_SERIES_N] = ...

        out: TSD[SCALAR, TIME_SERIES_OUT] = map(func, tss)(ts1, ..., tsn)

    The key field is optional, and if specified will be the first argument in the func signature. The map function
    looks for the first field to be of type SCALAR (the type of the keys argument).
    """