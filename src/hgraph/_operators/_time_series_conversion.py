from datetime import timedelta

from hgraph._types._ts_type import TS
from hgraph._types._scalar_types import DEFAULT, SCALAR
from hgraph._types._time_series_types import OUT
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._wiring._decorators import operator

__all__ = ("const", "convert", "combine", "collect", "emit")


@operator
def const(
    value: SCALAR,
    tp: type[OUT] = TS[SCALAR],
    delay: timedelta = timedelta(),
) -> DEFAULT[OUT]:
    """
    Produces a single tick at the start of the graph evaluation after which this node does nothing.

    :param value: The value in appropriate form to be applied to the time-series type specified in tp.
    :param tp: Used to resolve the correct type for the output, by default this is TS[SCALAR] where SCALAR is the type
               of the value.
    :param delay: The amount of time to delay the value by. The default is 0.
    :return: A single tick of the value supplied.
    """


@operator
def convert(ts: TIME_SERIES_TYPE, to: type[OUT] = DEFAULT[OUT], **kwargs) -> OUT:
    """
    Converts the incoming time series to the desired result type. This can be called in one of two ways:
    ::
        c = const(..., TS[set[str]])
        convert(c, TS[tuple[str, ...]])

    or:
    ::
        convert[TS[tuple[str, ...]]](c)

    """


@operator
def combine(*args: TIME_SERIES_TYPE, **kwargs) -> DEFAULT[OUT]:
    """
    Combines the incoming time series into one collection time-series output.
    """


@operator
def collect(ts: TIME_SERIES_TYPE) -> DEFAULT[OUT]:
    """
    Converts the `ts` value to a collection time-series. The time-series to convert to must be provided declaratively.
    This is done by setting the OUT to the desired result type, for example:
    ::
        ts = const(1)
        r = collect[OUT: TS[tuple[int, ...]](ts)

    Where here we set the resultant type to be a time-series of tuples, the input would be a time-series of integers.

    For processing a result type of `dict` or `TSD` the input must consist of a time-series of two time-series
    values where one represents the keys and the other represent the values.

    For example:
    ::
        ts = const(frozendict({"a": 1, "b": 2}), TSB[ts_schema(key=TS[str], value=TS[int])])
        r = collect[OUT: TS[Mapping[str, int]](ts)

    Alternative syntax includes:
    ::
        key = const("a")
        value = const(1)
        r = collect[OUT: TS[Mapping[str, int]](key=key, value=value)

    Or options such as the input being a ``TSL[TS[SCALAR], Size[2]]`` (in this case the key and value must be the same).
    It is also possible to accept a TS[Mapping[str, int]] or a TS[tuple[str, int]]
    """
    raise NotImplemented(f"Not implemented for input type {type(ts)}")


@operator
def emit(ts: TIME_SERIES_TYPE) -> DEFAULT[OUT]:
    """
    Accepts a collection representation, for example: ``TS[tuple[int, ...]]`` and returns a time-series of the values
    as a stream of individual ticks (in the example above that would be ``TS[int]``.
    """
