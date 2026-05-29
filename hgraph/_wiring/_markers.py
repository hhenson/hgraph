from dataclasses import replace

from hgraph._types import TSD, K, TIME_SERIES_TYPE
from hgraph._wiring._wiring_port import WiringPort

__all__ = ("pass_through", "no_key", "passive")


def pass_through(tsd: TSD[K, TIME_SERIES_TYPE]) -> TSD[K, TIME_SERIES_TYPE]:
    """
    Marks the TSD input as a pass through value. This will ensure the TSD is not included in the key mapping in the
    tsd_map function. This is useful when the function takes a template type and the TSD has the same SCALAR type as
    the implied keys for the tsd_map function.
    """
    # noinspection PyTypeChecker
    return replace(tsd, markers=(_PassthroughMarker,) + (tsd.markers or ()))


def no_key(tsd: TSD[K, TIME_SERIES_TYPE]) -> TSD[K, TIME_SERIES_TYPE]:
    """
    Marks the TSD input as not contributing to the keys of the tsd_map function.
    This is useful when the input TSD is likely to be larger than the desired keys to process.
    This is only required if no keys are supplied to the tsd_map function.
    """
    # noinspection PyTypeChecker
    return replace(tsd, markers=(_NoKeyMarker,) + (tsd.markers or ()))

def passive(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """
    Marks a time-series as being passive. This will ensure that the wiring signature of the receiving node will
    set the input to be passive (or raise an exception if all inputs become passive as a result).
    """
    # noinspection PyTypeChecker
    return replace(ts, markers=(_PassivateMarker,) + (ts.markers or ()))


class _Marker:
    """
    Provide a base type for a marker applied to a wiring port
    """
    ...


class _PassthroughMarker(_Marker): ...


class _NoKeyMarker(_Marker): ...


class _PassivateMarker(_Marker): ...
