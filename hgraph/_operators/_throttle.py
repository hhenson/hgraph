from functools import cache
from typing import Any

from hgraph._types import TIME_SERIES_TYPE, HgTypeMetaData

__all__ = ["collect_builder"]


@cache
def collect_builder(tp: TIME_SERIES_TYPE) -> Any:
    """
    Build and return an accumulator function used by throttle to collect per-type deltas between scheduled emits.

    The returned function has the signature:
        fn(input_ts, out_tick) -> new_out_tick
    Where:
      - input_ts is a time-series input instance of the given type
      - out_tick is the current accumulated tick structure (shape depends on type)
      - The function returns the updated accumulated structure to store in state
    """
    from hgraph._impl._operators._throttle import collect_converter

    return collect_converter(HgTypeMetaData.parse_type(tp))
