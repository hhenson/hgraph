from functools import cache
from typing import Any

from hgraph._types import TIME_SERIES_TYPE, HgTypeMetaData

__all__ = ["dedup_builder"]


@cache
def dedup_builder(tp: TIME_SERIES_TYPE) -> Any:
    """
    Build and return a callable that performs type-directed de-duplication for values of the
    provided time-series type. The returned function has the signature:

        fn(input_ts, output_ts) -> delta_or_none

    Where it inspects the concrete type shape (scalar TS, TSL, TSB, TSD, TSS) and only produces
    deltas when values actually change compared to the current output state. This mirrors the
    builder approach used by the JSON converter utilities.
    """
    from hgraph._impl._operators._dedup import dedup_converter

    return dedup_converter(HgTypeMetaData.parse_type(tp))
