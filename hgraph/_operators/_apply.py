from typing import Callable

from hgraph._types import DEFAULT, TSB, TS_SCHEMA, TS_SCHEMA_1, TS, OUT, TIME_SERIES_TYPE
from hgraph._wiring._decorators import compute_node, sink_node

__all__ = ["apply", "call"]


@compute_node(valid=("fn",), resolvers={TIME_SERIES_TYPE: lambda m, s: TS[s["fn"].__annotations__["return"]]})
def apply(fn: TS[Callable], *args: TSB[TS_SCHEMA], **kwargs: TSB[TS_SCHEMA_1]) -> DEFAULT[TIME_SERIES_TYPE]:
    """
    Apply the inputs to the fn provided.
    This allows a function to be passed as a time-series value, and it will be used to evaluate the inputs as they tick.
    This will not allow the function to be called unless are args are valid, but this is not required for kwargs.
    """
    if not all(a.valid for a in args):
        # Don't process unless all args are valid.
        return
    fn_ = fn.value
    out = fn_(
        *((a.value for a in args.values()) if args else tuple()),
        **({k: v.value for k, v in kwargs.items() if v.valid} if kwargs else {}),
    )
    return out


@sink_node(valid=("fn",))
def call(fn: TS[Callable], *args: TSB[TS_SCHEMA], **kwargs: TSB[TS_SCHEMA_1]):
    """
    Apply the inputs to the fn provided.
    This allows a function to be passed as a time-series value, and it will be used to evaluate the inputs as they tick.
    This will not allow the function to be called unless are args are valid, but this is not required for kwargs.
    """
    if not all(a.valid for a in args):
        # Don't process unless all args are valid.
        return
    fn_ = fn.value
    fn_(
        *((a.value for a in args.values()) if args else tuple()),
        **({k: v.value for k, v in kwargs.items() if v.valid} if kwargs else {}),
    )
