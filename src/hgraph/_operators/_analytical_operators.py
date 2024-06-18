from hgraph._types import OUT, SIGNAL, TS, NUMBER
from hgraph._wiring._decorators import operator

__all__ = ("diff", "count", "clip", "ewma")


@operator
def diff(ts: OUT) -> OUT:
    """
    Computes the difference between the current value and the previous value in the time-series.
    """


@operator
def count(ts: SIGNAL) -> TS[int]:
    """
    Performs a running count of the number of times the time-series has ticked (i.e. emitted a value).
    """


@operator
def clip(ts: OUT, min_: NUMBER, max_: NUMBER) -> OUT:
    """
    Clips the input value/s to be within the range provided.
    This can operate on a time-series of NUMBER or more complex data
    structures such as a data frame.
    """


@operator
def ewma(ts: OUT, alpha: float, min_periods: int = 0) -> OUT:
    """
    Perform an exponential moving average of the input value/s.
    """
