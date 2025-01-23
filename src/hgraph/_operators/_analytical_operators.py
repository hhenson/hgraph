from hgraph._types import OUT, SIGNAL, TS, NUMBER
from hgraph._wiring._decorators import operator

__all__ = ("diff", "count", "clip", "ewma", "center_of_mass_to_alpha", "span_to_alpha")


@operator
def diff(ts: OUT) -> OUT:
    """
    Computes the difference between the current value and the previous value in the time-series.
    """


@operator
def count(ts: SIGNAL, reset: SIGNAL = None) -> TS[int]:
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


def center_of_mass_to_alpha(com: float) -> float:
    if com <= 0:
        raise ValueError(f"Center of mass must be positive, got {com}")
    return 1.0 / (com + 1.0)


def span_to_alpha(span: float) -> float:
    if span <= 0:
        raise ValueError(f"Span must be positive, got {span}")
    return 2.0 / (span + 1.0)
