"""C++-first numerical analytics for hgraph."""

from __future__ import annotations

from hgraph import DivideByZero, NUMBER, SIGNAL, TS, operator_function

from . import _hgraph_analytics as _native


_diff = operator_function("hgraph.analytics.diff")
_count = operator_function("hgraph.analytics.count")
_clip = operator_function("hgraph.analytics.clip")
_ewma = operator_function("hgraph.analytics.ewma")
_pct_change = operator_function("hgraph.analytics.pct_change")


def diff(ts: TS[NUMBER]) -> TS[NUMBER]:
    """Return the difference from the preceding valid observation."""

    return _diff(ts)


def count(ts: SIGNAL, reset: SIGNAL = None) -> TS[int]:
    """Count valid input ticks, restarting when ``reset`` ticks."""

    return _count(ts) if reset is None else _count(ts, reset)


def clip(ts: TS[NUMBER], min_: NUMBER, max_: NUMBER) -> TS[NUMBER]:
    """Constrain each input value to the inclusive ``[min_, max_]`` range."""

    return _clip(ts, min_, max_)


def ewma(ts: TS[float], alpha: float) -> TS[float]:
    """Return the exponentially weighted moving average of ``ts``."""

    return _ewma(ts, alpha)


def center_of_mass_to_alpha(com: float) -> float:
    """Convert a positive center of mass to an EWMA smoothing factor."""

    if com <= 0:
        raise ValueError(f"Center of mass must be positive, got {com}")
    return 1.0 / (com + 1.0)


def span_to_alpha(span: float) -> float:
    """Convert a positive span to an EWMA smoothing factor."""

    if span <= 0:
        raise ValueError(f"Span must be positive, got {span}")
    return 2.0 / (span + 1.0)


def pct_change(
    ts: TS[NUMBER],
    period: int = 1,
    divide_by_zero: DivideByZero = DivideByZero.ERROR,
) -> TS[float]:
    """Return fractional change from ``period`` valid observations earlier.

    ``0.05`` denotes five percent. The first ``period`` observations do not
    produce output, invalid input cycles are not counted, and output triggers
    only on a valid input tick. ``period`` is a positive wiring-time observation
    count and defaults to one. ``divide_by_zero`` is a wiring-time policy for a
    zero prior value and defaults to ``DivideByZero.ERROR``. The retained state
    is the core lag history for ``period`` observations.

    A non-positive period raises ``WiringError`` while wiring. A zero prior
    raises during evaluation under the default division policy. This operator
    does not infer elapsed-time, dataframe-row, sampling, or financial-return
    semantics.
    """

    return _pct_change(ts, period, divide_by_zero)


__all__ = [
    "center_of_mass_to_alpha",
    "clip",
    "count",
    "diff",
    "ewma",
    "pct_change",
    "span_to_alpha",
]
