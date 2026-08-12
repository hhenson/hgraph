"""C++-first numerical analytics for hgraph."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Generic

from hgraph import (
    Array,
    DivideByZero,
    NUMBER,
    SCALAR,
    SIGNAL,
    SIZE,
    TS,
    TSB,
    TimeSeriesSchema,
    graph,
    operator_function,
)

from . import _hgraph_analytics as _native


_diff = operator_function("hgraph.analytics.diff")
_count = operator_function("hgraph.analytics.count")
_clip = operator_function("hgraph.analytics.clip")
_ewma = operator_function("hgraph.analytics.ewma")
_pct_change = operator_function("hgraph.analytics.pct_change")
_window_values = operator_function("hgraph.analytics.window_values")
_array_get_item = operator_function("hgraph.analytics.array_get_item")
_cumulative_sum = operator_function("hgraph.analytics.cumulative_sum")
_correlation = operator_function("hgraph.analytics.correlation")
_quantile = operator_function("hgraph.analytics.quantile")
_array_std = operator_function("hgraph.analytics.array_std")
_rolling_window = operator_function("hgraph.analytics.rolling_window")
_to_window = operator_function("to_window")


@dataclass
class RollingWindowResult(TimeSeriesSchema, Generic[SCALAR, SIZE]):
    """Shaped values and evaluation timestamps for a trailing tick window."""

    buffer: TS[Array[SCALAR, SIZE]]
    index: TS[Array[datetime, SIZE]]


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


def window_values(window, zero=None):
    """Materialize a fixed tick window as a shaped array.

    The operator emits after the window reaches its configured minimum
    population. Its output capacity is the fixed tick period, with the unused
    warm-up suffix padded by ``zero`` or the element type's default value.
    ``zero`` may be a live time series or a wiring-time scalar. Duration-based
    windows are not supported.
    """

    return _window_values(window) if zero is None else _window_values(window, zero)


def array_get_item(values, index):
    """Select an item or lower-rank slice from a shaped array.

    ``index`` is fixed while wiring and may be an integer or an integer tuple.
    Negative components count from the end. Every valid array tick emits and
    the operator retains no state. Invalid or excessive components raise while
    wiring or evaluation.
    """

    return _array_get_item(values, index)


def cumulative_sum(values, axis: int | None = None):
    """Return cumulative sums over a numeric shaped array.

    Omitting ``axis`` flattens the result; a wiring-time axis preserves the
    input shape and accepts negative indexing. Every valid array tick emits,
    the operator retains no state, and integer accumulation uses defined
    two's-complement wrapping. An out-of-range axis raises during evaluation.
    """

    return _cumulative_sum(values) if axis is None else _cumulative_sum(values, axis)


def correlation(x, y=None, rowvar: bool = True):
    """Return correlation coefficients for one or two numeric arrays.

    One- and two-dimensional arrays are supported. Rows represent variables by
    default; set wiring-time ``rowvar`` to false to use columns. A tick on
    either input schedules evaluation once all supplied inputs are valid. The
    operator retains no state. Inputs with unsupported ranks, no observations,
    or differing observation counts raise during evaluation.
    """

    if y is None:
        return _correlation(x) if rowvar is True else _correlation(x, rowvar)
    return _correlation(x, y) if rowvar is True else _correlation(x, y, rowvar)


def quantile(values, q: TS[float], method: str = "linear") -> TS[float]:
    """Return a scalar quantile over a numeric array or tick window.

    A tick on either input schedules evaluation. The operator emits when
    ``values`` and ``q`` are valid and a supplied tick window has reached its
    configured minimum population. ``method`` is fixed while wiring and may be
    ``linear``, ``lower``, ``higher``, ``midpoint``, or ``nearest``. The
    operator owns no state beyond a supplied window.

    Empty input, an unsupported method, or a live ``q`` outside ``[0, 1]``
    raises during evaluation.
    """

    return _quantile(values, q, method)


def array_std(values, ddof: int = 0) -> TS[float]:
    """Return standard deviation over all values in a numeric shaped array.

    Every valid array tick produces one floating-point result. All dimensions
    are reduced, ``ddof`` is fixed while wiring and defaults to zero, and the
    operator retains no state. A sample count insufficient for ``ddof`` yields
    NaN; a value outside Arrow's supported integer range raises during
    evaluation.
    """

    return _array_std(values, ddof)


@graph
def rolling_window(
    ts: TS[SCALAR], period: SIZE, min_window_period: int | None = None
) -> TSB[RollingWindowResult]:
    """Return shaped arrays for a trailing tick window's values and timestamps.

    ``period`` and ``min_window_period`` are tick counts fixed while wiring.
    The core typed window owns history and emits on valid ``ts`` ticks after the
    requested minimum population. A full-period warm-up produces fixed-size
    arrays; an earlier minimum produces arrays whose leading dimension reflects
    the current population. Invalid period/minimum combinations raise
    ``WiringError``.
    """

    window = (
        _to_window(ts, period)
        if min_window_period is None
        else _to_window(ts, period, min_window_period)
    )
    return _rolling_window(window)


__all__ = [
    "RollingWindowResult",
    "array_get_item",
    "array_std",
    "center_of_mass_to_alpha",
    "clip",
    "correlation",
    "count",
    "cumulative_sum",
    "diff",
    "ewma",
    "pct_change",
    "quantile",
    "rolling_window",
    "span_to_alpha",
    "window_values",
]
