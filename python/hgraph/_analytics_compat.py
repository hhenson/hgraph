"""Deprecated Python compatibility graphs for operators moved to analytics.

The imports of :mod:`hgraph_analytics` deliberately live inside the public
callables.  Importing core ``hgraph`` therefore remains independent of the
optional analytics distribution, while wiring a compatibility name both warns
and delegates to the implementation owned by that distribution.
"""

from __future__ import annotations

from datetime import timedelta
import warnings

from ._compat import DivideByZero
from ._types import (
    NUMBER,
    SCALAR,
    SIGNAL,
    SIZE,
    TS,
    TSL,
    TIME_SERIES_TYPE,
    TIME_SERIES_TYPE_1,
)
from ._wiring import graph


def _moved(name: str, replacement: str | None = None) -> str:
    target = replacement or name
    return (
        f"hgraph.{name} is deprecated; install hgraph-analytics and use "
        f"hgraph_analytics.{target}"
    )


@graph(deprecated=_moved("diff"))
def diff(ts: TS[NUMBER]) -> TS[NUMBER]:
    """Deprecated difference graph delegating to ``hgraph_analytics.diff``.

    The first valid observation is warm-up and does not emit; later valid
    ticks emit the current value minus the preceding valid value.  Invalid
    cycles neither trigger nor update history.  Wiring warns, and wiring fails
    with ``ModuleNotFoundError`` when ``hgraph-analytics`` is not installed.
    """
    from hgraph_analytics import diff as _diff

    return _diff(ts)


@graph(deprecated=_moved("count"))
def count(ts: SIGNAL, reset: SIGNAL = None) -> TS[int]:
    """Deprecated tick-count graph delegating to ``hgraph_analytics.count``.

    Every valid ``ts`` tick increments and emits the count.  A valid ``reset``
    tick clears state before a same-cycle input is counted.  ``reset`` defaults
    to no reset.  Wiring warns and requires ``hgraph-analytics``.
    """
    from hgraph_analytics import count as _count

    return _count(ts) if reset is None else _count(ts, reset)


@graph(deprecated=_moved("clip"))
def clip(ts: TS[NUMBER], min: NUMBER, max: NUMBER) -> TS[NUMBER]:
    """Deprecated numeric clipping graph delegating to analytics.

    Each valid input tick emits the value constrained to the inclusive scalar
    bounds.  The bounds are fixed while wiring, no state or warm-up is used,
    and an inverted interval raises when the node starts.  Wiring warns and
    requires ``hgraph-analytics``.
    """
    from hgraph_analytics import clip as _clip

    return _clip(ts, min, max)


@graph(deprecated=_moved("ewma"))
def ewma(ts: TS[float], alpha: float) -> TS[float]:
    """Deprecated exponentially weighted mean graph delegating to analytics.

    The first valid tick initializes and emits the mean; each later valid tick
    applies the fixed wiring-time ``alpha``.  Invalid cycles do not trigger or
    alter state.  The compatibility contract applies ``alpha`` as supplied
    without range validation.  Wiring warns and requires ``hgraph-analytics``.
    """
    from hgraph_analytics import ewma as _ewma

    return _ewma(ts, alpha)


@graph(deprecated=_moved("pct_change"))
def pct_change(
    ts: TS[NUMBER],
    period: int = 1,
    divide_by_zero: DivideByZero = DivideByZero.ERROR,
) -> TS[float]:
    """Deprecated fractional-change graph delegating to analytics.

    The first ``period`` valid observations are warm-up.  A later valid tick
    emits ``current / prior - 1``; invalid cycles do not count.  ``period`` and
    ``divide_by_zero`` are wiring-time policies with the shown defaults.
    Non-positive periods and zero-prior behavior follow the analytics errors.
    """
    from hgraph_analytics import pct_change as _pct_change

    return _pct_change(ts, period, divide_by_zero)


@graph(deprecated=_moved("as_array", "window_values"))
def as_array(
    tsw: TIME_SERIES_TYPE, zero: TIME_SERIES_TYPE_1 = None
):
    """Deprecated window materialization graph delegating to analytics.

    A ready fixed tick window emits a shaped array on each window tick.  Early
    valid windows pad their unused suffix with ``zero`` or the element default;
    duration windows are rejected.  ``zero`` may be live and defaults to the
    element identity.  Wiring warns and requires ``hgraph-analytics``.
    """
    from hgraph_analytics import window_values

    return window_values(tsw) if zero is None else window_values(tsw, zero)


@graph(deprecated=_moved("get_item", "array_get_item"))
def get_item(ts: TIME_SERIES_TYPE, idx: SCALAR):
    """Deprecated shaped-array indexing graph delegating to analytics.

    Every valid array tick emits the item or lower-rank slice selected by the
    wiring-time integer or integer tuple.  The graph has no state or warm-up;
    invalid and excessive indices follow the analytics errors.  Wiring warns
    and requires ``hgraph-analytics``.
    """
    from hgraph_analytics import array_get_item

    return array_get_item(ts, idx)


@graph(deprecated=_moved("cumsum", "cumulative_sum"))
def cumsum(a: TIME_SERIES_TYPE, axis: int | None = None):
    """Deprecated cumulative-array graph delegating to analytics.

    Each valid array tick emits cumulative sums.  Omitting ``axis`` flattens;
    a fixed wiring-time axis preserves shape and accepts negative indexing.
    The graph has no retained state, and invalid axes follow analytics errors.
    """
    from hgraph_analytics import cumulative_sum

    return cumulative_sum(a) if axis is None else cumulative_sum(a, axis)


@graph(deprecated=_moved("corrcoef", "correlation"))
def corrcoef(
    x: TIME_SERIES_TYPE,
    y: TIME_SERIES_TYPE_1 = None,
    rowvar: bool = True,
):
    """Deprecated correlation graph delegating to analytics.

    One- and two-dimensional arrays are supported.  Rows are variables unless
    the fixed ``rowvar`` policy is false.  A tick on either input triggers once
    all supplied inputs are valid; no history is retained.  Shape errors follow
    the analytics contract.  Wiring warns and requires ``hgraph-analytics``.
    """
    from hgraph_analytics import correlation

    if y is None:
        return correlation(x) if rowvar is True else correlation(x, rowvar=rowvar)
    return correlation(x, y) if rowvar is True else correlation(x, y, rowvar=rowvar)


@graph(deprecated=_moved("quantile"))
def quantile(
    a: TIME_SERIES_TYPE,
    q: TS[float],
    method: str | bool = "linear",
    keepdims: bool = False,
) -> TS[float]:
    """Deprecated scalar-quantile graph delegating to analytics.

    A tick on the values or live ``q`` emits once both are valid.  ``method``
    is a wiring-time interpolation policy.  The former ``keepdims`` flag is
    accepted for call compatibility but remains a scalar-output no-op.  Empty
    values, unsupported methods, and ``q`` outside ``[0, 1]`` raise as before.
    """
    from hgraph_analytics import quantile as _quantile

    # The old three-positional-argument overload treated a bool as keepdims.
    if isinstance(method, bool):
        keepdims = method
        method = "linear"
    del keepdims
    return _quantile(a, q, method=method)


@graph(deprecated=_moved("np_std", "array_std"))
def np_std(ts: TIME_SERIES_TYPE, ddof: int = 0) -> TS[float]:
    """Deprecated shaped-array deviation graph delegating to analytics.

    Every valid array tick emits one reduction over all elements.  ``ddof`` is
    fixed while wiring and defaults to zero; insufficient samples yield NaN.
    The graph has no retained state.  Wiring warns and requires
    ``hgraph-analytics``.
    """
    from hgraph_analytics import array_std

    return array_std(ts, ddof)


@graph(deprecated=_moved("np_quantile", "quantile"))
def np_quantile(
    ts: TIME_SERIES_TYPE,
    q: TS[float],
    method: str | bool = "linear",
    keepdims: bool = False,
) -> TS[float]:
    """Deprecated NumPy-named quantile graph delegating to analytics.

    Triggering, interpolation, validity, and errors match ``quantile`` above.
    The former scalar ``keepdims`` flag is accepted as a no-op.  Wiring emits a
    deprecation warning and requires ``hgraph-analytics``.
    """
    from hgraph_analytics import quantile as _quantile

    if isinstance(method, bool):
        keepdims = method
        method = "linear"
    del keepdims
    return _quantile(ts, q, method=method)


@graph(deprecated=_moved("np_rolling_window", "rolling_window"))
def np_rolling_window(
    ts: TS[SCALAR], period: SIZE, min_window_period: int | None = None
):
    """Deprecated NumPy-named rolling-window graph delegating to analytics.

    Valid ticks populate a fixed observation-count window.  Output begins at
    ``min_window_period`` or the full period and contains shaped values and
    evaluation timestamps.  Invalid period combinations fail while wiring.
    Wiring warns and requires ``hgraph-analytics``.
    """
    from hgraph_analytics import rolling_window

    return (
        rolling_window(ts, period)
        if min_window_period is None
        else rolling_window(ts, period, min_window_period)
    )


@graph(deprecated=_moved("std"))
def std(
    *values: TSL[TIME_SERIES_TYPE, SIZE],
    ddof: int | None = None,
    default_value: TIME_SERIES_TYPE_1 = None,
):
    """Deprecated standard-deviation graph delegating to analytics.

    Scalar streams retain running population state; current collections reduce
    their valid members; typed windows use fixed ``ddof``; and multiple inputs
    are element-wise.  Invalid cycles do not update running state.  Defaults,
    warm-up, NaN results, and errors are inherited from the analytics contract.
    """
    from hgraph_analytics import std as _std

    kwargs = {}
    if ddof is not None:
        kwargs["ddof"] = ddof
    if default_value is not None:
        kwargs["default_value"] = default_value
    return _std(*values, **kwargs)


@graph(deprecated=_moved("var"))
def var(
    *values: TSL[TIME_SERIES_TYPE, SIZE],
    default_value: TIME_SERIES_TYPE_1 = None,
):
    """Deprecated variance graph delegating to analytics.

    Scalar streams retain running population state; collections reduce their
    current valid members; and multiple inputs are element-wise.  Invalid
    cycles do not update state.  ``default_value`` remains an optional live
    compatibility input, and all estimator errors follow analytics.
    """
    from hgraph_analytics import var as _var

    return (
        _var(*values)
        if default_value is None
        else _var(*values, default_value=default_value)
    )


@graph(deprecated=_moved("rolling_average", "rolling_mean"))
def rolling_average(
    ts: TS[NUMBER],
    period: int | timedelta,
    min_window_period: int | timedelta | None = None,
) -> TS[float]:
    """Deprecated trailing-mean graph delegating to analytics.

    Valid input ticks populate the fixed count or duration horizon and emit
    after the optional minimum is met.  Invalid cycles add no observation.
    The minimum defaults to the analytics full-window policy, and invalid
    period combinations fail while wiring.
    """
    from hgraph_analytics import rolling_mean

    return (
        rolling_mean(ts, period)
        if min_window_period is None
        else rolling_mean(ts, period, min_window_period)
    )


@graph(deprecated=_moved("resample"))
def resample(ts: TIME_SERIES_TYPE, period: timedelta) -> TIME_SERIES_TYPE:
    """Deprecated periodic resampling graph delegating to analytics.

    Once ``ts`` is valid, its latest value emits at every fixed positive
    engine-time ``period``, including intervals without an input tick.  The
    latest value is retained; a non-positive period raises at start.  Wiring
    warns and requires ``hgraph-analytics``.
    """
    from hgraph_analytics import resample as _resample

    return _resample(ts, period)


def center_of_mass_to_alpha(com: float) -> float:
    """Deprecated scalar conversion delegated lazily to analytics."""
    warnings.warn(
        _moved("center_of_mass_to_alpha"), DeprecationWarning, stacklevel=2
    )
    from hgraph_analytics import center_of_mass_to_alpha as _convert

    return _convert(com)


def span_to_alpha(span: float) -> float:
    """Deprecated scalar conversion delegated lazily to analytics."""
    warnings.warn(_moved("span_to_alpha"), DeprecationWarning, stacklevel=2)
    from hgraph_analytics import span_to_alpha as _convert

    return _convert(span)


__all__ = (
    "as_array",
    "center_of_mass_to_alpha",
    "clip",
    "corrcoef",
    "count",
    "cumsum",
    "diff",
    "ewma",
    "get_item",
    "np_quantile",
    "np_rolling_window",
    "np_std",
    "pct_change",
    "quantile",
    "resample",
    "rolling_average",
    "span_to_alpha",
    "std",
    "var",
)
