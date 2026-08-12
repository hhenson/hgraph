"""C++-first numerical analytics for hgraph."""

from __future__ import annotations

from hgraph import DivideByZero, NUMBER, TS, operator_function

from . import _hgraph_analytics as _native


_pct_change = operator_function("hgraph.analytics.pct_change")


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


__all__ = ["pct_change"]
