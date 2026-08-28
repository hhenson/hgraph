"""Direct temporal value operations backed by the native hgraph runtime.

The graph operators live at module level on :mod:`hgraph`.  This module
provides the corresponding operations for ordinary values, including values
read inside a Python compute node.  Provider-backed operations use the active
graph ``GlobalState`` so their timezone semantics match the graph operators.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import _hgraph

from ._wiring._state import GlobalState, _active_global_state


def _active_state_handle(global_state=None):
    state = global_state if global_state is not None else _active_global_state()
    return state._impl if isinstance(state, GlobalState) else state


def checked_add(
    lhs: Any,
    rhs: Any,
    *,
    month_end_policy=_hgraph.MonthEndPolicy.REJECT,
    global_state=None,
):
    """Checked value addition with the same contracts as ``hgraph.add_``."""
    if isinstance(lhs, _hgraph.ZonedDateTime):
        return _hgraph._temporal_checked_add_zoned(
            _active_state_handle(global_state), lhs, rhs
        )
    if isinstance(rhs, _hgraph.ZonedDateTime):
        return _hgraph._temporal_checked_add_zoned(
            _active_state_handle(global_state), rhs, lhs
        )
    if isinstance(rhs, _hgraph.Period):
        if isinstance(lhs, datetime):
            raise TypeError("Period cannot be added to an Instant")
        if isinstance(lhs, (date, _hgraph.CivilDateTime)):
            return _hgraph._temporal_checked_add(
                lhs, rhs, month_end_policy
            )
    return _hgraph._temporal_checked_add(lhs, rhs)


def checked_subtract(
    lhs: Any,
    rhs: Any,
    *,
    month_end_policy=_hgraph.MonthEndPolicy.REJECT,
    global_state=None,
):
    """Checked value subtraction with the same contracts as ``hgraph.sub_``."""
    if isinstance(lhs, _hgraph.ZonedDateTime):
        return _hgraph._temporal_checked_add_zoned(
            _active_state_handle(global_state), lhs, -rhs
        )
    if isinstance(rhs, _hgraph.Period):
        if isinstance(lhs, datetime):
            raise TypeError(
                "Period cannot be subtracted from an Instant"
            )
        if isinstance(lhs, (date, _hgraph.CivilDateTime)):
            return _hgraph._temporal_checked_subtract(
                lhs, rhs, month_end_policy
            )
    return _hgraph._temporal_checked_subtract(lhs, rhs)


def checked_negate(value):
    return _hgraph._temporal_checked_negate(value)


def checked_multiply(value, factor):
    return _hgraph._temporal_checked_multiply(value, factor)


def checked_divide(value, divisor):
    return _hgraph._temporal_checked_divide(value, divisor)


def apply_period(
    value,
    period,
    *,
    month_end_policy=_hgraph.MonthEndPolicy.REJECT,
):
    return _hgraph._temporal_apply_period(
        value, period, month_end_policy
    )


def at_zone(instant, zone, *, global_state=None):
    return _hgraph._temporal_at_zone(
        _active_state_handle(global_state), instant, zone
    )


def resolve(
    local,
    zone,
    *,
    ambiguous=_hgraph.AmbiguousTimePolicy.REJECT,
    nonexistent=_hgraph.NonexistentTimePolicy.REJECT,
    global_state=None,
):
    return _hgraph._temporal_resolve(
        _active_state_handle(global_state), local, zone, ambiguous, nonexistent
    )


resolve_civil = resolve


def convert_zone(value, zone, *, global_state=None):
    return _hgraph._temporal_convert_zone(
        _active_state_handle(global_state), value, zone
    )


def to_instant(value):
    return value.instant


def to_civil(value):
    return value.civil


def range_contains(range_, value):
    return range_.contains(value)


def range_intersection(lhs, rhs):
    return lhs.intersection(rhs)


def range_overlaps(lhs, rhs):
    return lhs.overlaps(rhs)


def range_touches(lhs, rhs):
    return lhs.touches(rhs)


def range_adjacent(lhs, rhs):
    return lhs.adjacent(rhs)


def range_mergeable(lhs, rhs):
    return lhs.mergeable(rhs)


def range_difference(lhs, rhs):
    return lhs.difference(rhs)


def range_union(lhs, rhs):
    return lhs.set_union(rhs)


def range_merge(lhs, rhs):
    return lhs.merge(rhs)


def range_hull(lhs, rhs):
    return lhs.hull(rhs)


def range_shift(
    range_,
    delta,
    *,
    month_end_policy=_hgraph.MonthEndPolicy.REJECT,
):
    if isinstance(range_, _hgraph.CivilDateRange):
        return _hgraph._temporal_shift(
            range_, delta, month_end_policy
        )
    return _hgraph._temporal_shift(range_, delta)


shift = range_shift


def range_extent(range_):
    return _hgraph._temporal_extent(range_)


extent = range_extent


def floor(value, quantum, *, origin=datetime(1970, 1, 1)):
    if isinstance(value, timedelta):
        return _hgraph._temporal_floor(value, quantum)
    return _hgraph._temporal_floor(value, quantum, origin)


temporal_floor = floor


def ceil(value, quantum, *, origin=datetime(1970, 1, 1)):
    if isinstance(value, timedelta):
        return _hgraph._temporal_ceil(value, quantum)
    return _hgraph._temporal_ceil(value, quantum, origin)


temporal_ceil = ceil


def round(value, quantum, *, origin=datetime(1970, 1, 1)):
    if isinstance(value, timedelta):
        return _hgraph._temporal_round(value, quantum)
    return _hgraph._temporal_round(value, quantum, origin)


temporal_round = round


def bucket(value, width, *, origin=datetime(1970, 1, 1)):
    return _hgraph._temporal_bucket(value, width, origin)


temporal_bucket = bucket

parse_duration = _hgraph._temporal_parse_duration
format_duration = _hgraph._temporal_format_duration
format_instant = _hgraph._temporal_format_instant
format_civil_date = _hgraph._temporal_format_civil_date
format_civil_time = _hgraph._temporal_format_civil_time
format_civil_datetime = _hgraph._temporal_format_civil_datetime


__all__ = [
    "apply_period",
    "at_zone",
    "bucket",
    "ceil",
    "checked_add",
    "checked_divide",
    "checked_multiply",
    "checked_negate",
    "checked_subtract",
    "convert_zone",
    "extent",
    "floor",
    "format_civil_date",
    "format_civil_datetime",
    "format_civil_time",
    "format_duration",
    "format_instant",
    "parse_duration",
    "range_adjacent",
    "range_contains",
    "range_difference",
    "range_extent",
    "range_hull",
    "range_intersection",
    "range_merge",
    "range_mergeable",
    "range_overlaps",
    "range_shift",
    "range_touches",
    "range_union",
    "resolve",
    "resolve_civil",
    "round",
    "shift",
    "temporal_bucket",
    "temporal_ceil",
    "temporal_floor",
    "temporal_round",
    "to_civil",
    "to_instant",
]
