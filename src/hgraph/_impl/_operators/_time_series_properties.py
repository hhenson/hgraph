from datetime import datetime, date, timedelta, time

from hgraph import (
    TIME_SERIES_TYPE,
    REF,
    TS,
    compute_node,
    SIGNAL,
    valid,
    last_modified_date,
    last_modified_time,
    modified,
    SCHEDULER,
    TS_OUT,
    MIN_TD,
    evaluation_time_in_range,
    CmpResult,
    EvaluationClock,
    graph,
)
from hgraph._impl._operators._date_operators import add_date_time

__all__ = tuple()


@compute_node(valid=("ts",), active=("ts",), overloads=valid)
def valid_impl(ts: REF[TIME_SERIES_TYPE], ts_value: TIME_SERIES_TYPE = None) -> TS[bool]:
    if ts.modified:
        if ts_value.bound:
            ts_value.make_passive()
            ts_value.un_bind_output()

    if ts.value.is_empty:
        return False

    if not ts_value.bound:
        ts.value.bind_input(ts_value)
        if ts_value.valid:
            return True

        ts_value.make_active()

    if ts_value.valid:
        ts_value.make_passive()
        return True

    return False


@compute_node(overloads=modified)
def modified_impl(ts: SIGNAL, _schedule: SCHEDULER = None, _output: TS_OUT[bool] = None) -> TS[bool]:
    if ts.modified:
        _schedule.schedule(MIN_TD)
        return True
    if _schedule.is_scheduled:
        return False


@modified_impl.start
def modified_impl_start(_output: TS_OUT[bool]) -> TS[bool]:
    _output.apply_result(False)


@compute_node(overloads=last_modified_time)
def last_modified_time_impl(ts: SIGNAL) -> TS[datetime]:
    return ts.last_modified_time


@compute_node(overloads=last_modified_date)
def last_modified_date_impl(ts: SIGNAL) -> TS[date]:
    return ts.last_modified_time.date()


@compute_node(overloads=evaluation_time_in_range)
def evaluation_time_in_range_date_time(
    start_time: TS[datetime], end_time: TS[datetime], _scheduler: SCHEDULER = None, _clock: EvaluationClock = None
) -> TS[CmpResult]:
    """Datetime implementation"""
    start = start_time.value
    end = end_time.value
    if start > end:
        raise ValueError(f"Start time: {start} must be before end time: {end}")
    et = _clock.evaluation_time
    out = None
    if start <= et <= end:
        out = CmpResult.EQ
        _scheduler.schedule(end + MIN_TD, "_next")
    elif et < start:
        out = CmpResult.LT
        _scheduler.schedule(start, "_next")
    else:
        out = CmpResult.GT
        # If the end time was modified, we need to remove any potentially scheduled tasks
        _scheduler.un_schedule("_next")
    return out


@graph(overloads=evaluation_time_in_range)
def evaluation_time_in_range_date(start_time: TS[date], end_time: TS[date]) -> TS[CmpResult]:
    """Date implementation"""
    return evaluation_time_in_range_date_time(start_time + time(0, 0, 0, 0), end_time + time(23, 59, 59, 999999))


@compute_node(overloads=evaluation_time_in_range)
def evaluation_time_in_range_time(
    start_time: TS[time], end_time: TS[time], _scheduler: SCHEDULER = None, _clock: EvaluationClock = None
) -> TS[CmpResult]:
    et = _clock.evaluation_time
    dt = et.date()
    start = add_date_time(dt, start_time.value)
    end = add_date_time(dt, end_time.value)

    if start > end:
        if et < end:
            # Bring back start by a day to get the correct range
            start = add_date_time(dt - timedelta(days=1), start_time.value)
        else:
            # Push back end to get the correct range
            end = add_date_time(dt + timedelta(days=1), end_time.value)

    out = None
    if et < start:
        out = CmpResult.LT
        _scheduler.schedule(start, "_next")
    elif et <= end:
        out = CmpResult.EQ
        _scheduler.schedule(end + MIN_TD, "_next")
    else:
        out = CmpResult.LT
        # If the end time was modified, we need to remove any potentially scheduled tasks
        _scheduler.schedule(start + timedelta(days=1), "_next")
    return out
