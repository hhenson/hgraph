from datetime import datetime, date

from hgraph import TIME_SERIES_TYPE, REF, TS, compute_node, SIGNAL, valid, last_modified_date, last_modified_time

__all__ = tuple()


@compute_node(valid=("ts",), active=("ts",), overloads=valid)
def valid_impl(ts: REF[TIME_SERIES_TYPE], ts_value: TIME_SERIES_TYPE = None) -> TS[bool]:
    if ts.modified:
        if ts_value.bound:
            ts_value.make_passive()
            ts_value.un_bind_output()

    if not ts.value.valid:
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


@compute_node(overloads=last_modified_time)
def last_modified_time_impl(ts: SIGNAL) -> TS[datetime]:
    return ts.last_modified_time


@compute_node(overloads=last_modified_date)
def last_modified_date_impl(ts: SIGNAL) -> TS[date]:
    return ts.last_modified_time.date()
