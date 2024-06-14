from datetime import datetime, date

from hgraph import TIME_SERIES_TYPE, REF, TS, compute_node, SIGNAL

__all__ = ("valid", "last_modified_time", "last_modified_date")


@compute_node(valid=("ts",), active=('ts',))
def valid(ts: REF[TIME_SERIES_TYPE], ts_value: TIME_SERIES_TYPE = None) -> TS[bool]:
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


@compute_node
def last_modified_time(ts: SIGNAL) -> TS[datetime]:
    return ts.last_modified_time


@compute_node
def last_modified_date(ts: SIGNAL) -> TS[date]:
    return ts.last_modified_time.date()
