from datetime import datetime, date, time, timedelta

from hgraph import compute_node, graph, TS, SCALAR, SIGNAL
from hgraph._runtime._operators import getattr_, add_, sub_, lt_, gt_, eq_, le_, ge_

__all__ = (
    "datetime_date_as_datetime", "datetime_properties", "datetime_methods", "datetime_getattr", "datetime_add_delta",
    "date_add_delta", "datetime_sub_delta", "date_sub_delta", "modified_datetime", "modified_date")

_datetime_properties = {
    'year': int,
    'month': int,
    'day': int,
    'hour': int,
    'minute': int,
    'second': int,
    'microsecond': int,
}

_datetime_methods = {
    'weekday': int,
    'isoweekday': int,
    'timestamp': int,
    'date': date,
    'time': time,
}


@compute_node
def datetime_date_as_datetime(ts: TS[datetime]) -> TS[datetime]:
    value = ts.value
    return datetime(value.year, value.month, value.day)


_datetime_custom = {
    'datepart': datetime_date_as_datetime,
}


@compute_node(resolvers={SCALAR: lambda m, s: _datetime_properties[s['attribute']]})
def datetime_properties(ts: TS[datetime], attribute: str) -> TS[SCALAR]:
    return getattr(ts.value, attribute)


@compute_node(resolvers={SCALAR: lambda m, s: _datetime_methods[s['attribute']]})
def datetime_methods(ts: TS[datetime], attribute: str) -> TS[SCALAR]:
    return getattr(ts.value, attribute)()


@graph(overloads=getattr_)
def datetime_getattr(ts: TS[datetime], attribute: str) -> TS[SCALAR]:
    if attribute in _datetime_properties:
        return datetime_properties(ts, attribute)
    elif attribute in _datetime_methods:
        return datetime_methods(ts, attribute)
    elif attribute in _datetime_custom:
        return _datetime_custom[attribute](ts)
    else:
        raise AttributeError(f"TS[datetime] has no property {attribute}")


@compute_node(overloads=add_)
def datetime_add_delta(lhs: TS[datetime], rhs: TS[timedelta]) -> TS[datetime]:
    return lhs.value + rhs.value


@compute_node(overloads=add_)
def date_add_delta(lhs: TS[date], rhs: TS[timedelta]) -> TS[date]:
    return lhs.value + rhs.value


@compute_node(overloads=sub_)
def datetime_sub_delta(lhs: TS[datetime], rhs: TS[timedelta]) -> TS[datetime]:
    return lhs.value - rhs.value


@compute_node(overloads=sub_)
def date_sub_delta(lhs: TS[date], rhs: TS[timedelta]) -> TS[date]:
    return lhs.value - rhs.value


@compute_node(overloads=lt_)
def lt_date(lhs: TS[date], rhs: TS[date]) -> TS[bool]:
    return lhs.value < rhs.value


@compute_node(overloads=le_)
def le_date(lhs: TS[date], rhs: TS[date]) -> TS[bool]:
    return lhs.value <= rhs.value


@compute_node(overloads=gt_)
def gt_date(lhs: TS[date], rhs: TS[date]) -> TS[bool]:
    return lhs.value > rhs.value


@compute_node(overloads=ge_)
def lt_date(lhs: TS[date], rhs: TS[date]) -> TS[bool]:
    return lhs.value >= rhs.value


@compute_node(overloads=eq_)
def eq_date(lhs: TS[date], rhs: TS[date]) -> TS[bool]:
    return lhs.value == rhs.value


@compute_node(overloads=sub_)
def sub_date(lhs: TS[date], rhs: TS[date]) -> TS[timedelta]:
    return lhs.value - rhs.value


@compute_node(overloads=sub_)
def sub_datetime(lhs: TS[datetime], rhs: TS[datetime]) -> TS[timedelta]:
    return lhs.value - rhs.value


@compute_node
def modified_date(ts: SIGNAL) -> TS[date]:
    """
    The date that this ts was modified.
    """
    return ts.last_modified_time.date()


@compute_node
def modified_datetime(ts: SIGNAL) -> TS[datetime]:
    """
    The datetime that this time-series was modified.
    """
    return ts.last_modified_time
