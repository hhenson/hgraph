from datetime import datetime, date, time, timedelta

from hgraph import compute_node, graph, TS, TIME_SERIES_TYPE, SCALAR
from hgraph._runtime._operators import getattr_, add_, sub_

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
def datetime_add_delta(ts: TS[datetime], delta: TS[timedelta]) -> TS[datetime]:
    return ts.value + delta.value


@compute_node(overloads=add_)
def date_add_delta(ts: TS[date], delta: TS[timedelta]) -> TS[datetime]:
    return ts.value + delta.value


@compute_node(overloads=sub_)
def datetime_sub_delta(ts: TS[datetime], delta: TS[timedelta]) -> TS[datetime]:
    return ts.value - delta.value


@compute_node(overloads=sub_)
def date_sub_delta(ts: TS[date], delta: TS[timedelta]) -> TS[datetime]:
    return ts.value - delta.value

