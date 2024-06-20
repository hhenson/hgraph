from datetime import datetime, date, time, timedelta

from hgraph import (
    compute_node,
    graph,
    TS,
    SCALAR,
    getattr_,
    add_,
    sub_,
    WiringError,
    mul_,
    div_,
    NUMBER,
    sum_,
    mean,
    std,
    var,
)

__all__ = tuple()

_datetime_properties = {
    "year": int,
    "month": int,
    "day": int,
    "hour": int,
    "minute": int,
    "second": int,
    "microsecond": int,
}

_datetime_methods = {
    "weekday": int,
    "isoweekday": int,
    "timestamp": int,
    "date": date,
    "time": time,
}


@compute_node
def datetime_date_as_datetime(ts: TS[datetime]) -> TS[datetime]:
    value = ts.value
    return datetime(value.year, value.month, value.day)


_datetime_custom = {
    "datepart": datetime_date_as_datetime,
}


@compute_node(resolvers={SCALAR: lambda m, s: _datetime_properties[s["attribute"]]})
def datetime_properties(ts: TS[datetime], attribute: str) -> TS[SCALAR]:
    return getattr(ts.value, attribute)


@compute_node(resolvers={SCALAR: lambda m, s: _datetime_methods[s["attribute"]]})
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


@graph(overloads=add_)
def add_datetimes(lhs: TS[datetime], rhs: TS[datetime]) -> TS[datetime]:
    # This is to avoid the add_scalars getting wired and failing at runtime
    raise WiringError("Cannot add two datetimes together")


@graph(overloads=add_)
def add_dates(lhs: TS[date], rhs: TS[date]) -> TS[date]:
    # This is to avoid the add_scalars getting wired and failing at runtime
    raise WiringError("Cannot add two dates together")


@compute_node(overloads=add_)
def add_datetime_timedelta(lhs: TS[datetime], rhs: TS[timedelta]) -> TS[datetime]:
    return lhs.value + rhs.value


@compute_node(overloads=add_)
def add_date_timedelta(lhs: TS[date], rhs: TS[timedelta]) -> TS[date]:
    return lhs.value + rhs.value


@compute_node(overloads=sub_)
def sub_datetime_timedelta(lhs: TS[datetime], rhs: TS[timedelta]) -> TS[datetime]:
    return lhs.value - rhs.value


@compute_node(overloads=sub_)
def sub_date_timedelta(lhs: TS[date], rhs: TS[timedelta]) -> TS[date]:
    return lhs.value - rhs.value


@compute_node(overloads=sub_)
def sub_dates(lhs: TS[date], rhs: TS[date]) -> TS[timedelta]:
    return lhs.value - rhs.value


@compute_node(overloads=sub_)
def sub_datetimes(lhs: TS[datetime], rhs: TS[datetime]) -> TS[timedelta]:
    return lhs.value - rhs.value


@compute_node(overloads=mul_)
def mul_timedelta_number(lhs: TS[timedelta], rhs: TS[NUMBER]) -> TS[timedelta]:
    return lhs.value * rhs.value


@compute_node(overloads=div_)
def div_timedelta_number(lhs: TS[timedelta], rhs: TS[NUMBER]) -> TS[timedelta]:
    return lhs.value / rhs.value


@graph(overloads=sum_)
def sum_date_unary(ts: TS[date]) -> TS[date]:
    raise WiringError("Cannot sum dates")


@graph(overloads=sum_)
def sum_datetime_unary(ts: TS[datetime]) -> TS[datetime]:
    raise WiringError("Cannot sum datetimes")


@graph(overloads=mean)
def mean_date_unary(ts: TS[date]) -> TS[date]:
    raise WiringError("Cannot take the mean of dates")


@graph(overloads=mean)
def mean_datetime_unary(ts: TS[datetime]) -> TS[datetime]:
    raise WiringError("Cannot take the mean of datetimes")


@graph(overloads=std)
def std_datetime_unary(ts: TS[datetime]) -> TS[datetime]:
    raise WiringError("Cannot calculate the standard deviation of datetimes")


@graph(overloads=std)
def std_datetime_unary(ts: TS[datetime]) -> TS[datetime]:
    raise WiringError("Cannot calculate the standard deviation of dates")


@graph(overloads=var)
def var_datetime_unary(ts: TS[datetime]) -> TS[datetime]:
    raise WiringError("Cannot calculate the variance of datetimes")


@graph(overloads=var)
def var_datetime_unary(ts: TS[datetime]) -> TS[datetime]:
    raise WiringError("Cannot calculate the variance of dates")
