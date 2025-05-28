from datetime import date, time, datetime

import pytz

from hgraph import compute_node, TS, TSL, Size, explode, graph, day_of_month, month_of_year, year, add_

__all__ = ["add_date_time",]


@compute_node(overloads=explode)
def explode_date_impl(ts: TS[date], _output: TSL[TS[int], Size[3]] = None) -> TSL[TS[int], Size[3]]:
    if _output.valid:
        out = {}
        dt = ts.value
        day, month, year = dt.day, dt.month, dt.year
        if _output[2].value != day:
            out[2] = day
        if _output[1].value != month:
            out[1] = month
        if _output[0].value != year:
            out[0] = year
        return out
    else:
        dt = ts.value
        return (dt.year, dt.month, dt.day)


@graph(overloads=day_of_month)
def day_of_month_impl(ts: TS[date]) -> TS[int]:
    """The day of the moth of the given date."""
    return explode(ts)[2]


@graph(overloads=month_of_year)
def month_of_year_impl(ts: TS[date]) -> TS[int]:
    """The month of the year of the given date."""
    return explode(ts)[1]


@graph(overloads=year)
def year_impl(ts: TS[date]) -> TS[int]:
    """The year of the given date."""
    return explode(ts)[0]


@compute_node(overloads=add_)
def add_date_time_node(lhs: TS[date], rhs: TS[time]) -> TS[datetime]:
    """
    Add date and time to produce a datetime, this will be returned in UTC. Thus, if the time
    has a time-zone, it will be adjusted to UTC once combined and then returned.
    """
    dt: date = lhs.value
    tm: time = rhs.value
    return add_date_time(dt, tm)


def add_date_time(dt: date, tm: time) -> datetime:
    """
    Adds time to a date, taking into account the time-zone info if present.
    The return value is in UTC.
    """
    dt_tm: datetime = datetime.combine(dt, tm.replace(tzinfo=None))
    if (tz := tm.tzinfo) is not None:
        dt_tm = tz.localize(dt_tm)
        dt_tm = dt_tm.astimezone(pytz.UTC)
        dt_tm = dt_tm.replace(tzinfo=None)
    return dt_tm
