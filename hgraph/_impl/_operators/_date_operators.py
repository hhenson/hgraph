from datetime import date, time, datetime, timezone

import pytz

from hgraph import compute_node, TS, TSL, Size, explode, graph, day_of_month, month_of_year, year, add_

__all__ = ["add_date_time"]


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
def add_date_time_node(lhs: TS[date], rhs: TS[time], tz: TS[str] = None) -> TS[datetime]:
    """
    Add date and time to produce a datetime, this will be returned in UTC, 
    if tz is provided the time is assumed to be in the timezone and will be converted to UTC.
    """
    dt: date = lhs.value
    tm: time = rhs.value
    tz_name: str = tz.value if tz is not None and tz.valid else None
    return add_date_time(dt, tm, tz_name)


def add_date_time(dt: date, tm: time, tz_name: str = None) -> datetime:
    """
    Adds time to a date, taking into account the timezone if provided.
    The return value is in UTC.
    """
    dt_tm: datetime = datetime.combine(dt, tm)
    if tz_name is not None:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(tz_name)
        dt_tm = dt_tm.replace(tzinfo=tz)
        dt_tm = dt_tm.astimezone(timezone.utc)
        dt_tm = dt_tm.replace(tzinfo=None)  # Return naive UTC
    return dt_tm
