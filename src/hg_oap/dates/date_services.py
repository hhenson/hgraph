__all__ = ()

from datetime import date, datetime, timedelta
from typing import TypeVar

from frozendict import frozendict
from hgraph import reference_service, default_path, TS, service_impl, EvaluationEngineApi, generator

from hg_oap.dates import Calendar

"""
Provide a set of standard services that can be used to provide common date-based services.
"""


@reference_service
def business_days(path: str = default_path) -> TS[date]:
    """
    The date contains the value of the current business day, it ticks at the start of a business date.
    Depending on the calendar underlying this service, it can be that the date ticks at an offset which is not
    consistent with UTC, for example: if the calendar represents UK business days, then the calendar will tick
    at 00:00:00 BST.

    This can be used to advance or snap values based on the calendar ticking, or the date may just be consumed as
    an attribute of a trade.
    """


CALENDAR_TYPE = TypeVar("CALENDAR_TYPE", bound=Calendar)


@service_impl(interfaces=[business_days])
def business_days_from_calendar(
        calendar_tp: object, params: frozendict[str, object] = None, time_zone: str = "UTC"
) -> TS[date]:
    return _business_days_from_calendar(calendar_tp, params, time_zone)


@generator
def _business_days_from_calendar(
        calendar_tp: type[CALENDAR_TYPE], params: frozendict[str, object] = None, time_zone: str = "UTC",
        _api: EvaluationEngineApi = None
) -> TS[date]:
    """
    Uses the calendar provided to tick out the working days. This is useful to run simulations over a particular
    period. This will tick at start time with the current date (if it is a working day) and then subsequently
    tick out dates at the start-of-day for the time-zone provided.
    """
    from zoneinfo import ZoneInfo
    from hg_oap.dates.dt_utils import date_time_utc_to_tz, date_tz_to_utc, UTC
    calendar = calendar_tp(**({} if params is None else params))
    if time_zone == "UTC":
        tz = UTC
        to_tz = lambda dt: datetime(dt.year, dt.month, dt.day)
    else:
        tz = ZoneInfo(time_zone)
        to_tz = lambda dt: date_tz_to_utc(time_zone)
    current_date = date_time_utc_to_tz(_api.start_time, tz)
    last_date = date_time_utc_to_tz(_api.end_time, tz)
    # Yield out the current date if appropriate,
    # but at the current time as this is unlikely to be the correct start of day for the localised date.
    if calendar.is_business_day(current_date):
        yield _api.start_time, current_date
    current_date += timedelta(days=1)

    # Now we can just loop over days until we are done.
    while current_date <= last_date:
        if calendar.is_business_day(current_date):
            yield to_tz(current_date), current_date
        current_date += timedelta(days=1)
