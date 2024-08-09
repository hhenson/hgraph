from datetime import date, timedelta, datetime

from hgraph import compute_node, TS, OUT, WiringPort, combine, convert


@compute_node(overloads=convert, requires=lambda m, s: m[OUT].py_type == TS[datetime])
def convert_date_to_datetime(ts: TS[date]) -> TS[datetime]:
    return datetime(ts.value.year, ts.value.month, ts.value.day)


@compute_node(overloads=combine, requires=lambda m, s: m[OUT].py_type == TS[date])
def combine_date(year: TS[int], month: TS[int], day: TS[int]) -> TS[date]:
    return date(year.value, month.value, day.value)


@compute_node(
    overloads=combine,
    requires=lambda m, s: m[OUT].py_type == TS[timedelta],
    valid=lambda m, s: (k for k, v in s.items() if isinstance(v, WiringPort)) if s["__strict__"] else (),
)
def combine_timedelta(
    *,
    days: TS[int] = None,
    seconds: TS[int] = None,
    microseconds: TS[int] = None,
    milliseconds: TS[int] = None,
    minutes: TS[int] = None,
    hours: TS[int] = None,
    weeks: TS[int] = None,
    __strict__: bool = True,
) -> TS[timedelta]:
    return timedelta(
        days=days.value if days.valid else 0,
        seconds=seconds.value if seconds.valid else 0,
        microseconds=microseconds.value if microseconds.valid else 0,
        milliseconds=milliseconds.value if milliseconds.valid else 0,
        minutes=minutes.value if minutes.valid else 0,
        hours=hours.value if hours.valid else 0,
        weeks=weeks.value if weeks.valid else 0,
    )
