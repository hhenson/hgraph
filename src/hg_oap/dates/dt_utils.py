from datetime import date, datetime
from zoneinfo import ZoneInfo

__all__ = ("UTC", "date_tz_to_utc", "date_time_utc_to_tz")

UTC = ZoneInfo("UTC")

def date_tz_to_utc(dt: date, tz: ZoneInfo) -> datetime:
    """
    Provide the UTC ``datetime`` for a date given the tz info provided.
    """
    local = datetime.combine(dt, datetime.min.time(), tzinfo=tz)
    result = local.astimezone(UTC)
    return result.replace(tzinfo=None)

def date_time_utc_to_tz(dt: datetime, tz: ZoneInfo) -> date:
    """Returns the date represented by this dt (as UTC) in the tz provided."""
    return dt.replace(tzinfo=UTC).astimezone(tz).date()

