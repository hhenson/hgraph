from datetime import datetime, timedelta, timezone

__all__ = ("MIN_TD", "MIN_DT", "MAX_DT", "MIN_ST", "MAX_ET", "utc_now")


MIN_TD = timedelta(microseconds=1)  # The smallest engine time increment
MIN_DT = datetime(1970, 1, 1, 0, 0, 0, 0)  # The smallest engine time
MAX_DT = datetime(2300, 1, 1, 0, 0, 0, 0)  # The largest engine time
MIN_ST = MIN_DT + MIN_TD  # The smallest engine start time
MAX_ET = MAX_DT - MIN_TD  # The largest engine end time


def utc_now() -> datetime:
    """Return the current UTC time as a naive datetime.

    Replaces the deprecated ``datetime.utcnow()`` while remaining compatible
    with the rest of hgraph which uses naive (tzinfo-free) datetimes throughout.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)
