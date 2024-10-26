from datetime import date, datetime
from zoneinfo import ZoneInfo

from hg_oap.dates.dt_utils import date_tz_to_utc, date_time_utc_to_tz


def test_date_tz_to_utc():
    tz = ZoneInfo("Europe/London")
    assert date_tz_to_utc(date(2024, 1, 1), tz) == datetime(2024, 1, 1)
    assert date_tz_to_utc(date(2024, 9, 1), tz) == datetime(2024, 8, 31, 23)


def test_date_time_utc_to_tz():
    tz = ZoneInfo("Europe/London")
    assert date_time_utc_to_tz(datetime(2024, 1, 1), tz) == date(2024, 1, 1)
    assert date_time_utc_to_tz(datetime(2024, 8, 31, 23), tz) == date(2024, 9, 1)