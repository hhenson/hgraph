from datetime import date, time, datetime, timezone
from zoneinfo import ZoneInfo

from hgraph import default, explode, graph, TS
from hgraph.test import eval_node


import pytest
pytestmark = pytest.mark.smoke

def test_explode():
    assert eval_node(explode, [date(2024, 1, 1), date(2024, 1, 2), date(2024, 2, 2), date(2025, 2, 2)]) == [
        {0: 2024, 1: 1, 2: 1},
        {2: 2},
        {1: 2},
        {0: 2025},
    ]


@pytest.mark.skip("Skip as the C++ engine only deals with naive datetime objects")
def test_add_date_time():
    @graph
    def g(dt: TS[date], tm: TS[time]) -> TS[datetime]:
        return dt + tm

    assert eval_node(
        g,
        [date(2024, 1, 1)],
        [
            time(10, 0, 0),
            time(10, 0, 0, tzinfo=ZoneInfo('Africa/Johannesburg')),
            time(10, 0, 0, tzinfo=pytz.timezone('Africa/Johannesburg'))
        ],
    ) == [
               datetime(2024, 1, 1, 10, 0, 0),
               datetime(2024, 1, 1, 8, 0, 0),
               datetime(2024, 1, 1, 8, 0, 0),
           ]


@pytest.mark.skip("Skip as the C++ engine only deals with naive datetime objects")
def test_datetime_tzname():
    @graph
    def g(dt: TS[datetime]) -> TS[str]:
        return dt.tzname

    assert eval_node(
        g,
        [
            datetime(2024, 1, 1, 10, 0, 0),
            datetime(2024, 1, 1, 10, 0, 0, tzinfo=ZoneInfo('Africa/Johannesburg')),
        ],
    ) == [
               None,
               'SAST',
           ]
    
    
@pytest.mark.skip("Skip as the C++ engine only deals with naive datetime objects")
def test_add_date_time_tz():
    @graph
    def g(dt: TS[date], tm: TS[time]) -> TS[datetime]:
        return add_date_time_tz(dt, tm)

    assert eval_node(
        g,
        [date(2024, 1, 1)],
        [
            time(10, 0, 0),
            time(10, 0, 0, tzinfo=ZoneInfo('Africa/Johannesburg'))
        ],
    ) == [
               datetime(2024, 1, 1, 10, 0, 0),
               datetime(2024, 1, 1, 10, 0, 0, tzinfo=ZoneInfo('Africa/Johannesburg')),
           ]


@pytest.mark.skip("Skip as the C++ engine only deals with naive datetime objects")
def test_add_date_time_tz_tzname():
    @graph
    def g(dt: TS[date], tm: TS[time]) -> TS[str]:
        return default(add_date_time_tz(dt, tm).tzname, 'UTC')

    assert eval_node(
        g,
        [date(2024, 1, 1)],
        [
            time(10, 0, 0),
            time(10, 0, 0, tzinfo=ZoneInfo('Africa/Johannesburg'))
        ],
    ) == [
               'UTC',
               'SAST',
           ]


