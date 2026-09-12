from datetime import date, time, datetime, timezone
from zoneinfo import ZoneInfo

from hgraph import (
    TS, day, day_of_month, default, explode, graph, month, month_of_year, year,
)
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


@pytest.mark.skip(reason="deviation: the native DateTime scalar currently stores naive UTC values")
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


@pytest.mark.skip(reason="deviation: the native DateTime scalar currently stores naive UTC values")
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
    
    
@pytest.mark.skip(reason="deviation: standalone timezone-aware time values require a future zoned scalar")
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


@pytest.mark.skip(reason="deviation: standalone timezone-aware time values require a future zoned scalar")
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



def test_date_components_elide_an_unchanged_value():
    """Released hgraph spells these ``explode(ts)[n]`` over an explode that
    publishes only what changed, so an unchanged component is not an event
    there (issue #822). Verified against 0.5.41: ``day_of_month`` over these
    dates gives ``[21, None, 22]`` and ``month_of_year`` gives ``[1, 2, None]``.
    """
    dates = [date(2024, 1, 21), date(2024, 2, 21), date(2024, 2, 22)]

    assert eval_node(day_of_month, dates) == [21, None, 22]
    assert eval_node(month_of_year, dates) == [1, 2, None]
    assert eval_node(year, dates) == [2024, None, None]

    # explode, which the released implementation projects these from, agreed
    # already and must keep agreeing.
    assert eval_node(explode, dates) == [{0: 2024, 1: 1, 2: 21}, {1: 2}, {2: 22}]


def test_date_attributes_reemit_an_unchanged_component():
    """``ts.day`` is NOT ``day_of_month(ts)``; the two spellings reach
    different operators and must keep behaving differently.

    Upstream, the attribute form is ``getattr_(ts, "day")``, which recomputes
    and publishes every tick, while the operator form is ``explode(ts)[n]``
    over an explode that publishes only what changed. Verified against 0.5.41:
    ``.day`` gives [21, 21, 22] where ``day_of_month`` gives [21, None, 22].
    Collapsing them would silently drop events from every program that reads
    the attribute (issue #822).
    """
    dates = [date(2024, 1, 21), date(2024, 2, 21), date(2024, 2, 22)]

    @graph
    def day_attr(ts: TS[date]) -> TS[int]:
        return ts.day

    @graph
    def month_attr(ts: TS[date]) -> TS[int]:
        return ts.month

    @graph
    def year_attr(ts: TS[date]) -> TS[int]:
        return ts.year

    assert eval_node(day_attr, dates) == [21, 21, 22]
    assert eval_node(month_attr, dates) == [1, 2, 2]
    assert eval_node(year_attr, dates) == [2024, 2024, 2024]

    # The contrast is the point: the operator spelling still elides.
    assert eval_node(day_of_month, dates) == [21, None, 22]
    assert eval_node(month_of_year, dates) == [1, 2, None]
    assert eval_node(year, dates) == [2024, None, None]


def test_date_attribute_aliases_follow_their_component_operator():
    """``day`` and ``month`` are this runtime's names for the same
    implementations, so they elide identically.

    Neither exists in released hgraph, so no parity constraint applies to the
    names themselves; what would be incoherent is one spelling of a single
    implementation ticking where the other does not.
    """
    dates = [date(2024, 1, 21), date(2024, 2, 21), date(2024, 2, 22)]

    assert eval_node(day, dates) == eval_node(day_of_month, dates)
    assert eval_node(month, dates) == eval_node(month_of_year, dates)
