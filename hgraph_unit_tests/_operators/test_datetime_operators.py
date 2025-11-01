from datetime import date, datetime, timedelta

import pytest

from hgraph import sub_, add_, WiringError, mul_, div_, lt_, graph, TS, SCALAR
from hgraph.test import eval_node

import pytest
pytestmark = pytest.mark.smoke


@pytest.mark.parametrize(
    ["op", "d1", "d2", "expected"],
    [
        [sub_, date(2020, 1, 2), date(2020, 1, 1), timedelta(days=1)],
        [sub_, datetime(2020, 1, 2), datetime(2020, 1, 1), timedelta(days=1)],
        [sub_, date(2020, 1, 2), timedelta(days=1), date(2020, 1, 1)],
        [sub_, datetime(2020, 1, 2), timedelta(days=1), datetime(2020, 1, 1)],
        [add_, date(2020, 1, 2), timedelta(days=1), date(2020, 1, 3)],
        [add_, datetime(2020, 1, 2), timedelta(days=1), datetime(2020, 1, 3)],
        [add_, timedelta(hours=1), timedelta(seconds=1), timedelta(seconds=3601)],
    ],
)
def test_add_sub_date_datetime(op, d1, d2, expected):
    assert eval_node(op, d1, d2) == [expected]


def test_add_dates_attempt():
    with pytest.raises(WiringError) as e:
        eval_node(add_, date(2024, 1, 1), date(2024, 1, 2))
    assert "Cannot add two dates together" in str(e)


def test_add_datetimes_attempt():
    with pytest.raises(WiringError) as e:
        eval_node(add_, datetime(2024, 1, 1), datetime(2024, 1, 2))
    assert "Cannot add two datetimes together" in str(e)


def test_mul_timedelta_number():
    assert eval_node(mul_, timedelta(seconds=3), 4) == [timedelta(seconds=12)]


def test_div_timedelta_number():
    assert eval_node(div_, timedelta(seconds=4), 2) == [timedelta(seconds=2)]


def test_lt_timedelta():
    assert eval_node(lt_, timedelta(seconds=5), timedelta(minutes=1)) == [True]


@pytest.mark.parametrize(
    "attr,expected",
    (
        ("year", 2024),
        ("month", 11),
        ("day", 1),
        ("weekday", 4),
        ("isoformat", "2024-11-01"),
        ("isoweekday", 5),
    ),
)
def test_date_operators(attr, expected):
    @graph
    def g(d: TS[date]) -> TS[SCALAR]:
        return getattr(d, attr)

    assert eval_node(g, date(2024, 11, 1)) == [expected]
