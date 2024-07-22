from datetime import date, timedelta

import holidays
import pytest

from hg_oap.dates.calendar import WeekendCalendar, HolidayCalendar
from hg_oap.dates.dgen import (
    make_date,
    make_dgen,
    days,
    weeks,
    weekdays,
    weekends,
    months,
    years,
    business_days,
    quarters,
    roll_fwd,
    roll_bwd,
    DGenParameter,
    month_last_bday,
    month_first_bday,
)
from hg_oap.utils.op import Expression
from hg_oap.dates.tenor import Tenor
from hg_oap.utils.op import Expression


@pytest.mark.parametrize(
    ["s", "valid"],
    [
        ("2y3m1w6d", True),
        ("3m", True),
        ("1w", True),
        ("6d", True),
        ("2y", True),
        ("2b", True),
        ("2d2b", False),
        ("2 d", False),
        ("2", False),
        ("d", False),
        ("0d", True),
        ("1yd", False),
        ("20000y", True),
        ("-2y", True),
        ("-2b", True),
    ],
)
def test_tenor_str(s, valid):
    if valid:
        assert str(Tenor(s)) == s
    else:
        try:
            assert str(Tenor(s)) == s
            assert False, f"{s} is not valid tenor, the test should have failed"
        except ValueError:
            pass


def test_tenor_td():
    assert str(Tenor(timedelta(days=2))) == "2d"


def test_tenor_add():
    assert Tenor("1y1m").add_to(make_date("2023-01-31")) == date(2024, 2, 29)
    assert Tenor("1y1m1w").add_to(make_date("2023-01-31")) == date(2024, 3, 7)
    assert Tenor("1w23d").add_to(make_date("2023-01-31")) == date(2023, 3, 2)

    assert Tenor("1m").add_to(make_date("2023-12-31")) == date(2024, 1, 31)
    assert Tenor("1m").add_to(make_date("2023-11-30")) == date(2023, 12, 30)


def test_tenor_sub():
    assert Tenor("1y1m").sub_from(make_date("2023-01-31")) == date(2021, 12, 31)
    assert Tenor("1y1m1w").sub_from(make_date("2023-01-31")) == date(2021, 12, 24)
    assert Tenor("1w23d").sub_from(make_date("2023-01-31")) == date(2023, 1, 1)

    assert Tenor("1m").sub_from(make_date("2023-12-31")) == date(2023, 11, 30)
    assert Tenor("1m").sub_from(make_date("2024-01-30")) == date(2023, 12, 30)


def test_date_generator():
    c = make_dgen("2024-01-01")
    assert tuple(d for d in c()) == (date(2024, 1, 1),)

    c = make_dgen(["2024-01-01", "2024-01-02"])
    assert tuple(d for d in c()) == (date(2024, 1, 1), date(2024, 1, 2))

    c = make_dgen(["2024-01-01", "2024-01-02"])[1]
    assert tuple(d for d in c()) == (date(2024, 1, 2),)

    c = (
        "2024-01-15"
        < make_dgen(["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"])
        < "2024-03-15"
    )
    assert tuple(d for d in c()) == (date(2024, 2, 1), date(2024, 3, 1))

    c = "2024-01-01" < days <= "2024-01-05"
    assert tuple(d for d in c()) == (
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
    )

    c = "2024-01-01" < days[::2] <= "2024-01-05"
    assert tuple(d for d in c()) == (date(2024, 1, 2), date(2024, 1, 4))

    c = "2024-01-01" <= weeks <= "2024-02-01"
    assert tuple(d for d in c()) == (
        date(2024, 1, 1),
        date(2024, 1, 8),
        date(2024, 1, 15),
        date(2024, 1, 22),
        date(2024, 1, 29),
    )

    c = "2024-01-01" <= weeks[1:3] <= "2024-02-01"
    assert tuple(d for d in c()) == (date(2024, 1, 8), date(2024, 1, 15))

    c = "2024-01-01" <= weeks + "2d" <= "2024-02-01"
    assert tuple(d for d in c()) == (
        date(2024, 1, 3),
        date(2024, 1, 10),
        date(2024, 1, 17),
        date(2024, 1, 24),
        date(2024, 1, 31),
    )

    c = "2024-01-01" <= weeks.wed <= "2024-02-01"
    assert tuple(d for d in c()) == (
        date(2024, 1, 3),
        date(2024, 1, 10),
        date(2024, 1, 17),
        date(2024, 1, 24),
        date(2024, 1, 31),
    )

    c = "2024-01-03" <= weeks.fri <= "2024-02-01"
    assert tuple(d for d in c()) == (
        date(2024, 1, 5),
        date(2024, 1, 12),
        date(2024, 1, 19),
        date(2024, 1, 26),
    )

    c = "2024-01-01" <= weeks.fri <= "2024-12-31"
    assert tuple() == tuple(d.weekday() for d in c() if d.weekday() != 4)

    c = "2024-01-01" <= weekdays <= "2024-01-31"
    assert tuple(d for d in c()) == tuple(
        d
        for d in (date(2024, 1, 1) + timedelta(days=i) for i in range(31))
        if d.weekday() < 5
    )

    c = "2024-01-01" <= weekends <= "2024-01-31"
    assert tuple(d for d in c()) == tuple(
        d
        for d in (date(2024, 1, 1) + timedelta(days=i) for i in range(31))
        if d.weekday() >= 5
    )

    c = "2024-01-01" <= business_days.over(WeekendCalendar()) <= "2024-01-31"
    assert tuple(d for d in c()) == tuple(
        d
        for d in (date(2024, 1, 1) + timedelta(days=i) for i in range(31))
        if d.weekday() < 5
    )

    c = "2024-01-03" <= weeks.fri | "2024-01-15" <= "2024-02-01"
    assert tuple(d for d in c()) == (
        date(2024, 1, 5),
        date(2024, 1, 12),
        date(2024, 1, 15),
        date(2024, 1, 19),
        date(2024, 1, 26),
    )

    c = "2024-01-03" <= "2024-01-15" | weeks.fri <= "2024-02-01"
    assert tuple(d for d in c()) == (
        date(2024, 1, 5),
        date(2024, 1, 12),
        date(2024, 1, 15),
        date(2024, 1, 19),
        date(2024, 1, 26),
    )

    c = "2024-01-03" <= ("2024-01-15" | weeks.fri)[2] <= "2024-02-01"
    assert tuple(d for d in c()) == (date(2024, 1, 15),)

    c = "2024-01-03" <= weeks.fri & "2024-01-15" <= "2024-02-01"
    assert tuple(d for d in c()) == ()

    c = "2024-01-03" <= "2024-01-15" & weeks.fri <= "2024-02-01"
    assert tuple(d for d in c()) == ()

    c = "2024-01-03" <= weeks.fri - "2024-01-12" <= "2024-02-01"
    assert tuple(d for d in c()) == (
        date(2024, 1, 5),
        date(2024, 1, 19),
        date(2024, 1, 26),
    )

    c = (
        "2024-01-03"
        <= (date(2024, 1, 5), date(2024, 1, 12), date(2024, 1, 15)) - weeks.fri[1]
        <= "2024-02-01"
    )
    assert tuple(d for d in c()) == (date(2024, 1, 5), date(2024, 1, 15))

    c = "2024-01-03" <= months <= "2024-04-01"
    assert tuple(d for d in c()) == (
        date(2024, 2, 1),
        date(2024, 3, 1),
        date(2024, 4, 1),
    )

    c = "2024-01-03" <= months.end <= "2024-04-01"
    assert tuple(d for d in c()) == (
        date(2024, 1, 31),
        date(2024, 2, 29),
        date(2024, 3, 31),
    )

    c = "2024-01-03" <= months.weeks <= "2024-02-11"
    assert tuple(d for d in c()) == (
        date(2024, 1, 8),
        date(2024, 1, 15),
        date(2024, 1, 22),
        date(2024, 1, 29),
        date(2024, 2, 5),
    )

    c = "2024-01-03" <= months.weeks[2] <= "2024-02-11"
    assert tuple(d for d in c()) == (date(2024, 1, 15),)

    c = "2024-01-03" <= months.weeks[-2] <= "2024-02-28"
    assert tuple(d for d in c()) == (date(2024, 1, 22), date(2024, 2, 19))

    c = "2024-01-03" <= months.weeks.fri <= "2024-02-11"
    assert tuple(d for d in c()) == (
        date(2024, 1, 5),
        date(2024, 1, 12),
        date(2024, 1, 19),
        date(2024, 1, 26),
        date(2024, 2, 2),
        date(2024, 2, 9),
    )

    c = "2024-01-03" <= months.weeks[-2].fri <= "2024-02-28"
    assert tuple(d for d in c()) == (date(2024, 1, 26), date(2024, 2, 23))

    c = "2024-01-03" <= months.weekends <= "2024-02-03"
    assert tuple(d for d in c()) == tuple(
        d
        for d in (date(2024, 1, 3) + timedelta(days=i) for i in range(32))
        if d.weekday() >= 5
    )

    c = "2020-01-03" <= years <= "2024-01-03"
    assert tuple(d for d in c()) == (
        date(2021, 1, 1),
        date(2022, 1, 1),
        date(2023, 1, 1),
        date(2024, 1, 1),
    )

    c = "2020-01-03" <= years.end < "2024-12-31"
    assert tuple(d for d in c()) == (
        date(2020, 12, 31),
        date(2021, 12, 31),
        date(2022, 12, 31),
        date(2023, 12, 31),
    )

    c = "2020-01-03" <= years.months < "2024-12-31"
    assert tuple(d for d in c()) == tuple(
        d for d in ("2020-01-03" <= months < "2024-12-31")()
    )

    c = "2020-01-03" <= years.months[0:3] < "2024-12-31"
    assert tuple(d for d in c()) == tuple(
        d for d in ("2020-01-03" <= months < "2024-12-31")() if d.month <= 3
    )

    c = "2020-01-03" <= years.months.weeks < "2024-12-31"
    assert tuple(d for d in c()) == tuple(
        d for d in ("2020-01-03" <= weeks < "2024-12-31")()
    )

    c = "2020-01-03" <= years.months.weeks.fri < "2024-12-31"
    assert tuple(d for d in c()) == tuple(
        d for d in ("2020-01-03" <= weeks.fri < "2024-12-31")()
    )

    # not these are not third Fridays of Aprils, but Fridays on the third week that starts in April
    c = "2020-01-03" <= years.apr.weeks[2].fri < "2023-12-31"
    assert tuple(d for d in c()) == (
        date(2020, 4, 24),
        date(2021, 4, 23),
        date(2022, 4, 22),
        date(2023, 4, 21),
    )

    # these are third Fridays of Apr
    c = "2020-01-03" <= years.apr.fri[2] < "2023-12-31"
    assert tuple(d for d in c()) == (
        date(2020, 4, 17),
        date(2021, 4, 16),
        date(2022, 4, 15),
        date(2023, 4, 21),
    )

    calendar = HolidayCalendar(
        holidays.country_holidays("GB", "ENG")["2020-01-03":"2023-12-31"]
    )

    c = "2020-01-03" <= roll_fwd(years.apr.fri[2], calendar) < "2023-12-31"
    assert tuple(d for d in c()) == (
        date(2020, 4, 17),
        date(2021, 4, 16),
        date(2022, 4, 19),
        date(2023, 4, 21),
    )

    c = "2020-01-03" <= roll_bwd(years.apr.fri[2]).over(calendar) < "2023-12-31"
    assert tuple(d for d in c()) == (
        date(2020, 4, 17),
        date(2021, 4, 16),
        date(2022, 4, 14),
        date(2023, 4, 21),
    )

    c = "2020-01-03" <= month_last_bday(years.apr).over(calendar) < "2023-12-31"
    assert tuple(d for d in c()) == (
        date(2020, 4, 30),
        date(2021, 4, 30),
        date(2022, 4, 29),
        date(2023, 4, 28),
    )

    c = "2020-01-03" <= month_first_bday(years.may).over(calendar) < "2023-12-31"
    assert tuple(d for d in c()) == (
        date(2020, 5, 1),
        date(2021, 5, 4),
        date(2022, 5, 3),
        date(2023, 5, 2),
    )


def test_date_expressions():
    from hg_oap.utils.op import ParameterOp

    _0 = ParameterOp(_index=0)
    _1 = ParameterOp(_index=1)

    c = _0 <= months.fri <= _1
    assert list(Expression(c)("2020-01-01", "2020-02-01")()) == [
        date(2020, 1, 3),
        date(2020, 1, 10),
        date(2020, 1, 17),
        date(2020, 1, 24),
        date(2020, 1, 31),
    ]

    c = "2020-01-01" <= _0 < "2020-02-01"
    assert list(Expression(c)(months.fri)()) == [
        date(2020, 1, 3),
        date(2020, 1, 10),
        date(2020, 1, 17),
        date(2020, 1, 24),
        date(2020, 1, 31),
    ]

    c = months.fri[_0]
    assert list(Expression(c)(1)(after="2020-01-01", before="2020-02-01")) == [
        date(2020, 1, 10)
    ]


def test_dgen_parameter():
    _0 = DGenParameter("_0")
    _1 = DGenParameter("_1")

    c = _0 <= months.fri <= _1
    assert list(c(_0="2020-01-01", _1="2020-02-01")) == [
        date(2020, 1, 3),
        date(2020, 1, 10),
        date(2020, 1, 17),
        date(2020, 1, 24),
        date(2020, 1, 31),
    ]


def test_weekdays_in_month():
    c = '2024-01-01' <= months.weekdays < '2024-01-31'
    assert len(list((c()))) == 22


def test_roll_fwd():
    calendar = HolidayCalendar(holidays=(date(2023, 5, 1), date(2024, 1, 1)))
    days = '2023-04-28' <= years.days <= '2023-05-02'
    assert list(roll_fwd(days, calendar)()) == [date(2023, 4, 28), date(2023, 5, 2), date(2023, 5, 2), date(2023, 5, 2), date(2023, 5, 2)]

    days = '2023-12-29' <= years.days <= '2024-01-02'
    assert list(roll_fwd(days, calendar)()) == [date(2023, 12, 29), date(2024, 1, 2), date(2024, 1, 2), date(2024, 1, 2), date(2024, 1, 2)]


def test_roll_bwd():
    calendar = WeekendCalendar()
    days = '2024-06-21' <= years.days <= '2024-06-24'  # Fri to Mon
    assert list(roll_bwd(days, calendar)()) == [date(2024, 6, 21), date(2024, 6, 21), date(2024, 6, 21), date(2024, 6, 24)]


def test_quarters():
    qs = '2024-02-01' < quarters < '2024-11-02'
    assert list(qs()) == [date(2024, 4, 1), date(2024, 7, 1), date(2024, 10, 1)]

    days = '2024-01-01' <= quarters.days <= '2025-01-01'
    assert len(list(days())) == 367

    m = '2024-01-01' <= quarters.months <= '2025-01-01'
    assert len(list(m())) == 13
