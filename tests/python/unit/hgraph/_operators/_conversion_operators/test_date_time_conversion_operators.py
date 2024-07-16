from datetime import date, timedelta

from hgraph import combine, TS, graph
from hgraph.test import eval_node


def test_combine_date():
    @graph
    def g(year: TS[int], month: TS[int], day: TS[int]) -> TS[date]:
        return combine[TS[date]](year=year, month=month, day=day)

    assert eval_node(g, 2022, 1, 1) == [date(2022, 1, 1)]


def test_combine_timedelta():
    @graph
    def g(days: TS[int]) -> TS[timedelta]:
        return combine[TS[timedelta]](days=days)

    assert eval_node(g, 2022) == [timedelta(2022)]


def test_combine_timedelta_strict():
    @graph
    def g(
        days: TS[int],
        seconds: TS[int],
        microseconds: TS[int],
        milliseconds: TS[int],
        minutes: TS[int],
        hours: TS[int],
        weeks: TS[int],
    ) -> TS[timedelta]:
        return combine[TS[timedelta]](
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            milliseconds=milliseconds,
            microseconds=microseconds,
        )

    assert eval_node(g, weeks=1, days=1, hours=1, minutes=1, seconds=1, milliseconds=1, microseconds=[None, 1]) == [
        None,
        timedelta(days=8, hours=1, minutes=1, seconds=1, milliseconds=1, microseconds=1),
    ]


def test_combine_timedelta_non_strict():
    @graph
    def g(days: TS[int], hours: TS[int]) -> TS[timedelta]:
        return combine[TS[timedelta]](days=days, hours=hours, __strict__=False)

    assert eval_node(g, [2022], [None, 1]) == [timedelta(days=2022), timedelta(days=2022, hours=1)]
