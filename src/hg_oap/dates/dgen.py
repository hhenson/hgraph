from datetime import date, timedelta
from itertools import islice
from typing import cast

from hg_oap.dates.calendar import Calendar
from hg_oap.utils.op import Item, Op, lazy
from hg_oap.dates.tenor import Tenor

__all__ = ('is_dgen', 'make_date', 'make_dgen', 'years', 'months', 'weeks', 'weekdays', 'weekends', 'days',
           'business_days', 'roll_fwd', 'roll_bwd')


def is_dgen(obj):
    return isinstance(obj, DGen)


def make_date(obj):
    if isinstance(obj, DGen):
        return obj
    if isinstance(obj, date):
        return obj
    if isinstance(obj, str):
        return date.fromisoformat(obj)
    if isinstance(obj, (tuple, list)):
        return type(obj)(make_date(d) for d in obj)
    return None


def make_dgen(obj):
    if isinstance(obj, DGen):
        return obj
    if isinstance(obj, date):
        return ConstDGen(obj)
    if isinstance(obj, str):
        return ConstDGen(date.fromisoformat(obj))
    if isinstance(obj, (tuple, list)):
        return SequenceDGen(make_date(obj))


def is_negative_slice(item):
    return item.start is not None and item.start < 0 \
        or item.stop is not None and item.stop < 0 \
        or item.step is not None and item.step < 0


class DGen(Item):
    def __call__(self, input_date=None, start: date = date.min, end: date = date.max, after: date = date.min,
                 before: date = date.max, calendar: Calendar = None):
        return self.__invoke__(make_date(start), make_date(end), make_date(after), make_date(before), calendar)

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        raise StopIteration

    def is_single_date_gen(self):
        return False

    def cadence(self):
        return None

    def __iter__(self):
        return self

    def __or__(self, other):
        return JoinDGen(self, make_dgen(other))

    def __ror__(self, other):
        return JoinDGen(make_dgen(other), self)

    def __and__(self, other):
        return CommonDatesDGen(self, make_dgen(other))

    def __rand__(self, other):
        return CommonDatesDGen(make_dgen(other), self)

    def __gt__(self, other):
        if not is_dgen(other):
            return AfterDGen(self, make_date(other))
        elif other.is_single_date_gen():
            return AfterDGen(self, other)
        else:
            raise ValueError('Comparing two date generators is not supported')

    def __lt__(self, other):
        if is_dgen(other) and self.is_single_date_gen():
            return other.__ge__(self)
        else:
            if lhs := getattr(self, '__compared__', None):
                return BeforeDGen(lhs, make_date(other))
            if self.__expression__ is not None:
                return self.__expression__ < other
            return BeforeDGen(self, make_date(other))

    def __ge__(self, other):
        if not is_dgen(other):
            return AfterOrOnDGen(self, make_date(other))
        elif other.is_single_date_gen():
            return AfterOrOnDGen(self, other)
        else:
            raise ValueError('Comparing two dage generators is not supported')

    def __le__(self, other):
        if is_dgen(other) and self.is_single_date_gen():
            return other.__gt__(self)
        else:
            if lhs := getattr(self, '__compared__', None):
                return BeforeOrOnDGen(lhs, make_date(other))
            if self.__expression__ is not None:
                return self.__expression__ <= other
            return BeforeOrOnDGen(self, make_date(other))

    def __add__(self, other):
        return AddTenorDGen(self, Tenor(other))

    def __sub__(self, other):
        try:
            tenor = Tenor(other)
            return SubTenorDGen(self, tenor)
        except:
            pass

        try:
            gen = make_dgen(other)
            return RemoveDatesDGen(self, gen)
        except:
            pass

        raise ValueError(f'{other} is not a tenor or date generator')

    def __rsub__(self, other):
        gen = make_dgen(other)
        return RemoveDatesDGen(gen, self)

    def __getitem__(self, item):
        if isinstance(item, int):
            if item >= 0:
                return SliceDGen(self, slice(item, item + 1))
            else:
                raise ValueError(f"{type(self)} date generator does not support negative indices")
        if isinstance(item, slice):
            if is_negative_slice(item):
                raise ValueError(f"{type(self)} date generator does not support negative indices")
            return SliceDGen(self, item)
        if isinstance(item, Op):
            return lazy(self)[item]

    def over(self, calendar: Calendar):
        return WithCalendarDGen(self, calendar)


class ConstDGen(DGen):
    def __init__(self, date):
        self.date = date

    def is_single_date_gen(self):
        return True

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        yield self.date


class SequenceDGen(DGen):
    def __init__(self, dates):
        self.dates = dates

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        yield from self.dates


class AfterDGen(DGen):
    def __init__(self, gen, date):
        self.gen = gen
        self.date = date

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        if is_dgen(self.date):
            after = self.date(start, end, after, before, calendar)
        else:
            after = self.date

        after = after + timedelta(days=1)

        yield from (d for d in self.gen.__invoke__(start, end, after, before, calendar) if d >= after)

    def __bool__(self):
        if is_dgen(self.date):
            self.date.__compared__ = self
        self.gen.__compared__ = self
        return True


class AfterOrOnDGen(DGen):
    def __init__(self, gen, date):
        self.gen = gen
        self.date = date

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        if is_dgen(self.date):
            after = self.date(start, end, after, before, calendar)
        else:
            after = self.date

        yield from (d for d in self.gen.__invoke__(start, end, after, before, calendar) if d >= after)

    def __bool__(self):
        if is_dgen(self.date):
            self.date.__compared__ = self
        self.gen.__compared__ = self
        return True


class BeforeDGen(DGen):
    def __init__(self, gen, date):
        self.gen = gen
        self.date = date

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        if is_dgen(self.date):
            before = self.date(start, end, after, before, calendar)
        else:
            before = self.date

        yield from (d for d in self.gen.__invoke__(start, end, after, before, calendar) if d < before)


class BeforeOrOnDGen(DGen):
    def __init__(self, gen, date):
        self.gen = gen
        self.date = date

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        if is_dgen(self.date):
            before = self.date(start, end, after, before, calendar)
        else:
            before = self.date

        before = before + timedelta(days=1)

        yield from (d for d in self.gen.__invoke__(start, end, after, before, calendar) if d < before)


class EveryDayDGen(DGen):
    def cadence(self):
        return Tenor('1d')

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        start = start if start is not date.min else after
        end = end if end is not date.max else before

        while start <= end:
            yield start
            start += timedelta(days=1)


days = EveryDayDGen()


class WeekdaysDGen(DGen):
    def __init__(self, gen):
        self.gen = gen

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        we = calendar.weekend_days() if calendar else (5, 6)
        yield from (d for d in
                    self.gen.__invoke__(start, end, after, before, calendar)
                    if d.weekday() not in we)


weekdays = WeekdaysDGen(days)


class WeekendsDGen(DGen):
    def __init__(self, gen):
        self.gen = gen

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        we = calendar.weekend_days() if calendar else (5, 6)
        yield from (d for d in
                    self.gen.__invoke__(start, end, after, before, calendar)
                    if d.weekday() in we)


weekends = WeekendsDGen(EveryDayDGen())


class BusinessDaysDGen(DGen):
    def __init__(self, gen):
        self.gen = gen

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        assert calendar, 'Business days calculation requires a calendar'
        yield from (d for d in
                    self.gen.__invoke__(start, end, after, before, calendar)
                    if not calendar.is_holiday_or_weekend(d))


business_days = BusinessDaysDGen(EveryDayDGen())


class WeeksDGen(DGen):
    def cadence(self):
        return Tenor('1w')

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        start = start if start is not date.min else after
        end = end if end is not date.max else before

        monday = start + timedelta(days=7 - start.weekday()) if start.weekday() > 0 else start
        while monday <= end:
            yield monday
            monday += timedelta(days=7)

    @property
    def mon(self):
        return self

    @property
    def tue(self):
        return self + '1d'

    @property
    def wed(self):
        return self + '2d'

    @property
    def thu(self):
        return self + '3d'

    @property
    def fri(self):
        return self + '4d'

    @property
    def sat(self):
        return self + '5d'

    @property
    def sun(self):
        return self + '6d'


weeks = WeeksDGen()


class DayOfWeekDGen(DGen):
    def __init__(self, weekday):
        self.weekday = weekday

    def cadence(self):
        return Tenor('1w')

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        d = after + timedelta(days=(self.weekday - after.weekday()) % 7)
        while d < before:
            yield d
            d += timedelta(days=7)


class AddTenorDGen(DGen):
    def __init__(self, gen, tenor):
        self.gen = gen
        self.tenor = tenor

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        start = start if start is not date.min else after
        start = self.tenor.sub_from(start, calendar) if not self.tenor.is_neg() else start
        yield from (self.tenor.add_to(d, calendar) for d in self.gen.__invoke__(start, end, after, before, calendar))


class SubTenorDGen(DGen):
    def __init__(self, gen, tenor):
        self.gen = gen
        self.tenor = tenor

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        end = end if end is not date.max else before
        if end is not date.max:
            end = self.tenor.add_to(end, calendar) if not self.tenor.is_neg() else end
        yield from (self.tenor.sub_from(d, calendar) for d in self.gen.__invoke__(start, end, after, before, calendar))


class JoinDGen(DGen):
    def __init__(self, gen1, gen2):
        self.gen1 = gen1
        self.gen2 = gen2

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        g1 = self.gen1.__invoke__(start, end, after, before, calendar)
        g2 = self.gen2.__invoke__(start, end, after, before, calendar)

        d1 = next(g1, None)
        d2 = next(g2, None)
        while d1 is not None or d2 is not None:
            if d1 is None:
                yield d2
                while (d2 := next(g2, None)) is not None:
                    yield d2
            elif d2 is None:
                yield d1
                while (d1 := next(g1, None)) is not None:
                    yield d1
            elif d1 == d2:
                yield d1
                d1 = next(g1, None)
                d2 = next(g2, None)
            elif d1 < d2:
                yield d1
                d1 = next(g1, None)
            else:
                yield d2
                d2 = next(g2, None)


class CommonDatesDGen(DGen):
    def __init__(self, gen1, gen2):
        self.gen1 = gen1
        self.gen2 = gen2

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        g1 = self.gen1.__invoke__(start, end, after, before, calendar)
        g2 = self.gen2.__invoke__(start, end, after, before, calendar)

        d1 = next(g1, None)
        d2 = next(g2, None)
        while d1 is not None and d2 is not None:
            if d1 == d2:
                yield d1
                d1 = next(g1, None)
                d2 = next(g2, None)
            elif d1 < d2:
                d1 = next(g1, None)
            else:
                d2 = next(g2, None)


class RemoveDatesDGen(DGen):
    def __init__(self, gen1, gen2):
        self.gen1 = gen1
        self.gen2 = gen2

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        g1 = self.gen1.__invoke__(start, end, after, before, calendar)
        g2 = self.gen2.__invoke__(start, end, after, before, calendar)

        d1 = next(g1, None)
        d2 = next(g2, None)
        while d1 is not None and d2 is not None:
            if d1 == d2:
                d1 = next(g1, None)
                d2 = next(g2, None)
            elif d1 < d2:
                yield d1
                d1 = next(g1, None)
            else:
                d2 = next(g2, None)
        if d2 is None:
            while d1 is not None:
                yield d1
                d1 = next(g1, None)


class MonthsDGen(DGen):
    def cadence(self):
        return Tenor('1m')

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        start = start if start is not date.min else after
        end = end if end is not date.max else before

        first = date(start.year, start.month, 1)
        while first <= end:
            yield first
            year = first.year + first.month // 12
            month = first.month % 12 + 1
            first = date(year, month, 1)

    @property
    def end(self):
        return months - '1d'

    @property
    def weeks(self):
        return SubSequenceDGen(self, weeks)

    @property
    def days(self):
        return SubSequenceDGen(self, days)

    @property
    def weekdays(self):
        return SubSequenceDGen(self, weekdays)

    @property
    def weekends(self):
        return SubSequenceDGen(self, weekends)

    @property
    def mon(self):
        return SubSequenceDGen(self, DayOfWeekDGen(0))

    @property
    def tue(self):
        return SubSequenceDGen(self, DayOfWeekDGen(1))

    @property
    def wed(self):
        return SubSequenceDGen(self, DayOfWeekDGen(2))

    @property
    def thu(self):
        return SubSequenceDGen(self, DayOfWeekDGen(3))

    @property
    def fri(self):
        return SubSequenceDGen(self, DayOfWeekDGen(4))

    @property
    def sat(self):
        return SubSequenceDGen(self, DayOfWeekDGen(5))

    @property
    def sun(self):
        return SubSequenceDGen(self, DayOfWeekDGen(6))


months = MonthsDGen()


class SubSequenceDGen(DGen):
    def __init__(self, main_sequence, sub_sequence, slice = None):
        if main_sequence.cadence() in (None, Tenor('1d')):
            raise ValueError(f'cannot generate sub sequences from main sequence with cadence of {main_sequence.cadence()}')

        self.main_sequence = main_sequence
        self.sub_sequence = sub_sequence
        self.slice = slice

    def __getattr__(self, name):
        if isinstance(pr := getattr(type(self.sub_sequence),name),property):
            return pr.fget(self)

    def cadence(self):
        return self.sub_sequence.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        for begin in self.main_sequence.__invoke__(start, end, after, before, calendar):
            end = self.main_sequence.cadence().add_to(begin)
            sub_sequence = cast(DGen, begin <= self.sub_sequence < end)
            if self.slice is None:
                yield from (d for d in sub_sequence() if d < before)
            else:
                if is_negative_slice(self.slice):
                    yield from [d for d in sub_sequence() if d < before][self.slice]
                else:
                    yield from (d for d in islice(sub_sequence(), self.slice.start, self.slice.stop, self.slice.step) if d < before)

    def __getitem__(self, item):
        if isinstance(item, int):
            return SubSequenceDGen(self.main_sequence, self.sub_sequence, slice(item, item + 1))
        if isinstance(item, slice):
            return SubSequenceDGen(self.main_sequence, self.sub_sequence, item)
        if isinstance(item, Op):
            return lazy(self)[item]

class DaysOfMonthDGen(DGen):
    def __init__(self, months, days):
        self.months = months
        self.days = days

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        for month in self.months.__invoke__(start, end, after, before, calendar):
            next_month = Tenor('1m').add_to(month)
            yield from (month <= self.days < next_month)()


class YearsDGen(DGen):
    def cadence(self):
        return Tenor('1y')

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        start = start if start is not date.min else after
        end = end if end is not date.max else before

        first = date(start.year, 1, 1)
        while first <= end:
            yield first
            first = date(first.year + 1, 1, 1)

    @property
    def end(self):
        return years - '1d'

    @property
    def months(self):
        return SubSequenceDGen(self, months)

    @property
    def jan(self):
        return self.months[0]

    @property
    def feb(self):
        return self.months[1]

    @property
    def mar(self):
        return self.months[2]

    @property
    def apr(self):
        return self.months[3]

    @property
    def may(self):
        return self.months[4]

    @property
    def jun(self):
        return self.months[5]

    @property
    def jul(self):
        return self.months[6]

    @property
    def aug(self):
        return self.months[7]

    @property
    def sep(self):
        return self.months[8]

    @property
    def oct(self):
        return self.months[9]

    @property
    def nov(self):
        return self.months[10]

    @property
    def dec(self):
        return self.months[11]

    @property
    def weeks(self):
        return SubSequenceDGen(self, weeks)

    @property
    def days(self):
        return SubSequenceDGen(self, days)

    @property
    def weekdays(self):
        return SubSequenceDGen(self, weekdays)

    @property
    def weekends(self):
        return SubSequenceDGen(self, weekends)


years = YearsDGen()


class SliceDGen(DGen):
    def __init__(self, gen, slice):
        self.gen = gen
        self.slice = slice

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        yield from islice(self.gen.__invoke__(start, end, after, before, calendar), self.slice.start, self.slice.stop, self.slice.step)


class WithCalendarDGen(DGen):
    def __init__(self, gen, calendar):
        self.gen = gen
        self.calendar = calendar

    def cadence(self):
        return self.gen.cadence

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        yield from (d for d in self.gen.__invoke__(start, end, after, before, self.calendar))


class RollFwdDGen(DGen):
    def __init__(self, gen, calendar=None):
        self.gen = gen
        self.calendar = calendar

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        c = self.calendar or calendar
        assert c, 'Business days calculation requires a calendar'
        yield from (c.add_business_days(d, 0) for d in self.gen.__invoke__(start, end, after, before, calendar))


def roll_fwd(x, calendar=None):
    return RollFwdDGen(x, calendar)


class RollBwdDGen(DGen):
    def __init__(self, gen, calendar=None):
        self.gen = gen
        self.calendar = calendar

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None):
        c = self.calendar or calendar
        assert c, 'Business days calculation requires a calendar'
        yield from (c.sub_business_days(d, 0) for d in self.gen.__invoke__(start, end, after, before, calendar))


def roll_bwd(x, calendar=None):
    return RollBwdDGen(x, calendar)

