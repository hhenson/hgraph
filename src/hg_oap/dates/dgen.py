from datetime import date, timedelta
from itertools import islice
from typing import cast

from hg_oap.dates.calendar import Calendar
from hg_oap.utils.op import Item, Op, lazy, is_op
from hg_oap.dates.tenor import Tenor

__all__ = ('is_dgen', 'make_date', 'make_dgen', 'years', 'months', 'weeks', 'weekdays', 'weekends', 'days',
           'business_days', 'roll_fwd', 'roll_bwd', 'DGen', 'retain', 'DGenParameter')


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
                 before: date = date.max, calendar: Calendar = None, **kwargs):
        return self.__invoke__(make_date(start), make_date(end), make_date(after), make_date(before), calendar, **kwargs)

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        raise StopIteration

    def is_single_date_gen(self):
        return False

    def cadence(self):
        return None

    def __iter__(self):
        return self

    def __or__(self, other):
        if is_op(other):
            return lazy(self) | other
        else:
            return JoinDGen(self, make_dgen(other))

    def __ror__(self, other):
        if is_op(other):
            return other | lazy(self)
        else:
            return JoinDGen(make_dgen(other), self)

    def __and__(self, other):
        if is_op(other):
            return lazy(self) & other
        else:
            return CommonDatesDGen(self, make_dgen(other))

    def __rand__(self, other):
        if is_op(other):
            return other & lazy(self)
        else:
            return CommonDatesDGen(make_dgen(other), self)

    def __gt__(self, other):
        if is_op(other):
            return lazy(self) > other
        elif not is_dgen(other):
            return AfterDGen(self, make_date(other))
        elif other.is_single_date_gen():
            return AfterDGen(self, other)
        else:
            raise ValueError('Comparing two date generators is not supported')

    def __lt__(self, other):
        if is_op(other):
            return lazy(self) < other
        if is_dgen(other) and self.is_single_date_gen():
            return other.__gt__(self)
        else:
            if lhs := getattr(self, '__compared__', None):
                return BeforeDGen(lhs, make_date(other))
            if self.__expression__ is not None:
                return self.__expression__ < other
            return BeforeDGen(self, make_date(other))

    def __ge__(self, other):
        if is_op(other):
            return lazy(self) >= other
        if not is_dgen(other):
            return AfterOrOnDGen(self, make_date(other))
        elif other.is_single_date_gen():
            return AfterOrOnDGen(self, other)
        else:
            raise ValueError('Comparing two dage generators is not supported')

    def __le__(self, other):
        if is_op(other):
            return lazy(self) <= other
        if is_dgen(other) and self.is_single_date_gen():
            return other.__ge__(self)
        else:
            if lhs := getattr(self, '__compared__', None):
                return BeforeOrOnDGen(lhs, make_date(other))
            if self.__expression__ is not None:
                return self.__expression__ <= other
            return BeforeOrOnDGen(self, make_date(other))

    def __add__(self, other):
        if is_op(other):
            return lazy(self) + other
        else:
            return AddTenorDGen(self, Tenor(other))

    def __sub__(self, other):
        if is_op(other):
            return lazy(self) - other

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
        if is_op(other):
            return other - lazy(self)
        else:
            gen = make_dgen(other)
            return RemoveDatesDGen(gen, self)

    def __getitem__(self, item):
        if isinstance(item, int):
            if item >= 0:
                return SliceDGen(self, slice(item, item + 1))
            else:
                raise ValueError(f"{type(self)} date generator does not support negative indices")
        elif isinstance(item, slice):
            if is_negative_slice(item):
                raise ValueError(f"{type(self)} date generator does not support negative indices")
            return SliceDGen(self, item)
        elif is_op(item):
            return lazy(self)[item]

    def over(self, calendar: Calendar):
        if is_op(calendar):
            return lazy(self).over(calendar)
        else:
            return WithCalendarDGen(self, calendar)


class ConstDGen(DGen):
    def __init__(self, date):
        self.date = date

    def is_single_date_gen(self):
        return True

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        yield self.date

    def __repr__(self):
        return f"'{self.date}'"


class SequenceDGen(DGen):
    def __init__(self, dates):
        self.dates = dates

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        yield from self.dates

    def __repr__(self):
        dates = ','.join(f"'{d}'" for d in self.dates)
        return f"[{dates}]"


class AfterDGen(DGen):
    def __init__(self, gen, date):
        self.gen = gen
        self.date = date

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        if is_dgen(self.date):
            after = next(self.date.__invoke__(start, end, after, before, calendar, **kwargs))
        else:
            after = self.date

        after = after + timedelta(days=1)

        yield from (d for d in self.gen.__invoke__(start, end, after, before, calendar, **kwargs) if d >= after)

    def __bool__(self):
        if is_dgen(self.date):
            self.date.__compared__ = self
        self.gen.__compared__ = self
        return True

    def __repr__(self):
        return f"{self.date} < '{self.gen}'"


class AfterOrOnDGen(DGen):
    def __init__(self, gen, date):
        self.gen = gen
        self.date = date

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        if is_dgen(self.date):
            after = next(self.date.__invoke__(start, end, after, before, calendar, **kwargs))
        else:
            after = self.date

        yield from (d for d in self.gen.__invoke__(start, end, after, before, calendar, **kwargs) if d >= after)

    def __bool__(self):
        if is_dgen(self.date):
            self.date.__compared__ = self
        self.gen.__compared__ = self
        return True

    def __repr__(self):
        return f"{self.date} <= '{self.gen}'"


class BeforeDGen(DGen):
    def __init__(self, gen, date):
        self.gen = gen
        self.date = date

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        if is_dgen(self.date):
            before = next(self.date.__invoke__(start, end, after, before, calendar, **kwargs))
        else:
            before = self.date

        yield from (d for d in self.gen.__invoke__(start, end, after, before, calendar, **kwargs) if d < before)

    def __repr__(self):
        return f"'{self.gen}' < {self.date}"


class BeforeOrOnDGen(DGen):
    def __init__(self, gen, date):
        self.gen = gen
        self.date = date

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        if is_dgen(self.date):
            before = next(self.date.__invoke__(start, end, after, before, calendar, **kwargs))
        else:
            before = self.date

        before = before + timedelta(days=1)

        yield from (d for d in self.gen.__invoke__(start, end, after, before, calendar, **kwargs) if d < before)

    def __repr__(self):
        return f"'{self.gen}' <= {self.date}"


class EveryDayDGen(DGen):
    def cadence(self):
        return Tenor('1d')

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        start = start if start is not date.min else after
        end = end if end is not date.max else before

        while start <= end:
            yield start
            start += timedelta(days=1)

    def __repr__(self):
        return 'days'


days = EveryDayDGen()


class WeekdaysDGen(DGen):
    def __init__(self, gen):
        self.gen = gen

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        we = calendar.weekend_days() if calendar else (5, 6)
        yield from (d for d in
                    self.gen.__invoke__(start, end, after, before, calendar, **kwargs)
                    if d.weekday() not in we)

    def __repr__(self):
        return 'weekdays'


weekdays = WeekdaysDGen(days)


class WeekendsDGen(DGen):
    def __init__(self, gen):
        self.gen = gen

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        we = calendar.weekend_days() if calendar else (5, 6)
        yield from (d for d in
                    self.gen.__invoke__(start, end, after, before, calendar, **kwargs)
                    if d.weekday() in we)

    def __repr__(self):
        return 'weekends'


weekends = WeekendsDGen(EveryDayDGen())


class BusinessDaysDGen(DGen):
    def __init__(self, gen):
        self.gen = gen

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        assert calendar, 'Business days calculation requires a calendar'
        yield from (d for d in
                    self.gen.__invoke__(start, end, after, before, calendar, **kwargs)
                    if not calendar.is_holiday_or_weekend(d))

    def __repr__(self):
        return 'business_days'


business_days = BusinessDaysDGen(EveryDayDGen())


class WeeksDGen(DGen):
    def cadence(self):
        return Tenor('1w')

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        start = start if start is not date.min else after
        end = end if end is not date.max else before

        monday = start + timedelta(days=7 - start.weekday()) if start.weekday() > 0 else start
        while monday <= end:
            yield monday
            monday += timedelta(days=7)

    @property
    def mon(self):
        return DayOfWeekDGen(0, self)

    @property
    def tue(self):
        return DayOfWeekDGen(1, self)

    @property
    def wed(self):
        return DayOfWeekDGen(2, self)

    @property
    def thu(self):
        return DayOfWeekDGen(3, self)

    @property
    def fri(self):
        return DayOfWeekDGen(4, self)

    @property
    def sat(self):
        return DayOfWeekDGen(5, self)

    @property
    def sun(self):
        return DayOfWeekDGen(6, self)

    def __repr__(self):
        return 'weeks'


weeks = WeeksDGen()


class DayOfWeekDGen(DGen):
    def __init__(self, weekday, gen=None):
        self.gen = gen
        self.weekday = weekday

    def cadence(self):
        return Tenor('1w')

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        if self.gen is None:
            d = after + timedelta(days=(self.weekday - after.weekday()) % 7)
            while d < before:
                yield d
                d += timedelta(days=7)
        else:
            after -= timedelta(days=6)
            for d in self.gen.__invoke__(start, end, after, before, calendar, **kwargs):
                d_ = d + timedelta(days=(self.weekday - d.weekday()) % 7)
                if d_ < before:
                    yield d_

    def __repr__(self):
        weekday_name = ('mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun')[self.weekday]
        if self.gen is not None:
            return f'{self.gen}.{weekday_name}'
        else:
            return weekday_name


class AddTenorDGen(DGen):
    def __init__(self, gen, tenor):
        self.gen = gen
        self.tenor = tenor

    def cadence(self):
        return self.gen.cadence()

    def is_single_date_gen(self):
        return self.gen.is_single_date_gen()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        start = start if start is not date.min else after
        start = self.tenor.sub_from(start, calendar) if not self.tenor.is_neg() and start is not date.min else start
        yield from (self.tenor.add_to(d, calendar) for d in self.gen.__invoke__(start, end, after, before, calendar, **kwargs))

    def __repr__(self):
        return f'{self.gen} + {self.tenor}'


class SubTenorDGen(DGen):
    def __init__(self, gen, tenor):
        self.gen = gen
        self.tenor = tenor

    def cadence(self):
        return self.gen.cadence()

    def is_single_date_gen(self):
        return self.gen.is_single_date_gen()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        end = end if end is not date.max else before
        if end is not date.max:
            end = self.tenor.add_to(end, calendar) if not self.tenor.is_neg() else end
        yield from (self.tenor.sub_from(d, calendar) for d in self.gen.__invoke__(start, end, after, before, calendar, **kwargs))

    def __repr__(self):
        return f'{self.gen} - {self.tenor}'


class JoinDGen(DGen):
    def __init__(self, gen1, gen2):
        self.gen1 = gen1
        self.gen2 = gen2

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        g1 = self.gen1.__invoke__(start, end, after, before, calendar, **kwargs)
        g2 = self.gen2.__invoke__(start, end, after, before, calendar, **kwargs)

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

    def __repr__(self):
        return f'{self.gen1} | {self.gen2}'


class CommonDatesDGen(DGen):
    def __init__(self, gen1, gen2):
        self.gen1 = gen1
        self.gen2 = gen2

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        g1 = self.gen1.__invoke__(start, end, after, before, calendar, **kwargs)
        g2 = self.gen2.__invoke__(start, end, after, before, calendar, **kwargs)

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

    def __repr__(self):
        return f'{self.gen1} & {self.gen2}'


class RemoveDatesDGen(DGen):
    def __init__(self, gen1, gen2):
        self.gen1 = gen1
        self.gen2 = gen2

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        g1 = self.gen1.__invoke__(start, end, after, before, calendar, **kwargs)
        g2 = self.gen2.__invoke__(start, end, after, before, calendar, **kwargs)

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

    def __repr__(self):
        return f'{self.gen1} - {self.gen2}'


class MonthsDGen(DGen):
    def cadence(self):
        return Tenor('1m')

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
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

    def __repr__(self):
        return 'months'


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
                   calendar: Calendar = None, **kwargs):
        for begin in self.main_sequence.__invoke__(start, end, after, before, calendar, **kwargs):
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

    def __repr__(self):
        if self.slice is None:
            sl_str = ""
        else:
            if self.slice.start == self.slice.stop - 1:
                sl_str = f"[{self.slice.start}]"
            elif self.slice.step is None:
                sl_str = f"[{self.slice.start}:{self.slice.stop}]"
            else:
                sl_str = f"[{self.slice.start}:{self.slice.stop}:{self.slice.step}]"
        return f'{self.main_sequence}.{self.sub_sequence}{sl_str}'


class DaysOfMonthDGen(DGen):
    def __init__(self, months, days):
        self.months = months
        self.days = days

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        for month in self.months.__invoke__(start, end, after, before, calendar, **kwargs):
            next_month = Tenor('1m').add_to(month)
            yield from (month <= self.days < next_month)()

    def __repr__(self):
        return f'{self.months}.{self.days}'


class YearsDGen(DGen):
    def cadence(self):
        return Tenor('1y')

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
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

    def __repr__(self):
        return 'years'


years = YearsDGen()


class SliceDGen(DGen):
    def __init__(self, gen, slice):
        self.gen = gen
        self.slice = slice

    def cadence(self):
        return self.gen.cadence()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        yield from islice(self.gen.__invoke__(start, end, after, before, calendar, **kwargs), self.slice.start, self.slice.stop, self.slice.step)

    def __repr__(self):
        return f'{self.gen}[{self.slice}]'


class WithCalendarDGen(DGen):
    def __init__(self, gen, calendar):
        self.gen = gen
        self.calendar = calendar

    def cadence(self):
        return self.gen.cadence

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        yield from (d for d in self.gen.__invoke__(start, end, after, before, self.calendar))

    def __repr__(self):
        return f'{self.gen}.over({self.calendar})'


class RollFwdDGen(DGen):
    def __init__(self, gen, calendar=None):
        self.gen = gen
        self.calendar = calendar

    def cadence(self):
        return self.gen.cadence()

    def is_single_date_gen(self):
        return self.gen.is_single_date_gen()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        c = self.calendar or calendar
        assert c, 'Business days calculation requires a calendar'
        yield from (c.add_business_days(d, 0) for d in self.gen.__invoke__(start, end, after, before, calendar, **kwargs))

    def __repr__(self):
        return f'roll_fwd({self.gen})' if self.calendar is None else f'roll_fwd({self.gen}, {self.calendar})'


def roll_fwd(x, calendar=None):
    if is_op(x) or is_op(calendar):
        return lazy(roll_bwd)(x, calendar)
    else:
        return RollFwdDGen(x, calendar)


class RollBwdDGen(DGen):
    def __init__(self, gen, calendar=None):
        self.gen = gen
        self.calendar = calendar

    def cadence(self):
        return self.gen.cadence()

    def is_single_date_gen(self):
        return self.gen.is_single_date_gen()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        c = self.calendar or calendar
        assert c, 'Business days calculation requires a calendar'
        yield from (c.sub_business_days(d, 0) for d in self.gen.__invoke__(start, end, after, before, calendar, **kwargs))

    def __repr__(self):
        return f'roll_bwd({self.gen})' if self.calendar is None else f'roll_bwd({self.gen}, {self.calendar})'


def roll_bwd(x, calendar=None):
    if is_op(x) or is_op(calendar):
        return lazy(roll_bwd)(x, calendar)
    else:
        return RollBwdDGen(x, calendar)


class DGenRetain(DGen):
    def __init__(self, gen):
        self.gen = gen
        self.last = None
        self.all = []

    def cadence(self):
        return self.gen.cadence()

    def is_single_date_gen(self):
        return self.gen.is_single_date_gen()

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        for d in self.gen.__invoke__(start, end, after, before, calendar, **kwargs):
            self.last = d
            self.all.append(d)
            yield d

    def __repr__(self):
        return f'retain({self.gen})'


def retain(x):
    if is_op(x):
        return lazy(retain)(x)
    else:
        return DGenRetain(x)


class DGenParameter(DGen):
    def __init__(self, name):
        self.name = name

    def is_single_date_gen(self):
        return True

    def __invoke__(self, start: date = date.min, end: date = date.max, after: date = date.min, before: date = date.max,
                   calendar: Calendar = None, **kwargs):
        if v := kwargs.get(self.name, None):
            yield make_date(v)
        else:
            raise ValueError(f'Parameter {self.name} is not provided')

    def __repr__(self):
        return self.name
