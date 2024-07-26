from abc import abstractmethod
from bisect import bisect, bisect_left
from datetime import date, timedelta
from typing import Tuple, Set

__all__ = ('Calendar', 'WeekendCalendar', 'HolidayCalendar')


class Calendar:
    @abstractmethod
    def weekend_days(self) -> Tuple[int, ...]:
        """
        Returns a tuple of day numbers corresponding to the weekend days for this calendar
        """

    @abstractmethod
    def is_holiday(self, d: date) -> bool:
        """
        Returns True if the given date is a holiday date
        Returns False if the given date is a weekend day
        Returns False otherwise
        """

    @abstractmethod
    def is_holiday_or_weekend(self, d: date) -> bool:
        """
        Returns True if the given date is a holiday date
        Returns True if the given date is a weekend day
        Returns False otherwise
        """

    @abstractmethod
    def add_business_days(self, d: date, days: int) -> date:
        """
        Add `days business days to the given date `d and return the resulting date

        Adding business days has the following semantics:
        If the current date d is a business day:
          - Adding 0 days has no effect.
          - Adding 1 business day lands on the next day which is not a weekend or holiday date after the current date
        If the current date d is a weekend or holiday day:
          - Adding 0 days rolls to the next non-weekend and non-holiday day
          - Adding 1 business day rolls to the next non-weekend and non-holiday after that (and so on)

        Subtracting business days has similar semantics but moving backwards rather than forwards through time.
        """

    @abstractmethod
    def sub_business_days(self, d: date, days: int) -> date:
        """
        See `add_business_days above
        """


class WeekendCalendar(Calendar):
    _weekend_days: Tuple[int, ...]

    def __init__(self, weekend_days: Tuple[int, ...] = (5, 6)):
        self._weekend_days = weekend_days
        self._number_weekend_days = len(self._weekend_days)

    def weekend_days(self) -> Tuple[int, ...]:
        return self._weekend_days

    @abstractmethod
    def is_holiday(self, d: date) -> bool:
        return False

    @abstractmethod
    def is_holiday_or_weekend(self, d: date) -> bool:
        return d.weekday() in self._weekend_days

    @abstractmethod
    def add_business_days(self, d: date, days: int):
        if days < 0: return self.sub_business_days(d, -days)

        w = d.weekday()
        wd = 0
        while (w + wd) % 7 in self._weekend_days:
            wd += 1
        if wd:
            d += timedelta(days=wd)

        if days == 0: return d  # rolled to the next business day

        weeks = days // (7 - self._number_weekend_days)
        if weeks:
            d += timedelta(days=7 * weeks)
            days %= (7 - self._number_weekend_days)

        w = d.weekday() + 1
        wd = days
        while wd:
            if w % 7 in self._weekend_days:
                days += 1
            else:
                wd -= 1
            w += 1

        return (d + timedelta(days=days)) if days else d

    @abstractmethod
    def sub_business_days(self, d: date, days: int):
        if days < 0: return self.add_business_days(d, -days)

        w = d.weekday()
        wd = 0
        while (w - wd) % 7 in self._weekend_days:
            wd += 1
        if wd:
            d -= timedelta(days=wd)

        if days == 0: return d  # rolled to the prev business day

        weeks = days // (7 - self._number_weekend_days)
        if weeks:
            d -= timedelta(days=7 * weeks)
            days %= (7 - self._number_weekend_days)

        w = d.weekday() - 1
        wd = days
        while wd:
            if w % 7 in self._weekend_days:
                days += 1
            else:
                wd -= 1
            w -= 1

        return (d - timedelta(days=days)) if days else d


class HolidayCalendar(WeekendCalendar):
    _holidays: Tuple[date, ...]
    _holidays_set: Set[date]

    def __init__(self, holidays: Tuple[date, ...], weekend_days: Tuple[int, ...] = (5, 6)):
        super().__init__(weekend_days)
        self._holidays = tuple(sorted(holidays))
        self._holidays_set = set(holidays)

    def is_holiday(self, d: date) -> bool:
        return d in self._holidays_set

    def is_holiday_or_weekend(self, d: date) -> bool:
        return self.is_holiday(d) or super().is_holiday_or_weekend(d)

    def add_business_days(self, d: date, days: int):
        if days < 0: return self.sub_business_days(d, -days)

        i = bisect_left(self._holidays, d)
        while True:
            d = super().add_business_days(d, days)
            j = bisect(self._holidays, d)
            if i == j:
                return d
            else:
                days = j - i
                i = j

    def sub_business_days(self, d: date, days: int):
        if days < 0: return self.sub_business_days(d, -days)

        i = bisect(self._holidays, d)
        while True:
            d = super().sub_business_days(d, days)
            j = bisect_left(self._holidays, d)
            if i == j:
                return d
            else:
                days = i - j
                i = j
