import re
from calendar import monthrange
from datetime import timedelta, date

__all__ = ('Tenor',)


class Tenor:
    ymwd_b: tuple[int, int, int, int, int]

    def __init__(self, tenor=None, /, *, y=0, m=0, w=0, d=0, b=0):
        if tenor is not None:
            assert not any((y, m, w, d, b)), 'cannot specify both a tenor and individual components'

            if type(tenor) is str:
                if m := re.match(r"^(-)?(?:(?:(\d+)y)?(?:(\d+)m)?(?:(\d+)w)?(?:(\d+)d)?|(?:(\d+)b)?)$", tenor):
                    sign, y, m, w, d, b = m.groups()
                    s, y, m, w, d, b = not sign, int(y or 0), int(m or 0), int(w or 0), int(d or 0), int(b or 0)
                    if sign:
                        y, m, w, d, b = -y, -m, -w, -d, -b

                    self.ymwd_b = (y, m, w, d, b)
                else:
                    raise ValueError(f'"{tenor}" is a invalid tenor string')
            elif type(tenor) is Tenor:
                self.ymwd_b = tenor.ymwd_b
            elif type(tenor) is timedelta:
                self.ymwd_b = (0, 0, 0, tenor.days, 0)
            elif type(tenor) is tuple and len(tenor) == 5 and all(isinstance(i, int) for i in tenor):
                self.ymwd_b = tenor
            else:
                raise ValueError(f'"{tenor}" is a invalid tenor value')
        else:
            self.ymwd_b = (y, m, w, d, b)

    def __str__(self):
        s = sum(self.ymwd_b)
        if s == 0:
            return '0d'

        sign = '-' if s < 0 else ''
        return sign + ''.join(f'{abs(i)}{s}' for i, s in zip(self.ymwd_b, "ymwdb") if i != 0)

    def __repr__(self):
        return f'"{str(self)}"'

    def is_neg(self):
        return sum(self.ymwd_b) < 0

    def __neg__(self):
        return Tenor(tuple(-i for i in self.ymwd_b))

    def __radd__(self, other):
        if isinstance(other, date):
            return self.add_to(other)
        else:
            return other.__add__(self)

    def __mul__(self, other):
        if isinstance(other, int):
            return Tenor(tuple(i * other for i in self.ymwd_b))
        else:
            return other.__mul__(self)

    def __rmul__(self, other):
        return self.__mul__(other)

    def add_to(self, dt, calendar = None):
        if self.is_neg():
            return self.__neg__().sub_from(dt, calendar)

        if self.ymwd_b[-1] == 0: # not a business days tenor
            y, m, w, d, _ = self.ymwd_b
            year, month, day = dt.year, dt.month, dt.day
            year += y + (month + m - 1) // 12
            month = (month + m - 1) % 12 + 1
            if day > (last_day := monthrange(year, month)[1]):
                day = last_day
            return date(year, month, day) + timedelta(days=w*7 + d)
        elif calendar is None:
            raise ValueError('cannot add business days tenors without a calendar')
        else:
            return calendar.add_business_days(dt, self.ymwd_b[-1])

    def sub_from(self, dt, calendar = None):
        if self.is_neg():
            return self.__neg__().add_to(dt, calendar)

        if self.ymwd_b[-1] == 0: # not a business days tenor
            y, m, w, d, _ = self.ymwd_b
            year, month, day = dt.year, dt.month, dt.day
            year -= y - (month - m - 1) // 12
            month = (month - m - 1) % 12 + 1
            if day > (last_day := monthrange(year, month)[1]):
                day = last_day
            return date(year, month, day) + timedelta(days=-w*7 - d)
        elif calendar is None:
            raise ValueError('cannot subtract business days tenors without a calendar')
        else:
            return calendar.sub_business_days(dt, self.ymwd_b[-1])
