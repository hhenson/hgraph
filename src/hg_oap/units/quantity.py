from dataclasses import dataclass
from numbers import Number
from typing import Generic

from hg_oap.units.unit import Unit, NUMBER


@dataclass(frozen=True)
class Quantity(Generic[NUMBER]):
    value: NUMBER
    unit: Unit

    def __str__(self):
        return f"{self.value} {self.unit}"

    def __repr__(self):
        return f"{self.value}*{self.unit}"

    def __eq__(self, other):
        if isinstance(other, Quantity):
            return self.value == other.unit.convert(other.value, to=self.unit)

        return NotImplemented

    def __add__(self, other):
        if isinstance(other, Quantity):
            ret, conv = self.unit + other.unit
            return Quantity(self.value + other.unit.convert(other.value, to=conv), ret)

        return NotImplemented

    def __sub__(self, other):
        if isinstance(other, Quantity):
            ret, conv = self.unit - other.unit
            return Quantity(self.value - other.unit.convert(other.value, to=conv), ret)

        return NotImplemented

    def __mul__(self, other):
        if isinstance(other, Quantity):
            return Quantity(self.value * other.value, self.unit * other.unit)
        elif isinstance(other, type(self.value)):
            return Quantity(self.value * other, self.unit)

        return NotImplemented

    __rmul__ = __mul__

    def __truediv__(self, other):
        if isinstance(other, Quantity) and type(other.value) is type(self.value) and other.value != 0:
            return Quantity(self.value / other.value, self.unit / other.unit)
        elif isinstance(other, type(self.value)):
            return Quantity(self.value / other, self.unit)

        return NotImplemented

    def __rtruediv__(self, other):
        if isinstance(other, type(self.value)) and self.value != 0:
            return Quantity(other / self.value, self.unit**-1)

        return NotImplemented

    def __pow__(self, other):
        if isinstance(other, Number):
            return Quantity(self.value**other, self.unit**other)

        return NotImplemented

    def __round__(self, n=None):
        return Quantity(round(self.value, n), self.unit)

    def __lt__(self, other):
        if isinstance(other, Quantity):
            return self.value < other.unit.convert(other.value, to=self.unit)

        return NotImplemented

    def __le__(self, other):
        if isinstance(other, Quantity):
            return self.value <= other.unit.convert(other.value, to=self.unit)

        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, Quantity):
            return self.value > other.unit.convert(other.value, to=self.unit)

        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, Quantity):
            return self.value >= other.unit.convert(other.value, to=self.unit)

        return NotImplemented

    def __abs__(self):
        return Quantity(abs(self.value), self.unit)

    def __neg__(self):
        return Quantity(-self.value, self.unit)

    def __pos__(self):
        return Quantity(+self.value, self.unit)

    def as_(self, unit):
        return Quantity(self.unit.convert(self.value, to=unit), unit)

