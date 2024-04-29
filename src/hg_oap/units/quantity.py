from dataclasses import dataclass
from numbers import Number
from typing import Generic

from hg_oap.units.unit import Unit, NUMBER
from hgraph import CompoundScalar

__all__ = ("Quantity",)


@dataclass(frozen=True)
class Quantity(CompoundScalar, Generic[NUMBER]):
    qty: NUMBER
    unit: Unit

    def __str__(self):
        return f"{self.qty} {self.unit}"

    def __repr__(self):
        return f"{self.qty}*{self.unit}"

    def __eq__(self, other):
        if isinstance(other, Quantity):
            return self.qty == other.unit.convert(other.qty, to=self.unit)

        return NotImplemented

    def __add__(self, other):
        if isinstance(other, Quantity):
            ret, conv = self.unit + other.unit
            return Quantity[type(self.qty)](self.qty + other.unit.convert(other.qty, to=conv), ret)

        return NotImplemented

    def __sub__(self, other):
        if isinstance(other, Quantity):
            ret, conv = self.unit - other.unit
            return Quantity[type(self.qty)](self.qty - other.unit.convert(other.qty, to=conv), ret)

        return NotImplemented

    def __mul__(self, other):
        if isinstance(other, Quantity):
            return Quantity[type(self.qty)](self.qty * other.qty, self.unit * other.unit)
        elif isinstance(other, type(self.qty)):
            return Quantity[type(self.qty)](self.qty * other, self.unit)

        return NotImplemented

    __rmul__ = __mul__

    def __truediv__(self, other):
        if isinstance(other, Quantity) and type(other.qty) is type(self.qty) and other.qty != 0:
            return Quantity[type(self.qty)](self.qty / other.qty, self.unit / other.unit)
        elif isinstance(other, type(self.qty)):
            return Quantity[type(self.qty)](self.qty / other, self.unit)

        return NotImplemented

    def __rtruediv__(self, other):
        if isinstance(other, type(self.qty)):
            return Quantity[type(self.qty)](other / self.qty, self.unit ** -1)
        else:
            return Quantity[type(self.qty)](type(self.qty)(other) / self.qty, self.unit ** -1)

        return NotImplemented

    def __pow__(self, other):
        if isinstance(other, Number):
            return Quantity[type(self.qty)](self.qty ** other, self.unit ** other)

        return NotImplemented

    def __round__(self, n=None):
        return Quantity[type(self.qty)](round(self.qty, n), self.unit)

    def __lt__(self, other):
        if isinstance(other, Quantity):
            return self.qty < other.unit.convert(other.qty, to=self.unit)

        return NotImplemented

    def __le__(self, other):
        if isinstance(other, Quantity):
            return self.qty <= other.unit.convert(other.qty, to=self.unit)

        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, Quantity):
            return self.qty > other.unit.convert(other.qty, to=self.unit)

        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, Quantity):
            return self.qty >= other.unit.convert(other.qty, to=self.unit)

        return NotImplemented

    def __abs__(self):
        return Quantity[type(self.qty)](abs(self.qty), self.unit)

    def __neg__(self):
        return Quantity[type(self.qty)](-self.qty, self.unit)

    def __pos__(self):
        return Quantity[type(self.qty)](+self.qty, self.unit)

    def as_(self, unit):
        return Quantity[type(self.qty)](self.unit.convert(self.qty, to=unit), unit)

