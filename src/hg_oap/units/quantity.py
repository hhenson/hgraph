from dataclasses import dataclass
from numbers import Number
from typing import Generic

from hg_oap.units.unit import Unit, NUMBER
from hgraph import CompoundScalar, compute_node, div_, TS

__all__ = ("Quantity",)


EPSILON = 1e-9


@dataclass(frozen=True, eq=False, unsafe_hash=True, repr=False)
class Quantity(CompoundScalar, Generic[NUMBER]):
    qty: NUMBER
    unit: Unit

    def __str__(self):
        return f"{self.qty} {self.unit}"

    def __repr__(self):
        return f"{self.qty}*{self.unit}"

    def __eq__(self, other):
        if isinstance(other, Quantity):
            other_qty = other.unit.convert(other.qty, to=self.unit)
            return other_qty - EPSILON <= self.qty <= other_qty + EPSILON

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
        tp = self.qty.__class__
        if tp is float and other.__class__ in (int, float):
            return Quantity[float](self.qty * other, self.unit)
        elif isinstance(other, Quantity):
            return Quantity[tp](self.qty * other.qty, self.unit * other.unit)
        elif isinstance(other, tp):
            return Quantity[tp](self.qty * other, self.unit)

        return NotImplemented

    __rmul__ = __mul__

    def __truediv__(self, other):
        tp = self.qty.__class__
        if tp is float and other.__class__ in (int, float):
            return Quantity[tp](self.qty / other, self.unit)
        elif isinstance(other, Quantity) and type(other.qty) is tp:
            return Quantity[tp](self.qty / other.qty, self.unit / other.unit)
        elif isinstance(other, tp):
            return Quantity[tp](self.qty / other, self.unit)

        return NotImplemented

    def __rtruediv__(self, other):
        tp = self.qty.__class__
        if isinstance(other, tp):
            qty = other / self.qty
        else:
            qty = tp(other) / self.qty
        return Quantity[tp](qty, self.unit ** -1)

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


@compute_node(overloads=div_)
def div_qty(lhs: TS[Quantity], rhs: TS[Quantity]) -> TS[Quantity]:
    return lhs.value / rhs.value