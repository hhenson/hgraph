from dataclasses import dataclass
from numbers import Number
from typing import Generic

from hg_oap.units.unit import Unit, NUMBER


@dataclass(frozen=True)
class Quantity(Generic[NUMBER]):
    value: NUMBER
    unit: Unit

    def __mul__(self, other):
        if isinstance(other, Quantity):
            return Quantity(self.value * other.value, self.unit * other.unit)
        elif isinstance(other, type(self.value)):
            return Quantity(self.value * other, self.unit)

        return NotImplemented

    def __truediv__(self, other):
        if isinstance(other, Quantity) and type(other.value) is type(self.value) and other.value != 0:
            return Quantity(self.value / other.value, self.unit * other.unit)
        elif isinstance(other, type(self.value)):
            return Quantity(self.value / other, self.unit)

        return NotImplemented

    def __rtruediv__(self, other):
        if other == 1:
            return Quantity(1 / self.value, self.unit**-1)

        return NotImplemented
