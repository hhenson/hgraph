from dataclasses import dataclass

from hg_oap.units.unit import Unit


@dataclass(frozen=True)
class Quantity:
    value: float
    unit: Unit

    def __rtruediv__(self, other):
        if other == 1:
            return Quantity(1 / self.value, self.unit**-1)