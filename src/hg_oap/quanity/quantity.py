from dataclasses import dataclass
from typing import Self, TypeVar, Generic

from hgraph import CompoundScalar

from hg_oap.quanity.unit import Unit

UNIT = TypeVar("UNIT", bound=Unit)


@dataclass
class Quantity(CompoundScalar, Generic[UNIT]):
    """
    Quantities belong to a unit family, that is the magnitude (or value) is a representation of a particular dimension,
    for example length. A unit conversion is only possible within a unit family. Thus, length cannot be converted
    to temperature for example.
    """
    value: float
    unit: UNIT

    def value_as(self, unit: Unit) -> float:
        """Returns the quantity in the unit requested"""
        return self.unit.as_(self.value, unit)

    def convert_to(self, unit: Unit) -> Self:
        """
        Returns a quantity instance using the unit provided (converting the value to the new units)
        """
        return Quantity[unit.__class__](value=self.as_(unit), unit=unit)
