import math
from abc import abstractmethod


class UnconvertableTypes(RuntimeError):

    def __init__(self, tp: "Unit", to: "Unit", requires_force: bool = False):
        if requires_force:
            super().__init__(f"{tp} must be coerced to {to}")
        else:
            super().__init__(f"{tp} is not convertable to {to}")


class Unit:

    @property
    @abstractmethod
    def symbol(self) -> str:
        """The symbol of the unit. For compound units this will be the symbol combination"""

    @abstractmethod
    def convert_to(self, value: float, unit: "Unit") -> float:
        """Converts the value to the unit provided (assuming the value is currently in the units of self"""

    def __rmul__(self, other):
        from hg_oap.quanity.quantity import Quantity
        if type(other) in (int, float):
            return Quantity[self.__class__](float(other), self)
        else:
            raise RuntimeError("Only supports adding units to a numeric quantity")


class AtomicUnit(Unit):
    """
    A unit represents an instance of a dimension (or collection of dimensions and the scale of the dimension).
    """

    def __init__(self, unit_family, symbol: str):
        self._unit_family: str = unit_family
        self._symbol: str = symbol

    @property
    def symbol(self) -> str:
        return self._symbol

    def convert_to(self, value: float, unit: "Unit") -> float:
        """
        Returns a new Quantity unit in with the units requested. If this results in information loss an exception
        is raised
        """
        if unit.unit_family != self._unit_family:
            raise UnconvertableTypes(self, unit)
        return self.do_convert_to(value, unit)

    @abstractmethod
    def do_convert_to(self, value: float, unit: "Unit") -> float:
        """Converts the value to the unit provided"""

    def __str__(self) -> str:
        return self.symbol

    def __rmul__(self, other):
        if isinstance(other, Unit):
            if isinstance(other, AtomicUnit):
                return CompoundUnit((self, 1), (other, 1))
            else:
                return CompoundUnit((self, 1), *other._units)
        else:
            super().__rmul__(other)


class LinearAtomicUnit(AtomicUnit):
    """Assumes a linear conversion between units within a family"""

    def do_convert_to(self, value: float, unit: "Unit") -> float:
        """Converts the value to the unit provided"""
        return ((value * self.ratio_to_normal()) + self.offset_to_normal()) \
            / self.ratio_to_normal() - self.offset_to_normal()

    @abstractmethod
    def normal_unit(self) -> "AtomicUnit":
        """The atomic unit that represents the normalised unit for this family"""

    def ratio_to_normal(self) -> float:
        """The ratio from this unit to the normal unit"""
        return 1.0

    def offset_to_normal(self) -> float:
        """The offset from this unit to the normal unit"""
        return 0.0


class CompoundUnit(Unit):

    def __init__(self, *args: tuple[AtomicUnit, int]) -> None:
        # Ensure we don't have duplicates in the list of values.
        values = {}
        for k, v in args:
            if k in values:
                values[k] += v
            else:
                values[k] = v
        self._unit_power: tuple[tuple[AtomicUnit, int], ...] = tuple(
            sorted(values.items(), key=lambda item: item[0]._unit_family))

    def as_(self, value: float, unit: "Unit") -> float:
        assert type(unit) is CompoundUnit
        assert len(self._unit_power) == len(unit._units)
        assert all(u_s._unit_family == u_n.unity_family and p_s == p_n for (u_s, p_s), (u_n, p_n) in
                   zip(self._unit_power, unit._units))
        v = value
        for (u_s, p_s), (u_n, p_n) in zip(self._unit_power, unit):
            v = math.pow(-p_s) if p_s is not 1 else v
            v = u_s.as_(v, u_n)
            v = math.pow(v, p_s) if p_s != 1 else v
        return v

    def normalise(self) -> "CompoundUnit":
        """
        Convert all units to the generic of the unit.
        """
        values = {}
        for k, v in self._unit_power:
            normalised_key = k.normalise()
            if normalised_key in values:
                values[normalised_key] += v
            else:
                values[normalised_key] = v
        return CompoundUnit(*values.items())

    def __rmul__(self, other):
        if isinstance(other, Unit):
            if isinstance(other, AtomicUnit):
                return CompoundUnit((other, 1), *self._unit_power)
            else:
                return CompoundUnit(*self._unit_power, *other._unit_power)
        else:
            super().__rmul__(other)
