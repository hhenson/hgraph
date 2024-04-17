from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar, Tuple, Iterable


@dataclass
class UnitSystem:
    __instance__ = None

    __dimensions__: ClassVar[dict[str, "Dimension"]] = dict()
    __derived_dimensions__: ClassVar[dict[tuple[tuple["Dimension", int], ...], "Dimension"]] = dict()

    __primary_units__: ClassVar[dict[str, "Unit"]] = dict()
    __derived_units__: ClassVar[dict[Tuple["Unit", Decimal], "Unit"]] = dict()
    __complex_units__: ClassVar[dict[tuple[tuple["Unit", int], ...], "Unit"]] = dict()

    __contexts__: ClassVar[list["UnitConversionContext"]] = []

    @classmethod
    def instance(cls):
        return cls.__instance__

    def __enter__(self):
        if self.__class__.__instance__ is not None:
            raise ValueError(f"UnitSystems are not stackable contexts")

        self.__class__.__instance__ = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self.__class__.__instance__ == self

        self.__class__.__instance__ = None

    def __setattr__(self, key, value):
        from hg_oap.units.quantity import Quantity
        if isinstance(value, Quantity):
            from hg_oap.units.unit import PrimaryUnit, DerivedUnit, ComplexUnit
            if isinstance(value.unit, (PrimaryUnit, DerivedUnit)):
                value = DerivedUnit(value)
            elif isinstance(value.unit, ComplexUnit):
                value = ComplexUnit(value)

        if value.name != key:
            if desc := getattr(type(value), 'name', None):
                desc.__override_set__(value, key)
            else:
                object.__setattr__(value, 'name', key)

            from hg_oap.units.dimension import PrimaryDimension
            if isinstance(value, PrimaryDimension):
                self.__dimensions__[key] = value

        object.__setattr__(self, key, value)

    def enter_context(self, context: "UnitConversionContext"):
        self.__contexts__.append(context)

    def exit_context(self, context: "UnitConversionContext"):
        assert self.__contexts__[-1] == context
        self.__contexts__.pop()

    def conversion_factor(self, dimension: "Dimension") -> "Quantity[Decimal]":
        for context in reversed(self.__contexts__):
            if factor := context.conversion_factor(dimension):
                return factor

        return None


class UnitConversionContext:
    def __init__(self, conversion_factors: dict["Dimension", "Quantity[Decimal]"]):
        self.conversion_factors = conversion_factors

    def __enter__(self):
        UnitSystem.instance().enter_context(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        UnitSystem.instance().exit_context(self)

    def conversion_factor(self, dimension: "Dimension") -> "Quantity[Decimal]":
        return self.conversion_factors.get(dimension, None)

    @staticmethod
    def make_conversion_factors(factors: Iterable["Quantity[Decimal]"]):
        return {q.unit.dimension: q for q in factors}
