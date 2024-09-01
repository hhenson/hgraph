import operator
from dataclasses import dataclass, field
from functools import reduce
from itertools import chain, combinations
from typing import ClassVar, Tuple, Iterable

from hg_oap.utils.exprclass import CallableDescriptor

__all__ = ("UnitSystem", "UnitConversionContext")


@dataclass
class UnitSystem:
    __instance__: ClassVar['UnitSystem'] = None

    __dimensions__: dict[str, "Dimension"] = field(default_factory=dict)
    __derived_dimensions__: dict[tuple[tuple["Dimension", int], ...], "Dimension"] = field(default_factory=dict)

    __primary_units__: dict[str, "Unit"] = field(default_factory=dict)
    __derived_units__: dict[Tuple["Unit", float], "Unit"] = field(default_factory=dict)
    __complex_units__: dict[tuple[tuple["Unit", int], ...], "Unit"] = field(default_factory=dict)

    __contexts__: list["UnitConversionContext"] = field(default_factory=list)
    __prefixes__: dict[str, float] = field(default_factory=dict)

    @staticmethod
    def instance():
        if UnitSystem.__instance__ is None and UnitSystem.__default__ is not None:
            UnitSystem.__instance__ = U
        return UnitSystem.__instance__

    def register(self):
        """Register the unit system to make use of"""
        UnitSystem.__instance__ = self

    @staticmethod
    def de_register():
        """De-register the unit system"""
        UnitSystem.__instance__ = None

    def __enter__(self):
        """
        Context manager enter, used largely for testing. This will replace any existing unit system.
        """
        self.register()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.de_register()

    def __setattr__(self, key, value):
        if key in self.__class__.__dataclass_fields__:
            object.__setattr__(self, key, value)
            return

        from hg_oap.units.quantity import Quantity
        if isinstance(value, Quantity):
            from hg_oap.units.unit import PrimaryUnit, DerivedUnit, ComplexUnit
            if isinstance(value.unit, (PrimaryUnit, DerivedUnit)):
                value = DerivedUnit(value)
            elif isinstance(value.unit, ComplexUnit):
                value = ComplexUnit(value)

        if value.name != key:
            if (desc := getattr(type(value), 'name', None)) and isinstance(desc, CallableDescriptor):
                desc.__override__(value, key)
            else:
                object.__setattr__(value, 'name', key)

            from hg_oap.units.dimension import PrimaryDimension
            if isinstance(value, PrimaryDimension):
                self.__dimensions__[key] = value

        object.__setattr__(self, key, value)

        from hg_oap.units.unit import Unit
        if isinstance(value, Unit) and value.prefixes:
            self.add_prefixes(value, value.prefixes)

    def add_prefixes(self, unit, prefixes):
        for p in prefixes:
            self.__setattr__(f"{p}{unit.name}", self.__prefixes__[p] * unit)

    def enter_context(self, context: "UnitConversionContext"):
        self.__contexts__.append(context)

    def exit_context(self, context: "UnitConversionContext"):
        assert self.__contexts__[-1] == context
        self.__contexts__.pop()

    def conversion_factor(self, dimension: "Dimension") -> "Quantity[float]":
        for context in reversed(self.__contexts__):
            if factor := context.conversion_factor(dimension):
                return factor


class UnitConversionContext:
    def __init__(self, conversion_factors: tuple["Quantity[float]", ...] = ()):
        self.unit_conversion_factors = conversion_factors

    def __enter__(self):
        UnitSystem.instance().enter_context(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        UnitSystem.instance().exit_context(self)

    def conversion_factor(self, dimension: float) -> "Quantity[float]":
        if (ucf := getattr(self, "_unit_conversion_factors_lookup", None)) is None:
            ucf = UnitConversionContext.make_conversion_factors(self.unit_conversion_factors)
            object.__setattr__(self, '_unit_conversion_factors_lookup', ucf)

        return ucf.get(dimension, None)

    @staticmethod
    def make_conversion_factors(factors: Iterable["Quantity[float]"]):
        combination_factors = [[reduce(operator.mul, j)
                                for j in combinations(factors, i)] for i in range(2, len(factors) + 1)]

        all_factors = chain(*((f, 1.0/f) for f in chain(factors, chain.from_iterable(combination_factors))))

        return {q.unit.dimension: q for q in all_factors}
