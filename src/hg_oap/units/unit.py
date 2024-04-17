import operator
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from functools import reduce
from typing import Tuple, ForwardRef, TypeVar

from hg_oap.units.dimension import Dimension
from hg_oap.utils.exprclass import ExprClass
from hg_oap.units.unit_system import UnitSystem


@dataclass(frozen=True, kw_only=True)
class Unit(ExprClass):
    name: str = None
    dimension: Dimension

    def __str__(self):
        return self.name


UNIT = TypeVar("UNIT", bound=Unit)


@dataclass(frozen=True)
class PrimaryUnit(Unit):
    def __new__(cls, name=None, dimension: Dimension = None):
        if d := UnitSystem.instance().__primary_units__.get(name):
            assert d.dimension is dimension
            return d

        n = super().__new__(cls)
        object.__setattr__(n, 'dimension', dimension)
        object.__setattr__(n, 'name', name)
        UnitSystem.instance().__primary_units__[name] = n
        return n

    def __pow__(self, power, modulo=None):
        return ComplexUnit(components=((self, 2),))

    def __mul__(self, other):
        if isinstance(other, (PrimaryUnit, DerivedUnit)):
            components = defaultdict(int, ((other, 1),))
        elif isinstance(other, ComplexUnit):
            components = other.components
        else:
            raise ValueError(f"cannot multiply {self} and {other}")

        components[self] += 1
        return ComplexUnit(components=tuple(components.items()))

    def __truediv__(self, other):
        if isinstance(other, (PrimaryUnit, DerivedUnit)):
            components = defaultdict(int, ((other, -1),))
        elif isinstance(other, ComplexUnit):
            components = defaultdict(int, ((d, -p) for d, p in other.components))
        else:
            raise ValueError(f"cannot divide {self} and {other}")

        components[self] += 1
        return ComplexUnit(components=tuple(components.items()))


@dataclass(frozen=True, kw_only=True)
class DerivedUnit(Unit):
    primary_unit: Unit
    scaling_factor: Decimal
    dimension: Dimension = lambda s: s.primary_unit.dimension

    def __new__(cls, primary_unit: Unit | ForwardRef("Quantity"), scaling_factor: Decimal = Decimal(1), name=None):
        from hg_oap.quanity.quantity import Quantity
        if type(primary_unit) is Quantity:
            primary_unit = primary_unit.unit
            scaling_factor = primary_unit.value

        if type(primary_unit) is DerivedUnit:
            primary_unit = primary_unit.primary_unit
            scaling_factor += primary_unit.scaling_factor

        if d := UnitSystem.instance().__derived_units__.get((primary_unit, scaling_factor)):
            return d

        n = super().__new__(cls)
        object.__setattr__(n, 'primary_unit', primary_unit)
        object.__setattr__(n, 'scaling_factor', scaling_factor)
        if name:
            object.__setattr__(n, 'name', name)
        UnitSystem.instance().__derived_units__[(primary_unit, scaling_factor)] = n
        return n

    def __mul__(self, other):
        if isinstance(other, (PrimaryUnit, DerivedUnit)):
            components = defaultdict(int, ((other, 1),))
        elif isinstance(other, ComplexUnit):
            components = other.components
        else:
            raise ValueError(f"cannot multiply {self} and {other}")

        components[self] += 1
        return ComplexUnit(components=tuple(components.items()))

    def __truediv__(self, other):
        if isinstance(other, (PrimaryUnit, DerivedUnit)):
            components = defaultdict(int, ((other, -1),))
        elif isinstance(other, ComplexUnit):
            components = defaultdict(int, ((d, -p) for d, p in other.components))
        else:
            raise ValueError(f"cannot divide {self} and {other}")

        components[self] += 1
        return ComplexUnit(components=tuple(components.items()))


@dataclass(frozen=True, kw_only=True)
class ComplexUnit(Unit):
    components: Tuple[Tuple[Unit, int], ...]
    dimension: Dimension = lambda s: reduce(operator.mul, (u.dimension**m for u, m in s.components))

    def __new__(cls, components, name=None):
        if d := UnitSystem.instance().__complex_units__.get(components):
            return d

        n = super().__new__(cls)
        object.__setattr__(n, 'components', components)
        if name:
            object.__setattr__(n, 'name', name)
        UnitSystem.instance().__complex_units__[components] = n
        return n

    def __post_init__(self):
        if not self.name:
            up = '*'.join(f"{d}**{p}" if p != 1 else str(d) for d, p in self.components if p > 0) or '1'
            dn = ('*'.join(f"{d}**{abs(p)}" if p != -1 else str(d) for d, p in self.components if p < 0))
            dn = ('/' + dn) if dn else ''
            object.__setattr__(self, 'name', up + dn)

    def __mul__(self, other):
        if isinstance(other, (PrimaryUnit, DerivedUnit)):
            components = defaultdict(int, ((other, 1),))
        elif isinstance(other, ComplexUnit):
            components = other.components
        else:
            raise ValueError(f"cannot multiply {self} and {other}")

        for u, p in self.components:
            components[u] += p

        return ComplexUnit(components=tuple(components.items()))

    def __truediv__(self, other):
        if isinstance(other, (PrimaryUnit, DerivedUnit)):
            components = defaultdict(int, ((other, -1),))
        elif isinstance(other, ComplexUnit):
            components = defaultdict(int, ((d, -p) for d, p in other.components))
        else:
            raise ValueError(f"cannot divide {self} and {other}")

        for u, p in self.components:
            components[u] += p

        return ComplexUnit(components=tuple(components.items()))