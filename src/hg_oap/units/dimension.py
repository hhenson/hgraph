from collections import defaultdict
from dataclasses import dataclass
from typing import Tuple

from hg_oap.units.unit_system import UnitSystem


@dataclass(frozen=True, kw_only=True)
class Dimension:
    name: str = None

    def __new__(cls, name=None):
        assert cls is not Dimension, 'Base Dimension types is not instantiable.'

        if name:
            if d := UnitSystem.instance().__dimensions__.get(name):
                return d

        n = super().__new__(cls)
        if name:
            object.__setattr__(n, 'name', name)
            UnitSystem.instance().__dimensions__[name] = n

        return n

    def __str__(self):
        return self.name

    def __hash__(self):
        return id(self)

    def _to_components(self, power=1):
        return ((self, power),)


@dataclass(frozen=True)
class Dimensionless(Dimension):
    name: str = 'dimensionless'

    def __new__(cls):
        if d := UnitSystem.instance().__dimensions__.get('dimensionless'):
            return d

        n = super().__new__(cls, name='dimensionless')
        UnitSystem.instance().__dimensions__['dimensionless'] = n
        return n

    def __hash__(self):
        return id(self)

    def __pow__(self, power, modulo=None):
        return self

    def __mul__(self, other: Dimension):
        return other

    def __truediv__(self, other):
        return other**-1

    def _to_components(self, power=1):
        return ()


@dataclass(frozen=True)
class PrimaryDimension(Dimension):
    def __hash__(self):
        return id(self)

    def __pow__(self, power, modulo=None):
        return DerivedDimension(components=((self, power),))

    def __mul__(self, other: Dimension):
        if isinstance(other, Dimension):
            components = defaultdict(int, other._to_components())
        else:
            raise ValueError(f"cannot multiply {self} and {other}")

        components[self] += 1
        return DerivedDimension(components=tuple(components.items()))

    def __truediv__(self, other):
        if isinstance(other, Dimension):
            components = defaultdict(int, other._to_components(-1))
        else:
            raise ValueError(f"cannot divide {self} and {other}")

        components[self] += 1
        return DerivedDimension(components=tuple(components.items()))


@dataclass(frozen=True)
class DerivedDimension(Dimension):
    components: Tuple[Tuple[PrimaryDimension, int], ...]

    def __new__(cls, components, name=None):
        reduced_components = defaultdict(int)
        for d, p in components:
            if type(d) is PrimaryDimension:
                reduced_components[d] += p
            elif type(d) is DerivedDimension:
                for d1, p1 in d.components:
                    reduced_components[d1] += p1 * p

        reduced_components = tuple((d, p) for d, p in reduced_components.items() if p != 0)

        if len(reduced_components) == 0:
            return Dimensionless()

        if len(reduced_components) == 1 and reduced_components[0][1] == 1:
            return reduced_components[0][0]

        reduced_components = tuple(sorted(reduced_components, key=lambda x: x[0].name))

        if d := UnitSystem.instance().__derived_dimensions__.get(reduced_components):
            return d

        n = super().__new__(cls)
        object.__setattr__(n, 'components', reduced_components)
        if name:
            object.__setattr__(n, 'name', name)
        UnitSystem.instance().__derived_dimensions__[reduced_components] = n
        return n

    def __post_init__(self):
        if not self.name:
            up = '*'.join(f"{d}**{p}" if p != 1 else str(d) for d, p in self.components if p > 0) or '1'
            dn = ('*'.join(f"{d}**{abs(p)}" if p != -1 else str(d) for d, p in self.components if p < 0))
            dn = ('/' + dn) if dn else ''
            object.__setattr__(self, 'name', up + dn)

    def __hash__(self):
        return id(self)

    def __pow__(self, power, modulo=None):
        return DerivedDimension(components=tuple((d, p * power) for d, p in self.components))

    def __mul__(self, other: Dimension):
        if isinstance(other, Dimension):
            components = defaultdict(int, other._to_components())
        else:
            raise ValueError(f"cannot multiply {self} and {other}")

        for d, p in self.components:
            components[d] += p

        return DerivedDimension(components=tuple(components.items()))

    def __truediv__(self, other):
        if isinstance(other, Dimension):
            components = defaultdict(int, other._to_components(-1))
        else:
            raise ValueError(f"cannot multiply {self} and {other}")

        for d, p in self.components:
            components[d] += p

        return DerivedDimension(components=tuple(components.items()))

    def _to_components(self, power=1):
        if power == 1:
            return self.components
        else:
            return tuple((c, p*power) for c, p in self.components)
