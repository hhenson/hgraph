from collections import defaultdict
from dataclasses import dataclass
from typing import ClassVar, Tuple

from hg_oap.utils.unit_system import UnitSystem


@dataclass(frozen=True, kw_only=True)
class Dimension:
    name: str = None

    def __str__(self):
        return self.name


@dataclass(frozen=True)
class PrimaryDimension(Dimension):
    def __new__(cls, name=None):
        if d := UnitSystem.instance().__dimensions__.get(name):
            return d

        n = super().__new__(cls)
        object.__setattr__(n, 'name', name)
        UnitSystem.instance().__dimensions__[name] = n
        return n

    def __pow__(self, power, modulo=None):
        return DerivedDimension(components=((self, power),))

    def __mul__(self, other: Dimension):
        if isinstance(other, PrimaryDimension):
            components = defaultdict(int, ((other, 1),))
        elif isinstance(other, DerivedDimension):
            components = defaultdict(int, other.components)
        else:
            raise ValueError(f"cannot multiply {self} and {other}")

        components[self] += 1
        return DerivedDimension(components=tuple(components.items()))

    def __truediv__(self, other):
        if isinstance(other, PrimaryDimension):
            components = defaultdict(int, ((other, -1),))
        elif isinstance(other, DerivedDimension):
            components = defaultdict(int, ((d, -p) for d, p in other.components))
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

        reduced_components = tuple(reduced_components.items())

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

    def __pow__(self, power, modulo=None):
        return DerivedDimension(components=((d, p * power) for d, p in self.components))

    def __mul__(self, other: Dimension):
        if isinstance(other, PrimaryDimension):
            components = defaultdict(int, ((other, 1),))
        elif isinstance(other, DerivedDimension):
            components = defaultdict(int, other.components)
        else:
            raise ValueError(f"cannot multiply {self} and {other}")

        for d, p in self.components:
            components[d] += p

        return DerivedDimension(components=tuple(components.items()))

    def __truediv__(self, other):
        if isinstance(other, PrimaryDimension):
            components = defaultdict(int, ((other, -1),))
        elif isinstance(other, DerivedDimension):
            components = defaultdict(int, ((d, -p) for d, p in other.components))
        else:
            raise ValueError(f"cannot multiply {self} and {other}")

        for d, p in self.components:
            components[d] += p

        return DerivedDimension(components=tuple(components.items()))
