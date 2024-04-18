import math
from decimal import Decimal

from hg_oap.units.dimension import PrimaryDimension, Dimensionless
from hg_oap.units.unit import PrimaryUnit, DerivedUnit, OffsetDerivedUnit
from hg_oap.units.unit_system import UnitSystem


def units():
    std_prefixes = {'k': Decimal('1000'), 'M': Decimal('1_000_000'), 'G': Decimal('1_000_000_000'), 'T': Decimal('1_000_000_000_000'),
                    'c': Decimal('0.01'), 'm': Decimal('0.001'), 'u': Decimal('0.000_001'), 'n': Decimal('0.000_000_001'), 'p': Decimal('0.000_000_000_001')}

    with UnitSystem(__prefixes__=std_prefixes) as U:
        U.length = PrimaryDimension()
        U.m = PrimaryUnit(dimension=U.length, prefixes=('k', 'c', 'm', 'u', 'n'))
        U.mi = DerivedUnit(primary_unit=U.m, ratio=Decimal('1609.344'))
        U.nautical_mile = DerivedUnit(primary_unit=U.m, ratio=Decimal('1852'))

        U.area = U.length**2
        U.hectare = DerivedUnit(primary_unit=U.m**2, ratio=Decimal('10000'))
        U.acre = DerivedUnit(primary_unit=U.m**2, ratio=Decimal('4046.8564224'))

        U.volume = U.length**3
        U.l = DerivedUnit(primary_unit=U.m**3, ratio=Decimal('0.001'))
        U.bushel = DerivedUnit(primary_unit=U.m**3, ratio=Decimal('0.03523907'))

        U.weight = PrimaryDimension()
        U.kg = PrimaryUnit(dimension=U.weight)
        U.g = DerivedUnit(primary_unit=U.kg, ratio=Decimal('0.001'), prefixes=('k', 'm'))
        U.mt = Decimal('1000') * U.kg
        U.pound = DerivedUnit(primary_unit=U.kg, ratio=Decimal('0.45359237'))
        U.toz = DerivedUnit(primary_unit=U.kg, ratio=Decimal(1)/Decimal('32.1507'))

        U.time = PrimaryDimension()
        absolute_seconds = PrimaryUnit(dimension=U.time)
        U.seconds_since_epoch = OffsetDerivedUnit(primary_unit=absolute_seconds)
        U.s = U.seconds_since_epoch.diff
        U.add_prefixes(U.s, ('m', 'u', 'n'))

        U.min = Decimal('60') * U.s
        U.h = Decimal('60') * U.min
        U.day = Decimal('24') * U.h
        U.week = Decimal('7') * U.day

        U.velocity = U.length / U.time
        U.kph = U.km / U.h
        U.mph = U.mi / U.h

        U.temperature = PrimaryDimension()
        U.K = PrimaryUnit(dimension=U.temperature)
        U.degC = OffsetDerivedUnit(primary_unit=U.K, ratio=Decimal('1'), offset=Decimal('273.15'))
        U.degF = OffsetDerivedUnit(primary_unit=U.K, ratio=Decimal('5')/Decimal('9'), offset=Decimal('459.67'))

        U.energy = U.weight * U.length**2 / U.time**2
        U.J = PrimaryUnit(dimension=U.energy)
        U.cal = DerivedUnit(primary_unit=U.J, ratio=Decimal('4.184'))

        U.power = U.energy / U.time
        U.W = PrimaryUnit(dimension=U.power, prefixes=('k', 'M', 'G', 'T', 'm'))

        U.Wh = U.W * U.h
        U.add_prefixes(U.Wh, ('k', 'M', 'G', 'T'))

        U.dimentionless = Dimensionless()
        U.turn = PrimaryUnit(dimension=U.dimentionless)
        U.rad = DerivedUnit(primary_unit=U.turn, ratio=Decimal(2)*Decimal(math.pi))
        U.deg = DerivedUnit(primary_unit=U.rad, ratio=Decimal(1)/Decimal(360))

        U.rpm = U.turn / U.min


    return U


def test_quantity_1():
    with units() as U:
        assert 1.*U.m == 1.*U.m
        assert 1.*U.m == 100.*U.cm

        assert 60.*U.s == 1.*U.min

        assert 1.*U.kWh == 3600000.*U.J

        assert 1.25*(1./U.K) == 1.25*(1./U.degC.diff)

        assert 2.*(U.km/U.h) - 0.556*(U.m/U.s) < 0.001*(U.m/U.s)

        assert 100.*U.g + 1.*U.kg == 1100.*U.g
        assert 1.*U.m + 1.*U.cm == 101.*U.cm

        assert 1.*U.m**2 == 10000.*U.cm**2
        assert 1.*U.m * (1.*U.m) == 1.*U.m**2

        assert 1.*U.m**3 < 1000.1*U.l
        assert 1.*U.m**3 >= 1000.*U.l
        assert 1.*U.m**3 > 999.99*U.l
        assert 1.*U.m**3 <= 1000.*U.l

        assert (1.*U.m)**3 == 1000.*U.l

        assert 2.*U.rpm == (1/30.)*U.s**-1