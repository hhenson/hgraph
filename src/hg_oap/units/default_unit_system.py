import math

from hg_oap.units.dimension import PrimaryDimension, Dimensionless
from hg_oap.units.unit import PrimaryUnit, DerivedUnit, OffsetDerivedUnit
from hg_oap.units.unit_system import UnitSystem

__all__ = ("U")


std_prefixes = {'k': 1000.0, 'M': 1_000_000.0, 'G': 1_000_000_000.0, 'T': 1_000_000_000_000.0,
                'c': 0.01, 'm': 0.001, 'u': 0.000_001, 'n': 0.000_000_001, 'p': 0.000_000_000_001}

U = UnitSystem(__prefixes__=std_prefixes)
U.register()  # If this has been imported we can go ahead and register it.

U.length = PrimaryDimension()
U.m = PrimaryUnit(dimension=U.length, prefixes=('k', 'c', 'm', 'u', 'n'))
U.mi = DerivedUnit(primary_unit=U.m, ratio=1609.344)
U.nautical_mile = DerivedUnit(primary_unit=U.m, ratio=1852.0)

U.area = U.length**2
U.hectare = DerivedUnit(primary_unit=U.m**2, ratio=10000.0)
U.acre = DerivedUnit(primary_unit=U.m**2, ratio=4046.8564224)

U.volume = U.length**3
U.l = DerivedUnit(primary_unit=U.m**3, ratio=0.001)
U.bushel = DerivedUnit(primary_unit=U.m**3, ratio=0.03523907)
U.gallon = DerivedUnit(primary_unit=U.l, ratio=3.785411784)
U.bbl = DerivedUnit(primary_unit=U.gallon, ratio=42.0)

U.weight = PrimaryDimension()
U.kg = PrimaryUnit(dimension=U.weight)
U.g = DerivedUnit(primary_unit=U.kg, ratio=0.001, prefixes=('k', 'm'))
U.mt = 1000 * U.kg
U.pound = DerivedUnit(primary_unit=U.kg, ratio=0.45359237)
U.toz = DerivedUnit(primary_unit=U.kg, ratio=1.0/32.1507)

U.time = PrimaryDimension()
absolute_seconds = PrimaryUnit(dimension=U.time)
U.seconds_since_epoch = OffsetDerivedUnit(primary_unit=absolute_seconds)
U.s = U.seconds_since_epoch.diff
U.add_prefixes(U.s, ('m', 'u', 'n'))

U.min = 60.0 * U.s
U.h = 60.0 * U.min
U.hour = U.h
U.day = 24.0 * U.h
U.week = 7.0 * U.day

U.calendar_months = U.time.calendar_months
U.month = PrimaryUnit(dimension=U.calendar_months)

U.velocity = U.length / U.time
U.kph = U.km / U.h
U.mph = U.mi / U.h

U.temperature = PrimaryDimension()
U.K = PrimaryUnit(dimension=U.temperature)
U.degC = OffsetDerivedUnit(primary_unit=U.K, ratio=1.0, offset=273.15)
U.degF = OffsetDerivedUnit(primary_unit=U.K, ratio=5.0/9.0, offset=459.67)

U.energy = U.weight * U.length**2 / U.time**2
U.J = PrimaryUnit(dimension=U.energy)
U.cal = DerivedUnit(primary_unit=U.J, ratio=4.184)

U.Btu = DerivedUnit(primary_unit=U.J, ratio=1055.05585262)
U.MMBtu = DerivedUnit(primary_unit=U.Btu, ratio=1e6)
U.therm = DerivedUnit(primary_unit=U.Btu, ratio=100_000)

U.power = U.energy / U.time
U.W = PrimaryUnit(dimension=U.power, prefixes=('k', 'M', 'G', 'T', 'm'))

U.Wh = U.W * U.h
U.add_prefixes(U.Wh, ('k', 'M', 'G', 'T'))

U.dimensionless = Dimensionless()
U.turn = PrimaryUnit(dimension=U.dimensionless)
U.rad = DerivedUnit(primary_unit=U.turn, ratio=2.0*math.pi)
U.deg = DerivedUnit(primary_unit=U.rad, ratio=1.0/360.0)

U.rpm = U.turn / U.min

U.contracts = PrimaryDimension()
U.lot = PrimaryUnit(dimension=U.contracts)

U.money = PrimaryDimension()
