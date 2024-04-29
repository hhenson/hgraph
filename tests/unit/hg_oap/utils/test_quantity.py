import math
from decimal import Decimal

from hg_oap.units.dimension import PrimaryDimension, Dimensionless
from hg_oap.units.unit import PrimaryUnit, DerivedUnit, OffsetDerivedUnit
from hg_oap.units.unit_system import UnitSystem

def test_quantity_1():
    from hg_oap.units.U import U
    with U:
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