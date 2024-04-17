from hg_oap.units.dimension import PrimaryDimension, DerivedDimension
from hg_oap.units.unit import PrimaryUnit
from hg_oap.units.unit_system import UnitSystem


def test_units():
    with UnitSystem():
        length = PrimaryDimension(name='length')
        area = DerivedDimension(name='area', components=((length, 2),))
        meter = PrimaryUnit(name='meter', dimension=length)
        meter_sq = meter**2
        assert meter_sq.name == 'meter**2'
        assert meter_sq.dimension is area

        force = PrimaryDimension(name='force')
        newton = PrimaryUnit(name='newton', dimension=force)

        pressure = DerivedDimension(name='pressure', components=((force, 1), (area, -1)))
        pascal = newton / meter**2
        assert pascal.name == 'newton/meter**2'
        assert pascal.dimension is pressure
        assert pascal is newton / meter_sq


def test_unit_auto_naming():
    with UnitSystem() as U:
        U.length = PrimaryDimension()
        U.area = U.length**2
        U.meter = PrimaryUnit(dimension=U.length)
        meter_sq = U.meter**2
        assert meter_sq.name == 'meter**2'
        assert meter_sq.dimension is U.area

        U.meter_sq = meter_sq
        assert meter_sq.name == 'meter_sq'

        U.force = PrimaryDimension()
        U.newton = PrimaryUnit(dimension=U.force)

        U.pressure = U.force / U.area
        U.pascal = U.newton / U.meter**2
        assert U.pascal.name == 'pascal'
        assert U.pascal.dimension is U.pressure
        assert U.pascal is U.newton / U.meter_sq

