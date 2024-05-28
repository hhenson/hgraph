from dataclasses import dataclass

import pytest

import hg_oap.quanity.conversion
from hg_oap.units.unit import Unit
from hg_oap.units.dimension import PrimaryDimension, DerivedDimension, Dimension
from hg_oap.utils.exprclass import ExprClass
from hg_oap.units.quantity import Quantity
from hg_oap.units.unit import PrimaryUnit, DerivedUnit, OffsetDerivedUnit
from hg_oap.units.unit_system import UnitSystem, UnitConversionContext
from hgraph import CompoundScalar


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


def test_unit_conversion_1():
    with UnitSystem() as U:
        U.length = PrimaryDimension()
        U.meter = PrimaryUnit(dimension=U.length)

        assert U.meter.convert(100., to=U.meter) == 100.

        U.cm = DerivedUnit(primary_unit=U.meter, ratio=0.01)

        assert U.cm.convert(100., to=U.meter) == 1.
        assert U.cm.convert(100.0, to=U.meter) == 1.

        assert U.meter.convert(100., to=U.cm) == 10000.

        U.meter_sq = U.meter**2
        U.cm_sq = U.cm**2

        assert U.meter_sq.convert(1., to=U.cm_sq) == 10000.
        assert U.cm_sq.convert(1., to=U.meter_sq) == 0.0001

        U.timespan = PrimaryDimension()
        U.second = PrimaryUnit(dimension=U.timespan)
        U.minute = DerivedUnit(primary_unit=U.second, ratio=60.0)

        U.velocity = U.length / U.timespan

        assert (U.meter/U.second).convert(1., to=U.cm / U.minute) == 6000.
        assert (U.cm/U.second).convert(1., to=U.meter / U.minute) == 60. / 100.

        with pytest.raises(ValueError):
            (U.meter/U.second).convert(1., to=U.meter_sq)


def test_conversion_2():
    with UnitSystem(__prefixes__={'m': 0.001}) as U:
        U.length = PrimaryDimension()
        U.meter = PrimaryUnit(dimension=U.length)

        U.volume = U.length**3
        U.cubic_meter = U.meter**3
        U.liter = 0.001 * U.cubic_meter

        U.bushel = 35.2391 * U.liter
        U.pint = 0.568 * U.liter
        U.add_prefixes(U.pint, ('m',))

        assert (U.mpint**-1).convert(1., to=U.bushel**-1) == 1/0.000568 * 35.2391


def test_offset_units():
    with UnitSystem() as U:
        U.temperature = PrimaryDimension()
        U.kelvin = PrimaryUnit(dimension=U.temperature)
        U.celsius = OffsetDerivedUnit(primary_unit=U.kelvin, ratio=1.0, offset=273.15)

        assert U.celsius.convert(0., to=U.celsius) == 0.
        assert U.kelvin.convert(273.15, to=U.kelvin) == 273.15

        assert U.celsius.convert(0., to=U.kelvin) == 273.15
        assert U.kelvin.convert(273.15, to=U.celsius) == 0.

        assert U.celsius.convert(100., to=U.kelvin) == 373.15

        U.fahrenheit = OffsetDerivedUnit(primary_unit=U.kelvin, ratio=5.0/9.0, offset=459.67)

        assert U.fahrenheit.convert(32., to=U.fahrenheit) == 32.
        assert round(U.fahrenheit.convert(32., to=U.celsius), 2) == 0.
        assert round(U.fahrenheit.convert(32., to=U.kelvin), 2) == 273.15
        assert round(U.celsius.convert(0., to=U.fahrenheit), 2) == 32.
        assert round(U.kelvin.convert(273.15, to=U.fahrenheit), 2) == 32.

        with pytest.raises(AssertionError):
            U.kelvin * U.celsius

        U.energy = PrimaryDimension()
        U.joule = PrimaryUnit(dimension=U.energy)

        U.length = PrimaryDimension()
        U.meter = PrimaryUnit(dimension=U.length)

        U.timespan = PrimaryDimension()
        U.second = PrimaryUnit(dimension=U.timespan)

        with pytest.raises(AssertionError):
            U.thermal_conductivity = U.joule / (U.meter * U.second * U.celsius)

        U.thermal_conductivity = U.joule / (U.meter * U.second * U.celsius.diff)
        assert U.thermal_conductivity.convert(1., to=U.joule / (U.meter * U.second * U.fahrenheit.diff)) == 5./9.

        with pytest.raises(AssertionError):  # not allowed to use an offset unit
            U.thermal_conductivity.convert(1., to=U.joule / (U.meter * U.second * U.fahrenheit))


def test_time_units():
    with UnitSystem() as U:
        U.time = PrimaryDimension()
        U.seconds = PrimaryUnit(dimension=U.time)


def test_qualified_units():
    with UnitSystem() as U:
        U.money = PrimaryDimension()
        U.us_dollars = U.money.us_dollars
        U.USD = PrimaryUnit(dimension=U.us_dollars)
        U.USX = 0.01 * U.USD

        U.euros = U.money.euros
        U.EUR = PrimaryUnit(dimension=U.euros)
        U.EUX = 0.01 * U.EUR

        U.bitcoins = U.money.bitcoins
        U.BTC = PrimaryUnit(dimension=U.bitcoins)

        assert U.USX.convert(100., to=U.USD) == 1.
        assert U.EUX.convert(100., to=U.EUR) == 1.
        assert (U.EUR/U.USD).name == 'EUR/USD'  # EUR/USD

        with UnitConversionContext((1.15 * (U.USD/U.EUR),)):
            assert U.EUR.convert(1., to=U.USD) == 1.15


def test_contexts_and_conversion_factors():
    with UnitSystem() as U:
        U.length = PrimaryDimension()
        U.meter = PrimaryUnit(dimension=U.length)

        U.timespan = PrimaryDimension()
        U.second = PrimaryUnit(dimension=U.timespan)
        U.minute = DerivedUnit(primary_unit=U.second, ratio=60.)
        U.hour = DerivedUnit(primary_unit=U.minute, ratio=60.)

        U.velocity = U.length / U.timespan
        U.meter_per_second = U.meter / U.second

        my_speed = 2. * U.meter_per_second

        with UnitConversionContext((my_speed,)):
            assert U.hour.convert(1., to=U.meter) == 7200.


def test_contexts_and_conversion_factors_2():
    with UnitSystem() as U:
        U.currency = PrimaryDimension()
        U.currency_unit = PrimaryUnit(dimension=U.currency)
        U.cent = DerivedUnit(primary_unit=U.currency_unit, ratio=0.01)

        U.length = PrimaryDimension()
        U.meter = PrimaryUnit(dimension=U.length)

        U.volume = U.length**3
        U.cubic_meter = U.meter**3
        U.liter = 0.001 * U.cubic_meter

        U.bushel = 35.2391 * U.liter

        U.weight = PrimaryDimension()
        U.kg = PrimaryUnit(dimension=U.weight)
        U.mt = 1000. * U.kg
        U.pound = 0.453592 * U.kg

        U.future_contract = PrimaryDimension()
        U.lot = PrimaryUnit(dimension=U.future_contract)

        @dataclass
        class MyAsset:
            name: str
            density: Quantity

        @dataclass
        class MyInstrument(CompoundScalar, ExprClass, UnitConversionContext):
            asset: MyAsset
            lot_size: int
            unit: Unit
            price_unit: Unit
            price_tick_size: float
            price_currency: str

            unit_conversion_factors: tuple[Quantity] = \
                lambda self: (
                    Quantity(self.lot_size, self.unit / U.lot),
                    self.asset.density,
                )


        asset = MyAsset('corn', Quantity(0.75, U.kg / U.liter))
        instrument = MyInstrument(asset=asset, lot_size=10000., unit=U.bushel, price_unit=U.cent, price_tick_size=0.25, price_currency='USD')

        with instrument:
            assert U.lot.convert(1., to=U.bushel) == 10000.
            assert U.lot.convert(1., to=U.cubic_meter) == 352.391
            assert U.lot.convert(1., to=U.mt) == 264.29325

            assert round(U.mt.convert(1., to=U.lot), 5) == 0.00378
            assert round(U.pound.convert(1000., to=U.lot), 5) == 0.00172
