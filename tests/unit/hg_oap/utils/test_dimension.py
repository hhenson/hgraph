from hg_oap.units.dimension import PrimaryDimension, DerivedDimension, Dimensionless
from hg_oap.units.unit_system import UnitSystem


def test_dimensions():
    UnitSystem.__instance__ = None # TODO: Either make this stackable or only import U when requried
    with UnitSystem():
        length = PrimaryDimension(name='length')
        volume = length**3
        assert volume.name == 'length**3'

        weight = PrimaryDimension(name='weight')
        density = weight/volume
        assert density.name == 'weight/length**3'

        area = volume / length
        assert area.name == 'length**2'

        time = PrimaryDimension(name='time')
        acceleration = length / time**2
        assert acceleration.name == 'length/time**2'

        area_acceleration = length**2 / time**2
        assert area_acceleration.name == 'length**2/time**2'


def test_named_derived_dimensions():
    with UnitSystem():
        length = PrimaryDimension(name='length')
        area = DerivedDimension(name='area', components=((length, 2),))
        volume = DerivedDimension(name='volume', components=((length, 3),))

        assert area.name == 'area'
        assert area * length is volume


def test_auto_naming():
    with UnitSystem() as U:
        U.length = PrimaryDimension()
        U.area = U.length**2
        U.volume = U.area * U.length

        assert U.length.name == 'length'
        assert U.area.name == 'area'
        assert U.volume.name == 'volume'
        assert U.area * U.length is U.volume
        assert U.volume / U.length is U.area


def test_dimensionless():
    with UnitSystem() as U:
        U.dimensionless = Dimensionless()
        assert U.dimensionless**2 is U.dimensionless
        assert U.dimensionless / U.dimensionless is U.dimensionless
        assert U.dimensionless * U.dimensionless is U.dimensionless

        U.length = PrimaryDimension()
        assert U.length * U.dimensionless is U.length
        assert U.dimensionless * U.length is U.length
        assert U.length / U.dimensionless is U.length
        assert U.dimensionless / U.length**2 is U.length**-2
        assert U.length / U.length is U.dimensionless


def test_dimension_qualifiers():
    with UnitSystem() as U:
        U.money = PrimaryDimension()
        U.us_dollars = U.money.us_dollars
        U.euros = U.money.euros
        U.bitcoins = U.money.bitcoins

        assert (U.euros / U.us_dollars).name == 'euros/us_dollars'
