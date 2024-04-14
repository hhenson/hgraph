from hg_oap.utils.dimension import PrimaryDimension, DerivedDimension
from hg_oap.utils.unit_system import UnitSystem


def test_dimensions():
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
