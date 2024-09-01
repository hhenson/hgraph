from hg_oap.quanity.conversion import convert_units, has_conversion_ratio
from hg_oap.units.default_unit_system import U
from hgraph.test import eval_node


def test_convert_units():
    results = eval_node(convert_units, qty=[10.0], fr=[U.MWh], to=[U.MMBtu])
    assert results[-1] == 34.12141633127942


def test_is_convertible():
    assert U.tonne.is_convertible(U.MWh) == False
    assert U.tonne.is_convertible(U.lot) == False
    assert U.tonne.is_convertible(U.kg) == True
    assert U.MWh.is_convertible(U.therm) == True