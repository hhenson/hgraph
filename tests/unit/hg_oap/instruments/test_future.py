from calendar import MARCH

from hg_oap.instruments.future import month_code, month_from_code


def test_month_code():
    assert month_code(MARCH) == 'H'


def test_month_from_code():
    assert month_from_code('H') == MARCH

