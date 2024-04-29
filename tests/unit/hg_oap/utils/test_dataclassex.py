from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta

from hg_oap.dates.tenor import Tenor
from hg_oap.dates.dgen import days
from hg_oap.utils.exprclass import ExprClass, dataclassex, CallableDescriptor, exprclass
from hg_oap.utils.op import ParameterOp, lazy

SELF = ParameterOp(_name='SELF', _index=0)


def test_expr_descriptor():
    @dataclass
    class expr_1:
        a: int
        b: int = CallableDescriptor(SELF.a + 1)

    e = expr_1(a=2)
    assert e.b == 3

    e = expr_1(a=2, b=12)
    assert e.b == 12


def test_expr_descriptor_implicit():
    @dataclass
    class expr_1(ExprClass):
        a: int
        b: int = SELF.a + 1

    e = expr_1(a=2)
    assert e.b == 3

    e = expr_1(a=2, b=12)
    assert e.b == 12


def test_dataclassex():
    @dataclassex
    class expr_1:
        SELF: "expr_1"

        a: int
        b: int = SELF.a + 1

    e = expr_1(a=2)
    assert e.b == 3

    e = expr_1(a=2, b=12)
    assert e.b == 12


def test_dataclassex_date():

    @dataclassex
    class date_expr_1:
        SELF: 'date_expr_1'

        today: date = lambda x: date.today()
        tomorrow: date = SELF.today + Tenor('1d')

    e = date_expr_1()
    assert e.tomorrow == date.today() + timedelta(days=1)


def test_exprclass_dates():
    @dataclass
    @exprclass
    class date_expr_2:
        SELF: 'date_expr_2'

        today: date = lambda x: date.today()
        in_a_month: date = SELF.today + Tenor('1m')
        days_in_month: list[date] = SELF.today <= days < SELF.in_a_month
        number_of_days: int = lazy(len)(SELF.days_in_month)

    e = date_expr_2()
    assert e.number_of_days == monthrange(e.today.year, e.today.month)[1]

