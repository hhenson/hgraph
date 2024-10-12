import pytest

from hg_oap.units import Quantity
from hg_oap.units.default_unit_system import U
from hgraph import graph, TS
from hgraph.test import eval_node


def test_quantity_1():
    with U:
        assert 1.0 * U.m == 1.0 * U.m
        assert 1.0 * U.m == 100.0 * U.cm

        assert 60.0 * U.s == 1.0 * U.min

        assert 1.0 * U.kWh == 3600000.0 * U.J

        assert 1.25 * (1.0 / U.K) == 1.25 * (1.0 / U.degC.diff)

        assert 2.0 * (U.km / U.h) - 0.556 * (U.m / U.s) < 0.001 * (U.m / U.s)

        assert 100.0 * U.g + 1.0 * U.kg == 1100.0 * U.g
        assert 1.0 * U.m + 1.0 * U.cm == 101.0 * U.cm

        assert 1.0 * U.m**2 == 10000.0 * U.cm**2
        assert 1.0 * U.m * (1.0 * U.m) == 1.0 * U.m**2

        assert 1.0 * U.m**3 < 1000.1 * U.l
        assert 1.0 * U.m**3 >= 1000.0 * U.l
        assert 1.0 * U.m**3 > 999.99 * U.l
        assert 1.0 * U.m**3 <= 1000.0 * U.l

        assert (1.0 * U.m) ** 3 == 1000.0 * U.l

        assert 2.0 * U.rpm == (1 / 30.0) * U.s**-1


def test_quantity_operator_add():
    @graph
    def g(a: TS[Quantity[float]], b: TS[Quantity[float]]) -> TS[Quantity[float]]:
        with U:
            return a + b

    assert eval_node(g, [Quantity[float](1.0, U.m), Quantity[float](2.0, U.m)], [Quantity[float](1.0, U.m)]) == [
        Quantity[float](2.0, U.m),
        Quantity[float](3.0, U.m),
    ]


def test_quantity_operator_sub():
    @graph
    def g(a: TS[Quantity[float]], b: TS[Quantity[float]]) -> TS[Quantity[float]]:
        with U:
            return a - b

    assert eval_node(g, [Quantity[float](1.0, U.m), Quantity[float](2.0, U.m)], [Quantity[float](1.0, U.m)]) == [
        Quantity[float](0.0, U.m),
        Quantity[float](1.0, U.m),
    ]


@pytest.mark.skip("Runs stand alone but not with the rest of the tests")
def test_quantity_operator_mul():
    @graph
    def g(a: TS[Quantity[float]], b: TS[Quantity[float]]) -> TS[Quantity[float]]:
        with U:
            return a * b

    assert eval_node(g, [Quantity[float](1.0, U.m), Quantity[float](2.0, U.m)], [Quantity[float](1.0, U.m)]) == [
        Quantity[float](1.0, U.m**2),
        Quantity[float](2.0, U.m**2),
    ]


@pytest.mark.skip("Runs stand alone but not with the rest of the tests")
def test_quantity_operator_div():
    @graph
    def g(a: TS[Quantity[float]], b: TS[Quantity[float]]) -> TS[Quantity[float]]:
        with U:
            return a / b

    assert eval_node(g, [Quantity[float](1.0, U.m), Quantity[float](2.0, U.m)], [Quantity[float](1.0, U.s)]) == [
        Quantity[float](1.0, U.m / U.s),
        Quantity[float](2.0, U.m / U.s),
    ]


def test_quantity_operator_mul_float():
    @graph
    def g(a: TS[Quantity[float]], b: TS[float]) -> TS[Quantity[float]]:
        return a * b

    assert eval_node(g, [Quantity[float](1.0, U.m), Quantity[float](2.0, U.m)], [2.0]) == [
        Quantity[float](2.0, U.m),
        Quantity[float](4.0, U.m),
    ]


def test_quantity_operator_div_float():
    @graph
    def g(a: TS[Quantity[float]], b: TS[float]) -> TS[Quantity[float]]:
        return a / b

    assert eval_node(g, [Quantity[float](2.0, U.m), Quantity[float](6.0, U.m)], [2.0]) == [
        Quantity[float](1.0, U.m),
        Quantity[float](3.0, U.m),
    ]
