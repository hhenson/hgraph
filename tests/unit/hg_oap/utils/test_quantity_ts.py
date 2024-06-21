from hg_oap.quanity.conversion import convert_units
from hg_oap.units.default_unit_system import U
from hg_oap.units.quantity import Quantity
from hg_oap.units.unit import Unit
from hgraph import graph, TS, compute_node, TSB, combine
from hgraph.test import eval_node


def test_quantity_ts():

    @compute_node
    def convert(ts: TS[Quantity[float]], units: TS[Unit]) -> TS[Quantity[float]]:
        return ts.value.as_(units.value)

    @graph
    def g(ts: TS[float], u: TS[Unit], u1: TS[Unit]) -> TS[Quantity[float]]:
        v = combine[TS[Quantity[float]]](qty=ts, unit=u)
        return convert(v, u1)

    assert eval_node(g, ts=[1., None, 2.], u=[U.kg, None, None], u1=[None, U.kg, U.g]) == [None, 1.*U.kg, 2000.*U.g]


def test_quantity_tsb():

    @graph
    def g(ts: TS[float], u: TS[Unit], u1: TS[Unit]) -> TS[Quantity[float]]:
        v = TSB[Quantity[float]].from_ts(qty=ts, unit=u)
        return convert_units(v, u1).as_scalar_ts()

    assert eval_node(g, ts=[1.0, None, 2.0], u=[U.kg, None, None], u1=[None, U.kg, U.g]) == [None, 1.*U.kg, 2000.*U.g]

    assert eval_node(g,
                     ts=[274.15, None, 273.15, None],
                     u=[U.K, None, None, None],
                     u1=[U.K, U.degC, U.degF, U.K],
                     #__trace__=True
                     ) == \
    [274.15*U.K, 1.*U.degC, 32.*U.degF, 273.15*U.K]


def test_mwh_to_therm():
    @compute_node
    def convert(ts: TS[Quantity[float]], units: TS[Unit]) -> TS[Quantity[float]]:
        return ts.value.as_(units.value)

    @graph
    def g(ts: TS[float], u: TS[Unit], u1: TS[Unit]) -> TS[Quantity[float]]:
        v = combine[TS[Quantity[float]]](qty=ts, unit=u)
        return convert(v, u1)

    assert eval_node(g, ts=[1.0], u=[U.MWh], u1=[U.therm]) == [34.12141633127942*U.therm]

