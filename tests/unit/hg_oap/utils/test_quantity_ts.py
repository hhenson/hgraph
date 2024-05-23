from typing import TypeVar, Type

from hg_oap.quanity.conversion import convert
from hgraph import graph, TS, compute_node, TSB, TSL, AUTO_RESOLVE
from hgraph.nodes import cs_from_ts, route_ref, filter_, merge
from hgraph.test import eval_node

from hg_oap.units.default_unit_system import U
from hg_oap.units.quantity import Quantity
from hg_oap.units.unit import Unit, NUMBER


def test_quantity_ts():

    @compute_node
    def convert(ts: TS[Quantity[float]], units: TS[Unit]) -> TS[Quantity[float]]:
        return ts.value.as_(units.value)

    @graph
    def g(ts: TS[float], u: TS[Unit], u1: TS[Unit]) -> TS[Quantity[float]]:
        v = cs_from_ts(Quantity[float], qty=ts, unit=u)
        return convert(v, u1)

    assert eval_node(g, ts=[1., None, 2.], u=[U.kg, None, None], u1=[None, U.kg, U.g]) == [None, 1.*U.kg, 2000.*U.g]


def test_quantity_tsb():

    UNIT_1 = TypeVar("UNIT_1", bound=Unit)
    UNIT_2 = TypeVar("UNIT_2", bound=Unit)

    @graph
    def g(ts: TS[float], u: TS[Unit], u1: TS[Unit]) -> TS[Quantity[float]]:
        v = TSB[Quantity[float]].from_ts(qty=ts, unit=u)
        return convert(v, u1).as_scalar_ts()

    assert eval_node(g, ts=[1.0, None, 2.0], u=[U.kg, None, None], u1=[None, U.kg, U.g]) == [None, 1.*U.kg, 2000.*U.g]

    assert eval_node(g,
                     ts=[274.15, None, 273.15, None],
                     u=[U.K, None, None, None],
                     u1=[U.K, U.degC, U.degF, U.K]) == \
    [274.15*U.K, 1.*U.degC, 32.*U.degF, 273.15*U.K]
