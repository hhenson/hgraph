from decimal import Decimal
from typing import TypeVar, Type

from hgraph import graph, TS, compute_node, TSB, TSL, AUTO_RESOLVE
from hgraph.nodes import cs_from_ts, route_ref, filter_, merge
from hgraph.test import eval_node

from hg_oap.units import U
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
    def convert(qty: TS[NUMBER], fr: TS[UNIT_1], to: TS[UNIT_2], tp: Type[NUMBER] = AUTO_RESOLVE) -> TS[NUMBER]:
        """
        Cater for the three usa cases of conversion:
            - Same unit, no conversion required
            - Direct conversion ratio available - both units are multiplicative
            - One of both units are offset
        """

        pass_through, to_convert = route_ref(fr == to, qty)
        calc_ratio = has_conversion_ratio(fr, to)
        ratio_convert, offset_convert = route_ref(calc_ratio, to_convert)
        ratio = conversion_ratio[NUMBER:tp](filter_(calc_ratio, fr), filter_(calc_ratio, to))
        ratio_converted = ratio_convert * ratio
        offset_converted = convert_units(offset_convert, fr, to)
        return merge(TSL.from_ts(pass_through, ratio_converted, offset_converted))

    @graph(overloads=convert)
    def convert_qty(qty: TSB[Quantity[NUMBER]], to: TS[Unit]) -> TSB[Quantity[NUMBER]]:
        return {"qty": convert(qty.qty, qty.unit, to), "unit": to}

    @compute_node
    def has_conversion_ratio(fr: TS[Unit], to: TS[Unit]) -> TS[bool]:
        return fr.value._is_multiplicative and to.value._is_multiplicative

    @compute_node
    def conversion_ratio(fr: TS[Unit], to: TS[Unit], tp: Type[NUMBER] = AUTO_RESOLVE) -> TS[NUMBER]:
        if fr.value._is_multiplicative and to.value._is_multiplicative:
            return fr.value.convert(tp(1.), to=to.value)

    @compute_node
    def convert_units(qty: TS[NUMBER], fr: TS[Unit], to: TS[Unit]) -> TS[NUMBER]:
        return fr.value.convert(qty.value, to=to.value)

    @graph
    def g(ts: TS[Decimal], u: TS[Unit], u1: TS[Unit]) -> TS[Quantity[Decimal]]:
        v = TSB[Quantity[Decimal]].from_ts(qty=ts, unit=u)
        return convert(v, u1).as_scalar_ts()

    assert eval_node(g, ts=[Decimal(1.), None, Decimal(2.)], u=[U.kg, None, None], u1=[None, U.kg, U.g]) == [None, 1.*U.kg, 2000.*U.g]
    assert eval_node(g,
                     ts=[Decimal('274.15'), None, Decimal("273.15"), None],
                     u=[U.K, None, None, None],
                     u1=[U.K, U.degC, U.degF, U.K]) == \
    [Decimal('274.15')*U.K, 1.*U.degC, 32.*U.degF, Decimal("273.15")*U.K]
