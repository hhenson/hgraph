from typing import Type

from hg_oap.units import Unit, Quantity, UnitConversionContext
from hg_oap.units.unit import NUMBER
from hgraph import graph, TS, AUTO_RESOLVE, TSL, TSB, compute_node, CONTEXT
from hgraph.nodes import route_ref, filter_, index_of, valid


@graph
def convert(qty: TS[NUMBER], fr: TS[Unit], to: TS[Unit], tp: Type[NUMBER] = AUTO_RESOLVE) -> TS[NUMBER]:
    """
    Cater for the three use cases of conversion:
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
    return TSL.from_ts(pass_through, ratio_converted, offset_converted)[
        index_of(TSL.from_ts(valid(pass_through), valid(ratio_converted), valid(offset_converted)), True)]


@graph(overloads=convert)
def convert_qty(qty: TSB[Quantity[NUMBER]], to: TS[Unit]) -> TSB[Quantity[NUMBER]]:
    return {"qty": convert(qty.qty, qty.unit, to), "unit": to}


@compute_node
def has_conversion_ratio(fr: TS[Unit], to: TS[Unit]) -> TS[bool]:
    return fr.value._is_multiplicative and to.value._is_multiplicative


@compute_node
def conversion_ratio(fr: TS[Unit], to: TS[Unit], tp: Type[NUMBER] = AUTO_RESOLVE, context: CONTEXT[UnitConversionContext] = None) -> TS[NUMBER]:
    if fr.value._is_multiplicative and to.value._is_multiplicative:
        return fr.value.convert(tp(1.), to=to.value)


@compute_node
def convert_units(qty: TS[NUMBER], fr: TS[Unit], to: TS[Unit], context: CONTEXT[UnitConversionContext] = None) -> TS[NUMBER]:
    return fr.value.convert(qty.value, to=to.value)
