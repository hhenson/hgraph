from dataclasses import dataclass
from typing import Generic, TypeVar, Type, T

from hg_oap.assets.currency import Currency
from hg_oap.instruments.instrument import Instrument
from hg_oap.units.quantity import Quantity
from hg_oap.units.unit import Unit, NUMBER
from hg_oap.units.unit_system import UnitConversionContext
from hgraph import CompoundScalar, TSB, TSD, Frame, graph, TS, map_, add_, switch_, compute_node, TSL, AUTO_RESOLVE, \
    subscription_service
from hgraph.nodes import merge, filter_, route_ref, tuple_from_ts, drop_dups


#####################
# Reference data APIs

@subscription_service
def get_instrument(instrument: str) -> TS[Instrument]:
    ...


####################

UNIT_1 = TypeVar("UNIT_1", bound=Unit)
UNIT_2 = TypeVar("UNIT_2", bound=Unit)

class ContextInjectable:
    ...


class CONTEXT(Generic[T]):
    def __init__(self, context: T):
        self.context = context

    def __enter__(self):
        self.context.__enter__(self.context)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.context.__exit__(exc_type, exc_val, exc_tb)


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
def conversion_ratio(fr: TS[Unit], to: TS[Unit], tp: Type[NUMBER] = AUTO_RESOLVE, context: CONTEXT[UnitConversionContext] = None) -> TS[NUMBER]:
    if fr.value._is_multiplicative and to.value._is_multiplicative:
        return fr.value.convert(tp(1.), to=to.value)

@compute_node
def convert_units(qty: TS[NUMBER], fr: TS[Unit], to: TS[Unit], context: CONTEXT[UnitConversionContext] = None) -> TS[NUMBER]:
    return fr.value.convert(qty.value, to=to.value)



####################


QUANTITY = TypeVar('QUANTITY')

@dataclass(frozen=True)
class Position(CompoundScalar, Generic[QUANTITY]):
    """
    Position is a triplet of quantity, unit and instrument. In reality you almost never need to have an object
    representing a position, but rather a mapping of instruments to quantities
    """
    qty: QUANTITY
    unit: Unit
    instrument: Instrument


POSITIONS = TypeVar('POSITIONS', Position[float], Frame[Position[float]], TSD[str, TSB[Quantity[float]]])


###################################################


@dataclass
class Price(CompoundScalar, Generic[QUANTITY]):
    """
    Price is a triplet of quantity, unit and currency unit, representing the price in the
    units of the currency unit per unit of the thing being priced, for example a triplet of
    (600, USX, bushel) would represent a price of 600 US cents per bushel or 6 dollars per bushel
    """
    qty: QUANTITY
    currency_unit: Unit
    unit: Unit


#####################


@subscription_service
def get_price(instrument: TS[str]) -> TSB[Price]:
    ...


@compute_node
def fx_rate_symbol(fr: TS[Unit], to: TS[Unit]) -> TS[str]:
    return f"{fr.value.primary_unit}/{to.value.primary_unit}"  # FX rate naming convention is weird


def convert_price_to_currency_units(price: TSB[Price], currency_unit: TS[Unit]) -> TSB[Price]:
    # here the FXSpot instrument provides a property unit_conversion_factors which contains a Quantity
    # in units of to_currency_unit per from_currency_unit
    with get_price(fx_rate_symbol(price.currency, currency_unit)):
        return convert(price.qty, price.currency_unit, currency_unit)


###################################################

@graph
def calculate_notional(positions: Position[float], currency: TS[Currency]) -> Quantity[float]:
    return calculate_notional_tsb(TSB[Position].from_ts(
            qty=positions.qty,
            unit=drop_dups(positions.unit),
            instrument=drop_dups(positions.instrument)),
        drop_dups(currency))


@graph(overloads=calculate_notional)
def calculate_notional_tsb(position: TSB[Position[float]], currency_unit: TS[Unit]) -> Quantity[float]:
    price = get_price(position.instrument.name)
    requires_conversion = price.currency_unit != currency_unit
    requires_currency_conversion = price.currency_unit.dimension != currency_unit.dimension
    price_in_currency = switch_({
        (True, True): lambda p, c: convert_price_to_currency_units(p, to=c),
        (True, False): lambda p, c: convert(p.qty, p.currency_unit, to=c),
        (False, False): lambda p, c: price
    }, tuple_from_ts(TSL.from_ts(requires_currency_conversion, requires_conversion)), price, currency_unit)

    with position.instrument:
        return price_in_currency * convert(position.qty, position.unit, to=price.unit)

@graph(overloads=calculate_notional)
def calculate_notional_tsd(positions: TSD[str, TSB[Quantity[float]]], currency: TS[Currency]) -> Quantity[float]:
    """
    Calculate the notional value of a set of positions. The notional value is the value of the position if the position
    were to be closed out at the current market price.
    """
    return map_(
        lambda key, qty, c: calculate_notional(
            TSB[Position].from_ts(qty=qty.qty, unit=qty.unit, instrument=get_instrument(key)), c),
    positions, currency).reduce(add_)
