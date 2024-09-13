from dataclasses import dataclass
from datetime import date
from typing import Generic, TypeVar

from hgraph import CompoundScalar, TSB, TSD, Frame, graph, TS, map_, add_, switch_, compute_node, subscription_service, \
    request_reply_service, service_impl, register_service, combine, sample, flip, dedup, const
from hgraph import merge
from hgraph.nodes import make_tsd
from hgraph.test import eval_node

from hg_oap.assets.commodities import Commodity
from hg_oap.assets.currency import Currencies
from hg_oap.dates.calendar import WeekendCalendar
from hg_oap.dates.dgen import roll_bwd, years
from hg_oap.instruments.future import Settlement, SettlementMethod, FutureContractSpec, FutureContractSeries, \
    CONTRACT_BASE_DATE, Future, month_code
from hg_oap.instruments.fx import FXSpot
from hg_oap.instruments.instrument import Instrument, INSTRUMENT_ID
from hg_oap.instruments.physical import PhysicalCommodity
from hg_oap.quanity.conversion import convert_units
from hg_oap.units.default_unit_system import U
from hg_oap.units.quantity import Quantity
from hg_oap.units.unit import Unit, NUMBER
from hg_oap.units.unit_system import UnitConversionContext
from hg_oap.utils import SELF, ExprClass


#####################
# Reference data APIs

@subscription_service
def get_instrument(instrument: TS[str], path: str = "instrument_service") -> TS[Instrument]:
    ...


@request_reply_service
def register_instrument(instrument: TS[Instrument], path: str = "instrument_service"):
    ...


@service_impl(interfaces=(get_instrument, register_instrument))
def instrument_service(path: str = 'instrument_service'):
    instrument_submissions = register_instrument.wire_impl_inputs_stub(path)
    instruments = flip(instrument_submissions.instrument).key_set
    instrument_by_symbol = flip(map_(lambda key: key.symbol, __keys__=instruments))
    get_instrument.wire_impl_out_stub(
        path,
        map_(lambda x: x, __keys__=get_instrument.wire_impl_inputs_stub(path).instrument, x=instrument_by_symbol),
    )


####################


@dataclass(frozen=True)
class Position(CompoundScalar, Generic[NUMBER]):
    """
    Position is a triplet of quantity, unit and instrument. In reality you almost never need to have an object
    representing a position, but rather a mapping of instruments to quantities
    """
    qty: NUMBER
    unit: Unit
    instrument: Instrument


POSITIONS = TypeVar('POSITIONS', Position[float], Frame[Position[float]], TSD[str, TSB[Quantity[float]]])


###################################################


@dataclass
class Price(CompoundScalar, Generic[NUMBER], ExprClass, UnitConversionContext):
    """
    Price is a triplet of quantity, unit and currency unit, representing the price in the
    units of the currency unit per unit of the thing being priced, for example a triplet of
    (600, USX, bushel) would represent a price of 600 US cents per bushel or 6 dollars per bushel
    """
    qty: NUMBER
    currency_unit: Unit
    unit: Unit

    unit_conversion_factors: tuple[Quantity[float], ...] = lambda self: (self.qty * (self.currency_unit / self.unit),)


#####################


@subscription_service
def get_price(instrument: TS[INSTRUMENT_ID], path: str = "price_service") -> TSB[Price[float]]:
    ...


@request_reply_service
def submit_price(instrument: TS[INSTRUMENT_ID], price: TSB[Price[float]], path: str = "price_service"):
    ...


@service_impl(interfaces=(get_price, submit_price))
def price_service(path: str = 'price_service'):
    price_submissions = submit_price.wire_impl_inputs_stub(path)
    prices = map_(lambda i, p: make_tsd(i, p), price_submissions.instrument, price_submissions.price) \
        .reduce(lambda x, y: merge(x, y))
    prices = sample(prices, prices)  # AB: there is a scheduling bug between the above map/reduce and the map below
    # that is to do with references and not identified yet. As per usual sampling in the middle works around reference problems
    get_price.wire_impl_out_stub(
        path,
        map_(lambda x: x, __keys__=get_price.wire_impl_inputs_stub(path).instrument, x=prices),
    )


@compute_node
def fx_rate_symbol(fr: TS[Unit], to: TS[Unit]) -> TS[str]:
    return f"{fr.value.primary_unit}{to.value.primary_unit}"  # FX rate naming convention is weird


def convert_price_to_currency_units(price: TSB[Price], currency_unit: TS[Unit]) -> TSB[Price]:
    # here the FXSpot instrument provides a property unit_conversion_factors which contains a Quantity
    # in units of to_currency_unit per from_currency_unit
    with get_price(fx_rate_symbol(price.currency_unit, currency_unit)):
        return TSB[Price[float]].from_ts(qty=convert_units(price.qty, price.currency_unit, currency_unit),
                                         currency_unit=currency_unit, unit=price.unit)


###################################################

@graph
def calculate_notional(positions: Position[float], currency: TS[Unit]) -> TSB[Quantity[float]]:
    return calculate_notional_tsb(TSB[Position[float]].from_ts(
        qty=positions.qty,
        unit=dedup(const(positions.unit, TS[Unit])),
        instrument=dedup(positions.instrument)),
        dedup(currency))


@graph(overloads=calculate_notional)
def calculate_notional_tsb(position: TSB[Position[float]], currency_unit: TS[Unit]) -> TSB[Quantity[float]]:
    price = get_price(position.instrument.symbol)
    requires_conversion = price.currency_unit != currency_unit
    requires_currency_conversion = price.currency_unit.dimension != currency_unit.dimension
    price_in_currency = switch_({
        (True, True): lambda p, c: convert_price_to_currency_units(p, c),
        (True, False): lambda p, c: TSB[Price[float]].from_ts(qty=convert_units(p.qty, p.currency_unit, c),
                                                              currency_unit=c, unit=p.unit),
        (False, False): lambda p, c: p
    }, combine[TS[tuple[bool, bool]]](requires_currency_conversion, requires_conversion), price, currency_unit)

    with position.instrument:
        return TSB[Quantity[float]].from_ts(
            qty=price_in_currency.qty * convert_units(position.qty, position.unit, to=price.unit),
            unit=price_in_currency.currency_unit)


@graph(overloads=calculate_notional)
def calculate_notional_tsd(positions: TSD[str, TSB[Quantity[float]]], currency: TS[Unit]) -> TSB[Quantity[float]]:
    """
    Calculate the notional value of a set of positions. The notional value is the value of the position if the position
    were to be closed out at the current market price.
    """
    return map_(
        lambda key, qty, c: calculate_notional(
            TSB[Position].from_ts(qty=qty.qty, unit=qty.unit, instrument=get_instrument(key)), c),
        positions, currency).reduce(add_)


####################################################

class Agricultural(Commodity):
    ...


def test_example():
    @graph
    def g(prices: TSD[str, TSB[Price[float]]]) -> TS[Quantity[float]]:
        register_service("price_service", price_service)
        register_service("instrument_service", instrument_service)

        corn = Agricultural(symbol='C', name="corn", default_unit=U.bushel,
                            unit_conversion_factors=(Quantity[float](0.75, U.kg / U.l),))
        corn_future_months = FutureContractSeries(
            spec=FutureContractSpec(
                exchange_mic='CME',
                symbol='ZC',
                underlying=PhysicalCommodity(symbol='Corn', asset=corn),
                contract_size=Quantity[float](5000., U.bushel),
                currency=Currencies.USD.value,
                trading_calendar=WeekendCalendar(),
                settlement=Settlement(SettlementMethod.Deliverable),
                quotation_currency_unit=U.USX,
                quotation_unit=U.bushel,
                tick_size=Quantity[float](0.25, U.USX),
            ),
            name='M',
            symbol_expr=lambda
                future: f"{future.series.spec.symbol}{month_code(future.contract_base_date)}{future.contract_base_date.year % 10}",
            frequency=years.mar | years.may | years.jul | years.sep | years.dec,
            expiry=roll_bwd(CONTRACT_BASE_DATE + '15d').over(SELF.spec.trading_calendar),
            first_trading_date=CONTRACT_BASE_DATE - '3y' < years.dec.days[15],
            # dec 15th 3 years before the expiry date (actual CME rules are more complex)
            last_trading_date=SELF.expiry(CONTRACT_BASE_DATE)
        )
        zck5 = Future(series=corn_future_months, contract_base_date=date(2025, 5, 1))
        register_instrument(zck5)

        gbpusd = FXSpot(symbol='GBPUSD', base=Currencies.GBP.value, quote=Currencies.USD.value)
        register_instrument(gbpusd)

        zcm5_position = Position[float](qty=100., unit=U.lot, instrument=zck5)
        notional = calculate_notional(zcm5_position, currency=U.GBP)

        map_(lambda key, p: submit_price(key, p), prices)

        return combine[TS[Quantity[float]]](**notional.as_dict())

    assert eval_node(
        g,
        #__trace__=dict(start=False, stop=False),
        prices=[None, {
            'GBPUSD': Price[float](qty=1.25, currency_unit=U.USD, unit=U.GBP),
            'USDGBP': Price[float](qty=1 / 1.25, currency_unit=U.GBP, unit=U.USD),
            'ZCK5': Price[float](qty=500., currency_unit=U.USX, unit=U.bushel),
        }])[-1] == (500. / 1.25 * 5000.) * U.GBP  # 500 USX per bushel, 5000 bushels, 1.25 USD per GBP
