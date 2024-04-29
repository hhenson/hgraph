from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum

from hg_oap.assets.currency import Currency
from hg_oap.dates.calendar import Calendar
from hg_oap.dates.dgen import DGen
from hg_oap.dates.tenor import Tenor
from hg_oap.instruments.instrument import Instrument, INSTRUMENT_ID
from hg_oap.units import U
from hg_oap.units.quantity import Quantity
from hg_oap.units.unit import Unit
from hg_oap.units.unit_system import UnitConversionContext
from hg_oap.utils import ExprClass, SELF, ParameterOp, lazy
from hgraph import CompoundScalar


class SettlementMethod(Enum):
    Deliverable: str = "Deliverable"
    Financial: str = "Financial"


@dataclass(frozen=True)
class Settlement(CompoundScalar):
    """
    The settlement of a future contract.
    """
    method: SettlementMethod


@dataclass(frozen=True)
class FutureContractSpec(CompoundScalar, ExprClass, UnitConversionContext):
    """
    The specification of a future contract.
    """
    exchange: str
    symbol: str
    underlying: Instrument
    contract_size: Quantity[Decimal]
    currency: Currency

    trading_calendar: Calendar
    settlement: Settlement

    quotation_currency_unit: Unit
    quotation_unit: Unit
    tick_size: Quantity[Decimal]

    unit_conversion_factors: tuple[Quantity[Decimal]] = \
        lambda self: self.underlying.unit_conversion_factors + (self.contract_size/(Decimal(1.)*U.lot),)


@dataclass(frozen=True)
class FutureContractSeries(CompoundScalar, ExprClass, UnitConversionContext):
    """
    A series of future contracts.
    """

    spec: FutureContractSpec
    name: str
    symbol: INSTRUMENT_ID
    frequency: DGen  # a date generator that produces "contract base dates"

    expiry: DGen # given a contract base date, produces the expiry date

    first_trading_date: DGen  # given a contract base date, produces the first trading date
    last_trading_date: DGen  # given a contract base date, produces the last trading date


CONTRACT_BASE_DATE = ParameterOp(0, "contract_base_date")


def month_code(d: date) -> str:
    return ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'][d.month - 1]


@dataclass(frozen=True, kw_only=True)
class Future(Instrument):
    SELF: "Future" = SELF

    """
    A standardized legal agreement to buy or sell the underlyer at a predetermined price at a specific time in the
    future.
    """

    series: FutureContractSeries
    contract_base_date: date

    name: str = lambda self: self.series.name + self.contract_base_date.strftime("%b %y")
    symbol: str = lambda self: f"{self.series.symbol}{month_code(self.contract_base_date)}{self.contract_base_date.year % 10}"

    expiry: date = SELF.series.expiry(SELF.contract_base_date)
    first_trading_date: date = SELF.series.first_trading_date(SELF.contract_base_date)
    last_trading_date: date = SELF.series.last_trading_date(SELF.contract_base_date)

    unit_conversion_factors: tuple[Quantity[Decimal]] = SELF.series.spec.unit_conversion_factors