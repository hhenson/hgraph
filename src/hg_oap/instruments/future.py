from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Type

from hg_oap.assets.currency import Currency
from hg_oap.dates.calendar import Calendar
from hg_oap.dates.dgen import DGen, make_dgen
from hg_oap.instruments.instrument import Instrument
from hg_oap.units.default_unit_system import U
from hg_oap.units.quantity import Quantity
from hg_oap.units.unit import Unit
from hg_oap.units.unit_system import UnitConversionContext
from hg_oap.utils import ExprClass, Expression, SELF, ParameterOp
from hg_oap.utils.op import lazy
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
    exchange_mic: str
    symbol: str
    underlying: Instrument
    contract_size: Quantity[float]
    currency: Currency

    trading_calendar: Calendar  # TODO - we also need settlement calendar and reset calendar?  To get the expiry dates
    settlement: Settlement

    quotation_currency_unit: Unit
    quotation_unit: Unit
    tick_size: Quantity[float]

    unit_conversion_factors: tuple[Quantity[float]] = \
        lambda self: self.underlying.unit_conversion_factors + (self.contract_size/(1.*U.lot),)


@dataclass(frozen=True, kw_only=True)
class FutureContractSeries(CompoundScalar, ExprClass, UnitConversionContext):
    SELF: "FutureContractSeries" = SELF
    """
    A series of future contracts.
    """

    spec: FutureContractSpec
    name: str
    symbol: str = SELF.spec.symbol + SELF.name
    future_type: Type[Instrument] = lambda self: Future
    frequency: DGen  # a date generator that produces "contract base dates"

    symbol_expr: Expression[[Instrument], str]

    expiry: Expression[[date], date]  # given a contract base date, produces the expiry date

    first_trading_date: Expression[[date], date]  # given a contract base date, produces the first trading date
    last_trading_date: Expression[[date], date]  # given a contract base date, produces the last trading date


CONTRACT_BASE_DATE = lazy(make_dgen)(ParameterOp(_name="CONTRACT_BASE_DATE"))


def month_code(d: int | date) -> str:
    m = (d.month if type(d) is date else d) - 1
    return ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'][m]


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
    symbol: str = SELF.series.symbol_expr(SELF)

    currency_unit: Unit = SELF.series.spec.quotation_currency_unit
    unit: Unit = SELF.series.spec.quotation_unit
    tick_size: Quantity[float] = SELF.series.spec.tick_size

    expiry: date = SELF.series.expiry(CONTRACT_BASE_DATE=SELF.contract_base_date)
    first_trading_date: date = SELF.series.first_trading_date(CONTRACT_BASE_DATE=SELF.contract_base_date)
    last_trading_date: date = SELF.series.last_trading_date(CONTRACT_BASE_DATE=SELF.contract_base_date)

    unit_conversion_factors: tuple[Quantity[float]] = SELF.series.spec.unit_conversion_factors