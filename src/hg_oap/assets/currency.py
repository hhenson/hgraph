from dataclasses import dataclass
from enum import Enum

from hg_oap.instruments.instrument import Instrument
from hg_oap.assets.asset import FinancialAsset


@dataclass(frozen=True)
class Currency(FinancialAsset):
    """
    The medium of exchange for goods and services, you may have used this. For example USD, EUR, GBP, etc.
    These represent the most commonly used currencies in the USA, Europe or the United Kingdom.
    """
    is_minor_currency: bool = False  # For example US cents rather than dollars


@dataclass(frozen=True)
class MinorCurrency(Currency):
    """
    A minor currency is the fractional unit, for example US cents, typically this is a 1:100 ratio, the ratio
    is stored as ratio.
    """
    major_currency: Currency = None
    ratio: float = 100.0

    def to_major_currency(self, value: float) -> float:
        return value * self.ratio


class Currencies(Enum):
    """The collection of known currencies"""
    EUR = Currency("EUR")
    GBP = (gbp_ := Currency("GBP"))
    GBX = MinorCurrency("GBX", major_currency=gbp_)
    USD = (usd_ := Currency("USD"))
    USX = MinorCurrency("USX", major_currency=usd_)
