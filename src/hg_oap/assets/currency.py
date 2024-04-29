from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Optional

from hg_oap.assets.asset import FinancialAsset
from hg_oap.units import PrimaryUnit


@dataclass(frozen=True)
class Currency(FinancialAsset):
    """
    The medium of exchange for goods and services, you may have used this. For example USD, EUR, GBP, etc.
    These represent the most commonly used currencies in the USA, Europe or the United Kingdom.
    """
    minor_currency_ratio: Optional[int] = None  # For example US cents rather than dollars

    def __post_init__(self):
        from hg_oap.units import U
        setattr(U, f"currency.{self.symbol}", currency_dim := getattr(U.money, self.symbol))  # Add the currency to the unit system as a qualified dimansion
        setattr(U, self.symbol, unit := PrimaryUnit(dimension=currency_dim))
        if self.minor_currency_ratio is not None:
            minor_symbol = f"{self.symbol[:-1]}X"
            setattr(U, minor_symbol, Decimal(1)/Decimal(self.minor_currency_ratio) * unit)


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
    EUR = Currency("EUR", minor_currency_ratio=100)
    GBP = Currency("GBP", minor_currency_ratio=100)
    USD = Currency("USD", minor_currency_ratio=100)
