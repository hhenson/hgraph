from dataclasses import dataclass
from typing import Optional

from hg_oap.assets.asset import FinancialAsset
from hg_oap.units import PrimaryUnit, Unit


@dataclass(frozen=True, kw_only=True)
class Currency(FinancialAsset):
    """
    The medium of exchange for goods and services, you may have used this. For example USD, EUR, GBP, etc.
    These represent the most commonly used currencies in the USA, Europe or the United Kingdom.
    """
    minor_currency_ratio: Optional[int] = None  # For example US cents rather than dollars
    unit: Unit = None  # to be set in __post_init__

    def __post_init__(self):
        from hg_oap.units.default_unit_system import U
        setattr(U, f"currency.{self.symbol}", currency_dim := getattr(U.money, self.symbol))  # Add the currency to the unit system as a qualified dimension
        setattr(U, self.symbol, unit := PrimaryUnit(dimension=currency_dim))
        object.__setattr__(self, 'unit', unit)
        if self.minor_currency_ratio is not None:
            minor_symbol = f"{self.symbol[:-1]}X"
            setattr(U, minor_symbol, 1.0/self.minor_currency_ratio * unit)
