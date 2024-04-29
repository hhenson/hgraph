from dataclasses import dataclass
from typing import Generic

from hgraph import TimeSeriesSchema, TS, Array, SIZE, TSB, CompoundScalar

from hg_oap.assets.currency import Currency
from hg_oap.units.unit import UNIT


@dataclass
class Price(CompoundScalar):
    """
    A bundle schema representing the price as a float and the associated currency asset that the price is representing.
    """
    price: float
    currency: Currency

    def __add__(self, other):
        """This class should be same as quantity, but for now..."""
        if other.currency == self.currency:
            return Price(price=self.price + other.price, currency=self.currency)
        else:
            raise ValueError(f"Cannot add {self} to {other} of {self.currency}")


@dataclass
class L1Price(TimeSeriesSchema):
    """
    The l1 price represents the mid and spread or bid and ask price. Along with the currency the prices represent.
    """
    mid: TS[float]
    spread: TS[float]
    currency: TS[Currency]


@dataclass
class PriceProfile(TimeSeriesSchema, Generic[SIZE]):
    """
    The price offsets represent the relative price from whichever side the profile represents, the values of prices
    must be monotonically increasing and should not include duplicates (though it may temporarily do so).
    The value in the prices are relative to the TOB of the side the price represents.
    The quantities are always positive.
    """
    price_offsets: TS[Array[float, SIZE]]
    quantities: TS[Array[float, SIZE]]


@dataclass
class L2Price(L1Price, Generic[SIZE, UNIT]):
    """
    The set of price offsets and quantities on top of the L1 price.
    """
    bid_profile: TSB[PriceProfile[SIZE]]
    ask_profile: TSB[PriceProfile[SIZE]]
    qty_unit: TS[UNIT]
