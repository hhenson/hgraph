from typing import Generic

from hgraph import TimeSeriesSchema, TS, Array, SIZE, TSB

from hg_oap.assets.currency import Currency
from hg_oap.units.unit import UNIT


class Price(TimeSeriesSchema):
    """
    A bundle schema representing the price as a float and the associated currency asset that the price is representing.
    """
    price: TS[float]
    currency: TS[Currency]


class L1Price(TimeSeriesSchema):
    """
    The l1 price represents the mid and spread or bid and ask price. Along with the currency the prices represent.
    """
    mid: TS[float]
    spread: TS[float]
    currency: TS[Currency]


class PriceProfile(TimeSeriesSchema, Generic[SIZE]):
    """
    The price offsets represent the relative price from whichever side the profile represents, the values of prices
    must be monotonically increasing and should not include duplicates (though it may temporarily do so).
    The value in the prices are relative to the TOB of the side the price represents.
    The quantities are always positive.
    """
    price_offsets: TS[Array[float, SIZE]]
    quantities: TS[Array[float, SIZE]]


class L2Price(L1Price, Generic[SIZE, UNIT]):
    """
    The set of price offsets and quantities on top of the L1 price.
    """
    bid_profile: TSB[PriceProfile[SIZE]]
    ask_profile: TSB[PriceProfile[SIZE]]
    qty_unit: TS[UNIT]
