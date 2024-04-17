from typing import Generic

from hgraph import TimeSeriesSchema, TS, Array, SIZE, TSB

from hg_oap.assets.currency import Currency
from hg_oap.units.unit import Unit, UNIT


class Price(TimeSeriesSchema):
    """
    A bundle schema representing the price as a float and the associated currency asset that the price is representing.
    """
    price: TS[float]
    currency: TS[Currency]


class L1Price(TimeSeriesSchema):
    mid: TS[float]
    spread: TS[float]
    currency: TS[Currency]


class PriceProfile(TimeSeriesSchema, Generic[SIZE]):
    price_offsets: TS[Array[float, SIZE]]
    quantities: TS[Array[float, SIZE]]


class L2Price(L1Price, Generic[SIZE, UNIT]):
    bid_profile: TSB[PriceProfile[SIZE]]
    ask_profile: TSB[PriceProfile[SIZE]]
    qty_unit: TS[UNIT]
