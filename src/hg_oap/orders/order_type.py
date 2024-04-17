from dataclasses import dataclass
from hgraph import CompoundScalar

from hg_oap.instruments.instrument import Instrument
from hg_oap.quanity.price_unit import Price
from hg_oap.quanity.quantity import Quantity


@dataclass(frozen=True)
class OrderType(CompoundScalar):
    """Marker class to represent an order type"""


@dataclass(frozen=True)
class SingleLegOrderType(OrderType):
    instrument: Instrument
    quantity: Quantity


@dataclass(frozen=True)
class LimitOrderType(SingleLegOrderType):
    price: Price


@dataclass(frozen=True)
class MarketOrderType(SingleLegOrderType):
    pass
