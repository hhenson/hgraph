from dataclasses import dataclass
from typing import TypeVar, Any, cast, Generic

from hgraph import CompoundScalar, HgScalarTypeMetaData, HgScalarTypeVar

from hg_oap.instruments.instrument import Instrument
from hg_oap.pricing.price import Price
from hg_oap.units.quantity import Quantity

__all__ = (
    "OrderType",
    "SingleLegOrderType",
    "LimitOrderType",
    "MarketOrderType",
    "SINGLE_LEG_ORDER_TYPE",
    "SINGLE_LEG_ORDER_TYPE_1",
    "SINGLE_LEG_ORDER_TYPE_2",
    "MultiLegOrderType",
    "IfDone",
    "OneCancelOther",
    "IfDoneOneCancelOther",
    "is_order_type"
)


@dataclass(frozen=True)
class OrderType(CompoundScalar):
    """Marker class to represent an order type"""


@dataclass(frozen=True)
class SingleLegOrderType(OrderType):
    instrument: Instrument
    quantity: Quantity[float]


SINGLE_LEG_ORDER_TYPE = TypeVar("SINGLE_LEG_ORDER_TYPE", bound=SingleLegOrderType)
SINGLE_LEG_ORDER_TYPE_1 = TypeVar("SINGLE_LEG_ORDER_TYPE_1", bound=SingleLegOrderType)
SINGLE_LEG_ORDER_TYPE_2 = TypeVar("SINGLE_LEG_ORDER_TYPE_2", bound=SingleLegOrderType)


@dataclass(frozen=True)
class LimitOrderType(SingleLegOrderType):
    price: Price


@dataclass(frozen=True)
class MarketOrderType(SingleLegOrderType):
    pass


@dataclass(frozen=True)
class MultiLegOrderType(OrderType):
    """ Marker class for orders with multiple legs"""

    @property
    def leg_ids(self) -> tuple[str, ...]:
        """The leg ids of this order type instance"""
        return tuple(k for k, v in self._schema_items() if is_order_type(v))


@dataclass(frozen=True)
class IfDone(MultiLegOrderType, Generic[SINGLE_LEG_ORDER_TYPE_1, SINGLE_LEG_ORDER_TYPE_2]):
    """
    The 'if_' leg is processed first, if the 'if_' completed, the 'done' leg is then placed.
    """
    if_: SINGLE_LEG_ORDER_TYPE_1
    done: SINGLE_LEG_ORDER_TYPE_2


@dataclass(frozen=True)
class OneCancelOther(MultiLegOrderType, Generic[SINGLE_LEG_ORDER_TYPE_1, SINGLE_LEG_ORDER_TYPE_2]):
    """
    Both 'one' and 'other' are attempted to execute, then if either is traded, the other is cancelled.
    If the leg order supports partial fills, then fills for each leg are considered as reducing the open quantity
    of both legs. For example:
    Both orders have 10 lots,
    one gets filled with 2 lots, then other gets reduced to 8 lots on offer.

    This order type runs the risk of overfilling, so it is useful to present orders with the expectation of overfill.
    """
    one: SINGLE_LEG_ORDER_TYPE_1
    other: SINGLE_LEG_ORDER_TYPE_2


@dataclass(frozen=True)
class IfDoneOneCancelOther(MultiLegOrderType, Generic[SINGLE_LEG_ORDER_TYPE_1, SINGLE_LEG_ORDER_TYPE_2]):
    """
    As with IfDone, the 'if_' is processed first, one completion, the 'done_one' and 'done_other' are placed using the
    logic of OneCancelOther.
    """
    if_: SINGLE_LEG_ORDER_TYPE_1
    done_one: SINGLE_LEG_ORDER_TYPE_1
    done_other: SINGLE_LEG_ORDER_TYPE_2


def is_order_type(v: Any) -> bool:
    """Indicates if the value provide represents an order type or not."""
    if isinstance(v, type):
        return issubclass(v, OrderType)
    elif isinstance(v, HgScalarTypeMetaData):
        if type(v) is HgScalarTypeVar:
            return (b := cast(TypeVar, v.py_type).__bound__) and issubclass(b, OrderType)
    else:
        return isinstance(v, OrderType)


