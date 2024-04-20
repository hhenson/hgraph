from dataclasses import dataclass
from typing import Generic

from hgraph import TimeSeriesSchema, TS, TSS, CompoundScalar, TSD, TSB

from hg_oap.orders.order_type import OrderType, MultiLegOrderType, SingleLegOrderType
from hg_oap.pricing.price import Price
from hg_oap.quanity.quantity import Quantity, UNIT


@dataclass
class OriginatorInfo(CompoundScalar):
    account: str


@dataclass
class Order(TimeSeriesSchema):
    """
    The base order class schema.
    """
    order_id: TS[str]
    order_type: TS[OrderType]
    originator_info: TS[OriginatorInfo]
    is_done: TS[bool]
    suspension_keys: TSS[str]
    is_suspended: TS[bool]


@dataclass
class SingleLegOrder(Order, Generic[UNIT]):
    """
    Orders that operate on a single leg. These orders deal with a single instrument and a quantity.
    """
    order_type: TS[SingleLegOrderType]
    remaining_qty: TSB[Quantity[UNIT]]
    filled_qty: TSB[Quantity[UNIT]]
    filled_notional: TSB[Price]
    is_filled: TS[bool]


LEG_ID = str


@dataclass
class MultiLegOrder(Order, Generic[UNIT]):
    """
    Orders that operate over multiple legs. These orders operate over multiple single leg order types.
    Example of these types of orders include IfDone, OneCancelOther, etc.
    """
    order_type: TS[MultiLegOrderType]
    remaining_qty: TSD[LEG_ID, TSB[Quantity[UNIT]]]
    filled_qty: TSD[LEG_ID, TSB[Quantity[UNIT]]]
    filled_notional: TSD[LEG_ID, TSB[Price]]
    is_filled: TSD[LEG_ID, TS[bool]]
    is_leg_done: TSD[LEG_ID, TS[bool]]
