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
class Fill(CompoundScalar):
    """
    A fill for a single leg order (or a single leg of a multi-leg order).
    The fill id represent the unique identifier of this fill. In the case
    where a fill represents a collection of fills from a number of child orders, then
    the additional_ids will contain the fill ids of the child fills making up this fill.

    For example, a spread order may be split into the individual legs to trade and the
    fills bubbled up once both sides are filled, in which case the algo will select the id
    of one of the fills for the main fill_id and then add the addition fill ids to
    the additional_ids field.

    The qty represents how much was filled and the notional the total value of the fill,
    thus fill price is notional / qty.
    """
    fill_id: str
    additional_ids: tuple[str, ...]
    qty: float
    notional: float


@dataclass
class Order(TimeSeriesSchema):
    """
    The base order class schema.
    The fills time-series represent the stream of fills received on the order.
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
    fills: TS[Fill]


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
    fills: TSD[LEG_ID, TS[Fill]]
