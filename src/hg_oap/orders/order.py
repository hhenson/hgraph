from dataclasses import dataclass
from typing import Generic, TypeVar

from hgraph import TimeSeriesSchema, TS, TSS, CompoundScalar, TSD, TSB, reference_service

from hg_oap.orders.order_type import OrderType, MultiLegOrderType, SingleLegOrderType
from hg_oap.pricing.price import Price
from hg_oap.units.quantity import Quantity

__all__ = (
    'ORDER', 'LEG_ID', 'OriginatorInfo', 'Fill', 'Order', 'SingleLegOrder', 'MultiLegOrder', 'OrderState',
    'order_states')


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

    For example: a spread order may be split into the individual legs to trade and the
    fills bubbled up once both sides are filled, in which case the algo will select the id
    of one of the fills for the main fill_id and then add the addition fill ids to
    the additional_ids field.

    The qty represents how much was filled and the notional the total value of the fill,
    thus fill price is notional / qty.
    """
    fill_id: str
    qty: Quantity[float]
    notional: Price
    additional_ids: tuple[str, ...] = tuple()


@dataclass
class Order(TimeSeriesSchema):
    """
    The base order class schema.
    """
    order_id: TS[str]
    order_version: TS[int]
    last_updated_by: TS[str]  # The client id who last updated the order
    order_type: TS[OrderType]
    originator_info: TS[OriginatorInfo]
    suspension_keys: TSS[str]
    is_suspended: TS[bool]
    is_complete: TS[bool]


@dataclass
class SingleLegOrder(Order):
    """
    Orders that operate on a single leg. These orders deal with a single instrument and a quantity.
    The ``fills`` time-series represent the stream of fills received on the order. It does not
    provide the historical state of all received fills.
    """
    order_type: TS[SingleLegOrderType]
    remaining_qty: TSB[Quantity[float]]
    filled_qty: TSB[Quantity[float]]
    filled_notional: TSB[Price]
    is_filled: TS[bool]
    fills: TS[Fill]


LEG_ID = str


@dataclass
class MultiLegOrder(Order):
    """
    Orders that operate over multiple legs. These orders operate over multiple single leg order types.
    Examples include IfDone, OneCancelOther, etc.
    """
    order_type: TS[MultiLegOrderType]
    remaining_qty: TSD[LEG_ID, TSB[Quantity[float]]]
    filled_qty: TSD[LEG_ID, TSB[Quantity[float]]]
    filled_notional: TSD[LEG_ID, TSB[Price]]
    is_filled: TSD[LEG_ID, TS[bool]]
    fills: TSD[LEG_ID, TS[Fill]]
    is_leg_complete: TSD[LEG_ID, TS[bool]]


ORDER = TypeVar("ORDER", SingleLegOrder, MultiLegOrder)


@dataclass
class OrderState(TimeSeriesSchema, Generic[ORDER]):
    """
    The order state is the couple of requested and confirmed order state.
    The requested state the order as requested by the initiator of an order.
    The confirmed state is that which has been confirmed by the implementor
    of the order. These should ultimately converge.
    The requested state is effectively the confirmed state plus the
    effect of the pending requests.
    """
    requested: TSB[ORDER]
    confirmed: TSB[ORDER]


@reference_service
def order_states(path: str = None) -> TSD[str, TSB[OrderState[ORDER]]]:
    """
    The order states associated to an order end-point.
    """
