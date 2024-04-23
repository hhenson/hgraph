from typing import Any

from frozendict import frozendict
from hgraph import request_reply_service, reference_service, TSD, TS, CompoundScalar, \
    TimeSeriesSchema

from hg_oap.orders.order import SingleLegOrder, OriginatorInfo
from hg_oap.orders.order_type import OrderType


class SingleLegOrders(TimeSeriesSchema):
    requested: TSD[str, SingleLegOrder]
    confirmed: TSD[str, SingleLegOrder]


@reference_service
def single_leg_orders(path: str = None) -> SingleLegOrders:
    """
    Subscribe to the order given an order id. Once an id has been created, the order can be subscribed to.
    """


class OrderRequest(CompoundScalar):
    order_id: str
    version: int

    # The below is used for sequence validation
    user_id: str  # Who is updating
    prev_version: int  # The previous version
    prev_user_id: str  # The previous user id


class CreateOrderRequest(OrderRequest):
    order_type: OrderType
    originator_info: OriginatorInfo


class AmendOrderRequest(OrderRequest):
    order_type_details: frozendict[str, Any]
    originator_info_details: frozendict[str, Any]


class SuspendOrderRequest(OrderRequest):
    suspension_key: str


class ResumeOrderRequest(OrderRequest):
    suspension_key: str


class CancelOrderRequest(OrderRequest):
    """
    Cancel an order, use force=True to cancel an order without waiting for
    a response when the order has children or validation logic.
    """
    reason: str
    force: bool = False


class OrderResponse(CompoundScalar):
    order_id: str
    version: int
    original_request: OrderRequest


class OrderAcceptResponse(OrderResponse):
    """Indicates the request was accepted"""


class OrderReject(OrderResponse):
    """Indicates the request was rejected, the reason is also provided"""
    reason: str


@request_reply_service
def single_leg_order_client(path: str, request: TS[OrderRequest]) -> TS[OrderResponse]:
    ...


def single_leg_order_handler(fn, *, path: str = None):
    """
    Wraps a graph / compute_node that is designed to process an individual
    single leg order request. The handler takes the form:

    @single_leg_order_handler(path="MyOrderEndPoint")
    @graph
    def my_order_handler(
        request: TS[OrderRequest],
        requested_order: TS[OrderRequest],
        confirmed_order: TSB[SingleLegOrder]
    ) -> TS[OrderResponse]:
        ...

    If the handler is designed to handle multiple orders, the other options
    is to provide a signature as below, in this form all order requests destined
    for this end-point will be provided.

    @single_leg_order_handler(path="MyOrderEndPoint")
    @graph
    def my_order_handler(
        request: TS[tuple[OrderRequest,...]],
        requested_order: TSD[str, TSB[SingleLegOrder]],
        confirmed_order: TSD[str, TSB[SingleLegOrder]]
    ) -> TS[tuple[OrderResponse,...]]:
        ...

    """

