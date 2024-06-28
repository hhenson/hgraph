from dataclasses import dataclass
from typing import TypeVar, Any

from frozendict import frozendict
from hgraph import CompoundScalar, TSB

from hg_oap.orders.order import ORDER, OriginatorInfo, Fill
from hg_oap.orders.order_type import OrderType

__all__ = ("ORDER_REQUEST",
           "OrderRequest",
           "CreateOrderRequest",
           "AmendOrderRequest",
           "SuspendOrderRequest",
           "ResumeOrderRequest",
           "CancelOrderRequest",
           "OrderResponse",
           "OrderAcceptResponse",
           "OrderReject",
           "OrderEvent",
           "FillEvent",
           "UnsolicitedCancelEvent",
           "UnsolicitedSuspendEvent",
           "UnsolicitedResumeEvent",
           "FinishEvent"
           )


ORDER_REQUEST = TypeVar('ORDER_REQUEST', bound="OrderRequest")


@dataclass(frozen=True)
class OrderRequest(CompoundScalar):
    order_id: str
    version: int

    # The below is used for sequence validation
    user_id: str  # Who is updating
    prev_version: int  # The previous version
    prev_user_id: str  # The previous user id

    @staticmethod
    def create_request(
            tp: type[ORDER_REQUEST],
            current_request: TSB[ORDER] | None,
            user_id: str,
            **kwargs
    ) -> ORDER_REQUEST:
        current_request = None if current_request is None else current_request.value
        order_id = kwargs.pop('order_id') if current_request is None else current_request.order_id
        prev_version = -1 if current_request is None else current_request.version
        prev_user_id = "" if current_request is None else current_request.user_id
        return tp(
            order_id=order_id,
            version=prev_version + 1,
            user_id=user_id,
            prev_version=prev_version,
            prev_user_id=prev_user_id,
            **kwargs
        )


@dataclass(frozen=True)
class CreateOrderRequest(OrderRequest):
    """
    Request a new order to be created.
    Not the order id needs to be universally unique to the order management environment.
    """
    order_type: OrderType
    originator_info: OriginatorInfo


@dataclass(frozen=True)
class AmendOrderRequest(OrderRequest):
    """
    Amends the state of an order with the details provided. Not all amendments are allowed, those which are depend
    on the orders' implementation.
    """
    order_type_details: frozendict[str, Any]
    originator_info_details: frozendict[str, Any]


@dataclass(frozen=True)
class SuspendOrderRequest(OrderRequest):
    """
    Place a named suspension on the order, this adds the suspension key to a set of suspension keys.
    If this is the first suspension key added to the order, the order is marked as suspended.
    """
    suspension_key: str


@dataclass(frozen=True)
class ResumeOrderRequest(OrderRequest):
    """
    Send a request to release a suspension key, if all keys are released, then the order will be resumed.
    """
    suspension_key: str


@dataclass(frozen=True)
class CancelOrderRequest(OrderRequest):
    """
    Cancel an order, use force=True to cancel an order without waiting for
    a response from the orders' children / ignore any validation logic.
    """
    reason: str
    force: bool = False


@dataclass(frozen=True)
class OrderResponse(CompoundScalar):
    order_id: str
    version: int
    original_request: OrderRequest

    @staticmethod
    def accept(request: OrderRequest) -> "OrderAcceptResponse":
        """Create an 'accept' message"""
        return OrderAcceptResponse(
            order_id=request.order_id,
            version=request.version,
            original_request=request
        )

    @staticmethod
    def reject(request: OrderRequest, reason: str) -> "OrderRejectResponse":
        """Create a 'reject' message"""
        return OrderReject(
            order_id=request.order_id,
            version=request.version,
            original_request=request,
            reason=reason
        )


@dataclass(frozen=True)
class OrderAcceptResponse(OrderResponse):
    """Indicates the request was accepted"""


@dataclass(frozen=True)
class OrderReject(OrderResponse):
    """Indicates the request was rejected, the reason is also provided"""
    reason: str


@dataclass(frozen=True)
class OrderEvent(CompoundScalar):
    """
    Order events are created by order handlers, they can interact order state and make changes that are not as a direct
    response to a request. These include Fills, UnsolicitedCancels, UnsolicitedSuspend and UnsolicitedResume events.
    """
    order_id: str

    @staticmethod
    def create_fill(confirmed: dict, fill: Fill) -> "FillEvent":
        return FillEvent(order_id=confirmed['order_id'], fill=fill)


@dataclass(frozen=True)
class FillEvent(OrderEvent):
    """
    Represents an order has received a fill.
    """
    fill: Fill


@dataclass(frozen=True)
class UnsolicitedCancelEvent(OrderEvent):
    """
    Canceled from the server side, this can happen if the exchange decides to cancel an order or a component, such
    as an order validator, cancels an order as the order becomes invalid (for example, the tenor expires).
    """
    reason: str


@dataclass(frozen=True)
class UnsolicitedSuspendEvent(OrderEvent):
    """
    The order has been suspended by an order handler, not by a user / parent order request.
    An example of this is when an order may have experienced multiple rejects trying to send a state, in which case
    the order may be marked as suspended with a key of, for example, "MULTIPLE_REJECTS". This keeps the order ready to
    be quickly resumed once the issue is resolved or terminated if the order is in an incorrect state.
    """
    key: str


@dataclass(frozen=True)
class UnsolicitedResumeEvent(OrderEvent):
    """
    This represents a change in order state to mark a particular suspension key has been reverted (resumed).
    This is not due to a user / parent order request, but was generated by a handler process.
    """
    key: str


@dataclass(frozen=True)
class FinishEvent(OrderEvent):
    """
    The event sent to indicate that no further state events will be sent regarding this order.
    The order is in a completed state without an error state.
    """
