from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, cast

from frozendict import frozendict
from hgraph import request_reply_service, TSD, TS, service_impl, feedback, TSB, \
    compute_node, map_, TSB_OUT, HgTSTypeMetaData, STATE, TimeSeriesSchema, graph, emit

from hg_oap.assets.currency import Currencies
from hg_oap.orders.order import ORDER, OrderState, SingleLegOrder, MultiLegOrder, order_states
from hg_oap.orders.order_request_response_events import OrderRequest, CreateOrderRequest, OrderResponse, OrderEvent, \
    FillEvent
from hg_oap.orders.order_type import MultiLegOrderType, SingleLegOrderType

__all__ = ("order_client", "order_handler", "OrderHandlerOutputs", "OrderHandlerOutput")


@request_reply_service
def order_client(path: str, request: TS[OrderRequest]) -> TS[OrderResponse]:
    """
    Order client allows sending order requests to the order service.
    """


@dataclass
class OrderHandlerOutput(TimeSeriesSchema):
    """Response structure from a """
    order_response: TS[OrderResponse]
    order_event: TS[OrderEvent]


@dataclass
class OrderHandlerOutputs(TimeSeriesSchema):
    order_responses: TS[tuple[OrderResponse, ...]]
    order_events: TS[tuple[OrderEvent, ...]]


def order_handler(fn):
    """
    Wraps a graph / compute_node that is designed to process an order or
    a collection of orders. The handler takes the form:

    @order_handler
    @graph
    def my_order_handler(
        request: TS[OrderRequest],
        order_state: TSB[OrderState[SingleLegOrder]]
        **kwargs
    ) -> TSB[OrderHandlerOutput]:
        ...

    If the handler is designed to handle multiple orders, the other options
    is to provide a signature as below, in this form all order requests destined
    for this end-point will be provided.

    @order_handler
    @graph
    def my_order_handler(
        request: TSD[str, tuple[OrderRequest,...]],
        order_state: TSD[str, TSB[OrderState[SingleLegOrder]]]
        **kwargs
    ) -> TSD[str, TSB[OrderHandlerOutputs]]:
        ...

    The result of this is a service impl allows for handling order requests
    providing a validated stream of order requests and the appropriate
    request and confirmed order states.

    To use this, the user would then perform a registration of the handler
    as follows:

    register_service("orders.my_end_point", my_order_handler, **my_order_handler_kwargs)

    NOTE: The order_state will tick when the response is returned, so generally this should not be in the active
          set as it will cause the code to be re-evaluated the engine cycle after the node is completed.
    """
    # determine type or order state we are looking for based on the wrapped code.
    from hgraph import PythonWiringNodeClass
    signature = cast(PythonWiringNodeClass, fn).signature
    needs_map: bool = isinstance(signature.input_types['request'], HgTSTypeMetaData)
    if needs_map:
        bundle_tp = signature.input_types['order_state']
    else:
        bundle_tp = signature.input_types['order_state'].value_tp

    order_state_tp = bundle_tp.bundle_schema_tp.meta_data_schema['requested'].bundle_schema_tp.py_type
    assert order_state_tp in (SingleLegOrder, MultiLegOrder), \
        "Expect this to be either a SingleLegOrder or MultiLegOrder"

    @service_impl(interfaces=(order_states, order_client))
    def _order_handler_impl(path: str):
        order_responses_fb = feedback(TSD[str, TSB[OrderHandlerOutputs]])

        order_client_input = order_client.wire_impl_inputs_stub(path).request
        requests = _convert_to_tsd_by_order_id(order_client_input)

        _compute_order_state = _compute_order_state_single if order_state_tp is SingleLegOrder else _compute_order_state_multi
        order_state = map_(_compute_order_state, requests, order_responses_fb())
        order_states[ORDER: order_state_tp].wire_impl_out_stub(path, order_state)

        if needs_map:
            result: TSD[str, TSB[OrderHandlerOutputs]] = \
                map_(lambda request_, order_state_: _to_tuple(fn(emit(request_), order_state_)), requests,
                     order_state)
        else:
            requests = _flatten(requests)
            result: TSD[str, TSB[OrderHandlerOutputs]] = \
                fn(requests, order_state)

        order_responses_fb(result)
        order_client_outputs = _map_response_to_request(order_client_input, result)
        order_client.wire_impl_out_stub(path, order_client_outputs)

    return _order_handler_impl


@dataclass
class MapRequestToIdSate:
    requests: dict[tuple: int] = field(default_factory=dict)


def _key_from_request(request: OrderRequest) -> tuple:
    return request.order_id, request.version, request.user_id


@compute_node(valid=("requests",))
def _map_response_to_request(
        requests: TSD[int, TS[OrderRequest]], responses: TSD[str, TSB[OrderHandlerOutputs]],
        _state: STATE[MapRequestToIdSate] = None) -> TSD[int, TS[OrderResponse]]:
    d = _state.requests
    if requests.modified:
        for key, request in requests.modified_items():
            d[_key_from_request(request.value)] = key
    if responses.modified:
        out = {}
        for output in responses.modified_values():
            if output.order_responses.modified:
                for response in output.order_responses.value:
                    request = response.original_request
                    key = d.pop(_key_from_request(request))
                    out[key] = response
        return out


@compute_node
def _to_tuple(tsb: TSB[OrderHandlerOutput]) -> TSB[OrderHandlerOutputs]:
    out = {}
    if tsb.order_response.modified:
        out['order_responses'] = (tsb.order_response.value,)

    if tsb.order_event.modified:
        out['order_events'] = (tsb.order_event.value,)

    return out


@compute_node
def _flatten(tsd: TSD[str, TS[tuple[OrderRequest, ...]]]) -> TS[tuple[OrderRequest, ...]]:
    return tuple(r for requests in tsd.modified_values() for r in requests)


@compute_node
def _convert_to_tsd_by_order_id(requests: TSD[int, TS[OrderRequest]]) -> TSD[str, TS[tuple[OrderRequest, ...]]]:
    out = defaultdict(list)
    for request in requests.modified_values():
        request = request.value
        out[request.order_id].append(request)
    return frozendict({k: tuple(v) for k, v in out.items()})


@dataclass
class PendingRequests:
    pending_requests: list[OrderRequest] = field(default_factory=list)


@graph
def _compute_order_state_single(
        requests: TS[tuple[OrderRequest, ...]],
        responses: TSB[OrderHandlerOutputs]
) -> TSB[OrderState[SingleLegOrder]]:
    out = __compute_order_state_single(requests, responses)
    confirmed = out.confirmed
    requested: TSB[SingleLegOrder] = out.requested
    requested = requested.copy_with(
        remaining_qty=confirmed.remaining_qty,
        filled_qty=confirmed.filled_qty,
        filled_notional=confirmed.filled_notional,
        is_filled=confirmed.is_filled,
        fills=confirmed.fills
    )
    return TSB[OrderState[SingleLegOrder]].from_ts(requested=requested, confirmed=confirmed)


@compute_node(valid=("requests",))
def __compute_order_state_single(
        requests: TS[tuple[OrderRequest, ...]],
        responses: TSB[OrderHandlerOutputs],
        _state: STATE[PendingRequests] = None,
        _output: TSB_OUT[OrderState[ORDER]] = None
) -> TSB[OrderState[SingleLegOrder]]:
    out_confirmed = {}
    out_requested = {}
    confirmed = _output.confirmed.value
    requested = _output.requested.value

    if responses.modified:
        if responses.order_responses.modified:
            order_responses = responses.order_responses.value
            order_responses = {response.version: response for response in order_responses}
            _state.pending_requests = [request for request in _state.pending_requests if
                                       request.version not in responses]
            # Apply responses to confirmed state
            for response in order_responses.values():
                confirmed, delta = apply_confirmation(confirmed, response)
                out_confirmed.update(delta)

            requested = confirmed
            out_requested = requested
            for request in _state.pending_requests:
                requested, delta = apply_requested_single_leg(requested, request)
                out_requested.update(delta)

        if responses.order_events.modified:
            order_events = responses.order_events.value
            for order_event in order_events:
                confirmed, delta = apply_event_single_leg(confirmed, order_event)
                out_confirmed.update(delta)

    if requests.modified:
        _state.pending_requests.extend(requests.value)
        for request in requests.value:
            requested, delta = apply_requested_single_leg(requested, request)
            out_requested.update(delta)

    return {"requested": out_requested, "confirmed": out_confirmed}


def apply_event_single_leg(confirmed: dict, event: OrderEvent) -> tuple[dict, dict]:
    out = {}
    if isinstance(event, FillEvent):
        out['fills'] = event.fill
        out['filled_qty'] = confirmed['filled_qty'] + event.fill.qty
        out['remaining_qty'] = (remaining_qty := confirmed['remaining_qty'] - event.fill.qty)
        out['filled_notional'] = confirmed['filled_notional'] + event.fill.notional
        out['is_filled'] = bool(remaining_qty.qty <= 0.0)

    confirmed.update(confirmed)
    return confirmed, out


def apply_confirmation(confirmed: dict, response: OrderResponse) -> tuple[Any, dict]:
    request = response.original_request
    if isinstance(request, CreateOrderRequest):
        order_type: SingleLegOrderType = request.order_type
        v = dict(
            order_id=request.order_id,
            order_version=request.version,
            last_updated_by=request.user_id,
            order_type=request.order_type,
            originator_info=request.originator_info,
            is_complete=False,
            suspension_keys=frozenset(),
            is_suspended=False,
            remaining_qty=order_type.quantity,
            filled_qty=dict(qty=0.0, unit=order_type.quantity.unit),
            filled_notional=dict(price=0.0, currency=Currencies.USD.value),
            is_filled=False,
        )
        return v, v


def apply_requested_single_leg(requested: dict, request: OrderRequest) -> tuple[Any, dict]:
    if isinstance(request, CreateOrderRequest):
        order_type: SingleLegOrderType = request.order_type
        v = dict(
            order_id=request.order_id,
            order_version=request.version,
            last_updated_by=request.user_id,
            order_type=request.order_type,
            originator_info=request.originator_info,
            is_complete=False,
            suspension_keys=frozenset(),
            is_suspended=False,
            remaining_qty=order_type.quantity,
            filled_qty=dict(qty=0.0, unit=order_type.quantity.unit),
            filled_notional=dict(price=0.0, currency=Currencies.USD.value),
            is_filled=False,
        )
        return v, v


@compute_node
def _compute_order_state_multi(
        requests: TS[tuple[OrderRequest, ...]],
        responses: TSB[OrderHandlerOutputs],
        _state: STATE[PendingRequests] = None,
        _output: TSB_OUT[OrderState[ORDER]] = None
) -> TSB[OrderState[MultiLegOrderType]]:
    ...
