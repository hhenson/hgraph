from collections import defaultdict
from datetime import timedelta
from typing import Type

from hgraph import (
    sink_node,
    REF,
    TIME_SERIES_TYPE,
    GlobalState,
    compute_node,
    SCALAR,
    TS,
    STATE,
    Removed,
    pull_source_node,
    graph,
    AUTO_RESOLVE,
    TSD,
    TS_OUT,
    REMOVE_IF_EXISTS,
    TIME_SERIES_TYPE_1,
    null_sink,
    NODE,
    generator,
)
from hgraph._runtime._global_keys import output_key, output_subscriber_key

__all__ = (
    "capture_output_to_global_state",
    "capture_output_node_to_global_state",
    "write_subscription_key",
    "write_service_request",
    "get_shared_reference_output",
    "write_service_replies",
    "request_id",
)


@sink_node(valid=tuple())
def capture_output_to_global_state(path: str, ts: REF[TIME_SERIES_TYPE]):
    """This node serves to capture the output of a service node and record the output reference in the global state."""
    global_state = GlobalState.instance()
    k = output_key(path)
    sk = output_subscriber_key(path)
    global_state[k] = ts.value
    global_state[sk].notify(ts.last_modified_time)


@capture_output_to_global_state.start
def capture_output_to_global_state_start(path: str, ts: REF[TIME_SERIES_TYPE]):
    """Place the reference into the global state"""
    from hgraph import TimeSeriesSubscriber

    global_state = GlobalState.instance()
    k = output_key(path)
    sk = output_subscriber_key(path)
    global_state[k] = ts.value
    global_state[sk] = TimeSeriesSubscriber()


@capture_output_to_global_state.stop
def capture_output_to_global_state_stop(path: str):
    """Clean up references"""
    GlobalState.instance().pop(output_key(path))


@sink_node(active=tuple(), valid=tuple())
def capture_output_node_to_global_state(path: str, ts: TIME_SERIES_TYPE):
    """This node service to capture the node associated to the output provided into the path provided"""


@capture_output_node_to_global_state.start
def capture_output_node_to_global_state_start(path: str, ts: TIME_SERIES_TYPE):
    GlobalState.instance()[path] = ts.output.owning_node


@capture_output_node_to_global_state.stop
def capture_output_node_to_global_state_stop(path: str):
    del GlobalState.instance()[path]


@sink_node
def write_subscription_key(path: str, key: TS[SCALAR], _state: STATE = None):
    """
    Updates a TSS attached to the path with the key provided.
    This will also ensure use the associated tracker to ensure that we don't update unless required and performs
    the appropriate reference counting.
    """
    svc_node_in = GlobalState.instance().get(f"{path}/subs")

    key_value = key.value
    set_delta = set()

    if _state.previous_key is not None:
        if _state.previous_key != key_value:
            prev_subscriptions = _state.tracker[_state.previous_key]
            prev_subscriptions.discard(_state.subscription_id)
            if not prev_subscriptions:
                set_delta.add(Removed(_state.previous_key))
        else:
            return  # No change in key, no need to update

    _state.previous_key = key_value

    if key_value is not None:
        subscriptions = _state.tracker[key_value]
        subscriptions.add(_state.subscription_id)
        if len(subscriptions) == 1:
            set_delta.add(key_value)

    if set_delta:
        svc_node_in.apply_value(set_delta)


@write_subscription_key.start
def write_subscription_key_start(path: str, _state: STATE):
    tracker_path = f"{path}_tracker"
    _state.subscription_id = object()
    _state.previous_key = None
    global_state = GlobalState.instance()
    if tracker_path not in global_state:
        global_state[tracker_path] = defaultdict(set)
    _state.tracker = global_state[tracker_path]


@write_subscription_key.stop
def write_subscription_key_stop(path: str, _state: STATE):
    if (key := _state.previous_key) is not None:
        (s := _state.tracker[key]).discard(_state.subscription_id)
        if not s:
            del _state.tracker[key]
            if subs_in := GlobalState.instance().get(f"{path}/subs"):
                subs_in.apply_value({
                    Removed(key),
                })


@compute_node
def write_service_request(
    path: str, request: TIME_SERIES_TYPE, _output: TS_OUT[int] = None, _state: STATE = None
) -> TS[int]:
    """
    Updates TSDs attached to the path with the data provided.
    """
    global_state = GlobalState.instance()
    for arg, ts in request.items():
        if ts.modified:
            svc_node_in = global_state.get(f"{path}/request_{arg}")
            if svc_node_in is None:
                raise ValueError(f"request stub '{arg}' not found for service {path}")
            svc_node_in.apply_value({_state.requestor_id: ts.delta_value})

    if not _output.valid:
        return _state.requestor_id


@write_service_request.start
def write_service_request_start(path: str, _state: STATE):
    global_state = GlobalState.instance()
    i = global_state.get("request_id", id(_state)) + 1
    global_state.request_id = i
    _state.requestor_id = i


@write_service_request.stop
def write_service_request_stop(request: TIME_SERIES_TYPE, path: str, _state: STATE):
    if request.valid:
        global_state = GlobalState.instance()
        for arg, i in request.items():
            if i.valid:
                if svc_node_in := global_state.get(f"{path}/request_{arg}"):
                    svc_node_in.apply_value({_state.requestor_id: REMOVE_IF_EXISTS})


@sink_node
def write_service_replies(path: str, response: TIME_SERIES_TYPE):
    """
    Updates TSDs attached to the path with the data provided.
    """
    svc_node_in = GlobalState.instance().get(f"{path}/replies_fb")
    svc_node_in.apply_value(response.delta_value)


@graph
def _request_service(path: str, request: TIME_SERIES_TYPE):
    return null_sink(write_service_request(path, request), __return_sink_wp__=True)


@graph
def _request_reply_service(
    path: str, request: TIME_SERIES_TYPE, tp_out: Type[TIME_SERIES_TYPE_1] = AUTO_RESOLVE
) -> TIME_SERIES_TYPE_1:
    requestor_id = write_service_request(path, request)
    if tp_out:
        out = get_shared_reference_output[TIME_SERIES_TYPE : TSD[int, tp_out]](f"{path}/replies")
        return out[requestor_id]


@generator
def request_id(hash: int, _state: STATE = None) -> TS[int]:
    global_state = GlobalState.instance()
    i = global_state.get("request_id", id(_state)) + 1
    global_state.request_id = i
    yield timedelta(), i


@sink_node
def write_adaptor_request(
    path: str,
    arg: str,
    request: REF[TIME_SERIES_TYPE],
    requestor_id: TS[int] = None,
    _output: TS_OUT[int] = None,
    _state: STATE = None,
):
    """Connect the request time series to the node collecting all the requests into a TSD in the adaptor"""
    from_graph_node = GlobalState.instance().get(f"{path}/{arg}")
    if from_graph_node is None:
        raise ValueError(f"request stub '{arg}' not found for {path}")

    from_graph_node.output.get_or_create(requestor_id.value).value = request.value


@write_adaptor_request.stop
def write_adaptor_request_stop(path: str, arg: str, requestor_id: TS[int], _state: STATE):
    if from_graph_node := GlobalState.instance().get(f"{path}/{arg}"):
        if requestor_id.value in from_graph_node.output:
            del from_graph_node.output[requestor_id.value]


@pull_source_node
def adaptor_request(path: str, arg: str) -> TSD[int, REF[TIME_SERIES_TYPE]]: ...


@sink_node
def write_service_requests(path: str, request: TIME_SERIES_TYPE):
    """
    Updates TSDs attached to the path with the data provided.
    TIME_SERIES_TYPE is expected to be a bundle of TSDs (there is no generic way to represent it on the signature atm)
    """
    global_state = GlobalState.instance()
    for arg, ts in request.items():
        if ts.modified:
            svc_node_in = global_state.get(f"{path}/request_{arg}")
            if svc_node_in is None:
                raise ValueError(f"request stub '{arg}' not found for service {path}")
            svc_node_in.apply_value(ts.delta_value)


@pull_source_node
def get_shared_reference_output(
    path: str, strict: bool = True, node: NODE = None, _state: STATE = None
) -> REF[TIME_SERIES_TYPE]:
    """Uses the special node to extract a node from the global state."""
    from hgraph._runtime._global_state import GlobalState

    global_state = GlobalState.instance()
    has_shared_output = path in global_state
    if has_shared_output and _state.subscribed is False:
        global_state[f"{path}_subscriber"].subscribe(node)
        _state.subscribed = True

    if not has_shared_output and strict:
        raise RuntimeError(f"Missing shared output for path: {path}")

    return global_state.get(path)


@get_shared_reference_output.start
def get_shared_reference_output_start(path: str, node: NODE = None, _state: STATE = None):
    node.notify(node.graph.evaluation_clock.evaluation_time)
    _state.subscribed = False


@get_shared_reference_output.stop
def get_shared_reference_output_start(path: str, node: NODE = None):
    subscriber = GlobalState.instance().get(f"{path}_subscriber")
    if subscriber:
        subscriber.unsubscribe(node)
