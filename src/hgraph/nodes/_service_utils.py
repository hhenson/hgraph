from collections import defaultdict
from typing import Mapping, Any, Type

from hgraph import sink_node, REF, TIME_SERIES_TYPE, GlobalState, compute_node, SCALAR, TS, STATE, Removed, \
    pull_source_node, BaseWiringNodeClass, HgREFTypeMetaData, create_input_output_builders, \
    graph, AUTO_RESOLVE, TSD, TS_OUT, REMOVE_IF_EXISTS, TIME_SERIES_TYPE_1

__all__ = ("capture_output_to_global_state", "capture_output_node_to_global_state", "write_subscription_key",
           "write_service_request", "get_shared_reference_output", "write_service_replies")

from hgraph.nodes import null_sink


@sink_node(active=tuple(), valid=tuple())
def capture_output_to_global_state(path: str, ts: REF[TIME_SERIES_TYPE]):
    """This node serves to capture the output of a service node and record the output reference in the global state."""


@capture_output_to_global_state.start
def capture_output_to_global_state_start(path: str, ts: REF[TIME_SERIES_TYPE]):
    """Place the reference into the global state"""
    GlobalState.instance()[path] = ts.value


@capture_output_to_global_state.stop
def capture_output_to_global_state_stop(path: str):
    """Clean up references"""
    del GlobalState.instance()[path]


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
    (s := _state.tracker[(v := key.value)]).add(_state.subscription_id)
    set_delta = set()
    if _state.previous_key:
        (s_old := _state.tracker[_state.previous_key]).remove(_state.subscription_id)
        if not s_old:
            set_delta.add(Removed(_state.previous_key))
    _state.previous_key = v
    if len(s) == 1:
        set_delta.add(v)
    if set_delta:
        svc_node_in.apply_value(set_delta)


@write_subscription_key.start
def write_subscription_key_start(path: str, _state: STATE):
    tracker_path = f"{path}_tracker"
    _state.subscription_id = object()
    _state.previous_key = None
    if tracker_path not in GlobalState.instance():
        GlobalState.instance()[tracker_path] = defaultdict(set)
    _state.tracker = GlobalState.instance()[tracker_path]


@write_subscription_key.stop
def write_subscription_key_stop(path: str, _state: STATE):
    if key := _state.previous_key:
        (s := _state.tracker[key]).remove(_state.subscription_id)
        if not s:
            del _state.tracker[key]
            if subs_in := GlobalState.instance().get(f"{path}/subs"):
                subs_in.apply_value(Removed(key))


@graph
def _subscribe(path: str, key: TS[SCALAR], _s_tp: type[SCALAR] = AUTO_RESOLVE,
               _ts_tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE) -> TIME_SERIES_TYPE:
    """Implement the stub for subscription"""
    write_subscription_key(path, key)
    out = get_shared_reference_output[TIME_SERIES_TYPE: TSD[_s_tp, _ts_tp]](f"{path}/out")
    return out[key]


@compute_node
def write_service_request(path: str, request: TIME_SERIES_TYPE, _output: TS_OUT[int] = None, _state: STATE = None) -> \
        TS[int]:
    """
    Updates TSDs attached to the path with the data provided.
    """
    for arg, ts in request.items():
        if ts.modified:
            svc_node_in = GlobalState.instance().get(f"{path}/request_{arg}")
            svc_node_in.apply_value({id(_state.requestor_id): ts.delta_value})

    if not _output.valid:
        return id(_state.requestor_id)


@write_service_request.start
def write_service_request_start(path: str, _state: STATE):
    _state.requestor_id = object()


@write_service_request.stop
def write_service_request_stop(request: TIME_SERIES_TYPE, path: str, _state: STATE):
    if request.valid:
        for arg, i in request.items():
            if i.valid:
                if svc_node_in := GlobalState.instance().get(f"{path}/request_{arg}"):
                    svc_node_in.apply_value({id(_state.requestor_id): REMOVE_IF_EXISTS})


@sink_node
def write_service_replies(path: str, response: TIME_SERIES_TYPE):
    """
    Updates TSDs attached to the path with the data provided.
    """
    svc_node_in = GlobalState.instance().get(f"{path}/replies_fb")
    svc_node_in.apply_value(response.delta_value)


@graph
def _request_service(path: str, request: TIME_SERIES_TYPE):
    null_sink(write_service_request(path, request))


@graph
def _request_reply_service(path: str, request: TIME_SERIES_TYPE,
                     tp_out: Type[TIME_SERIES_TYPE_1] = AUTO_RESOLVE) -> TIME_SERIES_TYPE_1:
    requestor_id = write_service_request(path, request)
    if tp_out:
        out = get_shared_reference_output[TIME_SERIES_TYPE: TSD[int, tp_out]](f"{path}/replies")
        return out[requestor_id]


class SharedReferenceNodeClass(BaseWiringNodeClass):

    def create_node_builder_instance(self, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        output_type = node_signature.time_series_output
        if type(output_type) is not HgREFTypeMetaData:
            node_signature = node_signature.copy_with(time_series_output=HgREFTypeMetaData(output_type))

        from hgraph._impl._builder import PythonNodeImplNodeBuilder
        input_builder, output_builder, error_builder = create_input_output_builders(node_signature,
                                                                                    self.error_output_type)

        from hgraph._impl._runtime._node import BaseNodeImpl

        class _PythonSharedReferenceStubSourceNode(BaseNodeImpl):

            def do_eval(self):
                """The service must be available by now, so we can retrieve the output reference."""
                from hgraph._runtime._global_state import GlobalState
                shared_output = GlobalState.instance().get(self.scalars["path"])
                if shared_output is None:
                    raise RuntimeError(f"Missing shared output for path: {self.scalars['path']}")
                # NOTE: The output needs to be a reference value output so we can set the value and continue!
                self.output.value = shared_output

            def do_start(self):
                """Make sure we get notified to serve the service output reference"""
                self.notify()

            def do_stop(self):
                """Nothing to do, but abstract so must be implemented"""

        return PythonNodeImplNodeBuilder(
            signature=node_signature,
            scalars=scalars,
            input_builder=input_builder,
            output_builder=output_builder,
            error_builder=error_builder,
            node_impl=_PythonSharedReferenceStubSourceNode
        )


@pull_source_node(node_impl=SharedReferenceNodeClass)
def get_shared_reference_output(path: str) -> REF[TIME_SERIES_TYPE]:
    """Uses the special node to extract a node from the global state."""
