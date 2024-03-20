from dataclasses import field, dataclass
from typing import Callable, Mapping, Any, Sequence, TypeVar, Optional, Dict

from frozendict import frozendict

from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
from hgraph._wiring._wiring_node_class._pull_source_node_class import last_value_source_node
from hgraph._types._tss_meta_data import HgTSSTypeMetaData
from hgraph._builder._graph_builder import GraphBuilder
from hgraph._runtime._global_state import GlobalState
from hgraph._types._scalar_type_meta_data import HgAtomicType, HgObjectType
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._graph_builder import create_graph_builder
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError, WiringError
from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext
from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass, \
    BaseWiringNodeClass
from hgraph._wiring._wiring_node_instance import create_wiring_node_instance, WiringNodeInstanceContext
from hgraph._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType

__all__ = ("ServiceImplNodeClass",)

from hgraph._wiring._wiring_port import _wiring_port_for

from hgraph.nodes._service_utils import capture_output_node_to_global_state


class ServiceImplNodeClass(BaseWiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable, interfaces=None):
        # save original inputs
        self._original_signature = signature
        # Add "path" to the scalar signature
        # and remove time-series inputs
        time_series_args = signature.time_series_args
        super().__init__(
            signature.copy_with(
                args=('path',) + tuple(arg for arg in signature.args if arg not in time_series_args),

                input_types=frozendict(
                    {k: v for k, v in (signature.input_types | {"path": HgAtomicType.parse_type(str)}).items() if
                     k not in time_series_args}),
                time_series_args=tuple(),
            ), fn)
        if interfaces is None:
            raise CustomMessageWiringError("No interfaces provided")

        if not isinstance(interfaces, (tuple, list, set)):
            interfaces = (interfaces,)
        self.interfaces = tuple(interfaces)

        # Ensure the service impl signature is valid given the signature definitions of the interfaces.
        validate_signature_vs_interfaces(signature, fn, interfaces)

    def _validate_service_not_already_bound(self, path: str | None):
        paths = [interface.full_path(path) for interface in self.interfaces]
        gs = GlobalState.instance()
        if any(p in gs for p in paths):
            raise CustomMessageWiringError(
                f"This path: '{path}' has already been registered for this service implementation")
        # Reserve the paths now
        for p in paths:
            gs[p] = self  # use this as a placeholder until we have built the node

    def __call__(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None,
                 **kwargs) -> "WiringPort":

        with WiringContext(current_wiring_node=self, current_signature=self._original_signature):
            path = kwargs.get("path")
            self._validate_service_not_already_bound(path)
            path = "" if path is None else path
            kwargs["path"] = path

            # TODO: This is only going to resolve scalars or output values, we need to
            # take a look at resolving the actual signature if there are pre-resolved-types.
            kwargs_, resolved_signature = self._validate_and_resolve_signature(
                *args,
                __pre_resolved_types__=__pre_resolved_types__,
                **kwargs)

            resolved_signature = resolved_signature.copy_with(input_types=frozendict({
                        **resolved_signature.input_types,
                        'inner_graph': HgObjectType.parse_type(object)}))

            with WiringContext(current_wiring_node=self, current_signature=self.signature):
                inner_graph = create_inner_graph(self._original_signature, self.fn, kwargs_, self.interfaces)
                kwargs_['inner_graph'] = inner_graph

            # We pass in rank of -1 because service implementations are ranked at the end of the graph build
            wiring_node_instance = create_wiring_node_instance(self, resolved_signature,
                                                               frozendict(kwargs_), rank=-1)
            from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext
            WiringGraphContext.instance().add_built_service_impl(path, wiring_node_instance)

    def create_node_builder_instance(self, node_signature: "NodeSignature",
                                     scalars: Dict[str, Any]) -> "NodeBuilder":
        # The service impl node should only take scalar values in. The rest will be a
        # graph where we will stub out the inputs and outputs.
        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            from hgraph._impl._builder._service_impl_builder import PythonServiceImplNodeBuilder
            inner_graph = scalars['inner_graph']
            return PythonServiceImplNodeBuilder(node_signature, scalars, None, None, None, inner_graph)

    def __eq__(self, other):
        return super().__eq__(other) and self.interfaces == other.interfaces

    def __hash__(self):
        return super().__hash__() ^ hash(self.interfaces)


def validate_signature_vs_interfaces(signature: WiringNodeSignature, fn: Callable,
                                     interfaces: Sequence[WiringNodeClass]) -> WiringNodeSignature:
    """
    The final signature of a service is no inputs and a reference output.

    All services act like a pull source node. Some services do consume inputs, but these will be mapped into the
    service graph in a magical way to ensure the values are copied in (since they will be created at a random point
    in the graph and be fed into a sink node.)
    """

    if len(interfaces) == 1:
        # The signature for the service should be representative of the singular service, i.e. for a reference
        # service the signature is the same as the reference service, for the subscription service the singature
        # takes a single input of type TSS[SUBSCRIPTION] and returns a TSD[SUBSCRIPTION, TIME_SERIES_TYPE]
        # for a request reply service we should have a TSD[RequestorId, TS[REQUEST]] and returns a
        # TSD[RequestorId, TIME_SERIES_TYPE].
        interface_sig: WiringNodeSignature = interfaces[0].signature
        match interface_sig.node_type:
            case WiringNodeType.REF_SVC:
                if signature.time_series_args:
                    raise CustomMessageWiringError("The signature cannot have any time-series inputs")
                if not signature.output_type.dereference().matches(interface_sig.output_type.dereference()):
                    raise CustomMessageWiringError(
                        "The output type does not match that of the reference service signature")
                return signature
            case WiringNodeType.SUBS_SVC:
                # Check the input time-series type is a TSS[SCALAR] of the TS[SCALAR] of the service.
                if len(signature.time_series_args) != 1:
                    raise CustomMessageWiringError("The signature can only have one time-series input")
                ts_type: HgTSSTypeMetaData = signature.input_types.get(arg := next(iter(signature.time_series_args)))
                if type(ts_type) is not HgTSSTypeMetaData:
                    raise CustomMessageWiringError("The implementation signature input must be a TSS")
                if not ts_type.value_scalar_tp.matches(
                        (ts_int_type := next(iter(interface_sig.time_series_inputs.values()))).value_scalar_tp):
                    raise CustomMessageWiringError(
                        f"The implementation input {ts_type} scalar value does not match: {ts_int_type}")
                if not signature.output_type.dereference().value_tp.matches(interface_sig.output_type.dereference()):
                    raise CustomMessageWiringError(
                        "The output type does not match that of the subscription service signature")
            case WiringNodeType.REQ_REP_SVC:
                if len(signature.time_series_args) != 1:
                    raise CustomMessageWiringError("The signature can only have one time-series input")
                ts_type: HgTSDTypeMetaData = signature.input_types.get(arg := next(iter(signature.time_series_args)))
                if not ts_type.value_tp.matches(
                        (ts_int_type := next(iter(interface_sig.time_series_inputs.values())))):
                    raise CustomMessageWiringError(
                        f"The implementation input {ts_type} type value does not match: {ts_int_type}")
                if not signature.output_type.dereference().value_tp.matches(interface_sig.output_type.dereference()):
                    raise CustomMessageWiringError(
                        "The output type does not match that of the subscription service signature")
            case _:
                raise CustomMessageWiringError(f"Unknown service type: {interface_sig.node_type}")
    else:
        raise CustomMessageWiringError("Unable to handle multiple interfaces yet")


def create_inner_graph(wiring_signature: WiringNodeSignature, fn: Callable, scalars: Mapping[str, Any],
                       interfaces: list[WiringNodeSignature]) -> GraphBuilder:
    if len(interfaces) == 1:
        s: WiringNodeSignature = interfaces[0].signature
        match s.node_type:
            case WiringNodeType.REF_SVC:
                return wire_reference_data_service(wiring_signature, fn, scalars, interfaces[0])
            case WiringNodeType.SUBS_SVC:
                return wire_subscription_service(wiring_signature, fn, scalars, interfaces[0])
            case WiringNodeType.REQ_REP_SVC:
                return wire_request_reply_service(wiring_signature, fn, scalars, interfaces[0])
            case _:
                raise CustomMessageWiringError(f"Unknown service type: {s.node_type}")


def wire_subscription_service(wiring_signature: WiringNodeSignature, fn: Callable, scalars: Mapping[str, Any],
                              interface):
    path = (scalars := dict(scalars)).pop("path")
    path = interface.full_path(path if path else None)

    from hgraph._wiring._decorators import graph
    from hgraph.nodes._service_utils import capture_output_to_global_state

    @graph
    def subscription_service():
        # Call the implementation graph with the scalars provided
        sn_arg = next(iter(wiring_signature.time_series_args))
        subscriptions = last_value_source_node(f"{wiring_signature.name}_{sn_arg}",
                                               (tp_ := wiring_signature.input_types[sn_arg]))
        subscriptions = _wiring_port_for(tp_, subscriptions, tuple())
        capture_output_node_to_global_state(f"{path}_subs", subscriptions)
        out = fn(**{sn_arg: subscriptions} | scalars)
        capture_output_to_global_state(f"{path}_out", out)

    with WiringNodeInstanceContext(), WiringGraphContext(wiring_signature) as context:
        subscription_service()
        sink_nodes = context.pop_sink_nodes()
        return create_graph_builder(sink_nodes, False)


def wire_request_reply_service(wiring_signature: WiringNodeSignature, fn: Callable, scalars: Mapping[str, Any],
                              interface):
    path = (scalars := dict(scalars)).pop("path")
    path = interface.full_path(path if path else None)

    from hgraph._wiring._decorators import graph
    from hgraph.nodes._service_utils import capture_output_to_global_state

    @graph
    def request_reply_service():
        # Call the implementation graph with the scalars provided
        req_arg = next(iter(wiring_signature.time_series_args))
        requests = last_value_source_node(f"{wiring_signature.name}_{req_arg}",
                                               (tp_ := wiring_signature.input_types[req_arg]))
        requests = _wiring_port_for(tp_, requests, tuple())
        capture_output_node_to_global_state(f"{path}_requests", requests)
        out = fn(**{req_arg: requests} | scalars)
        capture_output_to_global_state(f"{path}_replies", out)

    with WiringNodeInstanceContext(), WiringGraphContext(wiring_signature) as context:
        request_reply_service()
        sink_nodes = context.pop_sink_nodes()
        return create_graph_builder(sink_nodes, False)


def wire_reference_data_service(
        wiring_signature: WiringNodeSignature,
        fn: Callable,
        scalars: Mapping[str, Any],
        interface) -> GraphBuilder:
    # The path was added to the scalars when initially wired to create the wiring node instance,
    # now we pop it off so that we can make use of both the scalars and the path.
    path = (scalars := dict(scalars)).pop("path")
    path = interface.full_path(path if path else None)

    from hgraph._wiring._decorators import graph
    from hgraph.nodes._service_utils import capture_output_to_global_state

    @graph
    def ref_svc_inner_graph():
        # Call the implementation graph with the scalars provided
        out = fn(**scalars)
        capture_output_to_global_state(path, out)

    with WiringNodeInstanceContext(), WiringGraphContext(wiring_signature) as context:
        ref_svc_inner_graph()
        sink_nodes = context.pop_sink_nodes()
        return create_graph_builder(sink_nodes, False)
