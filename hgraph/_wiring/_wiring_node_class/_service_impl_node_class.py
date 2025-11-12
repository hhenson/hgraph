from typing import Callable, Mapping, Any, Sequence, TypeVar, Dict

from frozendict import frozendict

from hgraph._builder._graph_builder import GraphBuilder
from hgraph._types._scalar_type_meta_data import HgAtomicType, HgObjectType
from hgraph._types._tss_meta_data import HgTSSTypeMetaData
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._decorators import graph
from hgraph._wiring._graph_builder import create_graph_builder
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext
from hgraph._wiring._wiring_node_class._pull_source_node_class import last_value_source_node
from hgraph._wiring._wiring_node_class._wiring_node_class import (
    WiringNodeClass,
    BaseWiringNodeClass,
    validate_and_resolve_signature,
)
from hgraph._wiring._wiring_node_instance import create_wiring_node_instance, WiringNodeInstanceContext
from hgraph._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType
from hgraph._wiring._wiring_port import _wiring_port_for

__all__ = ("ServiceImplNodeClass",)


class ServiceImplNodeClass(BaseWiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable, interfaces=None):
        # save original inputs
        self._original_signature = signature
        # Add "path" to the scalar signature
        # and remove time-series inputs
        time_series_args = signature.time_series_args
        has_path = "path" in signature.args
        super().__init__(
            signature.copy_with(
                args=(("path",) if not has_path else ())
                + tuple(arg for arg in signature.args if arg not in time_series_args),
                input_types=frozendict({
                    k: v
                    for k, v in (
                        signature.input_types | ({"path": HgAtomicType.parse_type(str)} if not has_path else {})
                    ).items()
                    if k not in time_series_args
                }),
                time_series_args=tuple(),
            ),
            fn,
        )
        if interfaces is None:
            raise CustomMessageWiringError("No interfaces provided")

        if not isinstance(interfaces, (tuple, list, set)):
            interfaces = (interfaces,)
        self.interfaces = tuple(interfaces)

        # Ensure the service impl signature is valid given the signature definitions of the interfaces.
        validate_signature_vs_interfaces(signature, fn, interfaces)

    def _validate_service_not_already_bound(
        self, path: str | None, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None
    ):
        if WiringGraphContext.instance().is_service_built(path, __pre_resolved_types__):
            raise CustomMessageWiringError(
                f"This path: '{path}' has already been registered for this service implementation"
            )

    def __call__(
        self,
        *args,
        __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None,
        __interface__: WiringNodeSignature = None,
        **kwargs,
    ) -> "WiringPort":
        with WiringContext(current_wiring_node=self, current_signature=self._original_signature):
            path = kwargs.get("path")
            path_types = __pre_resolved_types__

            if __interface__ is None:
                __interface__ = self.interfaces[0]

            if not __interface__.is_full_path(path):
                full_path = __interface__.full_path(path)
            else:
                full_path = path
                path = __interface__.path_from_full_path(full_path)

            self._validate_service_not_already_bound(full_path, __pre_resolved_types__)
            kwargs["path"] = path

            # TODO: This is only going to resolve scalars or output values, we need to
            # take a look at resolving the actual signature if there are pre-resolved-types.
            kwargs_, resolved_signature, resolution_dict = validate_and_resolve_signature(
                self.signature, *args, __pre_resolved_types__=__pre_resolved_types__, **kwargs
            )

            resolved_signature = resolved_signature.copy_with(
                input_types=frozendict(
                    {**resolved_signature.input_types, "inner_graph": HgObjectType.parse_type(object)}
                ),
                has_nested_graphs=True,
            )

            with WiringContext(current_wiring_node=self, current_signature=self.signature):
                inner_graph, ri, paths = create_inner_graph(
                    self._original_signature, self.fn, kwargs_, self.interfaces, resolution_dict, path_types
                )
                kwargs_["inner_graph"] = inner_graph

            wiring_node_instance = create_wiring_node_instance(
                self,
                resolved_signature,
                frozendict(kwargs_),
            )

            from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext

            WiringGraphContext.instance().reassign_items(ri, wiring_node_instance)
            for p in paths:
                WiringGraphContext.instance().add_built_service_impl(p, wiring_node_instance)

    def create_node_builder_instance(
        self, resolved_wiring_signature: "WiringNodeSignature", node_signature: "NodeSignature", scalars: Dict[str, Any]
    ) -> "NodeBuilder":
        # The service impl node should only take scalar values in. The rest will be a
        # graph where we will stub out the inputs and outputs.
        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            if ServiceImplNodeClass.BUILDER_CLASS is None:
                from hgraph._impl._builder._service_impl_builder import PythonServiceImplNodeBuilder

                ServiceImplNodeClass.BUILDER_CLASS = PythonServiceImplNodeBuilder

            inner_graph = scalars["inner_graph"]
            return ServiceImplNodeClass.BUILDER_CLASS(
                signature=node_signature,
                scalars=scalars,
                input_builder=None,
                output_builder=None,
                error_builder=None,
                recordable_state_builder=None,
                nested_graph=inner_graph,
            )

    def __eq__(self, other):
        return super().__eq__(other) and self.interfaces == other.interfaces

    def __hash__(self):
        return super().__hash__() ^ hash(self.interfaces)


def validate_signature_vs_interfaces(
    signature: WiringNodeSignature, fn: Callable, interfaces: Sequence[WiringNodeClass]
) -> WiringNodeSignature:
    """
    The final signature of a service is no inputs and a reference output.

    All services act like a pull source node. Some services do consume inputs, but these will be mapped into the
    service graph in a magical way to ensure the values are copied in (since they will be created at a random point
    in the graph and be fed into a sink node.)
    """

    if len(interfaces) == 1:
        # The signature for the service should be representative of the singular service, i.e. for a reference
        # service the signature is the same as the reference service, for the subscription service the signature
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
                        "The output type does not match that of the reference service signature"
                    )
                return signature
            case WiringNodeType.SUBS_SVC:
                # Check the input time-series type is a TSS[SCALAR] of the TS[SCALAR] of the service.
                if len(signature.time_series_args) != 1:
                    raise CustomMessageWiringError("The signature can only have one time-series input")
                ts_type: HgTSSTypeMetaData = signature.input_types.get(arg := next(iter(signature.time_series_args)))
                if type(ts_type) is not HgTSSTypeMetaData:
                    raise CustomMessageWiringError("The implementation signature input must be a TSS")
                if not ts_type.value_scalar_tp.matches(
                    (ts_int_type := next(iter(interface_sig.time_series_inputs.values()))).value_scalar_tp
                ):
                    raise CustomMessageWiringError(
                        f"The implementation input {ts_type} scalar value does not match: {ts_int_type}"
                    )
                if not signature.output_type.dereference().value_tp.matches(interface_sig.output_type.dereference()):
                    raise CustomMessageWiringError(
                        "The output type does not match that of the subscription service signature"
                    )
            case WiringNodeType.REQ_REP_SVC:
                for arg, ts_type in signature.input_types.items():
                    if not hasattr(ts_type, "value_tp"):
                        raise CustomMessageWiringError(f"For {arg}: invalid service signature type {ts_type}")
                    if not ts_type.value_tp.matches((ts_int_type := interface_sig.time_series_inputs.get(arg))):
                        raise CustomMessageWiringError(
                            f"For {arg} the implementation input {ts_type} type value does not match: {ts_int_type}"
                        )
                if not signature.output_type.dereference().value_tp.matches(interface_sig.output_type.dereference()):
                    raise CustomMessageWiringError(
                        "The output type does not match that of the subscription service signature"
                    )
            case _:
                raise CustomMessageWiringError(f"Unknown service type: {interface_sig.node_type}")
    else:
        pass  # multi-service implementations use the interface stub APIs to wire up the service so checking happens there


def create_inner_graph(
    wiring_signature: WiringNodeSignature,
    fn: Callable,
    scalars: Mapping[str, Any],
    interfaces: list[WiringNodeSignature],
    resolution_dict: dict[TypeVar, HgTypeMetaData] = None,
    interface_resolution_dict: dict[TypeVar, HgTypeMetaData] = None,
) -> (GraphBuilder, tuple, [str]):
    if len(interfaces) == 1:
        s: WiringNodeSignature = interfaces[0].signature
        match s.node_type:
            case WiringNodeType.REF_SVC:
                return wire_reference_data_service(
                    wiring_signature, fn, scalars, interfaces[0], resolution_dict, interface_resolution_dict
                )
            case WiringNodeType.SUBS_SVC:
                return wire_subscription_service(
                    wiring_signature, fn, scalars, interfaces[0], resolution_dict, interface_resolution_dict
                )
            case WiringNodeType.REQ_REP_SVC:
                return wire_request_reply_service(
                    wiring_signature, fn, scalars, interfaces[0], resolution_dict, interface_resolution_dict
                )
            case _:
                raise CustomMessageWiringError(f"Unknown service type: {s.node_type}")
    else:
        with WiringGraphContext(None) as context:
            graph, final_resolution_dict, ri = wire_multi_service(fn, scalars)
            for path, node in context.built_services().items():
                if node:
                    raise CustomMessageWiringError(
                        f"mutli-service implementations should not be registering service nodes"
                    )

                s: WiringNodeClass = context.find_service_impl(path)[0]
                match s.signature.node_type:
                    case WiringNodeType.REF_SVC:  # reference service does not require external stubs
                        pass
                    case WiringNodeType.SUBS_SVC:  # subscription service does not require external stubs
                        pass
                    case WiringNodeType.REQ_REP_SVC:
                        wire_request_reply_service_stubs(
                            s[final_resolution_dict].impl_signature(), path, s, final_resolution_dict
                        )
                    case _:
                        raise CustomMessageWiringError(f"Unknown service type: {s.signature.node_type}")

            return graph, ri, list(context.built_services().keys())


def wire_multi_service(fn: Callable, scalars: Mapping[str, Any], resolution_dict: dict[TypeVar, HgTypeMetaData] = None):
    with WiringNodeInstanceContext(), WiringGraphContext(None) as context:
        g = graph(fn)
        _, _, final_resolution_dict = validate_and_resolve_signature(
            g.signature, __pre_resolved_types__=resolution_dict, **scalars
        )
        g[resolution_dict](**scalars)
        sink_nodes = context.pop_sink_nodes()
        reassignable = context.pop_reassignable_items()
        builder = create_graph_builder(sink_nodes, False)

    return builder, final_resolution_dict, reassignable


def wire_subscription_service(
    wiring_signature: WiringNodeSignature,
    fn: Callable,
    scalars: Mapping[str, Any],
    interface,
    resolution_dict=None,
    interface_resolution_dict=None,
) -> (GraphBuilder, tuple, [str]):
    path = (scalars := dict(scalars)).pop("path")
    typed_full_path = interface.typed_full_path(path, interface_resolution_dict)

    from hgraph._wiring._decorators import graph

    @graph
    def subscription_service():
        g = graph(fn)
        if "path" in g.signature.args:
            scalars["path"] = path

        subscriptions = interface[interface_resolution_dict].wire_impl_inputs_stub(path)
        # Call the implementation graph with the scalars provided
        out = g[resolution_dict](**(subscriptions.as_dict() | scalars))
        interface[interface_resolution_dict].wire_impl_out_stub(path, out)

    with WiringNodeInstanceContext(), WiringGraphContext(wiring_signature) as context:
        subscription_service()
        sink_nodes = context.pop_sink_nodes()
        reassignable = context.pop_reassignable_items()
        builder = create_graph_builder(sink_nodes, False)

    return builder, reassignable, [typed_full_path]


def wire_request_reply_service_stubs(
    wiring_signature: WiringNodeSignature, typed_full_path, interface, resolution_dict=None
):
    from hgraph.nodes._service_utils import capture_output_node_to_global_state, capture_output_to_global_state

    for arg in wiring_signature.time_series_args:
        tp = wiring_signature.input_types[arg]
        request_node = last_value_source_node(f"{typed_full_path}/request_{arg}", tp.resolve(resolution_dict))
        request = _wiring_port_for(tp, request_node, tuple())
        capture_output_node_to_global_state(f"{typed_full_path}/request_{arg}", request)
        capture_output_to_global_state(f"{typed_full_path}/request_{arg}_out", request)
        WiringGraphContext.instance().register_service_stub(interface, typed_full_path, request_node)

    if wiring_signature.output_type is not None:
        replies_node = last_value_source_node(
            f"{typed_full_path}/replies_fb", wiring_signature.output_type.resolve(resolution_dict)
        )
        replies = _wiring_port_for(wiring_signature.output_type, replies_node, tuple())
        capture_output_node_to_global_state(f"{typed_full_path}/replies_fb", replies)
        capture_output_to_global_state(f"{typed_full_path}/replies", replies)
        WiringGraphContext.instance().register_service_stub(interface, typed_full_path, replies_node)


def wire_request_reply_service(
    wiring_signature: WiringNodeSignature,
    fn: Callable,
    scalars: Mapping[str, Any],
    interface,
    resolution_dict,
    interface_resolution_dict,
) -> (GraphBuilder, [str]):
    path = (scalars := dict(scalars)).pop("path")
    typed_full_path = interface.typed_full_path(path, interface_resolution_dict)

    wire_request_reply_service_stubs(wiring_signature, typed_full_path, interface, resolution_dict)

    from hgraph._wiring._decorators import graph

    def request_reply_service():
        g = graph(fn)
        if "path" in g.signature.args:
            scalars["path"] = path

        requests = interface[interface_resolution_dict].wire_impl_inputs_stub(path)
        # Call the implementation graph with the scalars provided
        out = g[resolution_dict](**(requests.as_dict() | scalars))
        interface[interface_resolution_dict].wire_impl_out_stub(path, out)

    with WiringNodeInstanceContext(), WiringGraphContext() as context:
        request_reply_service()
        sink_nodes = context.pop_sink_nodes()
        reassignable = context.pop_reassignable_items()
        builder = create_graph_builder(sink_nodes, False)

    return builder, reassignable, [typed_full_path]


def wire_reference_data_service(
    wiring_signature: WiringNodeSignature,
    fn: Callable,
    scalars: Mapping[str, Any],
    interface,
    resolution_dict,
    interface_resolution_dict=None,
) -> (GraphBuilder, [str]):
    # The path was added to the scalars when initially wired to create the wiring node instance,
    # now we pop it off so that we can make use of both the scalars and the path.
    path = (scalars := dict(scalars)).pop("path")
    typed_full_path = interface.typed_full_path(path, interface_resolution_dict)

    from hgraph._wiring._decorators import graph
    from hgraph.nodes._service_utils import capture_output_to_global_state

    def ref_svc_inner_graph():
        g = graph(fn)
        if "path" in g.signature.args:
            scalars["path"] = path

        # Call the implementation graph with the scalars provided
        out = g[resolution_dict](**scalars)
        with WiringGraphContext(out.node_instance.resolved_signature):
            capture_output_to_global_state(typed_full_path, out)

    with WiringNodeInstanceContext(), WiringGraphContext() as context:
        ref_svc_inner_graph()
        sink_nodes = context.pop_sink_nodes()
        reassignable = context.pop_reassignable_items()
        builder = create_graph_builder(sink_nodes, False)

    return builder, reassignable, [typed_full_path]
