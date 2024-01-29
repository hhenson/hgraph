from typing import Callable, Mapping, Any, Sequence, TypeVar

from frozendict import frozendict

from hgraph._builder._graph_builder import GraphBuilder
from hgraph._runtime._global_state import GlobalState
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass, \
    BaseWiringNodeClass
from hgraph._wiring._wiring_node_instance import create_wiring_node_instance
from hgraph._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType

__all__ = ("ServiceImplNodeClass",)


class ServiceImplNodeClass(BaseWiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable, interfaces=None):
        super().__init__(signature, fn)
        if interfaces is None:
            raise CustomMessageWiringError("No interfaces provided")
        if not isinstance(interfaces, (tuple, list, set)):
            interfaces = [interfaces]
        self.interfaces = interfaces
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

    def __call__(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None, path: str = None,
                 **kwargs) -> "WiringPort":

        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            self._validate_service_not_already_bound(path)

            # Now validate types and resolve any un-resolved types and provide an updated signature.
            # The resolution should either be due to pre-resolved types or scalar values. Since we do not currently
            # Support time-series inputs as valid values.
            kwargs_, resolved_signature = self._validate_and_resolve_signature(
                *args,
                __pre_resolved_types__=__pre_resolved_types__,
                **kwargs)

            wiring_node_instance = create_wiring_node_instance(self, resolved_signature, frozendict(kwargs_), rank=1)
            # Select the correct wiring port for the TS type! That we can provide useful wiring syntax
            # to support this like out.p1 on a bundle or out.s1 on a ComplexScalar, etc.

            from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext
            WiringGraphContext.instance().add_sink_node(wiring_node_instance)

    def create_node_builder_instance(self, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        # The service impl node should only take scalar values in, the rest will be a
        # graph where we will stub out the inputs and outputs.
        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            from hgraph._impl._builder._service_impl_builder import PythonServiceImplNodeBuilder
            inner_graph = create_inner_graph(node_signature, scalars, self.interfaces)
            return PythonServiceImplNodeBuilder(node_signature, scalars, None, None, None, inner_graph)


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
        s: WiringNodeSignature = interfaces[0].signature
        match s.node_type:
            case WiringNodeType.REF_SVC:
                if signature.time_series_args:
                    raise CustomMessageWiringError("The signature cannot have any time-series inputs")
                if not signature.output_type.dereference().matches(s.output_type.dereference()):
                    raise CustomMessageWiringError(
                        "The output type does not match that of the reference service signature")
                return signature
            case _:
                raise CustomMessageWiringError(f"Unknown service type: {s.node_type}")
    else:
        raise CustomMessageWiringError("Unable to handle multiple interfaces yet")


def create_inner_graph(self, wiring_signature: WiringNodeSignature, scalars: Mapping[str, Any],
                       interfaces: list[WiringNodeSignature]) -> GraphBuilder:
    ...
