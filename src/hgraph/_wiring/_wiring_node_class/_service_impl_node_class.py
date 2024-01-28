from typing import Callable, Mapping, Any, Sequence

from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._wiring_node_class import create_input_output_builders, WiringNodeClass, \
    BaseWiringNodeClass
from hgraph._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType
from hgraph._wiring._wiring_utils import wire_nested_graph

__all__ = ("ServiceImplNodeClass",)


class ServiceImplNodeClass(BaseWiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable, interfaces=None):
        # The service impl node should only take scalar values in, the rest will be a
        # graph where we will stub out the inputs and outputs.
        signature = validate_and_prepare_signature(signature, interfaces)
        super().__init__(signature, fn)
        self._interfaces = interfaces

    def create_node_builder_instance(self, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        input_builder, output_builder, error_builder = create_input_output_builders(node_signature,
                                                                                    self.error_output_type)
        # TODO: Make this correct
        inner_graph = wire_nested_graph(self.fn, self.signature.map_fn_signature.input_types, scalars, self.signature)
        from hgraph._impl._builder._service_impl_builder import PythonServiceImplNodeBuilder
        return PythonServiceImplNodeBuilder(
            node_signature,
            scalars,
            input_builder,
            output_builder,
            error_builder,
            inner_graph,
        )


def validate_and_prepare_signature(signature: WiringNodeSignature,
                                   interfaces: Sequence[WiringNodeClass]) -> WiringNodeSignature:
    """
    The final signature of a service is no inputs and a reference output.

    All services act like a pull source node. Some services do consume inputs, but these will be mapped into the
    service graph in a magical way to ensure the values are copied in (since they will be created at a random point
    in the graph and be fed into a sink node.)
    """
    if interfaces is None:
        raise CustomMessageWiringError("No interfaces provided")
    if not isinstance(interfaces, (tuple, list, set)):
        interfaces = [interfaces]
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
