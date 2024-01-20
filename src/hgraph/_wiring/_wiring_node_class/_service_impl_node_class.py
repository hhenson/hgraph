from typing import Callable, Mapping, Any, Sequence

from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass, create_input_output_builders, \
    WiringNodeClass
from hgraph._wiring._wiring_node_signature import WiringNodeSignature

__all__ = ("ServiceImplNodeClass",)


class ServiceImplNodeClass(BaseWiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable, interfaces=None):
        # The service impl node should only take scalar values in, the rest will be a
        # graph where we will stub out the inputs and outputs.
        signature = validate_and_prepare_signature(signature, interfaces)
        super().__init__(signature, fn)

    def create_node_builder_instance(self, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        input_builder, output_builder, error_builder = create_input_output_builders(node_signature,
                                                                                    self.error_output_type)


def validate_and_prepare_signature(signature: WiringNodeSignature,
                                   interfaces: Sequence[WiringNodeClass]) -> WiringNodeSignature:
    """
    The final signature of a service is no inputs and a reference output.

    All services act like a pull source node. Some services do consume inputs, but these will be mapped into the
    service graph in a magical way to ensure the values are copied in (since they will be created at a random point
    in the graph and be fed into a sink node.)
    """
