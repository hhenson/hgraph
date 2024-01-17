from typing import Callable, Mapping, Any, TYPE_CHECKING

from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass, create_input_output_builders

if TYPE_CHECKING:
    from hgraph._runtime._node import Node


class NodeImplWiringNodeClass(BaseWiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: "Node"):
        super().__init__(signature, fn)

    def create_node_builder_instance(self, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        from hgraph._impl._builder._node_impl_builder import PythonNodeImplNodeBuilder
        input_builder, output_builder, error_builder = create_input_output_builders(node_signature,
                                                                                    self.error_output_type)

        return PythonNodeImplNodeBuilder(signature=node_signature,
                                 scalars=scalars,
                                 input_builder=input_builder,
                                 output_builder=output_builder,
                                 error_builder=error_builder,
                                 node_impl=self.fn)
