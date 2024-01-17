from typing import Callable, Mapping, Any

from frozendict import frozendict

from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass, create_input_output_builders
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._types._ref_meta_data import HgREFTypeMetaData


__all__ = ("ServiceImplNodeClass",)


class ServiceImplNodeClass(BaseWiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        if not signature.output_type.is_reference:
            # The output must be a reference type, if it already is a reference then we are fine.
            signature = signature.copy_with(output_type=HgREFTypeMetaData(signature.output_type))
        super().__init__(signature, fn)

    def create_node_builder_instance(self, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        from hgraph._impl._builder import PythonNodeImplNodeBuilder
        input_builder, output_builder, error_builder = create_input_output_builders(node_signature,
                                                                                    self.error_output_type)