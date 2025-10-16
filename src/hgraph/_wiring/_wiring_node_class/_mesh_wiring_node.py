from dataclasses import dataclass
from typing import Any, Mapping, TYPE_CHECKING

from hgraph._wiring._wiring_node_class._map_wiring_node import TsdMapWiringSignature
from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass, create_input_output_builders
from hgraph._wiring._wiring_utils import extract_stub_node_indices

if TYPE_CHECKING:
    from hgraph._builder._node_builder import NodeBuilder

__all__ = ("MeshWiringNodeClass", "MeshWiringSignature")


@dataclass(frozen=True)
class MeshWiringSignature(TsdMapWiringSignature):
    context_path: str | None = None


class MeshWiringNodeClass(BaseWiringNodeClass):
    signature: MeshWiringSignature

    def create_node_builder_instance(
        self, resolved_wiring_signature, node_signature: "MeshWiringSignature", scalars: Mapping[str, Any]
    ) -> "NodeBuilder":
        if MeshWiringNodeClass.BUILDER_CLASS is None:
            from hgraph._impl._builder._mesh_builder import PythonMeshNodeBuilder

            MeshWiringNodeClass.BUILDER_CLASS = PythonMeshNodeBuilder

        inner_graph = self.signature.inner_graph
        input_node_ids, output_node_id = extract_stub_node_indices(
            inner_graph, set(node_signature.time_series_inputs.keys()) | {self.signature.key_arg}
        )
        input_builder, output_builder, error_builder = create_input_output_builders(
            node_signature, self.error_output_type
        )
        return MeshWiringNodeClass.BUILDER_CLASS(
            signature=node_signature,
            scalars=scalars,
            input_builder=input_builder,
            output_builder=output_builder,
            error_builder=error_builder,
            recordable_state_builder=None,
            nested_graph=inner_graph,
            input_node_ids=input_node_ids,
            output_node_id=output_node_id,
            multiplexed_args=self.signature.multiplexed_args,
            key_arg=self.signature.key_arg,
            key_tp=self.signature.key_tp,
            context_path=self.signature.context_path,
        )
