from dataclasses import dataclass, field
from typing import Any, Mapping, TYPE_CHECKING, cast, Optional

from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
from hgraph._wiring._wiring_node_class._wiring_node_class import (
    BaseWiringNodeClass,
    create_input_output_builders,
    WiringNodeClass,
)
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_utils import wire_nested_graph, extract_stub_node_indices

if TYPE_CHECKING:
    from hgraph._builder._node_builder import NodeBuilder
    from hgraph._builder._graph_builder import GraphBuilder
    from hgraph._runtime._node import NodeSignature

__all__ = ("TsdReduceWiringNodeClass", "TsdNonAssociativeReduceWiringNodeClass")


@dataclass(frozen=True)
class ReduceWiringSignature(WiringNodeSignature):
    inner_graph: Optional["GraphBuilder"] = field(default=None, hash=False, compare=False)
    has_nested_graphs: bool = True


class TsdReduceWiringNodeClass(BaseWiringNodeClass):
    signature: ReduceWiringSignature

    def create_node_builder_instance(
        self,
        resolved_wiring_signature: "WiringNodeSignature",
        node_signature: "NodeSignature",
        scalars: Mapping[str, Any],
    ) -> "NodeBuilder":
        if TsdReduceWiringNodeClass.BUILDER_CLASS is None:
            from hgraph._impl._builder._reduce_builder import PythonReduceNodeBuilder

            TsdReduceWiringNodeClass.BUILDER_CLASS = PythonReduceNodeBuilder

        fn_signature = cast(WiringNodeClass, self.fn).signature

        inner_graph = self.signature.inner_graph
        input_node_ids, output_node_id = extract_stub_node_indices(
            inner_graph, set(fn_signature.time_series_inputs.keys())
        )
        input_builder, output_builder, error_builder = create_input_output_builders(
            node_signature, self.error_output_type
        )
        return TsdReduceWiringNodeClass.BUILDER_CLASS(
            signature=node_signature,
            scalars=scalars,
            input_builder=input_builder,
            output_builder=output_builder,
            error_builder=error_builder,
            nested_graph=inner_graph,
            input_node_ids=tuple(input_node_ids[k] for k in fn_signature.time_series_inputs.keys()),
            output_node_id=output_node_id,
        )


class TsdNonAssociativeReduceWiringNodeClass(BaseWiringNodeClass):
    signature: ReduceWiringSignature

    def create_node_builder_instance(
        self,
        resolved_wiring_signature: "WiringNodeSignature",
        node_signature: "NodeSignature",
        scalars: Mapping[str, Any],
    ) -> "NodeBuilder":
        if TsdNonAssociativeReduceWiringNodeClass.BUILDER_CLASS is None:
            from hgraph._impl._builder._reduce_builder import PythonTsdNonAssociativeReduceNodeBuilder

            TsdNonAssociativeReduceWiringNodeClass.BUILDER_CLASS = PythonTsdNonAssociativeReduceNodeBuilder

        fn_signature = cast(WiringNodeClass, self.fn).signature

        inner_graph = self.signature.inner_graph
        input_node_ids, output_node_id = extract_stub_node_indices(
            inner_graph, set(fn_signature.time_series_inputs.keys())
        )
        input_builder, output_builder, error_builder = create_input_output_builders(
            node_signature, self.error_output_type
        )
        return TsdNonAssociativeReduceWiringNodeClass.BUILDER_CLASS(
            signature=node_signature,
            scalars=scalars,
            input_builder=input_builder,
            output_builder=output_builder,
            error_builder=error_builder,
            nested_graph=inner_graph,
            input_node_ids=tuple(input_node_ids[k] for k in fn_signature.time_series_inputs.keys()),
            output_node_id=output_node_id,
        )
