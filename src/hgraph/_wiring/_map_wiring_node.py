from dataclasses import dataclass
from typing import Any, Mapping, TYPE_CHECKING

from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData, HgAtomicType
from hgraph._wiring._wiring import BaseWiringNodeClass, create_input_output_builders
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_utils import wire_nested_graph, extract_stub_node_indices

if TYPE_CHECKING:
    from hgraph._builder._node_builder import NodeBuilder
    from hgraph._runtime._node import NodeSignature

__all__ = ("TsdMapWiringNodeClass", "TslMapWiringNodeClass")


@dataclass(frozen=True)
class TsdMapWiringSignature(WiringNodeSignature):
    map_fn_signature: WiringNodeSignature | None = None
    key_tp: HgScalarTypeMetaData | None = None
    key_arg: str | None = None  # The arg name of the key in the map function is there is one
    multiplexed_args: frozenset[str] | None = None  # The inputs that need to be de-multiplexed.


@dataclass(frozen=True)
class TslMapWiringSignature(WiringNodeSignature):
    map_fn_signature: WiringNodeSignature | None = None
    size_tp: HgAtomicType | None = None
    key_arg: str | None = None
    multiplexed_args: frozenset[str] | None = None  # The inputs that need to be de-multiplexed.


class TsdMapWiringNodeClass(BaseWiringNodeClass):
    signature: TsdMapWiringSignature

    def create_node_builder_instance(self, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        from hgraph._impl._builder._map_builder import PythonTsdMapNodeBuilder
        inner_graph = wire_nested_graph(self.fn, self.signature.map_fn_signature.input_types, scalars, self.signature)
        input_node_ids, output_node_id = extract_stub_node_indices(
            inner_graph,
            set(node_signature.time_series_inputs.keys()) | {'key'}
        )
        input_builder, output_builder = create_input_output_builders(node_signature)
        return PythonTsdMapNodeBuilder(node_signature, scalars, input_builder, output_builder, inner_graph,
                                       input_node_ids, output_node_id, self.signature.multiplexed_args)


class TslMapWiringNodeClass(BaseWiringNodeClass):
    signature: TslMapWiringSignature

    def create_node_builder_instance(self, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        from hgraph._impl._builder._map_builder import PythonTslMapNodeBuilder
        inner_graph = wire_nested_graph(self.fn, self.signature.map_fn_signature.input_types, scalars, self.signature)
        input_node_ids, output_node_id = extract_stub_node_indices(
            inner_graph,
            set(node_signature.time_series_inputs.keys()) | {'ndx'}
        )
        input_builder, output_builder = create_input_output_builders(node_signature)
        return PythonTslMapNodeBuilder(node_signature, scalars, input_builder, output_builder, inner_graph,
                                       input_node_ids, output_node_id, self.signature.multiplexed_args)


