from dataclasses import dataclass
from typing import Callable, Any, TypeVar, Mapping, TYPE_CHECKING

from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData, HgAtomicType
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._wiring import WiringPort, BaseWiringNodeClass, create_input_output_builders
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_utils import wire_nested_graph, extract_stub_node_indices

if TYPE_CHECKING:
    from hgraph._builder._node_builder import NodeBuilder
    from hgraph._runtime._node import NodeSignature
    from hgraph._wiring._graph_builder import GraphBuilder

__all__ = ("TsdMapWiringNodeClass", "TslMapWiringNodeClass")


@dataclass(frozen=True)
class TsdMapWiringSignature(WiringNodeSignature):
    map_fn_signature: WiringNodeSignature = None
    key_tp: HgScalarTypeMetaData = None
    key_arg: str | None = None  # The arg name of the key in the map function is there is one
    multiplexed_args: frozenset[str] | None = None  # The inputs that need to be de-multiplexed.


@dataclass(frozen=True)
class TslMapWiringSignature(WiringNodeSignature):
    map_fn_signature: WiringNodeSignature = None
    size_tp: HgAtomicType = None
    key_arg: str | None = None
    multiplexed_args: frozenset[str] | None = None  # The inputs that need to be de-multiplexed.


class TsdMapWiringNodeClass(BaseWiringNodeClass):
    signature: TsdMapWiringSignature

    def create_node_builder_instance(self, node_ndx: int, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        from hgraph._impl._builder._map_builder import PythonMapNodeBuilder
        inner_graph = wire_nested_graph(self.fn, self.signature.map_fn_signature, scalars, self.signature)
        input_node_ids, output_node_id = extract_stub_node_indices(
            inner_graph,
            set(node_signature.time_series_inputs.keys()) | {'key'}
        )
        input_builder, output_builder = create_input_output_builders(node_signature)
        return PythonMapNodeBuilder(node_ndx, node_signature, scalars, input_builder, output_builder, inner_graph,
                                    input_node_ids, output_node_id, self.signature.multiplexed_args)


class TslMapWiringNodeClass(BaseWiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        super().__init__(signature, fn)
        self.inner_graph: "GraphBuilder" = None

    def __call__(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None, **kwargs) -> "WiringPort":
        # This acts a bit like a graph, in that it needs to evaluate the inputs and build a sub-graph for
        # the mapping function.
        return super().__call__(*args, __pre_resolved_types__=__pre_resolved_types__, **kwargs)

    def create_node_builder_instance(self, node_ndx: int, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        # TODO: implement
        ...
