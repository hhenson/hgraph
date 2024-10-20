from typing import Mapping, Any, TYPE_CHECKING

from frozendict import frozendict

from hgraph._wiring._wiring_node_class._wiring_node_class import (
    BaseWiringNodeClass,
    WiringNodeClass,
    create_input_output_builders,
)
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_utils import wire_nested_graph, extract_stub_node_indices

if TYPE_CHECKING:
    from hgraph._runtime._node import NodeSignature
    from hgraph._builder._node_builder import NodeBuilder

__all__ = ("TryExceptWiringNodeClass",)


class TryExceptWiringNodeClass(BaseWiringNodeClass):
    """The outer try_except node"""

    def __init__(
        self,
        signature: WiringNodeSignature,
        nested_graph: WiringNodeClass,
        resolved_signature_inner: WiringNodeSignature,
    ):
        super().__init__(signature, None)
        self._nested_graph = nested_graph
        self._resolved_signature_inner = resolved_signature_inner

    def create_node_builder_instance(
        self, resolved_wiring_signature, node_signature: "NodeSignature", scalars: Mapping[str, Any]
    ) -> ("NodeBuilder", tuple):
        nested_graph_input_ids, nested_graph_output_id = extract_stub_node_indices(
            self._nested_graph, self._resolved_signature_inner.time_series_args
        )

        input_builder, output_builder, error_builder = create_input_output_builders(
            node_signature, self.error_output_type
        )
        from hgraph._impl._builder._try_except_builder import PythonTryExceptNodeBuilder

        return PythonTryExceptNodeBuilder(
            signature=node_signature,
            scalars=scalars,
            input_builder=input_builder,
            output_builder=output_builder,
            error_builder=error_builder,
            nested_graph=self._nested_graph,
            input_node_ids=frozendict(nested_graph_input_ids),
            output_node_id=nested_graph_output_id,
        )
