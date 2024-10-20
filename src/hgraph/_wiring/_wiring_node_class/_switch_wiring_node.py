from dataclasses import dataclass, field
from typing import Mapping, Any, TYPE_CHECKING

from frozendict import frozendict

from hgraph._types import SCALAR
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
    from hgraph._builder._graph_builder import GraphBuilder

__all__ = ("SwitchWiringNodeClass",)


@dataclass(frozen=True)
class SwitchWiringSignature(WiringNodeSignature):
    inner_graphs: Mapping[SCALAR, "GraphBuilder"] = field(default=None, hash=False, compare=False)


class SwitchWiringNodeClass(BaseWiringNodeClass):
    """The outer switch node"""

    def __init__(
        self,
        signature: SwitchWiringSignature,
        nested_graphs: Mapping[SCALAR, WiringNodeClass],
        resolved_signature_inner: WiringNodeSignature,
        reload_on_ticked: bool,
    ):
        super().__init__(signature, None)
        self._nested_graphs = nested_graphs
        self._resolved_signature_inner = resolved_signature_inner
        self._reload_on_ticked = reload_on_ticked

    def create_node_builder_instance(
        self,
        resolved_wiring_signature: "WiringNodeSignature",
        node_signature: "NodeSignature",
        scalars: Mapping[str, Any],
    ) -> "NodeBuilder":
        # create nested graphs
        nested_graphs = self.signature.inner_graphs
        nested_graph_input_ids = {}
        nested_graph_output_ids = {}
        for k, v in nested_graphs.items():
            ins, outs = extract_stub_node_indices(v, self._resolved_signature_inner.time_series_args)
            nested_graph_input_ids[k] = ins
            if outs:
                nested_graph_output_ids[k] = outs

        input_builder, output_builder, error_builder = create_input_output_builders(
            node_signature, self.error_output_type
        )
        from hgraph._impl._builder._switch_builder import PythonSwitchNodeBuilder

        return PythonSwitchNodeBuilder(
            signature=node_signature,
            scalars=scalars,
            input_builder=input_builder,
            output_builder=output_builder,
            error_builder=error_builder,
            nested_graphs=frozendict(nested_graphs),
            input_node_ids=frozendict(nested_graph_input_ids),
            output_node_id=frozendict(nested_graph_output_ids),
            reload_on_ticked=self._reload_on_ticked,
        )
