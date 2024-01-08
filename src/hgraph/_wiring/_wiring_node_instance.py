import typing
from dataclasses import dataclass
from typing import Any, MutableMapping

from frozendict import frozendict

if typing.TYPE_CHECKING:
    from hgraph import WiringNodeClass, WiringNodeSignature, HgTimeSeriesTypeMetaData, NodeSignature, NodeBuilder, Edge, \
        WiringPort


@dataclass(frozen=True, eq=False)  # We will write our own equality check, but still want a hash
class WiringNodeInstance:
    node: "WiringNodeClass"
    resolved_signature: "WiringNodeSignature"
    inputs: frozendict[str, Any]  # This should be a mix of WiringPort for time series inputs and scalar values.
    rank: int
    error_handler_registered: bool = False
    trace_back_depth: int = 1  # TODO: decide how to pick this up, probably via the error context?
    capture_values: bool = False
    _hash: int | None = None

    def __eq__(self, other):
        return type(self) is type(other) and self.node == other.node and \
            self.resolved_signature == other.resolved_signature and self.rank == other.rank and \
            self.inputs.keys() == other.inputs.keys() and \
            all(v.__orig_eq__(other.inputs[k]) if hasattr(v, '__orig_eq__') else v == other.inputs[k]
                for k, v in self.inputs.items())
        # Deals with possible WiringPort equality issues due to operator overloading in the syntactical sugar wrappers
        # NOTE: This need performance improvement as it will currently have to walk the reachable graph from here.

    def __hash__(self) -> int:
        if self._hash is None:
            super().__setattr__("_hash", hash((self.node, self.resolved_signature, self.rank, self.inputs)))
        return self._hash

    def mark_error_handler_registered(self, trace_back_depth: int = 1, capture_values: bool = False):
        super().__setattr__("error_handler_registered", True)
        super().__setattr__("trace_back_depth", trace_back_depth)
        super().__setattr__("capture_values", capture_values)

    @property
    def is_stub(self) -> bool:
        from hgraph._wiring import StubWiringNodeClass
        return isinstance(self.node, StubWiringNodeClass)

    @property
    def output_type(self) -> "HgTimeSeriesTypeMetaData":
        return self.resolved_signature.output_type

    @property
    def node_signature(self) -> "NodeSignature":
        from hgraph._runtime import NodeSignature, NodeTypeEnum
        return NodeSignature(
            name=self.resolved_signature.name,
            node_type=NodeTypeEnum(self.resolved_signature.node_type.value),
            args=self.resolved_signature.args,
            time_series_inputs=self.resolved_signature.time_series_inputs,
            time_series_output=self.resolved_signature.output_type,
            scalars=self.resolved_signature.scalar_inputs,
            src_location=self.resolved_signature.src_location,
            active_inputs=self.resolved_signature.active_inputs,
            valid_inputs=self.resolved_signature.valid_inputs,
            all_valid_inputs=self.resolved_signature.all_valid_inputs,
            uses_scheduler=self.resolved_signature.uses_scheduler,
            capture_exception=self.error_handler_registered,
            trace_back_depth=self.trace_back_depth,
            capture_values=self.capture_values
        )

    @property
    def error_output_type(self) -> "HgTimeSeriesTypeMetaData":
        return self.node.error_output_type

    def create_node_builder_and_edges(self, node_map: MutableMapping["WiringNodeInstance", int],
                                      nodes: ["NodeBuilder"]) -> tuple["NodeBuilder", set["Edge"]]:
        """Create an runtime node instance"""
        # Collect appropriate inputs and construct the node
        node_index = len(nodes)
        node_map[self] = node_index  # Update this wiring nodes index in the graph

        scalars = frozendict({k: t.injector if t.is_injectable else self.inputs[k] for k, t in
                              self.resolved_signature.scalar_inputs.items()})

        node_builder = self.node.create_node_builder_instance(self.node_signature, scalars)
        # Extract out edges

        edges = set()
        for ndx, arg in enumerate(raw_arg for raw_arg in self.resolved_signature.args if
                                  raw_arg in self.resolved_signature.time_series_args):
            input_: WiringPort = self.inputs.get(arg)
            if input_ is not None:
                edges.update(input_.edges_for(node_map, node_index, (ndx,)))

        return node_builder, edges


#TODO: Create node instance interning
