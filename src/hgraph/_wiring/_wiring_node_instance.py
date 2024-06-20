import typing
from dataclasses import dataclass, field
from typing import Any, MutableMapping

from frozendict import frozendict

from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_signature import WiringNodeType

if typing.TYPE_CHECKING:
    from hgraph import (
        WiringNodeClass,
        WiringNodeSignature,
        HgTimeSeriesTypeMetaData,
        NodeSignature,
        NodeBuilder,
        Edge,
        WiringPort,
    )

__all__ = ("WiringNodeInstance", "WiringNodeInstanceContext", "create_wiring_node_instance")


class InputsKey:

    def __init__(self, inputs):
        self._inputs = inputs

    def __eq__(self, other: Any) -> bool:
        return all(
            v.__orig_eq__(other._inputs[k]) if hasattr(v, "__orig_eq__") else v == other._inputs[k]
            for k, v in self._inputs.items()
        )

    def __hash__(self) -> int:
        return hash(self._inputs)


class WiringNodeInstanceContext:
    """
    This must exist when wiring and is used to cache the WiringNodeInstances created during the
    graph building process.
    """

    __stack__: ["WiringNodeInstanceContext"] = []

    def __init__(self, depth=1):
        self._node_instances: dict[tuple, WiringNodeInstance] = {}
        self._depth = depth

    def create_wiring_node_instance(
        self, node: "WiringNodeClass", resolved_signature: "WiringNodeSignature", inputs: frozendict[str, Any]
    ) -> "WiringNodeInstance":
        key = (InputsKey(inputs), resolved_signature, node)
        if (node_instance := self._node_instances.get(key, None)) is None:
            from hgraph import WiringGraphContext

            self._node_instances[key] = node_instance = WiringNodeInstance(
                node=node,
                resolved_signature=resolved_signature,
                inputs=inputs,
                wiring_path_name=(WiringGraphContext.instance() or WiringGraphContext(None)).wiring_path_name(),
            )
        return node_instance

    @classmethod
    def instance(cls) -> "WiringNodeInstanceContext":
        return cls.__stack__[-1]

    def graph_nesting_depth(self) -> int:
        return sum(c._depth for c in self.__stack__)

    def __enter__(self):
        self.__stack__.append(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__stack__.pop()


def create_wiring_node_instance(
    node: "WiringNodeClass",
    resolved_signature: "WiringNodeSignature",
    inputs: frozendict[str, Any],
) -> "WiringNodeInstance":
    return WiringNodeInstanceContext.instance().create_wiring_node_instance(node, resolved_signature, inputs)


@dataclass  # We will write our own equality check, but still want a hash
class WiringNodeInstance:
    node: "WiringNodeClass"
    resolved_signature: "WiringNodeSignature"
    inputs: frozendict[str, Any]  # This should be a mix of WiringPort for time series inputs and scalar values.
    wiring_path_name: str
    label: str = ""
    error_handler_registered: bool = False
    trace_back_depth: int = 1  # TODO: decide how to pick this up, probably via the error context?
    capture_values: bool = False
    _treat_as_source_node: bool = False
    non_input_dependencies: list["WiringNodeInstance"] = field(default_factory=list)
    ranking_alternatives: list["WiringNodeInstance"] = field(default_factory=list)

    def __lt__(self, other: "WiringNodeInstance") -> bool:
        # The last part gives potential for inconsistent ordering, a better solution would be to
        # consider the inputs, but that may contain values which do not support ordering.
        # So for now we use this simple hack which is sufficient for the short term.
        return self.resolved_signature.signature < other.resolved_signature.signature or id(self) < id(other)

    def __eq__(self, other):
        # Rely on WiringNodeInstances to be interned data structures
        return self is other

    def __hash__(self) -> int:
        # Rely on WiringNodeInstances to be interned data structures
        return id(self)

    def __repr__(self):
        return self.resolved_signature.signature

    def mark_error_handler_registered(self, trace_back_depth: int = 1, capture_values: bool = False):
        self.error_handler_registered = True
        self.trace_back_depth = trace_back_depth
        self.capture_values = capture_values

    @property
    def is_stub(self) -> bool:
        from hgraph._wiring._wiring_node_class._stub_wiring_node_class import StubWiringNodeClass

        return isinstance(self.node, StubWiringNodeClass)

    @property
    def is_source_node(self) -> bool:
        return (
            self.resolved_signature.node_type in (WiringNodeType.PUSH_SOURCE_NODE, WiringNodeType.PULL_SOURCE_NODE)
            or self._treat_as_source_node
        )

    def mark_treat_as_source_node(self):
        self._treat_as_source_node = True

    def add_ranking_alternative(self, wp: "WiringNodeInstance"):
        self.ranking_alternatives.append(wp)

    def add_indirect_dependency(self, wp: "WiringNodeInstance"):
        self.non_input_dependencies.append(wp)

    @property
    def output_type(self) -> "HgTimeSeriesTypeMetaData":
        return self.resolved_signature.output_type

    @property
    def node_signature(self) -> "NodeSignature":
        from hgraph._runtime import NodeSignature, NodeTypeEnum

        node_type: NodeTypeEnum
        match self.resolved_signature.node_type:
            case WiringNodeType.SINK_NODE:
                node_type = NodeTypeEnum.SINK_NODE
            case WiringNodeType.COMPUTE_NODE | WiringNodeType.REQ_REP_SVC | WiringNodeType.SUBS_SVC:
                node_type = NodeTypeEnum.COMPUTE_NODE
            case WiringNodeType.PULL_SOURCE_NODE | WiringNodeType.REF_SVC | WiringNodeType.SVC_IMPL:
                node_type = NodeTypeEnum.PULL_SOURCE_NODE
            case WiringNodeType.PUSH_SOURCE_NODE:
                node_type = NodeTypeEnum.PUSH_SOURCE_NODE
            case _:
                raise CustomMessageWiringError(f"Unknown node type: {self.resolved_signature.node_type}")

        return NodeSignature(
            name=self.resolved_signature.name,
            node_type=node_type,
            args=self.resolved_signature.args,
            time_series_inputs=self.resolved_signature.time_series_inputs,
            time_series_output=self.resolved_signature.output_type,
            scalars=self.resolved_signature.scalar_inputs,
            src_location=self.resolved_signature.src_location,
            active_inputs=self.resolved_signature.active_inputs,
            valid_inputs=self.resolved_signature.valid_inputs,
            all_valid_inputs=self.resolved_signature.all_valid_inputs,
            context_inputs=self.resolved_signature.context_inputs,
            injectable_inputs=self.resolved_signature.injectable_inputs,
            capture_exception=self.error_handler_registered,
            trace_back_depth=self.trace_back_depth,
            wiring_path_name=self.wiring_path_name,
            label=self.label,
            capture_values=self.capture_values,
            record_replay_id=self.resolved_signature.record_and_replay_id,
        )

    @property
    def error_output_type(self) -> "HgTimeSeriesTypeMetaData":
        return self.node.error_output_type

    def create_node_builder_and_edges(
        self, node_map: MutableMapping["WiringNodeInstance", int], nodes: ["NodeBuilder"]
    ) -> tuple["NodeBuilder", set["Edge"]]:
        """Create an runtime node instance"""
        # Collect appropriate inputs and construct the node
        node_index = len(nodes)
        node_map[self] = node_index  # Update this wiring nodes index in the graph

        scalars = frozendict(
            {
                k: t.injector if t.is_injectable else self.inputs[k]
                for k, t in self.resolved_signature.scalar_inputs.items()
            }
        )

        node_builder = self.node.create_node_builder_instance(self.node_signature, scalars)
        # Extract out edges

        edges = set()
        for ndx, arg in enumerate(
            raw_arg for raw_arg in self.resolved_signature.args if raw_arg in self.resolved_signature.time_series_args
        ):
            input_: WiringPort = self.inputs.get(arg)
            if input_ is not None:
                edges.update(input_.edges_for(node_map, node_index, (ndx,)))

        return node_builder, edges
