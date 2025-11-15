import functools
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
        HgTSBTypeMetaData,
        InjectableTypesEnum,
    )

__all__ = ("WiringNodeInstance", "WiringNodeInstanceContext", "create_wiring_node_instance")


class InputsKey:

    def __init__(self, inputs):
        self._inputs = inputs
        try:
            self._hash = hash(inputs)
        except TypeError:
            # Degrade to best effort hash, this works around issues where some of the values are not hashable
            # but are still logically immutable.
            self._hash = hash(tuple((k, _safe_hash(v)) for k, v in self._inputs.items()))

    def __eq__(self, other: Any) -> bool:
        return all(
            v.__orig_eq__(other._inputs[k]) if hasattr(v, "__orig_eq__") else v == other._inputs[k]
            for k, v in self._inputs.items()
        )

    def __hash__(self) -> int:
        return self._hash


def _safe_hash(v):
    try:
        return hash(v)
    except TypeError:
        return id(v)


class WiringNodeInstanceContext:
    """
    This must exist when wiring and is used to cache the WiringNodeInstances created during the
    graph building process.
    """

    __stack__: ["WiringNodeInstanceContext"] = []

    def __init__(self, depth=1, error_capture_options=None):
        self._node_instances: dict[tuple, WiringNodeInstance] = {}
        self._depth = depth
        self._error_capture_options = error_capture_options or (
            self.__stack__[-1]._error_capture_options if self.__stack__ else None
        )

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
                **(self._error_capture_options or {}),
            )
        return node_instance

    @classmethod
    def instance(cls) -> "WiringNodeInstanceContext":
        return cls.__stack__[-1] if cls.__stack__ else None

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

    NODE_SIGNATURE = None
    NODE_TYPE_ENUM = None
    INJECTABLE_TYPES_ENUM = None

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

    def __post_init__(self):
        self.label = self.label or self.resolved_signature.label

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
    @functools.cache
    def node_signature(self) -> "NodeSignature":
        if WiringNodeInstance.NODE_SIGNATURE is None:
            from hgraph._runtime import NodeSignature

            WiringNodeInstance.NODE_SIGNATURE = NodeSignature
        if WiringNodeInstance.NODE_TYPE_ENUM is None:
            from hgraph._runtime import NodeTypeEnum

            WiringNodeInstance.NODE_TYPE_ENUM = NodeTypeEnum
        if WiringNodeInstance.INJECTABLE_TYPES_ENUM is None:
            from hgraph._runtime import InjectableTypesEnum

            WiringNodeInstance.INJECTABLE_TYPES_ENUM = InjectableTypesEnum

        # node_type: NodeTypeEnum
        match self.resolved_signature.node_type:
            case WiringNodeType.SINK_NODE:
                node_type = WiringNodeInstance.NODE_TYPE_ENUM.SINK_NODE
            case (
                WiringNodeType.COMPUTE_NODE
                | WiringNodeType.REQ_REP_SVC
                | WiringNodeType.SUBS_SVC
                | WiringNodeType.COMPONENT
            ):
                node_type = WiringNodeInstance.NODE_TYPE_ENUM.COMPUTE_NODE
            case WiringNodeType.PULL_SOURCE_NODE | WiringNodeType.REF_SVC | WiringNodeType.SVC_IMPL:
                node_type = WiringNodeInstance.NODE_TYPE_ENUM.PULL_SOURCE_NODE
            case WiringNodeType.PUSH_SOURCE_NODE:
                node_type = WiringNodeInstance.NODE_TYPE_ENUM.PUSH_SOURCE_NODE
            case _:
                raise CustomMessageWiringError(f"Unknown node type: {self.resolved_signature.node_type}")

        injectable_inputs = self._extract_injectable_inputs(**self.resolved_signature.input_types)
        injectables = self.resolved_signature.injectables.value

        return WiringNodeInstance.NODE_SIGNATURE(
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
            injectable_inputs=injectable_inputs,
            injectables=injectables,
            capture_exception=self.error_handler_registered,
            trace_back_depth=self.trace_back_depth,
            wiring_path_name=self.wiring_path_name,
            label=self.label,
            capture_values=self.capture_values,
            record_replay_id=self.resolved_signature.record_and_replay_id,
            has_nested_graphs=self.resolved_signature.has_nested_graphs,
        )

    @property
    def error_output_type(self) -> "HgTimeSeriesTypeMetaData":
        return self.node.error_output_type

    @property
    def recordable_state_output_type(self) -> "HgTSBTypeMetaData":
        return self.resolved_signature.recordable_state.tsb_type

    def create_node_builder_and_edges(
        self, node_map: MutableMapping["WiringNodeInstance", int], nodes: ["NodeBuilder"]
    ) -> tuple["NodeBuilder", set["Edge"]]:
        """Create an runtime node instance"""
        # Collect appropriate inputs and construct the node
        node_index = len(nodes)
        node_map[self] = node_index  # Update this wiring nodes index in the graph

        scalars = frozendict({
            k: t.injector if t.is_injectable else self.inputs[k]
            for k, t in self.resolved_signature.scalar_inputs.items()
        })

        node_builder = self.node.create_node_builder_instance(self.resolved_signature, self.node_signature, scalars)
        # Extract out edges

        edges = set()
        for ndx, arg in enumerate(
            raw_arg for raw_arg in self.resolved_signature.args if raw_arg in self.resolved_signature.time_series_args
        ):
            input_: WiringPort = self.inputs.get(arg)
            if input_ is not None:
                edges.update(input_.edges_for(node_map, node_index, (ndx,)))

        return node_builder, edges

    @staticmethod
    def _extract_injectable_inputs(**kwargs) -> typing.Mapping[str, "InjectableTypesEnum"]:
        from hgraph import (
            HgSchedulerType,
            HgEvaluationClockType,
            HgEvaluationEngineApiType,
            HgStateType,
            HgRecordableStateType,
            HgOutputType,
            HgLoggerType,
            HgNodeType,
            HgTraitsType,
        )

        return frozendict({
            k: v_
            for k, v in kwargs.items()
            if (
                v_ := {
                    HgSchedulerType: WiringNodeInstance.INJECTABLE_TYPES_ENUM.SCHEDULER,
                    HgEvaluationClockType: WiringNodeInstance.INJECTABLE_TYPES_ENUM.CLOCK,
                    HgEvaluationEngineApiType: WiringNodeInstance.INJECTABLE_TYPES_ENUM.ENGINE_API,
                    HgStateType: WiringNodeInstance.INJECTABLE_TYPES_ENUM.STATE,
                    HgRecordableStateType: WiringNodeInstance.INJECTABLE_TYPES_ENUM.RECORDABLE_STATE,
                    HgOutputType: WiringNodeInstance.INJECTABLE_TYPES_ENUM.OUTPUT,
                    HgLoggerType: WiringNodeInstance.INJECTABLE_TYPES_ENUM.LOGGER,
                    HgNodeType: WiringNodeInstance.INJECTABLE_TYPES_ENUM.NODE,
                    HgTraitsType: WiringNodeInstance.INJECTABLE_TYPES_ENUM.TRAIT,
                }.get(type(v), WiringNodeInstance.INJECTABLE_TYPES_ENUM.NONE)
            )
            != WiringNodeInstance.INJECTABLE_TYPES_ENUM.NONE
        })
