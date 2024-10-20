from dataclasses import dataclass, field
from typing import Any, Mapping, TYPE_CHECKING, TypeVar, cast, Callable, Optional

from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData, HgAtomicType
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._graph_wiring_node_class import WiringGraphContext
from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass, create_input_output_builders
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_port import WiringPort
from hgraph._wiring._wiring_utils import extract_stub_node_indices

if TYPE_CHECKING:
    from hgraph._builder._node_builder import NodeBuilder
    from hgraph._builder._graph_builder import GraphBuilder

__all__ = ("TsdMapWiringNodeClass", "TslMapWiringNodeClass", "TsdMapWiringSignature", "TslMapWiringSignature")


@dataclass(frozen=True)
class TsdMapWiringSignature(WiringNodeSignature):
    map_fn_signature: WiringNodeSignature | None = None
    key_tp: HgScalarTypeMetaData | None = None
    key_arg: str | None = None  # The arg name of the key in the map function is there is one
    multiplexed_args: frozenset[str] | None = None  # The inputs that need to be de-multiplexed.
    inner_graph: Optional["GraphBuilder"] = field(default=None, hash=False, compare=False)


@dataclass(frozen=True)
class TslMapWiringSignature(WiringNodeSignature):
    map_fn_signature: WiringNodeSignature | None = None
    size_tp: HgAtomicType | None = None
    key_arg: str | None = None
    multiplexed_args: frozenset[str] | None = None  # The inputs that need to be de-multiplexed.
    inner_graph: Optional["GraphBuilder"] = field(default=None, hash=False, compare=False)


class TsdMapWiringNodeClass(BaseWiringNodeClass):
    signature: TsdMapWiringSignature

    def create_node_builder_instance(
        self,
        resolved_wiring_signature: "WiringNodeSignature",
        node_signature: "TsdMapWiringSignature",
        scalars: Mapping[str, Any],
    ) -> "NodeBuilder":
        from hgraph._impl._builder._map_builder import PythonTsdMapNodeBuilder

        inner_graph = self.signature.inner_graph
        input_node_ids, output_node_id = extract_stub_node_indices(
            inner_graph, set(node_signature.time_series_inputs.keys()) | {self.signature.key_arg}
        )
        input_builder, output_builder, error_builder = create_input_output_builders(
            node_signature, self.error_output_type
        )
        return PythonTsdMapNodeBuilder(
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
        )

    @property
    def error_output_type(self) -> "HgTimeSeriesTypeMetaData":
        from hgraph import NodeError, TS, TSD
        from hgraph import HgTimeSeriesTypeMetaData

        return HgTimeSeriesTypeMetaData.parse_type(TSD[self.signature.key_tp.py_type, TS[NodeError]])


class TslMapWiringNodeClass(BaseWiringNodeClass):
    signature: TslMapWiringSignature

    def __call__(
        self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData | Callable] = None, **kwargs
    ) -> "WiringPort":
        # This should be pre-resolved in previous steps.
        with WiringContext(current_wiring_node=self, current_signature=self.signature):
            if not self.signature.is_resolved:
                raise CustomMessageWiringError("The signature must have been resolved before calling this")
            if len(args) > 0:
                raise CustomMessageWiringError("Non-kwarg arguments are not expected at this point in the wiring")
            # But graph nodes are evaluated at wiring time, so this is the graph expansion happening here!
            with WiringGraphContext(self.signature) as g:
                out: WiringPort = self._map_no_index(**kwargs)
                # Since we did lots of checking before creating this, I imaged we should be safe to just let it all out
                return out

    def _map_with_index(self, **kwargs) -> "WiringPort": ...

    def _map_no_index(self, **kwargs) -> "WiringPort":
        """In this scenario, we can just map the nodes using the max size"""
        from hgraph._types._scalar_types import Size
        from hgraph._operators._time_series_conversion import const
        from hgraph._types._tsl_type import TSL

        out = []

        for i in range(cast(Size, self.signature.size_tp.py_type).SIZE):
            kwargs_ = {k: v[i] if k in self.signature.multiplexed_args else v for k, v in kwargs.items()}
            if self.signature.key_arg:
                kwargs_ = {self.signature.key_arg: const(i)} | kwargs_
            out.append(self.fn(**kwargs_))

        return TSL.from_ts(*out)
