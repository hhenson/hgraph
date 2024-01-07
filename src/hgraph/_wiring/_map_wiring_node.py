from dataclasses import dataclass
from typing import Any, Mapping, TYPE_CHECKING, TypeVar, cast

from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData, HgAtomicType
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._wiring import BaseWiringNodeClass, create_input_output_builders, WiringGraphContext
from hgraph._wiring._wiring_port import WiringPort
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_utils import wire_nested_graph, extract_stub_node_indices

if TYPE_CHECKING:
    from hgraph._builder._node_builder import NodeBuilder
    from hgraph._runtime._node import NodeSignature

__all__ = ("TsdMapWiringNodeClass", "TslMapWiringNodeClass", "TsdMapWiringSignature", "TslMapWiringSignature")


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
        input_builder, output_builder, error_builder = create_input_output_builders(node_signature,
                                                                                    self.error_output_type)
        return PythonTsdMapNodeBuilder(
            node_signature,
            scalars,
            input_builder,
            output_builder,
            error_builder,
            inner_graph,
            input_node_ids,
            output_node_id,
            self.signature.multiplexed_args
        )

    @property
    def error_output_type(self) -> "HgTimeSeriesTypeMetaData":
        from hgraph import NodeError, TS, TSD
        from hgraph import HgTimeSeriesTypeMetaData
        return HgTimeSeriesTypeMetaData.parse(TSD[self.signature.key_tp.py_type, TS[NodeError]])


class TslMapWiringNodeClass(BaseWiringNodeClass):
    signature: TslMapWiringSignature

    def __call__(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None, **kwargs) -> "WiringPort":
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

    def _map_with_index(self, **kwargs) -> "WiringPort":
        ...

    def _map_no_index(self, **kwargs) -> "WiringPort":
        """In this scenario, we can just map the nodes using the max size"""
        from hgraph._types._scalar_types import Size
        from hgraph.nodes._const import const
        from hgraph._types._tsl_type import TSL
        out = []

        for i in range(cast(Size, self.signature.size_tp.py_type).SIZE):
            kwargs_ = {k: (v[i] if k in self.signature.multiplexed_args else v) for k, v in kwargs.items()}
            if self.signature.key_arg:
                kwargs_ = {self.signature.key_arg: const(i)} | kwargs_
            out.append(self.fn(**kwargs_))

        return TSL.from_ts(*out)
