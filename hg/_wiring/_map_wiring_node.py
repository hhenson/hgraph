from dataclasses import dataclass
from typing import Callable, Any, TypeVar, Mapping, TYPE_CHECKING, cast

from hg._types._scalar_type_meta_data import HgScalarTypeMetaData, HgAtomicType
from hg._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hg._types._type_meta_data import HgTypeMetaData
from hg._wiring._stub_wiring_node import create_input_stub, create_output_stub
from hg._wiring._wiring import WiringNodeClass, WiringPort, BaseWiringNodeClass, WiringGraphContext
from hg._wiring._wiring_node_signature import WiringNodeSignature

if TYPE_CHECKING:
    from hg._builder._node_builder import NodeBuilder
    from hg._runtime._node import NodeSignature
    from hg._wiring._graph_builder import GraphBuilder

__all__ = ("TsdMapWiringNodeClass", "TslMapWiringNodeClass")


@dataclass(frozen=True)
class TsdMapWiringSignature(WiringNodeSignature):
    map_fn_signature: WiringNodeSignature = None
    key_tp: HgScalarTypeMetaData = None
    key_arg: str | None = None  # The arg name of the key in the map function is there is one
    keyable_args: frozenset[str] | None = None  # When keys is not present, these are the inputs to key from.
    multiplexed_args: frozenset[str] | None = None  # The inputs that need to be de-multiplexed.


@dataclass(frozen=True)
class TslMapWiringSignature(WiringNodeSignature):
    map_fn_signature: WiringNodeSignature = None
    size_tp: HgAtomicType = None
    key_arg: str | None = None
    multiplexed_args: frozenset[str] | None = None  # The inputs that need to be de-multiplexed.


class TsdMapWiringNodeClass(BaseWiringNodeClass):
    signature: TsdMapWiringSignature

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        super().__init__(signature, fn)

    def __call__(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None, **kwargs) -> "WiringPort":
        # This will only be called with kwargs, once all is resolved, so we can use them to build the inner graph.
        if len(args) > 0:
            raise RuntimeError("This function does not support positional arguments")
        self.inner_graph: "GraphBuilder" = _wire_inner_graph(self.fn, self.signature.map_fn_signature, kwargs,
                                                             self.signature)
        return super().__call__(*args, __pre_resolved_types__=__pre_resolved_types__, **kwargs)

    def create_node_builder_instance(self, node_ndx: int, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        # TODO: implement
        ...


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


def _wire_inner_graph(fn: WiringNodeClass, signature: WiringNodeSignature, kwargs: dict,
                      outer_wiring_node_signature: WiringNodeSignature) -> "GraphBuilder":
    """
    Wire the inner function using stub inputs and wrap stub outputs.
    """
    from hg._wiring._graph_builder import create_graph_builder
    inputs_ = {}
    for k, v in signature.input_types.items():
        if v.is_scalar:
            inputs_[k] = kwargs[k]
        else:
            inputs_[k] = create_input_stub(k, cast(HgTimeSeriesTypeMetaData, v))
    with WiringGraphContext(outer_wiring_node_signature) as context:
        out = fn(**inputs_)
        if out is not None:
            create_output_stub(cast(WiringPort, out))
        sink_nodes = context.pop_sink_nodes()
        return create_graph_builder(sink_nodes)
