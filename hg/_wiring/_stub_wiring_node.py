from pathlib import Path
from typing import Callable, Mapping, Any, TypeVar

from frozendict import frozendict

from hg._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hg._wiring import SourceCodeDetails, WiringGraphContext
from hg._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType
from hg._wiring._wiring import WiringNodeClass, WiringNodeInstance, WiringPort
from hg._types._type_meta_data import HgTypeMetaData


class StubWiringInputWiringNode(WiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, impl_fn: Callable):
        super().__init__(signature, impl_fn)

    def __call__(self, *args, **kwargs):
        """Not going to be called"""

    def resolve_signature(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None,
                          **kwargs) -> "WiringNodeSignature":
        pass

    def create_node_builder_instance(self, node_ndx: int, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        pass


class StubWiringOutputWiringNode(WiringNodeClass):

    def __init__(self, signature: WiringNodeSignature, impl_fn: Callable):
        super().__init__(signature, impl_fn)

    def __call__(self, *args, **kwargs):
        """Not going to be called"""

    def resolve_signature(self, *args, __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None,
                          **kwargs) -> "WiringNodeSignature":
        pass

    def create_node_builder_instance(self, node_ndx: int, node_signature: "NodeSignature",
                                     scalars: Mapping[str, Any]) -> "NodeBuilder":
        pass


def create_input_stub(key: str, tp: HgTimeSeriesTypeMetaData) -> WiringPort:
    """
    Creates a stub input for a wiring node input.
    """
    signature = WiringNodeSignature(
        node_type=WiringNodeType.PULL_SOURCE_NODE,
        name=f"stub:{key}",
        args=tuple(),
        defaults=frozendict(),
        input_types=frozendict(),
        output_type=tp,
        src_location=SourceCodeDetails(Path(__file__), 29),
        active_inputs=frozenset(),
        valid_inputs=frozenset(),
        unresolved_args=frozenset(),
        time_series_args=frozenset(),
        uses_scheduler=False,
        label=key
    )
    node = StubWiringInputWiringNode(signature, None)
    node_instance = WiringNodeInstance(node, signature, frozendict(), 1)
    return WiringPort(node_instance, signature, 0)


def create_output_stub(output: WiringPort):
    """
    Creates a stub output for a wiring node output.
    """
    tp = output.output_type
    signature = WiringNodeSignature(
        node_type=WiringNodeType.SINK_NODE,
        name=f"stub:out",
        args=tuple('out',),
        defaults=frozendict(),
        input_types=frozendict({'out': tp}),
        output_type=None,
        src_location=SourceCodeDetails(Path(__file__), 53),
        active_inputs=frozenset(),
        valid_inputs=frozenset(),
        unresolved_args=frozenset(),
        time_series_args=frozenset(),
        uses_scheduler=False,
    )
    node = StubWiringOutputWiringNode(signature, None)
    node_instance = WiringNodeInstance(node, signature, frozendict(), output.rank + 1)
    WiringGraphContext.instance().add_sink_node(node_instance)
