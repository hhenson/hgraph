from dataclasses import dataclass
from typing import Any, Callable, Mapping

from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass

if False:  # pragma: no cover
    from hgraph._runtime._node import NodeSignature
    from hgraph._wiring._wiring_node_signature import WiringNodeSignature

__all__ = ("CppStaticNodeBuilder", "CppStaticWiringNodeClass")


@dataclass(frozen=True)
class CppStaticNodeBuilder:
    signature: Any
    scalars: Mapping[str, Any]
    input_builder: Any = None
    output_builder: Any = None
    error_builder: Any = None
    recordable_state_builder: Any = None
    implementation_name: str = ""
    input_schema: Any = None
    output_schema: Any = None

    def make_instance(self, owning_graph_id: tuple[int, ...], node_ndx):
        raise NotImplementedError(
            f"C++ static node '{self.implementation_name}' is exported to Python for wiring/signature use, "
            "but execution still requires the v2 runtime integration."
        )

    def release_instance(self, item):
        return None


class CppStaticWiringNodeClass(BaseWiringNodeClass):

    def __init__(self, signature: "WiringNodeSignature", builder_factory: Callable):
        super().__init__(signature, builder_factory)
        self._builder_factory = builder_factory

    def create_node_builder_instance(
        self,
        resolved_wiring_signature: "WiringNodeSignature",
        node_signature: "NodeSignature",
        scalars: Mapping[str, Any],
    ) -> Any:
        return self._builder_factory(resolved_wiring_signature, node_signature, scalars)
