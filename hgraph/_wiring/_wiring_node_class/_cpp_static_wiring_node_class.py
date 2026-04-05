from typing import Any, Callable, Mapping

from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass

if False:  # pragma: no cover
    from hgraph._runtime._node import NodeSignature
    from hgraph._wiring._wiring_node_signature import WiringNodeSignature

__all__ = ("CppStaticWiringNodeClass",)


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
