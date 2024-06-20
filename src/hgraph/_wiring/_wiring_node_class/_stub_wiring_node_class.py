from typing import TYPE_CHECKING

from hgraph._wiring._wiring_node_class._wiring_node_class import BaseWiringNodeClass
from hgraph._wiring._wiring_port import WiringPort
from hgraph._types._tsb_meta_data import HgTSBTypeMetaData

if TYPE_CHECKING:
    from hgraph._builder._node_builder import NodeBuilder


class StubWiringNodeClass(BaseWiringNodeClass):

    def __call__(self, *args, **kwargs) -> "WiringPort":
        """Sub wiring classes are not callable"""
        raise NotImplementedError()

    def create_node_builder_instance(self, node_signature, scalars) -> "NodeBuilder":
        """Sub wiring classes do not create node builders"""
        raise NotImplementedError()


class NonPeeredWiringNodeClass(StubWiringNodeClass):
    """Used to represent Non-graph nodes to use when creating non-peered wiring ports"""

    def __call__(self, _tsb_meta_type: HgTSBTypeMetaData, **kwargs) -> "WiringPort": ...


class ContextStubWiringNodeClass(StubWiringNodeClass):

    def __call__(self, *args, **kwargs) -> "WiringPort":
        ...

