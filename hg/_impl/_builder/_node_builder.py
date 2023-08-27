from hg._impl._builder._builder import Builder, ITEM
from hg._impl._runtime._node import NodeImpl


class NodeBuilder(Builder[NodeImpl]):

    def make_instance(self) -> ITEM:
        pass

    def release_instance(self, item: ITEM):
        pass