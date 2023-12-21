from typing import TYPE_CHECKING

from hgraph._builder._builder import Builder

if TYPE_CHECKING:
    from hgraph._types._scalar_value import ScalarValue


__all__ = ('ScalarValueBuilder',)


class ScalarValueBuilder(Builder["ScalarValue"]):

    def make_instance(self) -> "ScalarValue":
        """A scalar value is a basic type"""
        raise NotImplementedError()

    def release_instance(self, item: "ScalarValue"):
        raise NotImplementedError()