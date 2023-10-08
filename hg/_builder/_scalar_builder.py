from hg._builder._builder import Builder
from hg._types._scalar_value import ScalarValue


__all__ = ('ScalarValueBuilder',)


class ScalarValueBuilder(Builder[ScalarValue]):

    def make_instance(self) -> ScalarValue:
        """A scalar value is a basic type"""
        pass

    def release_instance(self, item: ScalarValue):
        pass