from contextlib import nullcontext
from typing import TYPE_CHECKING, TypeVar, Any, NamedTuple

from hgraph import HgTypeMetaData, WiringContext, const, WiringNodeInstanceContext
from hgraph._wiring._wiring_node_class._wiring_node_class import (
    BaseWiringNodeClass,
    validate_and_resolve_signature,
)

if TYPE_CHECKING:
    pass

__all__ = ("PythonConstWiringNodeClass",)


ValueTuple = NamedTuple("ValueTuple", [("value", Any)])


class PythonConstWiringNodeClass(BaseWiringNodeClass):

    def __call__(
        self,
        *args,
        __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None,
        __return_sink_wp__: bool = False,
        **kwargs,
    ) -> "WiringPort":
        if WiringNodeInstanceContext.instance() is None:
            wiring_node_context = WiringNodeInstanceContext()
            in_graph = False
        else:
            wiring_node_context = nullcontext()
            in_graph = True

        with wiring_node_context, WiringContext(current_wiring_node=self, current_signature=self.signature):
            kwargs_, resolved_signature, _ = validate_and_resolve_signature(
                self.signature,
                *args,
                __pre_resolved_types__=__pre_resolved_types__,
                __enforce_output_type__=False,
                **kwargs,
            )
            # get the scalar value
            value = self.fn(**kwargs_)
            if in_graph:
                out = const(value, tp=resolved_signature.output_type.py_type)
                return out
            else:
                return ValueTuple(value=value)
