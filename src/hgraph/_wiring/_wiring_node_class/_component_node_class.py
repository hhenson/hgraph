from dataclasses import dataclass, field
from typing import Callable, TypeVar, Optional

from frozendict import frozendict

from hgraph import wire_nested_graph, extract_stub_node_indices, HgTimeSeriesTypeMetaData, HgTypeMetaData, WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._wiring_node_class import (
    BaseWiringNodeClass,
    create_input_output_builders,
    validate_and_resolve_signature,
)
from hgraph._wiring._wiring_node_signature import WiringNodeSignature

__all__ = ("ComponentNodeClass",)


@dataclass(frozen=True)
class ComponentWiringNodeSignature(WiringNodeSignature):
    inner_graph: Optional["GraphBuilder"] = field(default=None, hash=False, compare=False)


class ComponentNodeClass(BaseWiringNodeClass):
    """The outer try_except node"""

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        if not any(isinstance(tp, HgTimeSeriesTypeMetaData) for tp in signature.input_types.values()):
            raise SyntaxError(f"Component '{signature.signature}' has no time-series inputs")
        if not signature.output_type:
            raise SyntaxError(f"Component '{signature.signature}' has no output type")
        self._nested_graph_signature = signature
        self._nested_graph: Callable = fn
        signature = signature.copy_with(
            input_types=frozendict({k: v.as_reference() for k, v in signature.input_types.items()}),
            output_type=signature.output_type.as_reference(),
        )
        super().__init__(signature, None)

    def __call__(
        self,
        *args,
        __pre_resolved_types__: dict[TypeVar, HgTypeMetaData] = None,
        __return_sink_wp__: bool = False,
        **kwargs,
    ) -> "WiringPort":
        # Resolve the inner graph signature first
        with WiringContext(current_wiring_node=self, current_signature=self._nested_graph_signature):
            kwargs_, resolved_signature, _ = validate_and_resolve_signature(
                self._nested_graph_signature, *args, __pre_resolved_types__=__pre_resolved_types__, **kwargs
            )
            nested_graph, ss, cc = wire_nested_graph(
                self._nested_graph,
                resolved_signature.input_types,
                {
                    k: kwargs_[k]
                    for k in resolved_signature.input_types.keys()
                    if k not in resolved_signature.time_series_args
                },
                self.signature,
                None,
                # input_stub_fn=...,
                # output_stub_fn=...,
            )
            if ss or cc:
                raise CustomMessageWiringError("Components cannot have contexts or services contained in the code.")

        signature = self.signature
        try:
            # OK, let's swap out the generic signature for one with the nested graph included
            self.signature = ComponentWiringNodeSignature(inner_graph=nested_graph, **signature.as_dict())
            # Then resolve the outer node and return the wiring port
            return super().__call__(
                *args, __pre_resolved_types__=__pre_resolved_types__, __return_sink_wp__=__return_sink_wp__, **kwargs
            )
        finally:
            # Now put it back to its original state, so it can be used again
            self.signature = signature

    def create_node_builder_instance(
        self,
        resolved_wiring_signature: "ComponentWiringNodeSignature",
        node_signature: "NodeSignature",
        scalars: "NodeSignature",
    ) -> "NodeBuilder":

        nested_graph_input_ids, nested_graph_output_id = extract_stub_node_indices(
            resolved_wiring_signature.inner_graph, resolved_wiring_signature.time_series_args
        )

        input_builder, output_builder, error_builder = create_input_output_builders(
            node_signature, self.error_output_type
        )
        from hgraph._impl._builder._component_builder import PythonComponentNodeBuilder

        return PythonComponentNodeBuilder(
            node_signature,
            scalars,
            input_builder,
            output_builder,
            error_builder,
            resolved_wiring_signature.inner_graph,
            frozendict(nested_graph_input_ids),
            nested_graph_output_id,
        )
