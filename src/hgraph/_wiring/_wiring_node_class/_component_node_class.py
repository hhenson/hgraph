from dataclasses import dataclass, field
from typing import Callable, TypeVar, Optional

from frozendict import frozendict

from hgraph import (
    merge,
    RecordReplayEnum,
    RecordReplayContext,
    record,
    replay,
    compare,
    replay_const,
)
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._decorators import graph
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._wiring_node_class import (
    BaseWiringNodeClass,
    create_input_output_builders,
    validate_and_resolve_signature,
)
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_port import WiringPort
from hgraph._wiring._wiring_utils import wire_nested_graph, extract_stub_node_indices

__all__ = ("ComponentNodeClass",)


@dataclass(frozen=True)
class ComponentWiringNodeSignature(WiringNodeSignature):
    inner_graph: Optional["GraphBuilder"] = field(default=None, hash=False, compare=False)


class ComponentNodeClass(BaseWiringNodeClass):
    """The outer component node"""

    def __init__(self, signature: WiringNodeSignature, fn: Callable):
        if not any(isinstance(tp, HgTimeSeriesTypeMetaData) for tp in signature.input_types.values()):
            raise SyntaxError(f"Component '{signature.signature}' has no time-series inputs")
        if not signature.output_type:
            raise SyntaxError(f"Component '{signature.signature}' has no output type")
        self._nested_graph_signature = signature
        self._nested_graph: Callable = wrap_component(fn, signature)
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
            nested_graph, ri = wire_nested_graph(
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
            if any(i for i in ri):
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
            signature=node_signature,
            scalars=scalars,
            input_builder=input_builder,
            output_builder=output_builder,
            error_builder=error_builder,
            recordable_state_builder=None,
            nested_graph=resolved_wiring_signature.inner_graph,
            input_node_ids=frozendict(nested_graph_input_ids),
            output_node_id=nested_graph_output_id,
        )


def wrap_component(fn: Callable, signature: WiringNodeSignature) -> Callable:
    def component_wrapper(**kwargs):
        kwargs_ = {}
        for arg in signature.args:
            if arg in signature.time_series_inputs:
                kwargs_[arg] = input_wrapper(kwargs[arg], arg)
            else:
                kwargs_[arg] = kwargs[arg]
        out = fn(**kwargs_)
        return output_wrapper(out)

    return component_wrapper


@graph
def input_wrapper(ts: TIME_SERIES_TYPE, key: str) -> TIME_SERIES_TYPE:
    mode = RecordReplayContext.instance().mode
    if RecordReplayEnum.RECOVER in mode:
        ts = merge(ts, replay_const(key, ts.output_type.dereference().py_type))
    if ((RecordReplayEnum.REPLAY | RecordReplayEnum.COMPARE) & mode) != RecordReplayEnum.NONE:
        if RecordReplayEnum.RECOVER in mode:
            raise RuntimeError("Can't recover and replay / compare at the same time")
        ts: WiringPort
        ts = replay(key, ts.output_type.dereference().py_type)
    if RecordReplayEnum.RECORD in mode:
        record(ts, key)
    return ts


@graph
def output_wrapper(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    mode = RecordReplayContext.instance().mode
    if RecordReplayEnum.RECORD in mode:
        record(ts, "__out__")
    if RecordReplayEnum.REPLAY_OUTPUT in mode:
        ts: WiringPort
        ts = replay("__out__", ts.output_type.dereference().py_type)
    if RecordReplayEnum.COMPARE in mode:
        compare(ts, replay("__out__", ts.output_type.dereference().py_type))
    return ts
