import sys
from collections import namedtuple
from contextlib import AbstractContextManager
from typing import Mapping, Any, TYPE_CHECKING, Type

from hgraph._runtime._global_keys import context_output_key
from hgraph._runtime._global_state import GlobalState
from hgraph._types import (
    TIME_SERIES_TYPE,
    REF,
    STATE,
    clone_type_var,
    AUTO_RESOLVE,
    HgTimeSeriesTypeMetaData,
)
from hgraph._wiring._decorators import graph, sink_node, pull_source_node
from hgraph._wiring._wiring_node_class import BaseWiringNodeClass, create_input_output_builders
from hgraph._wiring._wiring_port import WiringPort

if TYPE_CHECKING:
    pass


__all__ = ("TimeSeriesContextTracker", "CONTEXT_TIME_SERIES_TYPE", "get_context")


CONTEXT_TIME_SERIES_TYPE = clone_type_var(TIME_SERIES_TYPE, name="CONTEXT_TIME_SERIES_TYPE")

ContextInfo = namedtuple(
    "ContextInfo", ["context", "scope", "depth", "path", "frame", "inner_graph_use", "wiring_context"]
)


class TimeSeriesContextTracker(AbstractContextManager):
    __instance__ = None
    __counter__ = 0

    def __init__(self):
        self.contexts: list[ContextInfo] = []

    @classmethod
    def instance(cls):
        if cls.__instance__ is None:
            cls.__instance__ = cls()
        return cls.__instance__

    def __enter__(self):
        self.__instance__ = self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__instance__ == self:
            self.__instance__ = None

    def enter_context(self, context, graph_scope, frame):
        from hgraph import WiringGraphContext

        wiring_context = WiringGraphContext(None)
        wiring_context.__enter__()

        self.__counter__ += 1
        path = f"{context.output_type}-{self.__counter__}"
        self.contexts.append(
            ContextInfo(context, graph_scope, graph_scope.graph_nesting_depth(), path, frame, {}, wiring_context)
        )

        return path

    def exit_context(self, context, capture=True):
        details = self.contexts.pop()
        if details.inner_graph_use and capture:
            from hgraph import WiringGraphContext

            context_capture: WiringPort = capture_context(details.path, details.context, __return_sink_wp__=True)
            clients = details.wiring_context.remove_context_clients(details.path, details.depth)
            for c in clients:
                c.add_indirect_dependency(context_capture.node_instance)

        details.wiring_context.__exit__(None, None, None)

    def _find_context_details(self, tp, graph_scope, name=None):
        for details in reversed(self.contexts):
            if tp.is_scalar and tp.matches(details.context.output_type.dereference().scalar_type()):
                match = True
            elif not tp.is_scalar and tp.matches(details.context.output_type.dereference()):
                match = True
            else:
                match = False

            if match:
                if name:
                    if name not in details.frame.f_locals or details.frame.f_locals[name] is not details.context:
                        continue

                return details

        return None

    def find_context(self, tp, graph_scope, name=None):
        if details := self._find_context_details(tp, graph_scope, name):
            return details.context

        return None

    def get_context(self, tp, graph_scope, name=None):
        from hgraph import CONTEXT_TIME_SERIES_TYPE, WiringGraphContext

        if details := self._find_context_details(tp, graph_scope, name):
            if graph_scope == details.scope:  # the consumer is on the same graph as the producer
                return details.context
            else:
                details.inner_graph_use[graph_scope.graph_nesting_depth()] = True
                port = get_context_output[CONTEXT_TIME_SERIES_TYPE : details.context.output_type](
                    details.path, details.depth - 1
                )
                WiringGraphContext.instance().register_context_client(details.path, details.depth, port.node_instance)
                return port

        return None

    def max_context_rank(self, scope):
        # we are making an assumption here that the rank of the capture_output_to_global_state node
        # is always 1 higher than the rank of the context manager node
        return max(0, 0, *(c[0].rank + 1 for c in self.contexts if c[1] is scope))


@sink_node(active=tuple(), valid=tuple())
def capture_context(path: str, ts: REF[TIME_SERIES_TYPE], state: STATE = None):
    """
    This node serves to capture the output of a context node and record the output reference in the global state
    with a prefix that would allow to distinguish it from same context node wired in other branches (if created on a branch).
    """


@capture_context.start
def capture_context_start(path: str, ts: REF[TIME_SERIES_TYPE], state: STATE):
    """Place the reference into the global state"""
    source = ts.output or ts.value.output
    state.path = context_output_key(source.owning_node.owning_graph_id, path)
    GlobalState.instance()[state.path] = ts


@capture_context.stop
def capture_context_stop(path: str, state: STATE):
    """Clean up references"""
    del GlobalState.instance()[state.path]


class ContextNodeClass(BaseWiringNodeClass):

    CONTEXT_STUB: "Node" = None

    def create_node_builder_instance(
        self,
        resolved_wiring_signature: "WiringNodeSignature",
        node_signature: "NodeSignature",
        scalars: Mapping[str, Any],
    ) -> "NodeBuilder":
        node_signature = node_signature.copy_with(time_series_output=node_signature.time_series_output.as_reference())

        input_builder, output_builder, error_builder = create_input_output_builders(
            node_signature, self.error_output_type
        )

        if ContextNodeClass.BUILDER_CLASS is None:
            from hgraph._impl._builder._node_impl_builder import PythonNodeImplNodeBuilder
            ContextNodeClass.BUILDER_CLASS = PythonNodeImplNodeBuilder

        if ContextNodeClass.CONTEXT_STUB is None:
            from hgraph._impl._runtime._context_node import PythonContextStubSourceNode
            ContextNodeClass.CONTEXT_STUB = PythonContextStubSourceNode

        return ContextNodeClass.BUILDER_CLASS(
            signature=node_signature,
            scalars=scalars,
            input_builder=input_builder,
            output_builder=output_builder,
            error_builder=error_builder,
            node_impl=ContextNodeClass.CONTEXT_STUB,
        )


@pull_source_node(node_impl=ContextNodeClass)
def get_context_output(path: str, depth: int) -> REF[CONTEXT_TIME_SERIES_TYPE]:
    """Uses the special node to extract a context output from the global state."""


@graph
def get_context(name: str, tp_: Type[CONTEXT_TIME_SERIES_TYPE] = AUTO_RESOLVE) -> CONTEXT_TIME_SERIES_TYPE:
    from hgraph import WiringNodeInstanceContext

    tp = HgTimeSeriesTypeMetaData.parse_type(tp_)
    return TimeSeriesContextTracker.instance().get_context(tp, WiringNodeInstanceContext.instance(), name)


def enter_ts_context(context: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext

    frame = sys._getframe(2)
    TimeSeriesContextTracker.instance().enter_context(context, WiringNodeInstanceContext.instance(), frame)
    return context


WiringPort.__enter__ = lambda s: enter_ts_context(s)


def exit_ts_context(context: TIME_SERIES_TYPE):
    TimeSeriesContextTracker.instance().exit_context(context)


WiringPort.__exit__ = lambda s, exc_type, exc_val, exc_tb: exit_ts_context(s)
