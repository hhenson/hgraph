import sys
from collections import namedtuple
from contextlib import AbstractContextManager
from typing import Mapping, Any, TYPE_CHECKING

from frozendict import frozendict

from hgraph._runtime._global_state import GlobalState
from hgraph._types import TS, SCALAR, TIME_SERIES_TYPE, REF, STATE, HgREFTypeMetaData, clone_typevar
from hgraph._wiring._decorators import graph, sink_node, pull_source_node
from hgraph._wiring._wiring_node_class import BaseWiringNodeClass, create_input_output_builders
from hgraph._wiring._wiring_port import WiringPort

if TYPE_CHECKING:
    from hgraph import WiringNodeInstance


__all__ = ("TimeSeriesContextTracker", "CONTEXT_TIME_SERIES_TYPE")


CONTEXT_TIME_SERIES_TYPE = clone_typevar(TIME_SERIES_TYPE, name="CONTEXT_TIME_SERIES_TYPE")

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
            elif not tp.is_scalar and tp.matches(details.context.output_type):
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
    state.path = f"context-{source.owning_node.owning_graph_id}-{path}"
    GlobalState.instance()[state.path] = ts


@capture_context.stop
def capture_context_stop(path: str, state: STATE):
    """Clean up references"""
    del GlobalState.instance()[state.path]


class ContextNodeClass(BaseWiringNodeClass):

    def create_node_builder_instance(
        self, node_signature: "NodeSignature", scalars: Mapping[str, Any]
    ) -> "NodeBuilder":
        output_type = node_signature.time_series_output
        if type(output_type) is not HgREFTypeMetaData:
            node_signature = node_signature.copy_with(time_series_output=HgREFTypeMetaData(output_type))

        from hgraph._impl._builder import PythonNodeImplNodeBuilder

        input_builder, output_builder, error_builder = create_input_output_builders(
            node_signature, self.error_output_type
        )

        from hgraph._impl._runtime._node import BaseNodeImpl

        class _PythonContextStubSourceNode(BaseNodeImpl):
            def do_eval(self):
                """The service must be available by now, so we can retrieve the output reference."""
                from hgraph._runtime._global_state import GlobalState

                path = f'context-{self.owning_graph_id[:self.scalars["depth"]]}-{self.scalars["path"]}'
                shared = GlobalState.instance().get(path)

                from hgraph import TimeSeriesOutput

                if shared is None:
                    raise RuntimeError(f"Missing shared output for path: {path}")
                elif isinstance(shared, TimeSeriesOutput):
                    output = shared
                elif shared.has_peer:  # it is a reference with a peer so its value might update
                    output = shared.output
                else:
                    output = None

                if output:
                    output.subscribe(self)
                    if self.subscribed_output is not None and self.subscribed_output is not output:
                        self.subscribed_output.unsubscribe(self)
                    self.subscribed_output = output

                # NOTE: The output needs to be a reference value output so we can set the value and continue!
                self.output.value = shared.value  # might be none

            def do_start(self):
                """Make sure we get notified to serve the reference"""
                self.subscribed_output = None
                self.notify()

            def do_stop(self):
                if self.subscribed_output is not None:
                    self.subscribed_output.unsubscribe(self)

        return PythonNodeImplNodeBuilder(
            signature=node_signature,
            scalars=scalars,
            input_builder=input_builder,
            output_builder=output_builder,
            error_builder=error_builder,
            node_impl=_PythonContextStubSourceNode,
        )


@pull_source_node(node_impl=ContextNodeClass)
def get_context_output(path: str, depth: int) -> REF[CONTEXT_TIME_SERIES_TYPE]:
    """Uses the special node to extract a context output from the global state."""


def enter_ts_context(context: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext

    frame = sys._getframe(2)
    TimeSeriesContextTracker.instance().enter_context(context, WiringNodeInstanceContext.instance(), frame)
    return context


WiringPort.__enter__ = lambda s: enter_ts_context(s)


def exit_ts_context(context: TIME_SERIES_TYPE):
    TimeSeriesContextTracker.instance().exit_context(context)


WiringPort.__exit__ = lambda s, exc_type, exc_val, exc_tb: exit_ts_context(s)
