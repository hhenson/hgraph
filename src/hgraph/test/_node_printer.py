import logging
from typing import TYPE_CHECKING

from hgraph._runtime._constants import MAX_ET
from hgraph._types import HgSchedulerType, Injector
from hgraph._builder import Builder

if TYPE_CHECKING:
    from hgraph import Graph, EvaluationClock
    from hgraph import Node

__all__ = ("EvaluationTrace",)

from hgraph import EvaluationLifeCycleObserver

logger = logging.getLogger(__name__)


class EvaluationTrace(EvaluationLifeCycleObserver):
    """
    Logs out the different steps as the engine evaluates the graph. This is voluminous but can be helpful tracing
    down unexpected behaviour.

    filter
        Used to restrict which node and graph events to report using a simple containment check of the node and
        its path.

    start
        Log start related events.

    eval
        Log eval related events.

    stop
        Log stop related events

    node
        Log node related events

    graph
        Log graph related events

    """

    _PRINT_ALL_VALUES: bool = False
    _USE_LOGGER: bool = True

    @staticmethod
    def set_print_all_values(value: bool):
        """To see all values (not only ticked ones) in the trace set this to True."""
        EvaluationTrace._PRINT_ALL_VALUES = value

    @staticmethod
    def set_use_logger(value: bool):
        """To use print instead of the logger set this to False."""
        EvaluationTrace._USE_LOGGER = value

    def __init__(
        self,
        filter: str = None,
        start: bool = True,
        eval: bool = True,
        stop: bool = True,
        node: bool = True,
        graph: bool = True,
    ):
        self.filter = filter
        self.start = start
        self.eval = eval
        self.stop = stop
        self.node = node
        self.graph = graph

    def _print(self, evaluation_clock: "EvaluationClock", msg: str) -> None:
        m = f"[{evaluation_clock.evaluation_time}] {msg}"
        if EvaluationTrace._USE_LOGGER:
            logger.info(m)
        else:
            print(m, flush=True)

    def _graph_name(self, graph: "Graph") -> str:
        graph_str = []
        while graph:
            if graph.parent_node:
                graph_str.append(
                    f"{graph.parent_node.signature.name}<{', '.join(str(i) for i in graph.graph_id)}>"
                    + f"{(':' + graph.parent_node.signature.label) if graph.parent_node.signature.label else ''}"
                )
                graph = graph.parent_node.graph
            else:
                graph = None
        return f"[{'::'.join(reversed(graph_str))}]"

    def _print_graph(self, graph: "Graph", msg: str) -> None:
        parent_details = self._graph_name(graph)
        self._print(graph.evaluation_clock, f"{parent_details} {msg}")

    def _print_signature(self, node: "Node"):
        node_signature = f"{node.signature.signature}"
        self._print(node.graph.evaluation_clock, f"{self._graph_name(node.graph)} Starting: {node_signature}")

    def _print_node(
        self,
        node: "Node",
        msg: str,
        add_input: bool = False,
        add_output: bool = False,
        add_scheduled_time: bool = False,
    ) -> None:
        node_signature = self._node_name(node)
        if node.signature.time_series_inputs:
            if add_input:
                inputs = node.inputs
                node_signature += ", ".join(
                    f"{f'*{arg}*' if (mod_ := (in_ := inputs[arg]).modified) else arg}={(in_.delta_value if (mod_ or self._PRINT_ALL_VALUES) else '') if in_.valid else '<UnSet>'}"
                    for arg in node.signature.time_series_inputs.keys()
                )
                if node.signature.uses_scheduler:
                    scheduler_arg, scheduler_value = [
                        (k, v) for k, v in node.signature.scalars.items() if isinstance(v, HgSchedulerType)
                    ][0]
                    node_signature += (
                        f", *{scheduler_arg}*" if node.scheduler.is_scheduled_now else f", {scheduler_arg}"
                    )
            else:
                node_signature += "..."
        node_signature += ")"
        if node.signature.time_series_output and add_output:
            mod_ = node.output.modified if node.output else False
            value_ = (
                (node.output.delta_value if mod_ else node.output.value)
                if (node.output and node.output.valid)
                else "<UnSet>"
            )
            node_signature += f"{' *->* ' if mod_ else ' -> '}{value_}"
        if add_scheduled_time:
            scheduled_msg = f" SCHED[{node.scheduler.next_scheduled_time}]"
        else:
            scheduled_msg = ""
        self._print(
            node.graph.evaluation_clock, f"{self._graph_name(node.graph)} {node_signature} {msg}{scheduled_msg}"
        )

    def _node_name(self, node):
        node_signature = (
            f"[{node.signature.wiring_path_name}."
            f"{(node.signature.label + ':') if node.signature.label else ''}"
            f"{node.signature.name}<{', '.join(str(i) for i in node.node_id)}>("
        )
        return node_signature

    def on_before_start_graph(self, graph: "Graph"):
        if self.start and self.graph and (self.filter is None or self.filter in self._graph_name(graph)):
            self._print_graph(graph, f">> {'.' * 15} Starting Graph {graph.label} {'.' * 15}")

    def on_after_start_graph(self, graph: "Graph"):
        if self.start and self.graph and (self.filter is None or self.filter in graph.label):
            self._print_graph(graph, f"<< {'.' * 15} Started Graph {'.' * 15}")

    def on_before_start_node(self, node: "Node"):
        if self.start and self.node and (self.filter is None or self.filter in self._node_name(node)):
            self._print_signature(node)

    def on_after_start_node(self, node: "Node"):
        if self.start and self.node and (self.filter is None or self.filter in self._node_name(node)):
            fmt_id = lambda id: f"<{', '.join(str(i) for i in id)}>"
            inputs = ", ".join(
                f"{k}: {fmt_id(n.output.owning_node.node_id) if n.output else '?'}" for k, n in node.inputs.items()
            )
            scalars = ", ".join(f"{k}: {v}" for k, v in node.scalars.items() if not isinstance(v, (Injector, Builder)))
            and_ = " and " if inputs and scalars else ""
            self._print_node(node, f"Started node with {inputs}{and_}{scalars}", add_output=True)

    def on_before_graph_evaluation(self, graph: "Graph"):
        if self.eval and self.graph and (self.filter is None or self.filter in self._graph_name(graph)):
            self._print_graph(graph, f"{'>' * 20} Eval Start {graph.label} {'>' * 20}")

    def on_before_node_evaluation(self, node: "Node"):
        if node.signature.is_source_node:
            return
        if self.eval and self.node and (self.filter is None or self.filter in self._node_name(node)):
            self._print_node(node, "[IN]", add_input=True)

    def on_after_node_evaluation(self, node: "Node"):
        if node.signature.is_sink_node:
            return
        if self.eval and self.node and (self.filter is None or self.filter in self._node_name(node)):
            self._print_node(
                node,
                "[OUT]",
                add_output=True,
                add_scheduled_time=node.signature.uses_scheduler
                and node.scheduler.next_scheduled_time == node.graph.schedule[node.node_ndx],
            )

    def on_after_graph_evaluation(self, graph: "Graph"):
        if self.eval and self.graph and (self.filter is None or self.filter in self._graph_name(graph)):
            if (
                graph.parent_node is not None
                and (nt := graph.parent_node.graph.schedule[graph.parent_node.node_ndx])
                > graph.evaluation_clock.evaluation_time
                and nt < MAX_ET
            ):
                next_scheduled = f" NEXT[{nt}]"
            elif graph.parent_node is None:
                next_scheduled = f" NEXT[{graph.evaluation_clock.next_scheduled_evaluation_time}]"
            else:
                next_scheduled = ""
            self._print_graph(graph, f"{'<' * 20} Eval Done {'<' * 20}{next_scheduled}")

    def on_before_stop_node(self, node: "Node"):
        # self._print_node(node, "Stopping node")
        ...

    def on_after_stop_node(self, node: "Node"):
        if self.stop and self.node and (self.filter is None or self.filter in self._node_name(node)):
            self._print_node(node, "Stopped node")

    def on_before_stop_graph(self, graph: "Graph"):
        if self.stop and self.graph and (self.filter is None or self.filter in self._graph_name(graph)):
            self._print_graph(graph, "vvvvvvv Graph stopping -------")

    def on_after_stop_graph(self, graph: "Graph"):
        if self.stop and self.graph and (self.filter is None or self.filter in self._graph_name(graph)):
            self._print_graph(graph, "------- Graph stopped  vvvvvvv")
