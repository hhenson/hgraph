from datetime import datetime
from typing import TYPE_CHECKING

from hgraph._runtime._node import NodeTypeEnum

if TYPE_CHECKING:
    from hgraph import Graph, EvaluationClock
    from hgraph import Node

__all__ = ("EvaluationTrace",)

from hgraph import EvaluationLifeCycleObserver


class EvaluationTrace(EvaluationLifeCycleObserver):

    def _print(self, evaluation_clock: "EvaluationClock", msg: str) -> None:
        print(f"[{datetime.utcnow()}][{evaluation_clock.evaluation_time}] {msg}")

    def _graph_name(self, graph: "Graph") -> str:
        return f"[{graph.parent_node.signature.name}<{', '.join(str(i) for i in graph.graph_id)}>]" \
            if graph.parent_node else "[root]"

    def _print_graph(self, graph: "Graph", msg: str) -> None:
        parent_details = self._graph_name(graph)
        self._print(graph.evaluation_clock, f"{parent_details} {msg}")

    def _print_node(self, node: "Node", msg: str, add_input: bool = False, add_output: bool = False) -> None:
        node_signature = f"[{node.signature.name}<{', '.join(str(i) for i in node.node_id)}>("
        if node.signature.time_series_inputs:
            if add_input:
                inputs = node.inputs
                node_signature += ", ".join(
                    f"{f'*{arg}*' if (mod_ := (in_ := inputs[arg]).modified) else arg}={(in_.delta_value if mod_ else in_.value) if in_.valid else '<UnSet>'}"
                    for
                    arg in
                    node.signature.time_series_inputs.keys())
            else:
                node_signature += "..."
        node_signature += ")"
        if node.signature.time_series_output and add_output:
            mod_ = node.output.modified
            value_ = node.output.delta_value if mod_ else node.output.value
            node_signature += (f"{' *->* ' if mod_ else ' -> '}"
                               f"{value_ if node.output.valid else '<UnSet>'}")
        self._print(node.graph.evaluation_clock,
                    f"{self._graph_name(node.graph)} {node_signature} {msg}")

    def on_before_start_graph(self, graph: "Graph"):
        self._print_graph(graph, f">> {'.' * 15} Starting Graph {'.' * 15}")

    def on_after_start_graph(self, graph: "Graph"):
        self._print_graph(graph, f"<< {'.' * 15} Started Graph {'.' * 15}")

    def on_before_start_node(self, node: "Node"):
        # self._print_node(node, "Starting node")
        ...

    def on_after_start_node(self, node: "Node"):
        self._print_node(node, "Started node", add_output=True)

    def on_before_graph_evaluation(self, graph: "Graph"):
        self._print_graph(graph, f"{'>' * 20} Eval Start {'>' * 20}")

    def on_before_node_evaluation(self, node: "Node"):
        if node.signature.node_type in (NodeTypeEnum.PULL_SOURCE_NODE, NodeTypeEnum.PUSH_SOURCE_NODE):
            return
        self._print_node(node, "[IN]", add_input=True)

    def on_after_node_evaluation(self, node: "Node"):
        if node.signature.node_type in (NodeTypeEnum.SINK_NODE,):
            return
        self._print_node(node, "[OUT]", add_output=True)

    def on_after_graph_evaluation(self, graph: "Graph"):
        self._print_graph(graph, f"{'<' * 20} Eval Done {'<' * 20}")

    def on_before_stop_node(self, node: "Node"):
        # self._print_node(node, "Stopping node")
        ...

    def on_after_stop_node(self, node: "Node"):
        self._print_node(node, "Stopped node")

    def on_before_stop_graph(self, graph: "Graph"):
        self._print_graph(graph, "vvvvvvv Graph stopping -------")

    def on_after_stop_graph(self, graph: "Graph"):
        self._print_graph(graph, "------- Graph stopped  vvvvvvv")
