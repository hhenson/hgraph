from hgraph._impl._runtime._nested_graph_node import PythonNestedGraphNodeImpl
from hgraph._runtime._node import Node
from hgraph._types._tsb_meta_data import HgTSBTypeMetaData


class PythonTryExceptNodeImpl(PythonNestedGraphNodeImpl):
    def _wire_outputs(self):
        if self.output_node_id:
            node: Node = self._active_graph.nodes[self.output_node_id]
            # Replace the nodes output with the map node's output
            node.output = self.output.out

    def eval(self):
        try:
            super().eval()
        except Exception as e:
            from hgraph._types._error_type import NodeError

            err = NodeError.capture_error(e, self)
            if type(self.signature.time_series_output) is HgTSBTypeMetaData:
                self.output.exception.value = err
            else:
                self.output.value = err

            self._active_graph.stop()
