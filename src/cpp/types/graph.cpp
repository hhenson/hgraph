#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

#include <utility>

namespace hgraph {
    /*
    def __init__(self, graph_id: tuple[int, ...], nodes: Iterable[Node], parent_node: Node = None, label: str = None):
        super().__init__()
        self._graph_id: tuple[int, ...] = graph_id
        self._nodes: list[Node] = nodes if type(nodes) is list else list(nodes)
        self._schedule: list[datetime, ...] = [MIN_DT] * len(nodes)
        self._evaluation_engine: EvaluationEngine | None = None
        self._parent_node: Node = parent_node
        self._label: str = label
     */

    Graph::Graph(
        std::vector<int64_t> graph_id_,
        std::vector<Node *> nodes_, std::optional<Node *> parent_node_,
        std::optional<std::string> label_) : ComponentLifeCycle(),
    graph_id{std::move(graph_id_)},
    nodes{std::move(nodes_)},
    parent_node{std::move(parent_node_)},
    label{std::move(label_)}{
        auto it{std::find_if(nodes_.begin(), nodes_.end(), [](const Node* v) {
            return v->signature.node_type != NodeTypeEnum::PUSH_SOURCE_NODE;
        })};
        _push_source_nodes_end = std::distance(nodes.begin(), it);
    }

    EvaluationEngineApi & Graph::evaluation_engine_api() const {
        return *_evaluation_engine;
    }

    EvaluationClock & Graph::evaluation_clock() const {
        return _evaluation_engine->engine_evaluation_clock();
    }

    EvaluationEngine & Graph::evaluation_engine() const {
        return *_evaluation_engine;
    }

    void Graph::set_evaluation_engine(EvaluationEngine *value) {
        _evaluation_engine = value;
    }

    int64_t Graph::push_source_nodes_end() const {
        return _push_source_nodes_end;
    }

    void Graph::schedule_node(int64_t node_ndx, engine_time_t when) {
    }

    std::vector<engine_time_t> & Graph::schedule() {
    }

    void Graph::evaluation_graph() {
    }

    std::unique_ptr<Graph> Graph::copy_with(std::vector<Node *> nodes) {
    }

}
