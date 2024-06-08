//
// Created by Howard Henson on 05/05/2024.
//

#ifndef GRAPH_H
#define GRAPH_H

#include<hgraph/util/lifecycle.h>
#include<optional>
#include<vector>

#include "hgraph/runtime/evaluation_engine.h"

namespace hgraph {
    struct Node;

    struct HGRAPH_EXPORT Graph : ComponentLifeCycle {
        Graph(std::vector<int64_t> graph_id_, std::vector<Node *> nodes_, std::optional<Node *> parent_node_,
              std::optional<std::string> label_);

        std::vector<int64_t> graph_id;
        std::vector<Node *> nodes;
        std::optional<Node *> parent_node;
        std::optional<std::string> label;

        [[nodiscard]] EvaluationEngineApi &evaluation_engine_api() const;

        [[nodiscard]] EvaluationClock &evaluation_clock() const;

        [[nodiscard]] EvaluationEngine &evaluation_engine() const;

        void set_evaluation_engine(EvaluationEngine *value);

        int64_t push_source_nodes_end() const;

        void schedule_node(int64_t node_ndx, engine_time_t when);

        std::vector<engine_time_t> &schedule();

        void evaluation_graph();

        std::unique_ptr<Graph> copy_with(std::vector<Node *> nodes);
    private:
        EvaluationEngine *_evaluation_engine;
        int64_t _push_source_nodes_end;
    };
}

#endif //GRAPH_H
