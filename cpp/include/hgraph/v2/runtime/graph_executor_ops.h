//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_EVALUATION_ENGINE_OPS_H
#define HGRAPH_CPP_ROOT_EVALUATION_ENGINE_OPS_H

#include <hgraph/runtime/graph_executor.h>

#include <hgraph/util/date_time.h>
#include <hgraph/v2/runtime/evaluation_context.h>
#include <hgraph/v2/types/graph/graph.h>
#include <hgraph/v2/types/graph/node.h>

namespace hgraph::v2
{
    struct GraphExecutorOps
    {
        void set_evaluation_time(void *data, engine_time_t et);
        void update_next_scheduled_evaluation_time(void *data, engine_time_t et);

        [[nodiscard]] engine_time_t next_scheduled_evaluation_time(void *data) const;

        void               advance_to_next_scheduled_time(void *data);
        void               mark_push_node_requires_scheduling(void *data);
        [[nodiscard]] bool push_node_requires_scheduling(void *data) const;
        void               reset_push_node_requires_scheduling(void *data);

        const EvaluationClock evaluation_clock(void *data);

        const EvaluationClock evaluation_clock(void *data) const {
            return const_cast<GraphExecutorOps *>(this)->evaluation_clock(data);
        }

        Graph build_graph(void *data);

        void advance_engine_time(void *data);

        void notify_before_evaluation(void *data);

        void notify_after_evaluation(void *data);

        void notify_before_start_graph(void *data, Graph graph);

        void notify_after_start_graph(void *data, Graph graph);

        void notify_before_start_node(void *data, Node node);

        void notify_after_start_node(void *data, Node node);

        void notify_before_graph_evaluation(void *data, Graph graph);

        void notify_after_graph_evaluation(void *data, Graph graph);

        void notify_after_push_nodes_evaluation(void *data, Graph graph);

        void notify_before_node_evaluation(void *data, Node node);

        void notify_after_node_evaluation(void *data, Node node);

        void notify_before_stop_node(void *data, Node node);

        void notify_after_stop_node(void *data, Node node);

        void notify_before_stop_graph(void *data, Graph graph);

        void notify_after_stop_graph(void *data, Graph graph);
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_EVALUATION_ENGINE_OPS_H
