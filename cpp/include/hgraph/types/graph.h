//
// Created by Howard Henson on 05/05/2024.
//

#ifndef GRAPH_H
#define GRAPH_H

#include <hgraph/api/python/api_ptr.h>

#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/traits.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <hgraph/util/sender_receiver_state.h>
#include <memory>

namespace hgraph
{
    struct HGRAPH_EXPORT Graph : ComponentLifeCycle, arena_enable_shared_from_this<Graph>
    {
        using ptr = Graph*;
        using s_ptr = std::shared_ptr<Graph>;
        using node_list = std::vector<node_s_ptr>;

        Graph(std::vector<int64_t> graph_id_, node_list nodes_,
              std::optional<node_ptr> parent_node_, std::string label_, const_traits_ptr parent_traits_);

        ~Graph() override;

        /**
         * Get the control block for this graph.
         * Extracts the control block from shared_from_this() to be used as donor for child objects.
         */
        //[[nodiscard]] control_block_ptr control_block() const;

        [[nodiscard]] const std::vector<int64_t> &graph_id() const;

        [[nodiscard]] const node_list &nodes() const;

        [[nodiscard]] node_ptr parent_node() const;

        [[nodiscard]] std::optional<std::string> label() const;

        [[nodiscard]] EvaluationEngineApi::s_ptr evaluation_engine_api();

        [[nodiscard]] EvaluationClock::s_ptr evaluation_clock();

        [[nodiscard]] EvaluationClock::s_ptr evaluation_clock() const;

        [[nodiscard]] const EngineEvaluationClock::s_ptr& evaluation_engine_clock();

        [[nodiscard]] const EvaluationEngine::s_ptr& evaluation_engine() const;

        void set_evaluation_engine(EvaluationEngine::s_ptr value);

        int64_t push_source_nodes_end() const;

        engine_time_t last_evaluation_time() const;

        void schedule_node(int64_t node_ndx, engine_time_t when);

        void schedule_node(int64_t node_ndx, engine_time_t when, bool force_set);

        std::vector<engine_time_t> &schedule();

        void evaluate_graph();

        s_ptr copy_with(node_list nodes);

        /**
         * Clone traits from another graph (copies the traits data).
         */
        void clone_traits_from(const Graph &other);

        /**
         * Get traits as a const reference.
         * Traits is stored as a value object inside Graph.
         */
        [[nodiscard]] const Traits &traits() const;

        [[nodiscard]] SenderReceiverState &receiver();

        void extend_graph(const GraphBuilder &graph_builder, bool delay_start = false);

        void reduce_graph(const GraphBuilder &graph_builder, int64_t start_node);

        void initialise_subgraph(int64_t start, int64_t end);

        void start_subgraph(int64_t start, int64_t end);

        void stop_subgraph(int64_t start, int64_t end);

        void dispose_subgraph(const GraphBuilder &graph_builder, int64_t start, int64_t end);

        // Performance: Cached clock pointer and evaluation time reference set during initialization
        [[nodiscard]] EngineEvaluationClock *cached_engine_clock() const { return _cached_engine_clock; }
        [[nodiscard]] const engine_time_t   *cached_evaluation_time_ptr() const { return _cached_evaluation_time_ptr; }

        // Performance: Direct access to evaluation time without shared_ptr overhead
        [[nodiscard]] engine_time_t evaluation_time() const { return *_cached_evaluation_time_ptr; }

      protected:
        void initialise() override;

        void start() override;

        void stop() override;

        void dispose() override;

        friend struct GraphBuilder;  // Allow GraphBuilder to access private members

    private:
        EvaluationEngine::s_ptr    _evaluation_engine;
        std::vector<int64_t>       _graph_id;
        node_list                  _nodes;
        std::vector<engine_time_t> _schedule;
        node_ptr                   _parent_node;  // back-pointer, not owned
        std::string                _label;
        Traits                     _traits;  // Stored as value object
        SenderReceiverState        _receiver;
        engine_time_t              _last_evaluation_time{MIN_DT};
        int64_t                    _push_source_nodes_end{-1};

        // Performance optimization: Cache clock pointer and evaluation time pointer at initialization
        // Set once when evaluation engine is assigned, never changes
        EngineEvaluationClock *_cached_engine_clock{nullptr};
        const engine_time_t   *_cached_evaluation_time_ptr{nullptr};
    };
}  // namespace hgraph

#endif  // GRAPH_H