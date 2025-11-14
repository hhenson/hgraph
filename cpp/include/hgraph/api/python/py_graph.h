#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/api/python/api_ptr.h>

namespace hgraph {

    struct EvaluationEngineApi;
    struct EvaluationClock;

    struct HGRAPH_EXPORT PyGraph {

        using api_ptr = ApiPtr<Graph>;

        explicit PyGraph(api_ptr graph);

        [[nodiscard]] nb::tuple graph_id() const;

        [[nodiscard]] nb::tuple nodes() const;

        [[nodiscard]] node_ptr parent_node() const;

        [[nodiscard]] nb::object label() const;

        [[nodiscard]] nb::ref<EvaluationEngineApi> evaluation_engine_api();

        [[nodiscard]] nb::ref<EvaluationClock> evaluation_clock() const;

        [[nodiscard]] nb::int_ push_source_nodes_end() const;

        void schedule_node(int64_t node_ndx, engine_time_t when, bool force_set);

        nb::tuple schedule();

        PyGraph copy_with(nb::object nodes);

        const Traits &traits() const;

        [[nodiscard]] SenderReceiverState &receiver();

         static void register_with_nanobind(nb::module_ &m);

    private:
        api_ptr _impl;
    };
} // namespace hgraph
