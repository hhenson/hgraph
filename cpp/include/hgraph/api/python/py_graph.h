#pragma once

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/api/python/py_evaluation_engine.h>
#include <hgraph/hgraph_base.h>

namespace hgraph {

    struct EvaluationEngineApi;
    struct EvaluationClock;

    struct PyTraits {
        using api_ptr = ApiPtr<Traits>;

        explicit PyTraits(api_ptr traits);

        void set_traits(nb::kwargs traits);

        void set_trait(const std::string &trait_name, nb::object value) const;

        [[nodiscard]] nb::object get_trait(const std::string &trait_name) const;

        [[nodiscard]] nb::object get_trait_or(const std::string &trait_name, nb::object def_value) const;

        static void register_with_nanobind(nb::module_ &m);

    private:
        api_ptr _impl;
    };

    struct HGRAPH_EXPORT PyGraph {

        using api_ptr = ApiPtr<Graph>;

        explicit PyGraph(api_ptr graph);

        [[nodiscard]] nb::tuple graph_id() const;

        [[nodiscard]] nb::tuple nodes() const;

        [[nodiscard]] nb::tuple node_info(size_t idx) const;

        [[nodiscard]] nb::object parent_node() const;

        [[nodiscard]] nb::object label() const;

        [[nodiscard]] PyEvaluationEngineApi evaluation_engine_api();

        [[nodiscard]] PyEvaluationClock evaluation_clock() const;

        [[nodiscard]] nb::int_ push_source_nodes_end() const;

        void schedule_node(int64_t node_ndx, engine_time_t when, bool force_set);

        nb::tuple schedule();

        PyGraph copy_with(nb::object nodes);

        [[nodiscard]] nb::object traits() const;

        [[nodiscard]] SenderReceiverState &receiver();

         static void register_with_nanobind(nb::module_ &m);

    private:
        friend graph_s_ptr unwrap_graph(const PyGraph &obj);
        api_ptr _impl;
    };
} // namespace hgraph
