//
// PyGraph - Python API wrapper for Graph
//

#ifndef HGRAPH_PY_GRAPH_H
#define HGRAPH_PY_GRAPH_H

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/hgraph_forward_declarations.h>
#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {
    struct Graph;
    struct Node;
    struct EvaluationEngine;
    struct EvaluationEngineApi;
    struct EvaluationClock;
    struct EngineEvaluationClock;
    struct Traits;
}

namespace hgraph::api {
    
    class PyNode;
    
    class PyGraph {
    public:
        PyGraph(Graph* impl, control_block_ptr control_block);
        
        PyGraph(PyGraph&&) noexcept = default;
        PyGraph& operator=(PyGraph&&) noexcept = default;
        PyGraph(const PyGraph&) = delete;
        PyGraph& operator=(const PyGraph&) = delete;
        
        [[nodiscard]] nb::tuple graph_id() const;
        [[nodiscard]] nb::tuple nodes() const;
        [[nodiscard]] nb::object parent_node() const;
        [[nodiscard]] std::string label() const;
        
        [[nodiscard]] nb::object evaluation_engine_api() const;
        [[nodiscard]] nb::object evaluation_clock() const;
        [[nodiscard]] nb::object engine_evaluation_clock() const;
        
        [[nodiscard]] nb::object evaluation_engine() const;
        void set_evaluation_engine(nb::object engine);
        
        [[nodiscard]] int64_t push_source_nodes_end() const;
        void schedule_node(int64_t node_ndx, engine_time_t when, bool force_set = false);
        [[nodiscard]] nb::object schedule() const;
        
        void evaluate_graph();
        [[nodiscard]] nb::object copy_with(nb::tuple nodes) const;
        [[nodiscard]] nb::object traits() const;
        
        [[nodiscard]] std::string str() const;
        [[nodiscard]] std::string repr() const;
        
        static void register_with_nanobind(nb::module_& m);
        
        [[nodiscard]] Graph* impl() const { return _impl.get(); }
        [[nodiscard]] bool is_valid() const { return _impl.has_value(); }
        
    private:
        ApiPtr<Graph> _impl;
    };
    
} // namespace hgraph::api

#endif // HGRAPH_PY_GRAPH_H

