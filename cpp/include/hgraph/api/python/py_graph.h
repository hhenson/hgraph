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
    struct GraphExecutorImpl;
    struct GraphBuilder;
}

namespace hgraph::api {
    
    class PyGraph {
        // Allow extracting raw graph pointer for C++ interop
        friend struct hgraph::GraphExecutorImpl;
        friend struct hgraph::GraphBuilder;
        
    public:
        PyGraph(Graph* impl, control_block_ptr control_block);
        
        PyGraph(PyGraph&&) noexcept = default;
        PyGraph& operator=(PyGraph&&) noexcept = default;
        PyGraph(const PyGraph&) = delete;
        PyGraph& operator=(const PyGraph&) = delete;
        
        // Graph identification
        [[nodiscard]] nb::tuple graph_id() const;
        [[nodiscard]] nb::tuple nodes() const;  // Tuple of cached PyNode wrappers
        [[nodiscard]] nb::object parent_node() const;  // Optional cached PyNode
        [[nodiscard]] std::string label() const;
        
        // Evaluation context (read-only)
        [[nodiscard]] nb::object evaluation_engine_api() const;
        [[nodiscard]] nb::object evaluation_clock() const;
        [[nodiscard]] nb::object engine_evaluation_clock() const;
        [[nodiscard]] nb::object traits() const;
        
        [[nodiscard]] std::string str() const;
        [[nodiscard]] std::string repr() const;
        
        static void register_with_nanobind(nb::module_& m);
        
        /**
         * Check if this wrapper is valid and usable.
         * Returns false if:
         * - The wrapper is empty/moved-from, OR
         * - The graph has been destroyed/disposed
         */
        [[nodiscard]] bool is_valid() const { 
            return _impl.has_value() && _impl.is_graph_alive(); 
        }
        
    private:
        ApiPtr<Graph> _impl;
    };
    
} // namespace hgraph::api

#endif // HGRAPH_PY_GRAPH_H

