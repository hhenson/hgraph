//
// PyNode - Python API wrapper for Node
// Part of arena allocation preparation - separates Python API from C++ implementation
//

#ifndef HGRAPH_PY_NODE_H
#define HGRAPH_PY_NODE_H

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/hgraph_forward_declarations.h>
#include <nanobind/nanobind.h>
#include <vector>
#include <string>

namespace nb = nanobind;

namespace hgraph {
    // Forward declarations for impl types
    struct Node;
    struct NodeSignature;
    struct Graph;
    struct TimeSeriesBundleInput;
    struct TimeSeriesInput;
    struct TimeSeriesOutput;
    struct TimeSeriesBundleOutput;
    struct NodeScheduler;
}

namespace hgraph::api {
    
    // Forward declarations for Python API types
    class PyGraph;
    class PyTimeSeriesBundleInput;
    class PyTimeSeriesInput;
    class PyTimeSeriesOutput;
    class PyTimeSeriesBundleOutput;
    class PyNodeScheduler;
    
    /**
     * PyNode - Python API wrapper for Node implementation
     * 
     * Exposes only the public API used by Python code (not wiring/runtime).
     * Move-only, delegates to Node implementation.
     */
    class PyNode {
    public:
        // Constructor from implementation and control block
        PyNode(Node* impl, control_block_ptr control_block);
        
        // Move constructor and assignment
        PyNode(PyNode&&) noexcept = default;
        PyNode& operator=(PyNode&&) noexcept = default;
        
        // Delete copy constructor and assignment
        PyNode(const PyNode&) = delete;
        PyNode& operator=(const PyNode&) = delete;
        
        // Properties exposed to Python
        [[nodiscard]] int64_t node_ndx() const;
        [[nodiscard]] nb::tuple owning_graph_id() const;
        [[nodiscard]] nb::tuple node_id() const;
        
        // Node metadata
        [[nodiscard]] nb::object signature() const;  // Returns NodeSignature wrapper
        [[nodiscard]] const nb::dict& scalars() const;
        
        // Graph access
        [[nodiscard]] nb::object graph() const;  // Returns PyGraph wrapper
        void set_graph(nb::object graph);  // Accepts PyGraph wrapper
        
        // Input/Output access
        [[nodiscard]] nb::object input() const;  // Returns PyTimeSeriesBundleInput wrapper
        void set_input(nb::object input);  // Accepts PyTimeSeriesBundleInput wrapper
        
        [[nodiscard]] nb::dict inputs() const;  // Returns dict of input name -> PyTimeSeriesInput
        [[nodiscard]] nb::list start_inputs() const;  // Returns list of PyTimeSeriesInput
        
        [[nodiscard]] nb::object output() const;  // Returns PyTimeSeriesOutput wrapper
        void set_output(nb::object output);  // Accepts PyTimeSeriesOutput wrapper
        
        // Recordable state
        [[nodiscard]] nb::object recordable_state() const;  // Returns PyTimeSeriesBundleOutput wrapper
        void set_recordable_state(nb::object state);  // Accepts PyTimeSeriesBundleOutput wrapper
        
        // Scheduler
        [[nodiscard]] nb::object scheduler() const;  // Returns PyNodeScheduler wrapper
        
        // Methods
        void eval();
        void notify();
        void notify(engine_time_t modified_time);
        
        // Python __str__ and __repr__
        [[nodiscard]] std::string str() const;
        [[nodiscard]] std::string repr() const;
        
        // Nanobind registration (as "Node" in Python)
        static void register_with_nanobind(nb::module_& m);
        
        // Internal: Get the raw implementation pointer (for internal use only)
        [[nodiscard]] Node* impl() const { return _impl.get(); }
        [[nodiscard]] bool is_valid() const { return _impl.has_value(); }
        
    private:
        ApiPtr<Node> _impl;
    };
    
} // namespace hgraph::api

#endif // HGRAPH_PY_NODE_H

