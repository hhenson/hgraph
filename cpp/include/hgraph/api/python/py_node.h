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
        
        // Graph access (read-only)
        [[nodiscard]] nb::object graph() const;  // Returns cached PyGraph wrapper
        
        // Input/Output access (read-only)
        [[nodiscard]] nb::object input() const;  // Returns cached PyTimeSeriesBundleInput wrapper
        [[nodiscard]] nb::dict inputs() const;  // Dict values are cached PyTimeSeriesInput wrappers
        [[nodiscard]] nb::object output() const;  // Returns cached specialized wrapper
        
        // Scheduler (read-only, for debugging)
        [[nodiscard]] nb::object scheduler() const;  // Returns cached PyNodeScheduler wrapper
        
        // Python __str__ and __repr__
        [[nodiscard]] std::string str() const;
        [[nodiscard]] std::string repr() const;
        
        // Nanobind registration (as "Node" in Python)
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
        ApiPtr<Node> _impl;
    };
    
} // namespace hgraph::api

#endif // HGRAPH_PY_NODE_H

