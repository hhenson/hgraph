//
// PyNode - Python API wrapper for Node
// Part of arena allocation preparation - separates Python API from C++ implementation
//

#ifndef HGRAPH_PY_NODE_H
#define HGRAPH_PY_NODE_H

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/util/date_time.h>
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
    struct LastValuePullNode;
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
        [[nodiscard]] nb::object recordable_state() const;
        
        // Scheduler (read-only, for debugging)
        [[nodiscard]] nb::object scheduler() const;  // Returns cached PyNodeScheduler wrapper
        
        // Node notification (scheduling)
        void notify(engine_time_t modified_time);
        
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
        
    protected:
        ApiPtr<Node> _impl;
        
        // Friend declarations for C++ code that needs to extract raw impl
        friend nb::object wrap_node(const hgraph::Node* impl, control_block_ptr control_block);
        friend hgraph::Node* unwrap_node(const nb::object& obj);
    };
    
    class PyLastValuePullNode : public PyNode {
    public:
        PyLastValuePullNode(LastValuePullNode* impl, control_block_ptr control_block);
        
        void apply_value(const nb::object& new_value);
        void copy_from_input(nb::object input);
        void copy_from_output(nb::object output);
        
        static void register_with_nanobind(nb::module_& m);
        
    private:
        ApiPtr<LastValuePullNode> _impl_last_value;
    };
    
} // namespace hgraph::api

#endif // HGRAPH_PY_NODE_H

