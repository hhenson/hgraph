//
// PyTimeSeriesInput/Output - Python API wrappers for base time series types
//

#ifndef HGRAPH_PY_TIME_SERIES_H
#define HGRAPH_PY_TIME_SERIES_H

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/hgraph_forward_declarations.h>
#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {
    struct TimeSeriesInput;
    struct TimeSeriesOutput;
    struct Node;
}

namespace hgraph::api {
    
    // Forward declarations
    class PyNode;
    class PyTimeSeriesOutput;
    
    /**
     * PyTimeSeriesInput - Base wrapper for all time series input types
     */
    class PyTimeSeriesInput {
    public:
        PyTimeSeriesInput(TimeSeriesInput* impl, control_block_ptr control_block);
        
        PyTimeSeriesInput(PyTimeSeriesInput&&) noexcept = default;
        PyTimeSeriesInput& operator=(PyTimeSeriesInput&&) noexcept = default;
        PyTimeSeriesInput(const PyTimeSeriesInput&) = delete;
        PyTimeSeriesInput& operator=(const PyTimeSeriesInput&) = delete;
        
        // Common properties
        [[nodiscard]] PyNode owning_node() const;
        [[nodiscard]] PyTimeSeriesInput parent_input() const;  // Returns appropriate specialized wrapper
        [[nodiscard]] bool has_parent_input() const;
        
        [[nodiscard]] bool valid() const;
        [[nodiscard]] bool modified() const;
        [[nodiscard]] bool all_valid() const;
        
        [[nodiscard]] bool active() const;
        void make_active();
        void make_passive();
        
        [[nodiscard]] bool bound() const;
        [[nodiscard]] bool has_peer() const;
        [[nodiscard]] PyTimeSeriesOutput output() const;  // Returns appropriate specialized wrapper
        
        // Binding (may be used in some cases)
        bool bind_output(nb::object output);
        void un_bind_output(bool unbind_refs = false);
        
        [[nodiscard]] bool is_reference() const;
        
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
        
    protected:
        ApiPtr<TimeSeriesInput> _impl;
    };
    
    /**
     * PyTimeSeriesOutput - Base wrapper for all time series output types
     */
    class PyTimeSeriesOutput {
    public:
        PyTimeSeriesOutput(TimeSeriesOutput* impl, control_block_ptr control_block);
        
        PyTimeSeriesOutput(PyTimeSeriesOutput&&) noexcept = default;
        PyTimeSeriesOutput& operator=(PyTimeSeriesOutput&&) noexcept = default;
        PyTimeSeriesOutput(const PyTimeSeriesOutput&) = delete;
        PyTimeSeriesOutput& operator=(const PyTimeSeriesOutput&) = delete;
        
        // Common properties
        [[nodiscard]] PyNode owning_node() const;
        [[nodiscard]] PyTimeSeriesOutput parent_output() const;  // Returns appropriate specialized wrapper
        [[nodiscard]] bool has_parent_output() const;
        
        [[nodiscard]] bool valid() const;
        [[nodiscard]] bool modified() const;
        [[nodiscard]] bool all_valid() const;
        
        [[nodiscard]] nb::object value() const;
        void set_value(nb::object value);
        [[nodiscard]] nb::object delta_value() const;
        
        void invalidate();
        
        [[nodiscard]] bool is_reference() const;
        
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
        
    protected:
        ApiPtr<TimeSeriesOutput> _impl;
    };
    
} // namespace hgraph::api

#endif // HGRAPH_PY_TIME_SERIES_H

