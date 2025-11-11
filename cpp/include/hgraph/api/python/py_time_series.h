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
        [[nodiscard]] nb::object owning_node() const;
        [[nodiscard]] nb::object parent_input() const;
        [[nodiscard]] bool has_parent_input() const;
        
        [[nodiscard]] bool valid() const;
        [[nodiscard]] bool modified() const;
        [[nodiscard]] bool all_valid() const;
        
        [[nodiscard]] bool active() const;
        void make_active();
        void make_passive();
        
        [[nodiscard]] bool bound() const;
        [[nodiscard]] bool has_peer() const;
        [[nodiscard]] nb::object output() const;
        
        [[nodiscard]] bool is_reference() const;
        
        [[nodiscard]] std::string str() const;
        [[nodiscard]] std::string repr() const;
        
        static void register_with_nanobind(nb::module_& m);
        
        [[nodiscard]] TimeSeriesInput* impl() const { return _impl.get(); }
        [[nodiscard]] bool is_valid() const { return _impl.has_value(); }
        
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
        [[nodiscard]] nb::object owning_node() const;
        [[nodiscard]] nb::object parent_output() const;
        [[nodiscard]] bool has_parent_output() const;
        
        [[nodiscard]] bool valid() const;
        [[nodiscard]] bool modified() const;
        [[nodiscard]] bool all_valid() const;
        
        [[nodiscard]] nb::object value() const;
        void set_value(nb::object value);
        [[nodiscard]] nb::object delta_value() const;
        
        void subscribe(nb::object node);
        void invalidate();
        
        [[nodiscard]] bool is_reference() const;
        
        [[nodiscard]] std::string str() const;
        [[nodiscard]] std::string repr() const;
        
        static void register_with_nanobind(nb::module_& m);
        
        [[nodiscard]] TimeSeriesOutput* impl() const { return _impl.get(); }
        [[nodiscard]] bool is_valid() const { return _impl.has_value(); }
        
    protected:
        ApiPtr<TimeSeriesOutput> _impl;
    };
    
} // namespace hgraph::api

#endif // HGRAPH_PY_TIME_SERIES_H

