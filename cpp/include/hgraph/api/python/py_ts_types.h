//
// Specialized Python API wrappers for time series types
// TS (Value), Signal, TSL (List), TSB (Bundle), TSD (Dict), TSS (Set), TSW (Window), REF (Reference)
//

#ifndef HGRAPH_PY_TS_TYPES_H
#define HGRAPH_PY_TS_TYPES_H

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/api/python/py_time_series.h>
#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {
    // Forward declarations for impl types
    struct TimeSeriesValueInput;
    struct TimeSeriesValueOutput;
    struct TimeSeriesSignalInput;
    struct TimeSeriesSignalOutput;
    struct TimeSeriesListInput;
    struct TimeSeriesListOutput;
    struct TimeSeriesBundleInput;
    struct TimeSeriesBundleOutput;
    struct TimeSeriesDictInput;
    struct TimeSeriesDictOutput;
    struct TimeSeriesSetInput;
    struct TimeSeriesSetOutput;
    struct TimeSeriesWindowInput;
    struct TimeSeriesWindowOutput;
    struct TimeSeriesReferenceInput;
    struct TimeSeriesReferenceOutput;
}

namespace hgraph::api {
    
    // ============================================================================
    // TS (Value) Types
    // ============================================================================
    
    class PyTimeSeriesValueInput : public PyTimeSeriesInput {
    public:
        PyTimeSeriesValueInput(TimeSeriesValueInput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object value() const;
        [[nodiscard]] nb::object delta_value() const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesValueOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesValueOutput(TimeSeriesValueOutput* impl, control_block_ptr control_block);
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    // ============================================================================
    // Signal Types
    // ============================================================================
    
    class PyTimeSeriesSignalInput : public PyTimeSeriesInput {
    public:
        PyTimeSeriesSignalInput(TimeSeriesSignalInput* impl, control_block_ptr control_block);
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesSignalOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesSignalOutput(TimeSeriesSignalOutput* impl, control_block_ptr control_block);
        
        void set_value();
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    // ============================================================================
    // TSL (List) Types
    // ============================================================================
    
    class PyTimeSeriesListInput : public PyTimeSeriesInput {
    public:
        PyTimeSeriesListInput(TimeSeriesListInput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object get_item(int64_t index) const;
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::iterator iter() const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesListOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesListOutput(TimeSeriesListOutput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object get_item(int64_t index) const;
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::iterator iter() const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    // ============================================================================
    // TSB (Bundle) Types
    // ============================================================================
    
    class PyTimeSeriesBundleInput : public PyTimeSeriesInput {
    public:
        PyTimeSeriesBundleInput(TimeSeriesBundleInput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object get_item(nb::object key) const;  // str or int
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::iterator iter() const;
        
        [[nodiscard]] nb::object schema() const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesBundleOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesBundleOutput(TimeSeriesBundleOutput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object get_item(nb::object key) const;  // str or int
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::iterator iter() const;
        
        [[nodiscard]] nb::object schema() const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    // ============================================================================
    // TSD (Dict) Types
    // ============================================================================
    
    class PyTimeSeriesDictInput : public PyTimeSeriesInput {
    public:
        PyTimeSeriesDictInput(TimeSeriesDictInput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object get_item(nb::object key) const;
        [[nodiscard]] nb::object get(nb::object key, nb::object default_value) const;
        [[nodiscard]] bool contains(nb::object key) const;
        
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::object keys() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object items() const;
        
        [[nodiscard]] nb::object valid_keys() const;
        [[nodiscard]] nb::object added_keys() const;
        [[nodiscard]] nb::object modified_keys() const;
        [[nodiscard]] nb::object removed_keys() const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesDictOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesDictOutput(TimeSeriesDictOutput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object get_item(nb::object key) const;
        [[nodiscard]] bool contains(nb::object key) const;
        
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::object keys() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object items() const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    // ============================================================================
    // TSS (Set) Types
    // ============================================================================
    
    class PyTimeSeriesSetInput : public PyTimeSeriesInput {
    public:
        PyTimeSeriesSetInput(TimeSeriesSetInput* impl, control_block_ptr control_block);
        
        [[nodiscard]] bool contains(nb::object item) const;
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::iterator iter() const;
        
        [[nodiscard]] nb::object added() const;
        [[nodiscard]] nb::object removed() const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesSetOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesSetOutput(TimeSeriesSetOutput* impl, control_block_ptr control_block);
        
        [[nodiscard]] bool contains(nb::object item) const;
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::iterator iter() const;
        
        void add(nb::object item);
        void remove(nb::object item);
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    // ============================================================================
    // TSW (Window) Types
    // ============================================================================
    
    class PyTimeSeriesWindowInput : public PyTimeSeriesInput {
    public:
        PyTimeSeriesWindowInput(TimeSeriesWindowInput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object get_item(int64_t index) const;
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::iterator iter() const;
        
        [[nodiscard]] nb::object times() const;
        [[nodiscard]] nb::object values() const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesWindowOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesWindowOutput(TimeSeriesWindowOutput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object get_item(int64_t index) const;
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::iterator iter() const;
        
        void append(nb::object value);
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    // ============================================================================
    // REF (Reference) Types
    // ============================================================================
    
    class PyTimeSeriesReferenceInput : public PyTimeSeriesInput {
    public:
        PyTimeSeriesReferenceInput(TimeSeriesReferenceInput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object value_ref() const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesReferenceOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesReferenceOutput(TimeSeriesReferenceOutput* impl, control_block_ptr control_block);
        
        void set_value_ref(nb::object ref);
        
        static void register_with_nanobind(nb::module_& m);
    };
    
} // namespace hgraph::api

#endif // HGRAPH_PY_TS_TYPES_H

