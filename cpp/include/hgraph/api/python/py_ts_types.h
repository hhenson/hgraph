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
    struct TimeSeriesInput;
    struct TimeSeriesOutput;
    struct TimeSeriesListInput;
    struct TimeSeriesListOutput;
    struct TimeSeriesBundleInput;
    struct TimeSeriesBundleOutput;
    struct TimeSeriesDictInput;
    struct TimeSeriesDictOutput;
    struct TimeSeriesSetInput;
    struct TimeSeriesSetOutput;
    struct TimeSeriesReferenceInput;
    struct TimeSeriesReferenceOutput;
}

namespace hgraph::api {
    
    // ============================================================================
    // TS (Value) Types
    // ============================================================================
    
    class PyTimeSeriesValueInput : public PyTimeSeriesInput {
    public:
        PyTimeSeriesValueInput(TimeSeriesInput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object value() const;
        [[nodiscard]] nb::object delta_value() const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesValueOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesValueOutput(TimeSeriesOutput* impl, control_block_ptr control_block);
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    // ============================================================================
    // Signal Types
    // ============================================================================
    
    class PyTimeSeriesSignalInput : public PyTimeSeriesInput {
    public:
        PyTimeSeriesSignalInput(TimeSeriesInput* impl, control_block_ptr control_block);
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    // Note: Signal types are INPUT-ONLY (no PyTimeSeriesSignalOutput)
    
    // ============================================================================
    // TSL (List) Types
    // ============================================================================
    
    class PyTimeSeriesListInput : public PyTimeSeriesInput {
    public:
        PyTimeSeriesListInput(TimeSeriesListInput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object get_item(int64_t index) const;
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::object iter() const;
        
        [[nodiscard]] nb::list keys() const;
        [[nodiscard]] nb::object values() const;  // Returns iterator
        [[nodiscard]] nb::list items() const;
        [[nodiscard]] nb::list valid_keys() const;
        [[nodiscard]] nb::object valid_values() const;  // Returns iterator
        [[nodiscard]] nb::list valid_items() const;
        [[nodiscard]] nb::list modified_keys() const;
        [[nodiscard]] nb::object modified_values() const;  // Returns iterator
        [[nodiscard]] nb::dict modified_items() const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesListOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesListOutput(TimeSeriesListOutput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object get_item(int64_t index) const;
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::object iter() const;
        
        [[nodiscard]] nb::list keys() const;
        [[nodiscard]] nb::object values() const;  // Returns iterator
        [[nodiscard]] nb::list items() const;
        [[nodiscard]] nb::list valid_keys() const;
        [[nodiscard]] nb::object valid_values() const;  // Returns iterator
        [[nodiscard]] nb::list valid_items() const;
        [[nodiscard]] nb::list modified_keys() const;
        [[nodiscard]] nb::object modified_values() const;  // Returns iterator
        [[nodiscard]] nb::list modified_items() const;
        [[nodiscard]] bool can_apply_result(nb::object value) const;
        void apply_result(nb::object value);
        
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
        [[nodiscard]] nb::object iter() const;
        [[nodiscard]] bool contains(const std::string& key) const;
        
        [[nodiscard]] nb::list keys() const;
        [[nodiscard]] nb::object values() const;  // Returns iterator
        [[nodiscard]] nb::list items() const;
        [[nodiscard]] nb::list modified_keys() const;
        [[nodiscard]] nb::object modified_values() const;  // Returns iterator
        [[nodiscard]] nb::list modified_items() const;
        [[nodiscard]] nb::list valid_keys() const;
        [[nodiscard]] nb::object valid_values() const;  // Returns iterator
        [[nodiscard]] nb::list valid_items() const;
        [[nodiscard]] bool can_apply_result(nb::object value) const;
        void apply_result(nb::object value);
        
        [[nodiscard]] nb::object schema() const;
        [[nodiscard]] nb::object getattr(nb::handle key) const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesBundleOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesBundleOutput(TimeSeriesBundleOutput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object get_item(nb::object key) const;  // str or int
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::object iter() const;
        [[nodiscard]] bool contains(const std::string& key) const;
        
        [[nodiscard]] nb::list keys() const;
        [[nodiscard]] nb::object values() const;  // Returns iterator
        [[nodiscard]] nb::list items() const;
        [[nodiscard]] nb::list modified_keys() const;
        [[nodiscard]] nb::object modified_values() const;  // Returns iterator
        [[nodiscard]] nb::list modified_items() const;
        [[nodiscard]] nb::list valid_keys() const;
        [[nodiscard]] nb::object valid_values() const;  // Returns iterator
        [[nodiscard]] nb::list valid_items() const;
        [[nodiscard]] bool can_apply_result(nb::object value) const;
        void apply_result(nb::object value);
        
        [[nodiscard]] nb::object schema() const;
        [[nodiscard]] nb::object getattr(nb::handle key) const;
        
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
        [[nodiscard]] nb::object valid_values() const;
        [[nodiscard]] nb::object valid_items() const;
        [[nodiscard]] nb::object added_keys() const;
        [[nodiscard]] nb::object added_values() const;
        [[nodiscard]] nb::object added_items() const;
        [[nodiscard]] nb::object modified_keys() const;
        [[nodiscard]] nb::object modified_values() const;
        [[nodiscard]] nb::object modified_items() const;
        [[nodiscard]] nb::object removed_keys() const;
        [[nodiscard]] nb::object removed_values() const;
        [[nodiscard]] nb::object removed_items() const;
        
        [[nodiscard]] nb::object key_set() const;  // Returns PyTimeSeriesSetInput wrapper
        
        // TSD key management (used by operators) - TSDKeyObserver interface
        void _create(nb::object key);
        void on_key_added(nb::object key);
        void on_key_removed(nb::object key);
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesDictOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesDictOutput(TimeSeriesDictOutput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object get_item(nb::object key) const;
        [[nodiscard]] nb::object get(nb::object key, nb::object default_value) const;
        [[nodiscard]] bool contains(nb::object key) const;
        
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::object keys() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object items() const;
        [[nodiscard]] nb::object valid_keys() const;
        [[nodiscard]] nb::object valid_values() const;
        [[nodiscard]] nb::object valid_items() const;
        [[nodiscard]] nb::object added_keys() const;
        [[nodiscard]] nb::object added_values() const;
        [[nodiscard]] nb::object added_items() const;
        [[nodiscard]] nb::object getattr(nb::handle key) const;
        
        void set_item(nb::object key, nb::object value);
        void del_item(nb::object key);
        [[nodiscard]] nb::object pop(nb::object key, nb::object default_value) const;
        void clear();
        
        // Modified tracking
        [[nodiscard]] nb::object modified_keys() const;
        [[nodiscard]] nb::object modified_values() const;
        [[nodiscard]] nb::object modified_items() const;
        [[nodiscard]] nb::object removed_keys() const;
        [[nodiscard]] nb::object removed_values() const;
        [[nodiscard]] nb::object removed_items() const;
        
        [[nodiscard]] nb::object get_ref(nb::object key, nb::object requester) const;
        void release_ref(nb::object key, nb::object requester);
        
        [[nodiscard]] nb::object key_set() const;  // Returns PyTimeSeriesSetOutput wrapper
        
        // TSD key management
        void _create(nb::object key);
        [[nodiscard]] nb::object get_or_create(nb::object key);
        
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
        [[nodiscard]] bool empty() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object added() const;
        [[nodiscard]] nb::object removed() const;
        [[nodiscard]] bool was_added(nb::object item) const;
        [[nodiscard]] bool was_removed(nb::object item) const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesSetOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesSetOutput(TimeSeriesSetOutput* impl, control_block_ptr control_block);
        
        [[nodiscard]] bool contains(nb::object item) const;
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object added() const;
        [[nodiscard]] nb::object removed() const;
        [[nodiscard]] bool was_added(nb::object item) const;
        [[nodiscard]] bool was_removed(nb::object item) const;
        
        void add(nb::object item);
        void remove(nb::object item);
        
        [[nodiscard]] nb::object is_empty_output();
        [[nodiscard]] nb::object get_contains_output(nb::object item, nb::object requester);
        void release_contains_output(nb::object item, nb::object requester);
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    // ============================================================================
    // TSW (Window) Types
    // ============================================================================
    
    class PyTimeSeriesWindowInput : public PyTimeSeriesInput {
    public:
        PyTimeSeriesWindowInput(TimeSeriesInput* impl, control_block_ptr control_block);
        
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object times() const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesWindowOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesWindowOutput(TimeSeriesOutput* impl, control_block_ptr control_block);
        
        [[nodiscard]] int64_t len() const;
        [[nodiscard]] int64_t size() const;
        [[nodiscard]] int64_t min_size() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object times() const;
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    // ============================================================================
    // REF (Reference) Types
    // ============================================================================
    
    class PyTimeSeriesReferenceInput : public PyTimeSeriesInput {
    public:
        PyTimeSeriesReferenceInput(TimeSeriesInput* impl, control_block_ptr control_block);
        
        [[nodiscard]] nb::object get_item(int64_t index) const;  // Access nested references
        
        static void register_with_nanobind(nb::module_& m);
    };
    
    class PyTimeSeriesReferenceOutput : public PyTimeSeriesOutput {
    public:
        PyTimeSeriesReferenceOutput(TimeSeriesOutput* impl, control_block_ptr control_block);
        
        void observe_reference(nb::object input);
        void stop_observing_reference(nb::object input);
        void clear();
        
        static void register_with_nanobind(nb::module_& m);
    };
    
} // namespace hgraph::api

#endif // HGRAPH_PY_TS_TYPES_H

