//
// Wrapper Factory Implementation
//

#include <fmt/format.h>
#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/api/python/py_evaluation_engine.h>
#include <hgraph/api/python/py_node.h>
#include <hgraph/api/python/py_ref.h>
#include <hgraph/api/python/py_signal.h>
#include <hgraph/api/python/py_special_nodes.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/api/python/py_ts.h>
#include <hgraph/api/python/py_tsb.h>
#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/py_tsl.h>
#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/py_tsw.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_visitor.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/ts_indexed.h>
#include <hgraph/types/ts_signal.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/tsl.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/tsw.h>
#include <stdexcept>
#include <utility>

namespace hgraph
{

    /**
     * Try to create a wrapper of PyType if the api_ptr can be cast to UnderlyerType.
     * Returns std::nullopt if the cast fails.
     */
    template <typename PyType, typename UnderlyerType, typename ApiPtrType>
    std::optional<nb::object> try_create(ApiPtrType &impl) {
        if (impl.template dynamic_cast_<UnderlyerType>()) {
            return nb::cast(PyType(std::move(impl)));
        }
        return std::nullopt;
    }

    /**
     * Try to create a mesh node wrapper if the api_ptr can be cast to MeshNode<T>.
     * Returns std::nullopt if the cast fails.
     */
    template <typename T, typename ApiPtrType>
    std::optional<nb::object> try_create_mesh_node(ApiPtrType &impl) {
        if (impl.template dynamic_cast_<MeshNode<T>>()) {
            return nb::cast(PyMeshNestedNode::make_mesh_node<T>(std::move(impl)));
        }
        return std::nullopt;
    }

    // Main implementation - takes ApiPtr<Node>
    nb::object wrap_node(PyNode::api_ptr impl) {
        if (!impl) { return nb::none(); }

        // Try specialized node types in order
        if (auto r = try_create<PyLastValuePullNode, LastValuePullNode>(impl)) { return *r; }
        if (auto r = try_create<PyPushQueueNode, PushQueueNode>(impl)) { return *r; }
        // Mesh nodes
        if (auto r = try_create_mesh_node<bool>(impl)) { return *r; }
        if (auto r = try_create_mesh_node<int64_t>(impl)) { return *r; }
        if (auto r = try_create_mesh_node<double>(impl)) { return *r; }
        if (auto r = try_create_mesh_node<engine_date_t>(impl)) { return *r; }
        if (auto r = try_create_mesh_node<engine_time_t>(impl)) { return *r; }
        if (auto r = try_create_mesh_node<engine_time_delta_t>(impl)) { return *r; }
        if (auto r = try_create_mesh_node<nb::object>(impl)) { return *r; }
        // Other nested nodes
        if (auto r = try_create<PyNestedNode, NestedNode>(impl)) { return *r; }
        // Default to base PyNode
        return nb::cast(PyNode(std::move(impl)));
    }

    // Overload for shared_ptr
    nb::object wrap_node(const node_s_ptr &impl) {
        return wrap_node(PyNode::api_ptr(impl));
    }

    nb::object wrap_graph(const Graph::s_ptr &impl) {
        if (!impl) { return nb::none(); }
        return nb::cast(PyGraph(PyGraph::api_ptr(impl)));
    }

    nb::object wrap_node_scheduler(const NodeScheduler *impl, const control_block_ptr &control_block) {
        if (!impl) { return nb::none(); }
        return nb::cast(PyNodeScheduler(PyNodeScheduler::api_ptr(impl, control_block)));
    }

    nb::object wrap_node_scheduler(const NodeScheduler::s_ptr &impl) {
        if (!impl) { return nb::none(); }
        return nb::cast(PyNodeScheduler(PyNodeScheduler::api_ptr(impl)));
    }

    // Simple double dispatch visitor for wrapping TimeSeriesInput objects
    struct WrapInputVisitor : TimeSeriesInputVisitor
    {
        ApiPtr<TimeSeriesInput> impl;
        nb::object              wrapped_visitor;

        explicit WrapInputVisitor(ApiPtr<TimeSeriesInput> impl_) : impl(std::move(impl_)) {}

        // Helper to create an aliasing ApiPtr for a specific derived type
        template<typename T>
        ApiPtr<T> make_api_ptr(T* ptr) {
            return ApiPtr<T>(ptr, impl.control_block());
        }

        // Bring base class template methods into scope so our template methods can shadow them
        using TimeSeriesInputVisitor::visit;

        // Override the virtual method to handle value inputs
        void visit_value_input_impl(TimeSeriesType *input) override {
            // PyTimeSeriesValueInput constructor takes TimeSeriesType*, so we can pass it directly
            // Now that PyTimeSeriesValueInput is move-constructible, we can use nb::cast
            wrapped_visitor = nb::cast(PyTimeSeriesValueInput(make_api_ptr(input)));
        }

        // Also keep the template methods for direct calls (they'll call the virtual method)
        template <typename T> void visit(TimeSeriesValueInput<T> &source) { visit_value_input_impl(&source); }

        template <typename T> void visit(const TimeSeriesValueInput<T> &source) {
            visit_value_input_impl(const_cast<TimeSeriesValueInput<T> *>(&source));
        }

        // Override virtual methods for template types
        void visit_dict_input_impl(TimeSeriesType *input) override {
            // Try to determine the key type by trying all registered types (in order of likelihood)
            if (auto *dict_obj = dynamic_cast<TimeSeriesDictInput_T<nb::object> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<nb::object>>(make_api_ptr(dict_obj)));
            } else if (auto *dict_int = dynamic_cast<TimeSeriesDictInput_T<int64_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<int64_t>>(make_api_ptr(dict_int)));
            } else if (auto *dict_double = dynamic_cast<TimeSeriesDictInput_T<double> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<double>>(make_api_ptr(dict_double)));
            } else if (auto *dict_bool = dynamic_cast<TimeSeriesDictInput_T<bool> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<bool>>(make_api_ptr(dict_bool)));
            } else if (auto *dict_date = dynamic_cast<TimeSeriesDictInput_T<engine_date_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<engine_date_t>>(make_api_ptr(dict_date)));
            } else if (auto *dict_time = dynamic_cast<TimeSeriesDictInput_T<engine_time_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<engine_time_t>>(make_api_ptr(dict_time)));
            } else if (auto *dict_timedelta = dynamic_cast<TimeSeriesDictInput_T<engine_time_delta_t> *>(input)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesDictInput_T<TimeSeriesDictInput_T<engine_time_delta_t>>(make_api_ptr(dict_timedelta)));
            } else {
                wrapped_visitor = nb::none();
            }
        }

        void visit_set_input_impl(TimeSeriesType *input) override {
            // Try all registered key types (in order of likelihood)
            if (auto *set_obj = dynamic_cast<TimeSeriesSetInput_T<nb::object> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<nb::object>>(make_api_ptr(set_obj)));
            } else if (auto *set_int = dynamic_cast<TimeSeriesSetInput_T<int64_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<int64_t>>(make_api_ptr(set_int)));
            } else if (auto *set_double = dynamic_cast<TimeSeriesSetInput_T<double> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<double>>(make_api_ptr(set_double)));
            } else if (auto *set_bool = dynamic_cast<TimeSeriesSetInput_T<bool> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<bool>>(make_api_ptr(set_bool)));
            } else if (auto *set_date = dynamic_cast<TimeSeriesSetInput_T<engine_date_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<engine_date_t>>(make_api_ptr(set_date)));
            } else if (auto *set_time = dynamic_cast<TimeSeriesSetInput_T<engine_time_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<engine_time_t>>(make_api_ptr(set_time)));
            } else if (auto *set_timedelta = dynamic_cast<TimeSeriesSetInput_T<engine_time_delta_t> *>(input)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesSetInput_T<TimeSeriesSetInput_T<engine_time_delta_t>>(make_api_ptr(set_timedelta)));
            } else {
                wrapped_visitor = nb::none();
            }
        }

        void visit_window_input_impl(TimeSeriesType *input) override {
            // Window inputs are template types - we need to determine the value type T
            // Try all registered types (in order of likelihood)
            if (auto *window_obj = dynamic_cast<TimeSeriesWindowInput<nb::object> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowInput<nb::object>(make_api_ptr(window_obj)));
            } else if (auto *window_int = dynamic_cast<TimeSeriesWindowInput<int64_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowInput<int64_t>(make_api_ptr(window_int)));
            } else if (auto *window_double = dynamic_cast<TimeSeriesWindowInput<double> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowInput<double>(make_api_ptr(window_double)));
            } else if (auto *window_bool = dynamic_cast<TimeSeriesWindowInput<bool> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowInput<bool>(make_api_ptr(window_bool)));
            } else if (auto *window_date = dynamic_cast<TimeSeriesWindowInput<engine_date_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowInput<engine_date_t>(make_api_ptr(window_date)));
            } else if (auto *window_time = dynamic_cast<TimeSeriesWindowInput<engine_time_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowInput<engine_time_t>(make_api_ptr(window_time)));
            } else if (auto *window_timedelta = dynamic_cast<TimeSeriesWindowInput<engine_time_delta_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowInput<engine_time_delta_t>(make_api_ptr(window_timedelta)));
            } else {
                wrapped_visitor = nb::none();
            }
        }

        // Keep template methods for direct calls (they'll call the virtual methods)
        template <typename K> void visit(TimeSeriesSetInput_T<K> &source) { visit_set_input_impl(&source); }

        template <typename K> void visit(const TimeSeriesSetInput_T<K> &source) {
            visit_set_input_impl(const_cast<TimeSeriesSetInput_T<K> *>(&source));
        }

        template <typename K> void visit(TimeSeriesDictInput_T<K> &source) { visit_dict_input_impl(&source); }

        template <typename K> void visit(const TimeSeriesDictInput_T<K> &source) {
            visit_dict_input_impl(const_cast<TimeSeriesDictInput_T<K> *>(&source));
        }

        template <typename T> void visit(TimeSeriesWindowInput<T> &source) { visit_window_input_impl(&source); }

        template <typename T> void visit(const TimeSeriesWindowInput<T> &source) {
            visit_window_input_impl(const_cast<TimeSeriesWindowInput<T> *>(&source));
        }

        // Handle reference inputs
        void visit(TimeSeriesValueReferenceInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesValueReferenceInput(make_api_ptr(&source)));
        }

        void visit(TimeSeriesBundleReferenceInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesBundleReferenceInput(make_api_ptr(&source)));
        }

        void visit(TimeSeriesSetReferenceInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesSetReferenceInput(make_api_ptr(&source)));
        }

        void visit(TimeSeriesListReferenceInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesListReferenceInput(make_api_ptr(&source)));
        }

        void visit(TimeSeriesDictReferenceInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesDictReferenceInput(make_api_ptr(&source)));
        }

        void visit(TimeSeriesWindowReferenceInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesWindowReferenceInput(make_api_ptr(&source)));
        }

        // Handle other input types
        void visit(TimeSeriesBundleInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesBundleInput(make_api_ptr(&source)));
        }

        void visit(const TimeSeriesBundleInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesBundleInput(make_api_ptr(const_cast<TimeSeriesBundleInput *>(&source))));
        }

        void visit(TimeSeriesListInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesListInput(make_api_ptr(&source)));
        }

        void visit(const TimeSeriesListInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesListInput(make_api_ptr(const_cast<TimeSeriesListInput *>(&source))));
        }

        void visit(TimeSeriesSignalInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesSignalInput(make_api_ptr(&source)));
        }

        void visit(const TimeSeriesSignalInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesSignalInput(make_api_ptr(const_cast<TimeSeriesSignalInput *>(&source))));
        }

        void visit(TimeSeriesReferenceInput &source) override {
            // This should not be called - specialized types override accept() to call their specific visit method
            // But if it is called (e.g., for the base type), we need to determine the actual type
            // Try all possible reference input types
            if (auto *value_ref = dynamic_cast<TimeSeriesValueReferenceInput *>(&source)) {
                visit(*value_ref);
            } else if (auto *bundle_ref = dynamic_cast<TimeSeriesBundleReferenceInput *>(&source)) {
                visit(*bundle_ref);
            } else if (auto *set_ref = dynamic_cast<TimeSeriesSetReferenceInput *>(&source)) {
                visit(*set_ref);
            } else if (auto *list_ref = dynamic_cast<TimeSeriesListReferenceInput *>(&source)) {
                visit(*list_ref);
            } else if (auto *dict_ref = dynamic_cast<TimeSeriesDictReferenceInput *>(&source)) {
                visit(*dict_ref);
            } else if (auto *window_ref = dynamic_cast<TimeSeriesWindowReferenceInput *>(&source)) {
                visit(*window_ref);
            } else {
                // BUG: Encountered a base TimeSeriesReferenceInput that doesn't match any specialized type.
                // There should not be naked instances of TimeSeriesReferenceInput - they should always be
                // one of the specialized types. This indicates a bug where a base TimeSeriesReferenceInput
                // was created instead of a specialized type.
                throw std::runtime_error(
                    "WrapInputVisitor::visit(TimeSeriesReferenceInput): Encountered a base TimeSeriesReferenceInput "
                    "that doesn't match any specialized type. This is a bug - there should not be naked instances of "
                    "TimeSeriesReferenceInput. Check where this input was created (likely in reduce_node.cpp::zero_node).");
            }
        }
    };

    nb::object wrap_input(ApiPtr<TimeSeriesInput> impl) {
        if (!impl) { return nb::none(); }
        WrapInputVisitor visitor(std::move(impl));
        visitor.impl->accept(visitor);
        return visitor.wrapped_visitor.is_valid() ? visitor.wrapped_visitor : nb::none();
    }

    // Simple double dispatch visitor for wrapping TimeSeriesOutput objects
    struct WrapOutputVisitor : TimeSeriesOutputVisitor
    {
        ApiPtr<TimeSeriesOutput> impl;
        nb::object               wrapped_visitor;

        explicit WrapOutputVisitor(ApiPtr<TimeSeriesOutput> impl_) : impl(std::move(impl_)) {}

        // Helper to create an aliasing ApiPtr for a specific derived type
        template<typename T>
        ApiPtr<T> make_api_ptr(T* ptr) {
            return ApiPtr<T>(ptr, impl.control_block());
        }

        // Bring base class template methods into scope so our template methods can shadow them
        using TimeSeriesOutputVisitor::visit;

        // Override the virtual method to handle value outputs
        void visit_value_output_impl(TimeSeriesType *output) override {
            wrapped_visitor = nb::cast(PyTimeSeriesValueOutput(make_api_ptr(output)));
        }

        // Also keep the template methods for direct calls (they'll call the virtual method)
        template <typename T> void visit(TimeSeriesValueOutput<T> &source) { visit_value_output_impl(&source); }

        template <typename T> void visit(const TimeSeriesValueOutput<T> &source) {
            visit_value_output_impl(const_cast<TimeSeriesValueOutput<T> *>(&source));
        }

        // Override virtual methods for template types
        void visit_dict_output_impl(TimeSeriesType *output) override {
            if (auto *dict_obj = dynamic_cast<TimeSeriesDictOutput_T<nb::object> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<nb::object>>(make_api_ptr(dict_obj)));
            } else if (auto *dict_int = dynamic_cast<TimeSeriesDictOutput_T<int64_t> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<int64_t>>(make_api_ptr(dict_int)));
            } else if (auto *dict_double = dynamic_cast<TimeSeriesDictOutput_T<double> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<double>>(make_api_ptr(dict_double)));
            } else if (auto *dict_bool = dynamic_cast<TimeSeriesDictOutput_T<bool> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<bool>>(make_api_ptr(dict_bool)));
            } else if (auto *dict_date = dynamic_cast<TimeSeriesDictOutput_T<engine_date_t> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<engine_date_t>>(make_api_ptr(dict_date)));
            } else if (auto *dict_time = dynamic_cast<TimeSeriesDictOutput_T<engine_time_t> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<engine_time_t>>(make_api_ptr(dict_time)));
            } else if (auto *dict_timedelta = dynamic_cast<TimeSeriesDictOutput_T<engine_time_delta_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesDictOutput_T<TimeSeriesDictOutput_T<engine_time_delta_t>>(make_api_ptr(dict_timedelta)));
            } else {
                wrapped_visitor = nb::none();
            }
        }

        void visit_set_output_impl(TimeSeriesType *output) override {
            if (auto *set_obj = dynamic_cast<TimeSeriesSetOutput_T<nb::object> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<nb::object>>(make_api_ptr(set_obj)));
            } else if (auto *set_int = dynamic_cast<TimeSeriesSetOutput_T<int64_t> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<int64_t>>(make_api_ptr(set_int)));
            } else if (auto *set_double = dynamic_cast<TimeSeriesSetOutput_T<double> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<double>>(make_api_ptr(set_double)));
            } else if (auto *set_bool = dynamic_cast<TimeSeriesSetOutput_T<bool> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<bool>>(make_api_ptr(set_bool)));
            } else if (auto *set_date = dynamic_cast<TimeSeriesSetOutput_T<engine_date_t> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<engine_date_t>>(make_api_ptr(set_date)));
            } else if (auto *set_time = dynamic_cast<TimeSeriesSetOutput_T<engine_time_t> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<engine_time_t>>(make_api_ptr(set_time)));
            } else if (auto *set_timedelta = dynamic_cast<TimeSeriesSetOutput_T<engine_time_delta_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesSetOutput_T<TimeSeriesSetOutput_T<engine_time_delta_t>>(make_api_ptr(set_timedelta)));
            } else {
                wrapped_visitor = nb::none();
            }
        }

        void visit_fixed_window_output_impl(TimeSeriesType *output) override {
            if (auto *window_obj = dynamic_cast<TimeSeriesFixedWindowOutput<nb::object> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<nb::object>>(make_api_ptr(window_obj)));
            } else if (auto *window_int = dynamic_cast<TimeSeriesFixedWindowOutput<int64_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<int64_t>>(make_api_ptr(window_int)));
            } else if (auto *window_double = dynamic_cast<TimeSeriesFixedWindowOutput<double> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<double>>(make_api_ptr(window_double)));
            } else if (auto *window_bool = dynamic_cast<TimeSeriesFixedWindowOutput<bool> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<bool>>(make_api_ptr(window_bool)));
            } else if (auto *window_date = dynamic_cast<TimeSeriesFixedWindowOutput<engine_date_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<engine_date_t>>(make_api_ptr(window_date)));
            } else if (auto *window_time = dynamic_cast<TimeSeriesFixedWindowOutput<engine_time_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<engine_time_t>>(make_api_ptr(window_time)));
            } else if (auto *window_timedelta = dynamic_cast<TimeSeriesFixedWindowOutput<engine_time_delta_t> *>(output)) {
                wrapped_visitor = nb::cast(
                    PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<engine_time_delta_t>>(make_api_ptr(window_timedelta)));
            } else {
                wrapped_visitor = nb::none();
            }
        }

        void visit_time_window_output_impl(TimeSeriesType *output) override {
            if (auto *window_obj = dynamic_cast<TimeSeriesTimeWindowOutput<nb::object> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<nb::object>>(make_api_ptr(window_obj)));
            } else if (auto *window_int = dynamic_cast<TimeSeriesTimeWindowOutput<int64_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<int64_t>>(make_api_ptr(window_int)));
            } else if (auto *window_double = dynamic_cast<TimeSeriesTimeWindowOutput<double> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<double>>(make_api_ptr(window_double)));
            } else if (auto *window_bool = dynamic_cast<TimeSeriesTimeWindowOutput<bool> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<bool>>(make_api_ptr(window_bool)));
            } else if (auto *window_date = dynamic_cast<TimeSeriesTimeWindowOutput<engine_date_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<engine_date_t>>(make_api_ptr(window_date)));
            } else if (auto *window_time = dynamic_cast<TimeSeriesTimeWindowOutput<engine_time_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<engine_time_t>>(make_api_ptr(window_time)));
            } else if (auto *window_timedelta = dynamic_cast<TimeSeriesTimeWindowOutput<engine_time_delta_t> *>(output)) {
                wrapped_visitor = nb::cast(
                    PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<engine_time_delta_t>>(make_api_ptr(window_timedelta)));
            } else {
                wrapped_visitor = nb::none();
            }
        }

        // Keep template methods for direct calls (they'll call the virtual methods)
        template <typename K> void visit(TimeSeriesDictOutput_T<K> &source) { visit_dict_output_impl(&source); }

        template <typename K> void visit(const TimeSeriesDictOutput_T<K> &source) {
            visit_dict_output_impl(const_cast<TimeSeriesDictOutput_T<K> *>(&source));
        }

        template <typename K> void visit(TimeSeriesSetOutput_T<K> &source) { visit_set_output_impl(&source); }

        template <typename K> void visit(const TimeSeriesSetOutput_T<K> &source) {
            visit_set_output_impl(const_cast<TimeSeriesSetOutput_T<K> *>(&source));
        }

        // Handle reference outputs
        void visit(TimeSeriesValueReferenceOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesValueReferenceOutput(make_api_ptr(&source)));
        }

        void visit(TimeSeriesBundleReferenceOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesBundleReferenceOutput(make_api_ptr(&source)));
        }

        void visit(TimeSeriesSetReferenceOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesSetReferenceOutput(make_api_ptr(&source)));
        }

        void visit(TimeSeriesListReferenceOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesListReferenceOutput(make_api_ptr(&source)));
        }

        void visit(TimeSeriesDictReferenceOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesDictReferenceOutput(make_api_ptr(&source)));
        }

        void visit(TimeSeriesWindowReferenceOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesWindowReferenceOutput(make_api_ptr(&source)));
        }

        // Handle other output types
        void visit(TimeSeriesBundleOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesBundleOutput(make_api_ptr(&source)));
        }

        void visit(const TimeSeriesBundleOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesBundleOutput(make_api_ptr(const_cast<TimeSeriesBundleOutput *>(&source))));
        }

        void visit(TimeSeriesListOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesListOutput(make_api_ptr(&source)));
        }

        void visit(const TimeSeriesListOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesListOutput(make_api_ptr(const_cast<TimeSeriesListOutput *>(&source))));
        }
    };

    nb::object wrap_output(ApiPtr<TimeSeriesOutput> impl) {
        if (!impl) { return nb::none(); }
        WrapOutputVisitor visitor(std::move(impl));
        visitor.impl->accept(visitor);
        return visitor.wrapped_visitor.is_valid() ? visitor.wrapped_visitor : nb::none();
    }

    nb::object wrap_time_series(ApiPtr<TimeSeriesInput> impl) {
        return wrap_input(std::move(impl));
    }

    nb::object wrap_time_series(ApiPtr<TimeSeriesOutput> impl) {
        return wrap_output(std::move(impl));
    }

    nb::object wrap_output(const time_series_output_s_ptr &impl) {
        if (!impl) { return nb::none(); }
        return wrap_output(ApiPtr<TimeSeriesOutput>(impl));
    }

    node_s_ptr unwrap_node(const nb::handle &obj) {
        if (auto *py_node = nb::inst_ptr<PyNode>(obj)) { return unwrap_node(*py_node); }
        return {};
    }

    node_s_ptr unwrap_node(const PyNode &node_) { return node_._impl.control_block_typed<Node>(); }

    time_series_input_s_ptr unwrap_input(const nb::handle &obj) {
        if (auto *py_input = nb::inst_ptr<PyTimeSeriesInput>(obj)) { return unwrap_input(*py_input); }
        return {};
    }

    time_series_input_s_ptr unwrap_input(const PyTimeSeriesInput &input_) { return input_.impl_s_ptr<TimeSeriesInput>(); }

    time_series_output_s_ptr unwrap_output(const nb::handle &obj) {
        if (auto *py_output = nb::inst_ptr<PyTimeSeriesOutput>(obj)) { return unwrap_output(*py_output); }
        return {};
    }

    time_series_output_s_ptr unwrap_output(const PyTimeSeriesOutput &output_) { return output_.impl_s_ptr<TimeSeriesOutput>(); }

    nb::object wrap_evaluation_engine_api(EvaluationEngineApi::s_ptr impl) {
        if (!impl) { return nb::none(); }
        return nb::cast(PyEvaluationEngineApi(PyEvaluationEngineApi::api_ptr(std::move(impl))));
    }

    nb::object wrap_evaluation_clock(EvaluationClock::s_ptr impl) {
        if (!impl) { return nb::none(); }
        return nb::cast(PyEvaluationClock(PyEvaluationClock::api_ptr(std::move(impl))));
    }

    nb::object wrap_traits(const Traits *impl, const control_block_ptr &control_block) {
        if (!impl) { return nb::none(); }
        return nb::cast(PyTraits(PyTraits::api_ptr(impl, control_block)));
    }

}  // namespace hgraph
