//
// Wrapper Factory Implementation
//

#include <fmt/format.h>
#include <hgraph/api/python/py_node.h>
#include <hgraph/api/python/py_special_nodes.h>
#include <hgraph/api/python/py_ref.h>
#include <hgraph/api/python/py_signal.h>
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
     * Generic wrapper caching function.
     * Checks for cached wrapper via intrusive_base::self_py(), creates if needed.
     *
     * @tparam ImplType The C++ implementation type (must inherit from intrusive_base)
     * @tparam Creator Function/lambda type that creates the wrapper
     * @param impl Pointer to the C++ implementation object
     * @param control_block Control block for lifetime tracking
     * @param creator Function that creates the wrapper: (ImplType*, control_block_ptr) -> Wrapper
     * @return nb::object containing the cached or newly created wrapper
     */
    template <typename ImplType, typename Creator>
    nb::object get_or_create_wrapper(const ImplType *impl, const control_block_ptr &control_block, Creator &&creator) {
        if (!impl) { return nb::none(); }

        // const_cast is safe here - we need non-const to access intrusive_base methods
        auto *mutable_impl = const_cast<ImplType *>(impl);

        // Check if we already have a cached Python wrapper
        PyObject *cached_ptr = mutable_impl->self_py();
        if (cached_ptr) { return nb::borrow(cached_ptr); }

        // Create new wrapper using the provided creator
        auto       wrapper = creator(mutable_impl, control_block);
        nb::object py_obj  = nb::cast(std::move(wrapper));

        // Cache and return
        mutable_impl->set_self_py(py_obj.ptr());
        return py_obj;
    }

    nb::object wrap_node(const Node *impl) { return wrap_node(impl, impl->graph()->control_block()); }

    nb::object wrap_node(const Node *impl, const control_block_ptr &control_block) {
        //TODO: Make this better, but for now:
        if (dynamic_cast<const LastValuePullNode*>(impl)) {
            return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) { return PyLastValuePullNode({impl, cb}); });
        }
        if (dynamic_cast<const NestedNode*>(impl)) {
            return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) { return PyNestedNode({impl, cb}); });
        }
        return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) { return PyNode({impl, cb}); });
    }

    nb::object wrap_graph(const Graph *impl, const control_block_ptr &control_block) {
        return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) { return PyGraph({impl, cb}); });
    }

    nb::object wrap_node_scheduler(const NodeScheduler *impl, const control_block_ptr &control_block) {
        return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) { return PyNodeScheduler({impl, cb}); });
    }

    // Simple double dispatch visitor for wrapping TimeSeriesInput objects
    struct WrapInputVisitor : TimeSeriesInputVisitor
    {
        control_block_ptr control_block;
        nb::object        wrapped_visitor;

        explicit WrapInputVisitor(control_block_ptr control_block_) : control_block(std::move(control_block_)) {}

        // Bring base class template methods into scope so our template methods can shadow them
        using TimeSeriesInputVisitor::visit;

        // Override the virtual method to handle value inputs
        void visit_value_input_impl(TimeSeriesType *input) override {
            // PyTimeSeriesValueInput constructor takes TimeSeriesType*, so we can pass it directly
            // Now that PyTimeSeriesValueInput is move-constructible, we can use nb::cast
            wrapped_visitor = nb::cast(PyTimeSeriesValueInput(input, control_block));
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
                wrapped_visitor = nb::cast(PyTimeSeriesDictInput<TimeSeriesDictInput_T<nb::object>>(dict_obj, control_block));
            } else if (auto *dict_int = dynamic_cast<TimeSeriesDictInput_T<int64_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictInput<TimeSeriesDictInput_T<int64_t>>(dict_int, control_block));
            } else if (auto *dict_double = dynamic_cast<TimeSeriesDictInput_T<double> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictInput<TimeSeriesDictInput_T<double>>(dict_double, control_block));
            } else if (auto *dict_bool = dynamic_cast<TimeSeriesDictInput_T<bool> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictInput<TimeSeriesDictInput_T<bool>>(dict_bool, control_block));
            } else if (auto *dict_date = dynamic_cast<TimeSeriesDictInput_T<engine_date_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictInput<TimeSeriesDictInput_T<engine_date_t>>(dict_date, control_block));
            } else if (auto *dict_time = dynamic_cast<TimeSeriesDictInput_T<engine_time_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictInput<TimeSeriesDictInput_T<engine_time_t>>(dict_time, control_block));
            } else if (auto *dict_timedelta = dynamic_cast<TimeSeriesDictInput_T<engine_time_delta_t> *>(input)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesDictInput<TimeSeriesDictInput_T<engine_time_delta_t>>(dict_timedelta, control_block));
            } else {
                wrapped_visitor = nb::none();
            }
        }

        void visit_set_input_impl(TimeSeriesType *input) override {
            // Try all registered key types (in order of likelihood)
            if (auto *set_obj = dynamic_cast<TimeSeriesSetInput_T<nb::object> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetInput<TimeSeriesSetInput_T<nb::object>>(set_obj, control_block));
            } else if (auto *set_int = dynamic_cast<TimeSeriesSetInput_T<int64_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetInput<TimeSeriesSetInput_T<int64_t>>(set_int, control_block));
            } else if (auto *set_double = dynamic_cast<TimeSeriesSetInput_T<double> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetInput<TimeSeriesSetInput_T<double>>(set_double, control_block));
            } else if (auto *set_bool = dynamic_cast<TimeSeriesSetInput_T<bool> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetInput<TimeSeriesSetInput_T<bool>>(set_bool, control_block));
            } else if (auto *set_date = dynamic_cast<TimeSeriesSetInput_T<engine_date_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetInput<TimeSeriesSetInput_T<engine_date_t>>(set_date, control_block));
            } else if (auto *set_time = dynamic_cast<TimeSeriesSetInput_T<engine_time_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetInput<TimeSeriesSetInput_T<engine_time_t>>(set_time, control_block));
            } else if (auto *set_timedelta = dynamic_cast<TimeSeriesSetInput_T<engine_time_delta_t> *>(input)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesSetInput<TimeSeriesSetInput_T<engine_time_delta_t>>(set_timedelta, control_block));
            } else {
                wrapped_visitor = nb::none();
            }
        }

        void visit_window_input_impl(TimeSeriesType *input) override {
            // Window inputs are template types - we need to determine the value type T
            // Try all registered types (in order of likelihood)
            if (auto *window_obj = dynamic_cast<TimeSeriesWindowInput<nb::object> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowInput<nb::object>(window_obj, control_block));
            } else if (auto *window_int = dynamic_cast<TimeSeriesWindowInput<int64_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowInput<int64_t>(window_int, control_block));
            } else if (auto *window_double = dynamic_cast<TimeSeriesWindowInput<double> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowInput<double>(window_double, control_block));
            } else if (auto *window_bool = dynamic_cast<TimeSeriesWindowInput<bool> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowInput<bool>(window_bool, control_block));
            } else if (auto *window_date = dynamic_cast<TimeSeriesWindowInput<engine_date_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowInput<engine_date_t>(window_date, control_block));
            } else if (auto *window_time = dynamic_cast<TimeSeriesWindowInput<engine_time_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowInput<engine_time_t>(window_time, control_block));
            } else if (auto *window_timedelta = dynamic_cast<TimeSeriesWindowInput<engine_time_delta_t> *>(input)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowInput<engine_time_delta_t>(window_timedelta, control_block));
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
            wrapped_visitor = nb::cast(PyTimeSeriesValueReferenceInput(&source, control_block));
        }

        void visit(TimeSeriesBundleReferenceInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesBundleReferenceInput(&source, control_block));
        }

        void visit(TimeSeriesSetReferenceInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesSetReferenceInput(&source, control_block));
        }

        void visit(TimeSeriesListReferenceInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesListReferenceInput(&source, control_block));
        }

        void visit(TimeSeriesDictReferenceInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesDictReferenceInput(&source, control_block));
        }

        void visit(TimeSeriesWindowReferenceInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesWindowReferenceInput(&source, control_block));
        }

        // Handle other input types
        void visit(TimeSeriesBundleInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesBundleInput(&source, control_block));
        }

        void visit(const TimeSeriesBundleInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesBundleInput(const_cast<TimeSeriesBundleInput *>(&source), control_block));
        }

        void visit(TimeSeriesListInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesListInput(&source, control_block));
        }

        void visit(const TimeSeriesListInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesListInput(const_cast<TimeSeriesListInput *>(&source), control_block));
        }

        void visit(TimeSeriesSignalInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesSignalInput(&source, control_block));
        }

        void visit(const TimeSeriesSignalInput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesSignalInput(const_cast<TimeSeriesSignalInput *>(&source), control_block));
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

    nb::object wrap_input(const TimeSeriesInput *impl) { return wrap_input(impl, impl->owning_graph()->control_block()); }

    nb::object wrap_input(const TimeSeriesInput *impl, const control_block_ptr &control_block) {
        return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) {
            WrapInputVisitor visitor(cb);
            impl->accept(visitor);
            return visitor.wrapped_visitor.is_valid() ? visitor.wrapped_visitor : nb::none();
        });
    }

    // Simple double dispatch visitor for wrapping TimeSeriesOutput objects
    struct WrapOutputVisitor : TimeSeriesOutputVisitor
    {
        control_block_ptr control_block;
        nb::object        wrapped_visitor;

        explicit WrapOutputVisitor(control_block_ptr control_block_) : control_block(std::move(control_block_)) {}

        // Bring base class template methods into scope so our template methods can shadow them
        using TimeSeriesOutputVisitor::visit;

        // Override the virtual method to handle value outputs
        void visit_value_output_impl(TimeSeriesType *output) override {
            // PyTimeSeriesValueOutput constructor takes TimeSeriesType*, so we can pass it directly
            // Now that PyTimeSeriesValueOutput is move-constructible, we can use nb::cast
            wrapped_visitor = nb::cast(PyTimeSeriesValueOutput(output, control_block));
        }

        // Also keep the template methods for direct calls (they'll call the virtual method)
        template <typename T> void visit(TimeSeriesValueOutput<T> &source) { visit_value_output_impl(&source); }

        template <typename T> void visit(const TimeSeriesValueOutput<T> &source) {
            visit_value_output_impl(const_cast<TimeSeriesValueOutput<T> *>(&source));
        }

        // Override virtual methods for template types
        void visit_dict_output_impl(TimeSeriesType *output) override {
            // Try to determine the key type by trying all registered types (in order of likelihood)
            if (auto *dict_obj = dynamic_cast<TimeSeriesDictOutput_T<nb::object> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictOutput<TimeSeriesDictOutput_T<nb::object>>(dict_obj, control_block));
            } else if (auto *dict_int = dynamic_cast<TimeSeriesDictOutput_T<int64_t> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictOutput<TimeSeriesDictOutput_T<int64_t>>(dict_int, control_block));
            } else if (auto *dict_double = dynamic_cast<TimeSeriesDictOutput_T<double> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictOutput<TimeSeriesDictOutput_T<double>>(dict_double, control_block));
            } else if (auto *dict_bool = dynamic_cast<TimeSeriesDictOutput_T<bool> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictOutput<TimeSeriesDictOutput_T<bool>>(dict_bool, control_block));
            } else if (auto *dict_date = dynamic_cast<TimeSeriesDictOutput_T<engine_date_t> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictOutput<TimeSeriesDictOutput_T<engine_date_t>>(dict_date, control_block));
            } else if (auto *dict_time = dynamic_cast<TimeSeriesDictOutput_T<engine_time_t> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesDictOutput<TimeSeriesDictOutput_T<engine_time_t>>(dict_time, control_block));
            } else if (auto *dict_timedelta = dynamic_cast<TimeSeriesDictOutput_T<engine_time_delta_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesDictOutput<TimeSeriesDictOutput_T<engine_time_delta_t>>(dict_timedelta, control_block));
            } else {
                wrapped_visitor = nb::none();
            }
        }

        void visit_set_output_impl(TimeSeriesType *output) override {
            // Try all registered key types (in order of likelihood)
            if (auto *set_obj = dynamic_cast<TimeSeriesSetOutput_T<nb::object> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetOutput<TimeSeriesSetOutput_T<nb::object>>(set_obj, control_block));
            } else if (auto *set_int = dynamic_cast<TimeSeriesSetOutput_T<int64_t> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetOutput<TimeSeriesSetOutput_T<int64_t>>(set_int, control_block));
            } else if (auto *set_double = dynamic_cast<TimeSeriesSetOutput_T<double> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetOutput<TimeSeriesSetOutput_T<double>>(set_double, control_block));
            } else if (auto *set_bool = dynamic_cast<TimeSeriesSetOutput_T<bool> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetOutput<TimeSeriesSetOutput_T<bool>>(set_bool, control_block));
            } else if (auto *set_date = dynamic_cast<TimeSeriesSetOutput_T<engine_date_t> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetOutput<TimeSeriesSetOutput_T<engine_date_t>>(set_date, control_block));
            } else if (auto *set_time = dynamic_cast<TimeSeriesSetOutput_T<engine_time_t> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesSetOutput<TimeSeriesSetOutput_T<engine_time_t>>(set_time, control_block));
            } else if (auto *set_timedelta = dynamic_cast<TimeSeriesSetOutput_T<engine_time_delta_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesSetOutput<TimeSeriesSetOutput_T<engine_time_delta_t>>(set_timedelta, control_block));
            } else {
                wrapped_visitor = nb::none();
            }
        }

        void visit_fixed_window_output_impl(TimeSeriesType *output) override {
            // Try all registered value types - PyTimeSeriesWindowOutput takes the underlying type directly
            if (auto *window_obj = dynamic_cast<TimeSeriesFixedWindowOutput<nb::object> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<nb::object>>(window_obj, control_block));
            } else if (auto *window_int = dynamic_cast<TimeSeriesFixedWindowOutput<int64_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<int64_t>>(window_int, control_block));
            } else if (auto *window_double = dynamic_cast<TimeSeriesFixedWindowOutput<double> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<double>>(window_double, control_block));
            } else if (auto *window_bool = dynamic_cast<TimeSeriesFixedWindowOutput<bool> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<bool>>(window_bool, control_block));
            } else if (auto *window_date = dynamic_cast<TimeSeriesFixedWindowOutput<engine_date_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<engine_date_t>>(window_date, control_block));
            } else if (auto *window_time = dynamic_cast<TimeSeriesFixedWindowOutput<engine_time_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<engine_time_t>>(window_time, control_block));
            } else if (auto *window_timedelta = dynamic_cast<TimeSeriesFixedWindowOutput<engine_time_delta_t> *>(output)) {
                wrapped_visitor = nb::cast(
                    PyTimeSeriesWindowOutput<TimeSeriesFixedWindowOutput<engine_time_delta_t>>(window_timedelta, control_block));
            } else {
                wrapped_visitor = nb::none();
            }
        }

        void visit_time_window_output_impl(TimeSeriesType *output) override {
            // Try all registered value types - PyTimeSeriesWindowOutput takes the underlying type directly
            if (auto *window_obj = dynamic_cast<TimeSeriesTimeWindowOutput<nb::object> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<nb::object>>(window_obj, control_block));
            } else if (auto *window_int = dynamic_cast<TimeSeriesTimeWindowOutput<int64_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<int64_t>>(window_int, control_block));
            } else if (auto *window_double = dynamic_cast<TimeSeriesTimeWindowOutput<double> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<double>>(window_double, control_block));
            } else if (auto *window_bool = dynamic_cast<TimeSeriesTimeWindowOutput<bool> *>(output)) {
                wrapped_visitor = nb::cast(PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<bool>>(window_bool, control_block));
            } else if (auto *window_date = dynamic_cast<TimeSeriesTimeWindowOutput<engine_date_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<engine_date_t>>(window_date, control_block));
            } else if (auto *window_time = dynamic_cast<TimeSeriesTimeWindowOutput<engine_time_t> *>(output)) {
                wrapped_visitor =
                    nb::cast(PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<engine_time_t>>(window_time, control_block));
            } else if (auto *window_timedelta = dynamic_cast<TimeSeriesTimeWindowOutput<engine_time_delta_t> *>(output)) {
                wrapped_visitor = nb::cast(
                    PyTimeSeriesWindowOutput<TimeSeriesTimeWindowOutput<engine_time_delta_t>>(window_timedelta, control_block));
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
            wrapped_visitor = nb::cast(PyTimeSeriesValueReferenceOutput(&source, control_block));
        }

        void visit(TimeSeriesBundleReferenceOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesBundleReferenceOutput(&source, control_block));
        }

        void visit(TimeSeriesSetReferenceOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesSetReferenceOutput(&source, control_block));
        }

        void visit(TimeSeriesListReferenceOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesListReferenceOutput(&source, control_block));
        }

        void visit(TimeSeriesDictReferenceOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesDictReferenceOutput(&source, control_block));
        }

        void visit(TimeSeriesWindowReferenceOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesWindowReferenceOutput(&source, control_block));
        }

        // Handle other output types
        void visit(TimeSeriesBundleOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesBundleOutput(&source, control_block));
        }

        void visit(const TimeSeriesBundleOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesBundleOutput(const_cast<TimeSeriesBundleOutput *>(&source), control_block));
        }

        void visit(TimeSeriesListOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesListOutput(&source, control_block));
        }

        void visit(const TimeSeriesListOutput &source) override {
            wrapped_visitor = nb::cast(PyTimeSeriesListOutput(const_cast<TimeSeriesListOutput *>(&source), control_block));
        }
    };

    // wrap when no control block is readily available.
    nb::object wrap_output(const TimeSeriesOutput *impl) { return wrap_output(impl, impl->owning_graph()->control_block()); }

    nb::object wrap_output(const TimeSeriesOutput *impl, const control_block_ptr &control_block) {
        return get_or_create_wrapper(impl, control_block, [](auto impl, const auto &cb) {
            WrapOutputVisitor visitor(cb);
            impl->accept(visitor);
            return visitor.wrapped_visitor.is_valid() ? visitor.wrapped_visitor : nb::none();
        });
    }

    nb::object wrap_time_series(const TimeSeriesInput *impl, const control_block_ptr &control_block) {
        return wrap_input(impl, control_block);
    }

    nb::object wrap_time_series(const TimeSeriesInput *impl) {
        return wrap_input(impl, impl->owning_graph()->control_block());
    }

    nb::object wrap_time_series(const TimeSeriesOutput *impl, const control_block_ptr &control_block) {
        return wrap_output(impl, control_block);
    }

    nb::object wrap_time_series(const TimeSeriesOutput *impl) {
        return wrap_output(impl, impl->owning_graph()->control_block());
    }

    Node *unwrap_node(const nb::handle &obj) {
        if (auto *py_node = nb::inst_ptr<PyNode>(obj)) { unwrap_node(*py_node); }
        return nullptr;
    }

    Node *unwrap_node(const PyNode &node_) { return node_._impl.get(); }

    TimeSeriesInput *unwrap_input(const nb::handle &obj) {
        if (auto *py_input = nb::inst_ptr<PyTimeSeriesInput>(obj)) { return unwrap_input(*py_input); }
        return nullptr;
    }

    TimeSeriesInput *unwrap_input(const PyTimeSeriesInput &input_) {
        // return input_._impl.get();
        return input_.impl();
    }

    TimeSeriesOutput *unwrap_output(const nb::handle &obj) {
        if (auto *py_output = nb::inst_ptr<PyTimeSeriesOutput>(obj)) { return unwrap_output(*py_output); }
        return nullptr;
    }

    TimeSeriesOutput *unwrap_output(const PyTimeSeriesOutput &output_) { return output_.impl(); }

    // nb::object wrap_evaluation_engine_api(const EvaluationEngineApi *impl, control_block_ptr control_block) {
    //     return get_or_create_wrapper(impl, std::move(control_block), [](EvaluationEngineApi *impl, control_block_ptr cb)
    //     {
    //         return PyEvaluationEngineApi(impl, std::move(cb));
    //     });
    // }
    //
    // nb::object wrap_evaluation_clock(const EvaluationClock *impl, control_block_ptr control_block) {
    //     if (impl == nullptr) { return nb::none(); }
    //
    //     return get_or_create_wrapper(impl, control_block, [](EvaluationClock *mutable_impl, control_block_ptr cb) {
    //         return PyEvaluationClock(mutable_impl, std::move(cb));
    //     });
    // }

    nb::object wrap_traits(const Traits *impl, const control_block_ptr &control_block) {
        // Don't cache traits wrappers - traits is a member of Graph, not a separate heap object
        // Caching on intrusive_base could cause issues during graph teardown
        if (!impl) { return nb::none(); }
        auto *mutable_impl = const_cast<Traits *>(impl);
        auto  wrapper      = PyTraits({mutable_impl, std::move(control_block)});
        return nb::cast(std::move(wrapper));
    }

}  // namespace hgraph
