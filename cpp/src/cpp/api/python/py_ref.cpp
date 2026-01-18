#include <hgraph/api/python/py_ref.h>
#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/ts_overlay_storage.h>
#include <hgraph/types/time_series/ts_ref_target_link.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>

#include <unordered_map>
#include <utility>

namespace hgraph
{
    // Forward declarations
    extern std::unordered_map<const TSValue*, nb::object> g_ref_output_cache;
    void clear_ref_output_cache();

    void ref_register_with_nanobind(nb::module_ &m) {
        // Register cleanup function to be called before shutdown
        m.def("_clear_ref_output_cache", &clear_ref_output_cache,
              "Clear the REF output cache - call this before shutdown to prevent crashes");

        nb::class_<TimeSeriesReference>(m, "TimeSeriesReference")
            .def("__str__", &TimeSeriesReference::to_string)
            .def("__repr__", &TimeSeriesReference::to_string)
            .def(
                "__eq__",
                [](const TimeSeriesReference &self, nb::object other) {
                    if (other.is_none()) { return false; }
                    if (nb::isinstance<TimeSeriesReference>(other)) {
                        return self == nb::cast<TimeSeriesReference>(other);
                    }
                    return false;
                },
                nb::arg("other"), nb::is_operator())
            .def(
                "bind_input",
                [](TimeSeriesReference &self, PyTimeSeriesInput &ts_input) {
                    if (self.is_empty()) {
                        // Empty reference - unbind
                        ts_input.un_bind_output();
                        return;
                    }
                    if (self.is_view_bound()) {
                        // VIEW_BOUND - bind the passthrough input to the referenced output
                        const TSValue* output = self.view_output();
                        ts_input.set_bound_output(output);
                        return;
                    }
                    // BOUND or UNBOUND - not supported for view-based wrappers yet
                    throw std::runtime_error("TimeSeriesReference::bind_input: only VIEW_BOUND and EMPTY supported for view-based wrappers");
                },
                "input_"_a)
            .def_prop_ro("has_output", &TimeSeriesReference::has_output)
            .def_prop_ro("is_empty", &TimeSeriesReference::is_empty)
            .def_prop_ro("is_bound", &TimeSeriesReference::is_bound)
            .def_prop_ro("is_unbound", &TimeSeriesReference::is_unbound)
            .def_prop_ro("is_valid", &TimeSeriesReference::is_valid)
            .def_prop_ro("output", [](TimeSeriesReference &self) -> nb::object {
                if (self.is_empty()) {
                    return nb::none();
                }
                if (self.is_view_bound()) {
                    // VIEW_BOUND - wrap the TSValue* as a Python output view
                    // The underlying TSValue is actually mutable (it's an output node's value)
                    // We need output wrapper to support methods like get_ref, release_ref, etc.
                    const TSValue* view_out = self.view_output();
                    if (!view_out) {
                        return nb::none();
                    }

                    // Check if this is a reference to an element within a container
                    int elem_idx = self.view_element_index();
                    if (elem_idx >= 0 && view_out->ts_meta() && view_out->ts_meta()->kind() == TSTypeKind::TSL) {
                        // Navigate to the element within the TSL
                        // For TSL elements, use input view for now (output methods not commonly needed)
                        TSView container_view(*view_out);
                        TSLView list_view(container_view.value_view().data(),
                                         static_cast<const TSLTypeMeta*>(view_out->ts_meta()),
                                         static_cast<ListTSOverlay*>(container_view.overlay()));
                        TSView elem_view = list_view.element(static_cast<size_t>(elem_idx));
                        return wrap_input_view(elem_view);
                    }

                    // Get the type metadata to determine the appropriate wrapper
                    const auto* meta = view_out->ts_meta();
                    if (!meta) {
                        return nb::none();
                    }
                    // Create a TSMutableView from the TSValue (safe since it's actually mutable)
                    TSValue* mutable_view_out = const_cast<TSValue*>(view_out);
                    TSMutableView view(*mutable_view_out);
                    return wrap_output_view(view);
                }
                if (self.is_bound()) {
                    // BOUND - return the wrapped legacy output
                    const auto& ptr = self.output();
                    if (!ptr) {
                        return nb::none();
                    }
                    // Return it as a Python object - the user can call .delta_value on it
                    return nb::cast(ptr);
                }
                if (self.is_python_bound()) {
                    // PYTHON_BOUND - return the stored Python object directly
                    // This is used for synthetic outputs like CppKeySetIsEmptyOutput
                    return self.python_output();
                }
                // UNBOUND - no single output
                return nb::none();
            })
            .def_prop_ro("items", &TimeSeriesReference::items)
            .def("__getitem__", &TimeSeriesReference::operator[])
            .def_static(
                "make",
                [](nb::object ts, nb::object items) -> nb::object {
                    // Case 1: Empty reference
                    if (ts.is_none() && items.is_none()) {
                        return nb::cast(TimeSeriesReference::make());
                    }

                    // Case 2: from_items - create unbound reference
                    if (!items.is_none()) {
                        std::vector<TimeSeriesReference> refs;
                        for (auto item : items) {
                            // Each item should be a TimeSeriesReference
                            if (nb::isinstance<TimeSeriesReference>(item)) {
                                refs.push_back(nb::cast<TimeSeriesReference>(item));
                            } else {
                                throw std::runtime_error("TimeSeriesReference.make: from_items must contain TimeSeriesReference objects");
                            }
                        }
                        return nb::cast(TimeSeriesReference::make(std::move(refs)));
                    }

                    // Case 3: ts parameter provided
                    // Check if it's a view-based output wrapper (PyTimeSeriesOutput)
                    if (nb::isinstance<PyTimeSeriesOutput>(ts)) {
                        auto& output = nb::cast<PyTimeSeriesOutput&>(ts);
                        const TSValue* ts_value = output.view().root();
                        if (ts_value) {
                            return nb::cast(TimeSeriesReference::make_view_bound(ts_value));
                        }
                        throw std::runtime_error("TimeSeriesReference.make: PyTimeSeriesOutput has no root TSValue");
                    }

                    // Check if it's a view-based input wrapper (PyTimeSeriesInput)
                    if (nb::isinstance<PyTimeSeriesInput>(ts)) {
                        auto& input = nb::cast<PyTimeSeriesInput&>(ts);
                        // For inputs, check if there's a bound output
                        const TSValue* bound = input.bound_output();
                        if (bound) {
                            return nb::cast(TimeSeriesReference::make_view_bound(bound));
                        }
                        // No bound output - check the view's root
                        const TSValue* ts_value = input.view().root();
                        if (ts_value) {
                            return nb::cast(TimeSeriesReference::make_view_bound(ts_value));
                        }
                        throw std::runtime_error("TimeSeriesReference.make: PyTimeSeriesInput has no bound output or root TSValue");
                    }

                    // Check if it's a TimeSeriesReferenceInput - return its value
                    if (nb::isinstance<TimeSeriesReferenceInput>(ts)) {
                        auto& ref_input = nb::cast<TimeSeriesReferenceInput&>(ts);
                        return nb::cast(ref_input.value());
                    }

                    // Check if it's already a TimeSeriesReference - just return it
                    if (nb::isinstance<TimeSeriesReference>(ts)) {
                        return ts;  // Already a Python object wrapping TimeSeriesReference
                    }

                    // Check for CppKeySetIsEmptyOutput - use Python's BoundTimeSeriesReference
                    // CppKeySetIsEmptyOutput doesn't have an underlying TSValue, so we need to
                    // use Python's BoundTimeSeriesReference which can wrap any object with
                    // value/valid/modified properties.
                    if (nb::isinstance<CppKeySetIsEmptyOutput>(ts)) {
                        auto ref_module = nb::module_::import_("hgraph._impl._types._ref");
                        auto bound_ref_class = ref_module.attr("BoundTimeSeriesReference");
                        return bound_ref_class(ts);  // Return Python BoundTimeSeriesReference
                    }

                    // Check for legacy TimeSeriesOutput
                    try {
                        auto output_ptr = nb::cast<time_series_output_s_ptr>(ts);
                        if (output_ptr) {
                            return nb::cast(TimeSeriesReference::make(std::move(output_ptr)));
                        }
                    } catch (...) {
                        // Not a legacy output, continue checking
                    }

                    throw std::runtime_error("TimeSeriesReference.make: unsupported ts type");
                },
                "ts"_a = nb::none(), "from_items"_a = nb::none());

        // PyTS wrapper classes registration
        PyTimeSeriesReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesReferenceInput::register_with_nanobind(m);
        PyTimeSeriesValueReferenceInput::register_with_nanobind(m);
        PyTimeSeriesListReferenceInput::register_with_nanobind(m);
        PyTimeSeriesBundleReferenceInput::register_with_nanobind(m);
        PyTimeSeriesDictReferenceInput::register_with_nanobind(m);
        PyTimeSeriesSetReferenceInput::register_with_nanobind(m);
        PyTimeSeriesWindowReferenceInput::register_with_nanobind(m);
        PyTimeSeriesValueReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesListReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesBundleReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesDictReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesSetReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesWindowReferenceOutput::register_with_nanobind(m);
    }

    // ========== PyTimeSeriesReferenceOutput ==========

    PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput(TSMutableView view)
        : PyTimeSeriesOutput(view) {}

    nb::str PyTimeSeriesReferenceOutput::to_string() const {
        auto s = fmt::format("TimeSeriesReferenceOutput[valid={}]", _view.ts_valid());
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesReferenceOutput::to_repr() const {
        return to_string();
    }

    // Global cache for REF output values (workaround for view-based storage)
    // Key is the TSValue pointer, value is the stored TimeSeriesReference
    // Used by TSView::to_python() and TSMutableView::from_python() for REF types
    std::unordered_map<const TSValue*, nb::object> g_ref_output_cache;

    // Cleanup function to clear the caches before Python shuts down
    // This prevents crashes during module unload when Python objects are
    // destroyed after the interpreter starts finalizing
    void clear_ref_output_cache() {
        g_ref_output_cache.clear();
    }

    void PyTimeSeriesReferenceOutput::register_with_nanobind(nb::module_ &m) {
        // Note: value, delta_value, set_value, apply_result are inherited from
        // PyTimeSeriesOutput. The view layer handles REF-specific behavior via
        // TSTypeKind::REF dispatch in TSView::to_python() and TSMutableView::from_python().
        nb::class_<PyTimeSeriesReferenceOutput, PyTimeSeriesOutput>(m, "TimeSeriesReferenceOutput")
            .def("__str__", &PyTimeSeriesReferenceOutput::to_string)
            .def("__repr__", &PyTimeSeriesReferenceOutput::to_repr);
    }

    // ========== PyTimeSeriesReferenceInput ==========

    PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput(TSView view)
        : PyTimeSeriesInput(view) {
    }

    // Note: valid() and modified() are inherited from PyTimeSeriesInput.
    // The REF-specific logic is in TSView::ts_valid() and TSView::modified_at().

    nb::str PyTimeSeriesReferenceInput::to_string() const {
        auto s = fmt::format("TimeSeriesReferenceInput[valid={}]", _view.ts_valid());
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesReferenceInput::to_repr() const { return to_string(); }

    void PyTimeSeriesReferenceInput::register_with_nanobind(nb::module_ &m) {
        // Note: value and delta_value are inherited from PyTimeSeriesInput.
        // The view layer handles REF-specific behavior via TSTypeKind::REF
        // dispatch in TSView::to_python(), including link navigation for inputs.
        // Note: valid and modified are inherited from PyTimeSeriesInput base class.
        // The REF-specific logic is in TSView::ts_valid() and TSView::modified_at().
        nb::class_<PyTimeSeriesReferenceInput, PyTimeSeriesInput>(m, "TimeSeriesReferenceInput")
            .def("__str__", &PyTimeSeriesReferenceInput::to_string)
            .def("__repr__", &PyTimeSeriesReferenceInput::to_repr)
            // Explicitly register bind_output with nb::object and nb::arg().none() to accept None
            // C++ get_ref may return None for missing keys
            .def("bind_output", [](nb::handle self_handle, nb::object output) {
                auto& self = nb::cast<PyTimeSeriesReferenceInput&>(self_handle);

                // First, unregister from previous output if any (clean up stale observers)
                // This is important when keys are removed and recreated - old observers
                // would otherwise have stale overlay pointers
                nb::object prev_output = self.get_bound_py_output();
                if (prev_output.is_valid() && !prev_output.is_none() && nb::hasattr(prev_output, "stop_observing_reference")) {
                    try {
                        prev_output.attr("stop_observing_reference")(self_handle);
                    } catch (...) {
                        // Ignore errors during cleanup
                    }
                }

                self.bind_output(output);

                // If output is a TimeSeriesReferenceOutput, register this input as an observer.
                // This matches Python's TimeSeriesReferenceInput.do_bind_output() behavior.
                // The observer is notified when the REF output's value changes (e.g., key removed).
                if (!output.is_none() && nb::hasattr(output, "observe_reference")) {
                    try {
                        // Pass self_handle to observe_reference - it's the Python wrapper for this input
                        output.attr("observe_reference")(self_handle);
                    } catch (const std::exception& e) {
                        fmt::print(stderr, "[DEBUG bind_output] observe_reference failed: {}\n", e.what());
                    }
                }
            }, nb::arg("output").none())
            // Override un_bind_output to also unregister from observer list
            .def("un_bind_output", [](nb::handle self_handle) {
                auto& self = nb::cast<PyTimeSeriesReferenceInput&>(self_handle);

                // Unregister from previous output if any
                nb::object prev_output = self.get_bound_py_output();
                if (prev_output.is_valid() && !prev_output.is_none() && nb::hasattr(prev_output, "stop_observing_reference")) {
                    try {
                        prev_output.attr("stop_observing_reference")(self_handle);
                    } catch (...) {
                        // Ignore errors during cleanup
                    }
                }

                self.un_bind_output();
            });
    }

    // ========== Specialized Reference Input Classes ==========

    PyTimeSeriesValueReferenceInput::PyTimeSeriesValueReferenceInput(TSView view)
        : PyTimeSeriesReferenceInput(view) {}

    void PyTimeSeriesValueReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesValueReferenceInput");
    }

    PyTimeSeriesListReferenceInput::PyTimeSeriesListReferenceInput(TSView view)
        : PyTimeSeriesReferenceInput(view) {}

    size_t PyTimeSeriesListReferenceInput::size() const {
        throw std::runtime_error("PyTimeSeriesListReferenceInput::size not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesListReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesListReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesListReferenceInput")
            .def("__len__", &PyTimeSeriesListReferenceInput::size);
    }

    PyTimeSeriesBundleReferenceInput::PyTimeSeriesBundleReferenceInput(TSView view)
        : PyTimeSeriesReferenceInput(view) {}

    nb::int_ PyTimeSeriesBundleReferenceInput::size() const {
        throw std::runtime_error("PyTimeSeriesBundleReferenceInput::size not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesBundleReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesBundleReferenceInput")
            .def("__len__", &PyTimeSeriesBundleReferenceInput::size);
    }

    PyTimeSeriesDictReferenceInput::PyTimeSeriesDictReferenceInput(TSView view)
        : PyTimeSeriesReferenceInput(view) {}

    void PyTimeSeriesDictReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesDictReferenceInput");
    }

    PyTimeSeriesSetReferenceInput::PyTimeSeriesSetReferenceInput(TSView view)
        : PyTimeSeriesReferenceInput(view) {}

    void PyTimeSeriesSetReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesSetReferenceInput");
    }

    PyTimeSeriesWindowReferenceInput::PyTimeSeriesWindowReferenceInput(TSView view)
        : PyTimeSeriesReferenceInput(view) {}

    void PyTimeSeriesWindowReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesWindowReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesWindowReferenceInput");
    }

    // ========== Specialized Reference Output Classes ==========

    PyTimeSeriesValueReferenceOutput::PyTimeSeriesValueReferenceOutput(TSMutableView view)
        : PyTimeSeriesReferenceOutput(view) {}

    void PyTimeSeriesValueReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesValueReferenceOutput");
    }

    PyTimeSeriesListReferenceOutput::PyTimeSeriesListReferenceOutput(TSMutableView view)
        : PyTimeSeriesReferenceOutput(view) {}

    nb::int_ PyTimeSeriesListReferenceOutput::size() const {
        throw std::runtime_error("PyTimeSeriesListReferenceOutput::size not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesListReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesListReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesListReferenceOutput")
            .def("__len__", &PyTimeSeriesListReferenceOutput::size);
    }

    PyTimeSeriesBundleReferenceOutput::PyTimeSeriesBundleReferenceOutput(TSMutableView view)
        : PyTimeSeriesReferenceOutput(view) {}

    nb::int_ PyTimeSeriesBundleReferenceOutput::size() const {
        throw std::runtime_error("PyTimeSeriesBundleReferenceOutput::size not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesBundleReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesBundleReferenceOutput")
            .def("__len__", &PyTimeSeriesBundleReferenceOutput::size);
    }

    PyTimeSeriesDictReferenceOutput::PyTimeSeriesDictReferenceOutput(TSMutableView view)
        : PyTimeSeriesReferenceOutput(view) {}

    void PyTimeSeriesDictReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesDictReferenceOutput");
    }

    PyTimeSeriesSetReferenceOutput::PyTimeSeriesSetReferenceOutput(TSMutableView view)
        : PyTimeSeriesReferenceOutput(view) {}

    void PyTimeSeriesSetReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesSetReferenceOutput");
    }

    PyTimeSeriesWindowReferenceOutput::PyTimeSeriesWindowReferenceOutput(TSMutableView view)
        : PyTimeSeriesReferenceOutput(view) {}

    void PyTimeSeriesWindowReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesWindowReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesWindowReferenceOutput");
    }

}  // namespace hgraph
