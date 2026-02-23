#include <hgraph/api/python/py_ref.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_reference_ops.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/node.h>
#include <hgraph/types/constants.h>

#include <utility>

namespace hgraph
{
    // Implements TSReference.bind_input() protocol:
    //   EMPTY      → unbind the input
    //   PEERED     → rebind input to the resolved output (with reactivation if needed)
    //   NON_PEERED → iterate children and recursively bind each child ref
    static void ts_reference_bind_input(TSReference& self, nb::object input) {
        if (self.is_empty()) {
            input.attr("un_bind_output")();
        } else if (self.is_peered()) {
            bool is_bound = nb::cast<bool>(input.attr("bound"));
            bool reactivate = false;
            if (is_bound && !nb::cast<bool>(input.attr("has_peer"))) {
                reactivate = nb::cast<bool>(input.attr("active"));
                if (reactivate) input.attr("make_passive")();
                input.attr("un_bind_output")();
            }
            nb::object output = nb::cast(self).attr("output");
            if (!output.is_none()) {
                input.attr("bind_output")(output);
            }
            if (reactivate) input.attr("make_active")();
        } else if (self.is_non_peered()) {
            bool is_bound = nb::cast<bool>(input.attr("bound"));
            bool reactivate = false;
            if (is_bound && nb::cast<bool>(input.attr("has_peer"))) {
                reactivate = nb::cast<bool>(input.attr("active"));
                if (reactivate) input.attr("make_passive")();
                input.attr("un_bind_output")();
            }
            auto items = self.items();
            size_t idx = 0;
            for (auto child : input) {
                if (idx < items.size()) {
                    auto& ref = items[idx];
                    if (!ref.is_empty()) {
                        nb::cast(ref).attr("bind_input")(child);
                    } else if (nb::cast<bool>(nb::borrow(child).attr("bound"))) {
                        nb::borrow(child).attr("un_bind_output")();
                    }
                }
                idx++;
            }
            if (reactivate) input.attr("make_active")();
        }
    }

    void ref_register_with_nanobind(nb::module_ &m) {
        // ============================================================
        // TSReference - Value-stack time-series reference
        // ============================================================

        // Note: PortType enum is registered in py_ts_input_output.cpp

        // TSReference class
        nb::class_<TSReference>(m, "TimeSeriesReference")
            .def(nb::init<>())  // Default constructor (EMPTY)
            .def(nb::init<const TSReference&>())  // Copy constructor
            .def_static("empty", &TSReference::empty, "Create an empty reference")
            .def_prop_ro("is_empty", &TSReference::is_empty)
            .def_prop_ro("is_peered", &TSReference::is_peered)
            .def_prop_ro("is_non_peered", &TSReference::is_non_peered)
            .def_prop_ro("has_output", &TSReference::has_output)
            .def_prop_ro("kind", &TSReference::kind)
            .def("size", &TSReference::size)
            .def("__str__", &TSReference::to_string)
            .def("__repr__", &TSReference::to_string)
            .def("__eq__", [](const TSReference& self, const TSReference& other) {
                return self == other;
            }, nb::is_operator())
            .def("to_fq", &TSReference::to_fq, "Convert to FQReference for serialization")
            // Factory method compatible with Python TimeSeriesReference.make() API
            // Returns TSReference (path-based) - the new system
            .def_static(
                "make",
                [](nb::object ts, nb::object from_items) -> TSReference {
                    // Case 1: No arguments - return empty reference
                    if (ts.is_none() && from_items.is_none()) {
                        return TSReference::empty();
                    }

                    // Case 2: from_items is provided - create NON_PEERED
                    if (!from_items.is_none()) {
                        std::vector<TSReference> items;
                        for (auto item : from_items) {
                            if (nb::isinstance<TSReference>(item)) {
                                items.push_back(nb::cast<TSReference>(item));
                            } else {
                                items.push_back(TSReference::empty());
                            }
                        }
                        return TSReference::non_peered(std::move(items));
                    }

                    // Case 3: ts is a TSReference directly - return it
                    if (nb::isinstance<TSReference>(ts)) {
                        return nb::cast<TSReference>(ts);
                    }

                    // Case 4: ts is an output - extract ShortPath
                    if (nb::isinstance<PyTimeSeriesOutput>(ts)) {
                        auto& py_output = nb::cast<PyTimeSeriesOutput&>(ts);
                        const ShortPath& path = py_output.output_view().short_path();
                        if (path.valid()) {
                            return TSReference::peered(path);
                        }
                        // Path is invalid (e.g., feature output not in graph).
                        // Store as PYTHON_BOUND so rebind() can extract ViewData.
                        return TSReference::python_bound(nb::object(ts));
                    }

                    // Case 5: ts is a REF input - extract TSReference from its value
                    if (nb::isinstance<PyTimeSeriesReferenceInput>(ts)) {
                        auto& py_input = nb::cast<PyTimeSeriesReferenceInput&>(ts);
                        auto input_view = py_input.input_view();
                        if (input_view.valid()) {
                            // Get the TSReference value from the REF input
                            auto val_view = input_view.value();
                            return val_view.as<TSReference>();
                        }
                        throw std::runtime_error("TSReference.make: REF input is not valid");
                    }

                    // Case 6: ts is a regular input with peer - wrap the bound output
                    if (nb::isinstance<PyTimeSeriesInput>(ts)) {
                        auto& py_input = nb::cast<PyTimeSeriesInput&>(ts);
                        const auto& input_view = py_input.input_view();
                        if (input_view.is_bound()) {
                            TSOutput* bound_output = input_view.bound_output();
                            if (bound_output) {
                                ShortPath path = bound_output->root_path();
                                return TSReference::peered(std::move(path));
                            }
                        }
                        throw std::runtime_error("TSReference.make: Input is not bound");
                    }

                    // Fallback: empty reference
                    return TSReference::empty();
                },
                "ts"_a = nb::none(), "from_items"_a = nb::none(),
                "Create a TSReference from a time-series input/output or from items")
            // .output property: resolves PEERED reference to a TimeSeriesOutput wrapper
            .def_prop_ro("output", [](const TSReference& self) -> nb::object {
                if (!self.is_peered()) {
                    return nb::none();
                }
                try {
                    TSView resolved = self.resolve(self.resolve_time());
                    if (resolved) {
                        // If the resolved target is itself a REF (e.g., TSD element of type REF[TSB]),
                        // follow the indirection: read the inner TSReference and resolve it
                        // to get the actual underlying output (e.g., the TSB).
                        const auto& vd = resolved.view_data();
                        if (vd.meta && vd.meta->kind == TSKind::REF) {
                            auto val = resolved.value();
                            if (val.valid()) {
                                TSReference inner_ref = val.as<TSReference>();
                                if (inner_ref.is_peered()) {
                                    inner_ref.set_resolve_time(self.resolve_time());
                                    TSView target = inner_ref.resolve(self.resolve_time());
                                    if (target) {
                                        TSOutputView target_ov(std::move(target), nullptr);
                                        return wrap_output_view(std::move(target_ov));
                                    }
                                }
                            }
                        }
                        TSOutputView output_view(std::move(resolved), nullptr);
                        return wrap_output_view(std::move(output_view));
                    }
                } catch (...) {}
                return nb::none();
            })
            // ABC-compatible properties to match Python TimeSeriesReference
            .def_prop_ro("is_valid", [](const TSReference& self) {
                return self.is_valid(self.resolve_time());
            })
            .def_prop_ro("is_bound", [](const TSReference& self) {
                return self.is_peered();
            })
            .def_prop_ro("is_unbound", [](const TSReference& self) {
                return self.is_non_peered();
            })
            .def_prop_ro("items", [](const TSReference& self) -> nb::object {
                if (!self.is_non_peered()) {
                    return nb::none();
                }
                auto items = self.items();
                for (auto& item : items) {
                    item.set_resolve_time(self.resolve_time());
                }
                return nb::cast(std::move(items));
            })
            .def("__getitem__", [](const TSReference& self, size_t idx) {
                TSReference item = self[idx];
                item.set_resolve_time(self.resolve_time());
                return item;
            })
            // bind_input: matches Python TimeSeriesReference.bind_input() behavior
            // Called by operators like valid_impl: ts.value.bind_input(ts_value)
            .def("bind_input", &ts_reference_bind_input, "input"_a);

        // TSReference::Kind enum
        nb::enum_<TSReference::Kind>(m, "TSReferenceKind")
            .value("EMPTY", TSReference::Kind::EMPTY)
            .value("PEERED", TSReference::Kind::PEERED)
            .value("NON_PEERED", TSReference::Kind::NON_PEERED);

        // Backward-compatible alias expected by tests and older callers.
        m.attr("TSReference") = m.attr("TimeSeriesReference");

        // FQReference class
        nb::class_<FQReference>(m, "FQReference")
            .def(nb::init<>())  // Default constructor (EMPTY)
            .def_static("empty", &FQReference::empty, "Create an empty FQReference")
            .def_static("peered", &FQReference::peered,
                "node_id"_a, "port_type"_a, "indices"_a,
                "Create a peered FQReference")
            .def_static("non_peered", &FQReference::non_peered,
                "items"_a,
                "Create a non-peered FQReference")
            .def_prop_ro("is_empty", &FQReference::is_empty)
            .def_prop_ro("is_peered", &FQReference::is_peered)
            .def_prop_ro("is_non_peered", &FQReference::is_non_peered)
            .def_prop_ro("has_output", &FQReference::has_output)
            .def_prop_ro("is_valid", &FQReference::is_valid)
            .def_prop_ro("kind", [](const FQReference& self) { return self.kind; })
            .def_prop_ro("node_id", [](const FQReference& self) { return self.node_id; })
            .def_prop_ro("port_type", [](const FQReference& self) { return self.port_type; })
            .def_prop_ro("indices", [](const FQReference& self) { return self.indices; })
            .def_prop_ro("items", [](const FQReference& self) { return self.items; })
            .def("__str__", &FQReference::to_string)
            .def("__repr__", &FQReference::to_string)
            .def("__eq__", [](const FQReference& self, const FQReference& other) {
                return self == other;
            }, nb::is_operator());

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

    // Base REF Output constructor
    PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput(TSOutputView view)
        : PyTimeSeriesOutput(std::move(view)) {}

    nb::str PyTimeSeriesReferenceOutput::to_string() const {
        auto v = fmt::format("TimeSeriesReferenceOutput[view]");
        return nb::str(v.c_str());
    }

    nb::str PyTimeSeriesReferenceOutput::to_repr() const {
        return to_string();
    }

    void PyTimeSeriesReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesReferenceOutput, PyTimeSeriesOutput>(m, "TimeSeriesReferenceOutput")
            .def("__str__", &PyTimeSeriesReferenceOutput::to_string)
            .def("__repr__", &PyTimeSeriesReferenceOutput::to_repr)
            .def("__getitem__", [](PyTimeSeriesReferenceOutput& self, nb::object key) -> nb::object {
                // Resolve through the REF to get the underlying output, then navigate
                const auto& ov = self.output_view();
                const auto& vd = ov.ts_view().view_data();
                if (!vd.meta || vd.meta->kind != TSKind::REF) {
                    throw nb::index_error("TimeSeriesReferenceOutput: not a REF kind");
                }
                auto val = ov.ts_view().value();
                if (!val.valid()) {
                    throw nb::index_error("TimeSeriesReferenceOutput: REF has no value");
                }
                TSReference inner_ref = val.as<TSReference>();
                if (!inner_ref.is_peered()) {
                    throw nb::index_error("TimeSeriesReferenceOutput: REF is not peered");
                }
                inner_ref.set_resolve_time(ov.ts_view().current_time());
                TSView target = inner_ref.resolve(ov.ts_view().current_time());
                if (!target) {
                    throw nb::index_error("TimeSeriesReferenceOutput: could not resolve REF target");
                }
                // Navigate to child by string name or integer index
                TSOutputView target_ov(std::move(target), nullptr);
                if (nb::isinstance<nb::str>(key)) {
                    std::string name = nb::cast<std::string>(key);
                    TSOutputView child = target_ov.field(name);
                    return wrap_output_view(std::move(child));
                } else if (nb::isinstance<nb::int_>(key)) {
                    size_t idx = nb::cast<size_t>(key);
                    TSOutputView child = target_ov[idx];
                    return wrap_output_view(std::move(child));
                }
                throw nb::type_error("TimeSeriesReferenceOutput: key must be str or int");
            });
    }

    // Base REF Input constructor
    PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput(TSInputView view)
        : PyTimeSeriesInput(std::move(view)) {}

    // Helper: wrap a concrete (non-REF) output TSView as BoundTimeSeriesReference
    static nb::object make_bound_ref(TSView&& target_view) {
        // Ensure link_data is clear: resolved TSValue link storage may contain
        // REFLinks from internal graph wiring that shouldn't be followed.
        target_view.view_data().link_data = nullptr;
        TSOutputView ov(std::move(target_view), nullptr);
        nb::object wrapper = wrap_output_view(std::move(ov));
        auto ref_module = nb::module_::import_("hgraph._types._ref_type");
        auto make_fn = ref_module.attr("TimeSeriesReference").attr("make");
        return make_fn(wrapper);
    }

    nb::object PyTimeSeriesReferenceInput::ref_value() const {
        const auto& iv = input_view();
        const auto& vd = iv.ts_view().view_data();
        const engine_time_t time = iv.ts_view().current_time();

        if (vd.ops && vd.ops->ref_resolve_value) {
            auto result = vd.ops->ref_resolve_value(vd, time);
            switch (result.case_type) {
                case RefValueResolution::CONCRETE_OUTPUT:
                    return make_bound_ref(TSView(result.output_vd, time));
                case RefValueResolution::REF_AS_IS:
                    return nb::cast(std::move(result.ref));
                case RefValueResolution::EMPTY:
                default:
                    return nb::cast(TSReference::empty());
            }
        }

        // Fallback for non-REF ops (shouldn't happen for REF inputs)
        return nb::cast(TSReference::empty());
    }

    static nb::object delegate_ref_collection_method(const PyTimeSeriesReferenceInput& self, const char* method_name) {
        nb::object ref = self.ref_value();
        if (ref.is_none()) {
            return nb::list();
        }

        try {
            if (nb::hasattr(ref, "has_output") && nb::cast<bool>(ref.attr("has_output"))) {
                nb::object output = ref.attr("output");
                if (!output.is_none()) {
                    if (nb::isinstance<PyTimeSeriesOutput>(output)) {
                        auto& py_output = nb::cast<PyTimeSeriesOutput&>(output);
                        nb::object input_wrapper =
                            wrap_input_view(TSInputView(py_output.output_view().ts_view(), nullptr));
                        if (!input_wrapper.is_none() && nb::hasattr(input_wrapper, method_name)) {
                            return input_wrapper.attr(method_name)();
                        }
                    }

                    if (nb::hasattr(output, method_name)) {
                        return output.attr(method_name)();
                    }
                }
            }
            if (nb::hasattr(ref, method_name)) {
                return ref.attr(method_name)();
            }
        } catch (...) {
            // Fall through to empty list when delegation cannot be resolved.
        }

        return nb::list();
    }

    nb::object PyTimeSeriesReferenceInput::modified_items() const {
        return delegate_ref_collection_method(*this, "modified_items");
    }

    nb::object PyTimeSeriesReferenceInput::removed_keys() const {
        return delegate_ref_collection_method(*this, "removed_keys");
    }

    nb::str PyTimeSeriesReferenceInput::to_string() const {
        return nb::str("TimeSeriesReferenceInput[view]");
    }

    nb::str PyTimeSeriesReferenceInput::to_repr() const { return to_string(); }

    void PyTimeSeriesReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesReferenceInput, PyTimeSeriesInput>(m, "TimeSeriesReferenceInput")
            .def_prop_ro("value", &PyTimeSeriesReferenceInput::ref_value)
            .def_prop_ro("delta_value", &PyTimeSeriesReferenceInput::ref_value)
            .def("modified_items", &PyTimeSeriesReferenceInput::modified_items)
            .def("removed_keys", &PyTimeSeriesReferenceInput::removed_keys)
            .def("__str__", &PyTimeSeriesReferenceInput::to_string)
            .def("__repr__", &PyTimeSeriesReferenceInput::to_repr);
    }

    // Specialized Reference Input classes - nanobind registration only
    void PyTimeSeriesValueReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesValueReferenceInput");
    }

    void PyTimeSeriesListReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesListReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesListReferenceInput");
    }

    void PyTimeSeriesBundleReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesBundleReferenceInput");
    }

    void PyTimeSeriesDictReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesDictReferenceInput");
    }

    void PyTimeSeriesSetReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesSetReferenceInput");
    }

    void PyTimeSeriesWindowReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesWindowReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesWindowReferenceInput");
    }

    // Specialized Reference Output classes - nanobind registration only
    void PyTimeSeriesValueReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesValueReferenceOutput");
    }

    void PyTimeSeriesListReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesListReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesListReferenceOutput");
    }

    void PyTimeSeriesBundleReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesBundleReferenceOutput");
    }

    void PyTimeSeriesDictReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesDictReferenceOutput");
    }

    void PyTimeSeriesSetReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesSetReferenceOutput");
    }

    void PyTimeSeriesWindowReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesWindowReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesWindowReferenceOutput");
    }

    // ============================================================
    // resolve_python_bound_reference - called from ts_ops.cpp
    // ============================================================
    // Extracts ViewData from a PYTHON_BOUND TSReference.
    // This lives here (not in ts_ops.cpp) because it needs PyTimeSeriesOutput
    // from py_time_series.h. Including that header in ts_ops.cpp changes
    // nanobind type resolution and causes regressions.
    std::optional<ViewData> resolve_python_bound_reference(const TSReference* ts_ref, engine_time_t current_time) {
        if (!ts_ref || !ts_ref->is_python_bound()) {
            return std::nullopt;
        }
        try {
            nb::object py_obj = ts_ref->python_object();
            PyTimeSeriesOutput* py_output_ptr = nullptr;

            if (nb::isinstance<PyTimeSeriesOutput>(py_obj)) {
                // Case (a): direct PyTimeSeriesOutput
                py_output_ptr = &nb::cast<PyTimeSeriesOutput&>(py_obj);
            } else if (nb::hasattr(py_obj, "has_output") &&
                       nb::cast<bool>(py_obj.attr("has_output"))) {
                // Case (b): BoundTimeSeriesReference wrapping a PyTimeSeriesOutput
                nb::object output_obj = py_obj.attr("output");
                if (nb::isinstance<PyTimeSeriesOutput>(output_obj)) {
                    py_output_ptr = &nb::cast<PyTimeSeriesOutput&>(output_obj);
                }
            }

            if (py_output_ptr) {
                const ViewData& vd = py_output_ptr->output_view().ts_view().view_data();
                if (vd.value_data) {
                    return vd;
                }
            }
        } catch (...) {
            // Failed to extract ViewData from Python object
        }
        return std::nullopt;
    }

}  // namespace hgraph
