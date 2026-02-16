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
                return !self.is_empty();
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
                return nb::cast(self.items());
            })
            .def("__getitem__", [](const TSReference& self, size_t idx) {
                return self[idx];
            })
            // bind_input: matches Python TimeSeriesReference.bind_input() behavior
            // Called by operators like valid_impl: ts.value.bind_input(ts_value)
            .def("bind_input", [](TSReference& self, nb::object input) {
                if (self.is_empty()) {
                    // EmptyTimeSeriesReference.bind_input: just unbind
                    input.attr("un_bind_output")();
                } else if (self.is_peered()) {
                    // BoundTimeSeriesReference.bind_input: rebind to resolved output
                    bool reactivate = false;
                    bool is_bound = nb::cast<bool>(input.attr("bound"));
                    if (is_bound && !nb::cast<bool>(input.attr("has_peer"))) {
                        reactivate = nb::cast<bool>(input.attr("active"));
                        if (reactivate) input.attr("make_passive")();
                        input.attr("un_bind_output")();
                    }
                    // Resolve the PEERED reference to its target output
                    nb::object output = nb::cast(self).attr("output");
                    if (!output.is_none()) {
                        input.attr("bind_output")(output);
                    }
                    if (reactivate) input.attr("make_active")();
                } else if (self.is_non_peered()) {
                    // UnBoundTimeSeriesReference.bind_input: iterate children
                    bool reactivate = false;
                    bool is_bound = nb::cast<bool>(input.attr("bound"));
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
            }, "input"_a);

        // TSReference::Kind enum
        nb::enum_<TSReference::Kind>(m, "TSReferenceKind")
            .value("EMPTY", TSReference::Kind::EMPTY)
            .value("PEERED", TSReference::Kind::PEERED)
            .value("NON_PEERED", TSReference::Kind::NON_PEERED);

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

    // Helper: given a REF output's stored TSReference, return the appropriate Python
    // TimeSeriesReference object. Matches Python's PythonTimeSeriesReferenceOutput.value
    // which returns NonPeeredTimeSeriesReference(self._tp) for concrete outputs.
    static nb::object ref_output_value_to_python(const TSReference& stored_ref, engine_time_t time) {
        if (stored_ref.is_peered()) {
            // PEERED inner ref → resolve to concrete output → BoundTimeSeriesReference
            TSReference ref_copy = stored_ref;
            ref_copy.set_resolve_time(time);
            TSView target = ref_copy.resolve(time);
            if (target) {
                TSOutputView ov(std::move(target), nullptr);
                nb::object wrapper = wrap_output_view(std::move(ov));
                auto ref_module = nb::module_::import_("hgraph._types._ref_type");
                auto make_fn = ref_module.attr("TimeSeriesReference").attr("make");
                return make_fn(wrapper);
            }
        }
        if (stored_ref.is_non_peered()) {
            // NON_PEERED → return as-is (dereference uses .items path)
            TSReference ref_copy = stored_ref;
            ref_copy.set_resolve_time(time);
            return nb::cast(std::move(ref_copy));
        }
        return nb::cast(TSReference::empty());
    }

    // Helper: wrap a concrete (non-REF) output TSView as BoundTimeSeriesReference
    static nb::object make_bound_ref(TSView&& target_view) {
        TSOutputView ov(std::move(target_view), nullptr);
        nb::object wrapper = wrap_output_view(std::move(ov));
        auto ref_module = nb::module_::import_("hgraph._types._ref_type");
        auto make_fn = ref_module.attr("TimeSeriesReference").attr("make");
        return make_fn(wrapper);
    }

    nb::object PyTimeSeriesReferenceInput::ref_value() const {
        // Match Python PythonTimeSeriesReferenceInput.value semantics:
        //   if self._output is not None: return self._output.value
        //   return self._value
        //
        // Three binding scenarios:
        // 1. TS→REF (non-peered): LinkTarget points to non-REF output → BoundTimeSeriesReference
        // 2. REF→REF (peered): LinkTarget points to REF output → read output's stored ref
        // 3. Unbound/direct: TSReference stored in own value_data → may need resolution

        const auto& iv = input_view();
        const auto& vd = iv.ts_view().view_data();
        const engine_time_t time = iv.ts_view().current_time();

        if (vd.uses_link_target && vd.link_data) {
            auto* lt = static_cast<const LinkTarget*>(vd.link_data);
            if (lt->valid()) {
                if (lt->meta->kind != TSKind::REF) {
                    // Case 1: Non-peered binding (TS→REF) → BoundTimeSeriesReference
                    ViewData target_vd;
                    target_vd.path = lt->target_path;
                    target_vd.value_data = lt->value_data;
                    target_vd.time_data = lt->time_data;
                    target_vd.observer_data = lt->observer_data;
                    target_vd.delta_data = lt->delta_data;
                    target_vd.link_data = lt->link_data;
                    target_vd.ops = lt->ops;
                    target_vd.meta = lt->meta;

                    return make_bound_ref(TSView(target_vd, time));
                }
                if (lt->value_data) {
                    // Case 2: REF→REF (peered) → read the output's stored TSReference
                    auto* output_ref = static_cast<const TSReference*>(lt->value_data);
                    return ref_output_value_to_python(*output_ref, time);
                }
            }
        }

        // Case 3: Unbound/direct — TSReference in own value_data
        // Used by reduce inner stubs where set_ref_input_value writes directly.
        auto val = iv.ts_view().value();
        if (val.valid()) {
            TSReference ref = val.as<TSReference>();
            if (ref.is_peered()) {
                // Resolve the PEERED ref to its target
                ref.set_resolve_time(time);
                TSView resolved = ref.resolve(time);
                if (resolved && resolved.view_data().meta) {
                    if (resolved.view_data().meta->kind == TSKind::REF) {
                        // Target is a REF output → return what IT stores
                        // (matches Python: self._output.value where _output is REF output)
                        auto inner_val = resolved.value();
                        if (inner_val.valid()) {
                            return ref_output_value_to_python(inner_val.as<TSReference>(), time);
                        }
                        return nb::cast(TSReference::empty());
                    }
                    // Target is concrete output → BoundTimeSeriesReference
                    return make_bound_ref(std::move(resolved));
                }
            }
            // NON_PEERED or EMPTY: return as-is
            ref.set_resolve_time(time);
            return nb::cast(std::move(ref));
        }
        return nb::cast(TSReference::empty());
    }

    nb::str PyTimeSeriesReferenceInput::to_string() const {
        return nb::str("TimeSeriesReferenceInput[view]");
    }

    nb::str PyTimeSeriesReferenceInput::to_repr() const { return to_string(); }

    void PyTimeSeriesReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesReferenceInput, PyTimeSeriesInput>(m, "TimeSeriesReferenceInput")
            .def_prop_ro("value", &PyTimeSeriesReferenceInput::ref_value)
            .def_prop_ro("delta_value", &PyTimeSeriesReferenceInput::ref_value)
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
