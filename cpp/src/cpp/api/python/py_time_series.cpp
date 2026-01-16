#include "hgraph/api/python/wrapper_factory.h"

#include <fmt/format.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/time_series/ts_link.h>
#include <hgraph/types/time_series/ts_ref_target_link.h>
#include <iostream>

namespace hgraph
{
    void PyTimeSeriesType::register_with_nanobind(nb::module_ &m) {
        // Base class is a marker - no methods, just the type for isinstance checks
        nb::class_<PyTimeSeriesType>(m, "TimeSeriesType");
    }

    // ========== PyTimeSeriesOutput implementation (view-only) ==========

    PyTimeSeriesOutput::PyTimeSeriesOutput(TSMutableView view)
        : _view(view) {}

    nb::object PyTimeSeriesOutput::value() const {
        return _view.to_python();
    }

    nb::object PyTimeSeriesOutput::delta_value() const {
        // Use view's delta conversion - handles Window types specially
        return _view.to_python_delta();
    }

    nb::object PyTimeSeriesOutput::owning_node() const {
        Node* n = _view.owning_node();
        return n ? wrap_node(n->shared_from_this()) : nb::none();
    }

    nb::object PyTimeSeriesOutput::owning_graph() const {
        Node* n = _view.owning_node();
        if (n && n->graph()) {
            return wrap_graph(n->graph()->shared_from_this());
        }
        return nb::none();
    }

    engine_time_t PyTimeSeriesOutput::last_modified_time() const {
        return _view.last_modified_time();
    }

    nb::bool_ PyTimeSeriesOutput::modified() const {
        Node* n = _view.owning_node();
        if (n && n->cached_evaluation_time_ptr()) {
            engine_time_t eval_time = *n->cached_evaluation_time_ptr();
            return nb::bool_(_view.modified_at(eval_time));
        }
        return nb::bool_(false);
    }

    nb::bool_ PyTimeSeriesOutput::valid() const {
        return nb::bool_(_view.ts_valid());
    }

    nb::bool_ PyTimeSeriesOutput::all_valid() const {
        return nb::bool_(_view.all_valid());
    }

    nb::bool_ PyTimeSeriesOutput::is_reference() const {
        const TSMeta* meta = _view.ts_meta();
        return nb::bool_(meta && meta->is_reference());
    }

    void PyTimeSeriesOutput::set_value(nb::object value) {
        _view.from_python(value);
    }

    void PyTimeSeriesOutput::apply_result(nb::object value) {
        if (!value.is_none()) {
            _view.from_python(value);
        }
    }

    void PyTimeSeriesOutput::copy_from_output(const PyTimeSeriesOutput &output) {
        _view.copy_from(output.view());
    }

    void PyTimeSeriesOutput::copy_from_input(const PyTimeSeriesInput &input) {
        _view.copy_from(input.view());
    }

    void PyTimeSeriesOutput::clear() {
        _view.invalidate_ts();
    }

    void PyTimeSeriesOutput::invalidate() {
        _view.invalidate_ts();
    }

    bool PyTimeSeriesOutput::can_apply_result(nb::object value) {
        // View-based instances can always apply a result
        return true;
    }

    void PyTimeSeriesOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesOutput, PyTimeSeriesType>(m, "TimeSeriesOutput")
            // Common time-series properties
            .def_prop_ro("owning_node", &PyTimeSeriesOutput::owning_node)
            .def_prop_ro("owning_graph", &PyTimeSeriesOutput::owning_graph)
            .def_prop_ro("delta_value", &PyTimeSeriesOutput::delta_value)
            .def_prop_ro("modified", &PyTimeSeriesOutput::modified)
            .def_prop_ro("valid", &PyTimeSeriesOutput::valid)
            .def_prop_ro("all_valid", &PyTimeSeriesOutput::all_valid)
            .def_prop_ro("last_modified_time", &PyTimeSeriesOutput::last_modified_time)
            .def("is_reference", &PyTimeSeriesOutput::is_reference)
            // Output-specific methods
            .def_prop_rw("value", &PyTimeSeriesOutput::value, &PyTimeSeriesOutput::set_value, nb::arg("value").none())
            .def("can_apply_result", &PyTimeSeriesOutput::can_apply_result)
            .def("apply_result", &PyTimeSeriesOutput::apply_result, nb::arg("value").none())
            .def("clear", &PyTimeSeriesOutput::clear)
            .def("invalidate", &PyTimeSeriesOutput::invalidate)
            .def("copy_from_output", &PyTimeSeriesOutput::copy_from_output)
            .def("copy_from_input", &PyTimeSeriesOutput::copy_from_input)
            // Internal: expose root TSValue for C++ subscription (used by TSRefTargetLink)
            .def_prop_ro("_cpp_root", [](PyTimeSeriesOutput& self) -> const TSValue* {
                return self.view().root();
            }, nb::rv_policy::reference);
    }

    // ========== PyTimeSeriesInput implementation (view-only) ==========

    PyTimeSeriesInput::PyTimeSeriesInput(TSView view)
        : _view(view) {}

    nb::object PyTimeSeriesInput::value() const {
        const TSMeta* meta = _view.ts_meta();

        // Check if we have a TSRefTargetLink that may have rebound
        const TSValue* link_source = _view.link_source();
        const auto& path = _view.path();

        if (link_source && path.depth() > 0) {
            size_t field_index = path.elements[0];
            TSRefTargetLink* ref_link = const_cast<TSValue*>(link_source)->ref_link_at(field_index);

            if (ref_link && ref_link->valid()) {
                // Get the current target view from ref_link
                TSView target_view = ref_link->view();
                return target_view.to_python();
            }

            // Check for TSLink with KEY_SET binding (TSD viewed as TSS)
            TSLink* link = const_cast<TSValue*>(link_source)->link_at(field_index);
            if (link && link->is_key_set_binding() && link->valid()) {
                // For REF[TSS] inputs (kind == REF), wrap the TSD's key_set in a TimeSeriesReference
                // For non-REF TSS inputs, return the raw TSS value
                if (meta && meta->kind() == TSTypeKind::REF) {
                    // REF[TSS] input bound to TSD.key_set
                    // Python expects a TimeSeriesReference whose .output is the TSD's key_set TSS
                    // C++ TSD doesn't have a separate key_set TSS output like Python
                    // We need to wrap the key_set in a Python-accessible way
                    const TSValue* tsd_output = link->output();
                    if (tsd_output) {
                        // Create a wrapped TSD output that Python can access .key_set on
                        // This uses the Python TSD wrapper which has the key_set property
                        auto py_output = wrap_output_view(const_cast<TSValue*>(tsd_output)->mutable_view());
                        // Create a TimeSeriesReference using Python's API
                        auto ref_module = nb::module_::import_("hgraph._impl._types._ref");
                        auto bound_ref_class = ref_module.attr("BoundTimeSeriesReference");
                        // Get the key_set from the wrapped TSD output
                        auto key_set = py_output.attr("key_set");
                        // Create BoundTimeSeriesReference wrapping the key_set
                        return bound_ref_class(key_set);
                    }
                    // Fallback: return empty reference
                    auto ref_module = nb::module_::import_("hgraph._impl._types._ref");
                    auto empty_ref_class = ref_module.attr("EmptyTimeSeriesReference");
                    return empty_ref_class();
                }
                // Non-REF TSS input - TSLink::view() returns a TSSView of the TSD's keys
                return link->view().to_python();
            }
        }
        return _view.to_python();
    }

    nb::object PyTimeSeriesInput::delta_value() const {
        // For actual REF inputs (declared as REF[...]), delta_value should equal value
        // (both return TimeSeriesReference). Only auto-dereference for non-REF inputs
        // that are bound to REF outputs (e.g., TIME_SERIES_TYPE bound to REF output).
        const TSMeta* meta = _view.ts_meta();
        if (meta && meta->kind() == TSTypeKind::REF && !_view.should_auto_deref()) {
            // This is an actual REF input (not an auto-dereferencing non-REF input bound to REF)
            // For actual REF inputs, delta_value equals value (both return TimeSeriesReference)
            return _view.to_python();
        }

        // If we have a bound output from REF binding, use its delta value
        // This handles the case where a non-REF input is linked to a REF output
        // and the target has been rebound after initialization
        if (_bound_output) {
            return _bound_output->view().to_python_delta();
        }

        // Check if the view's link_source has a TSRefTargetLink with a bound target
        // This handles dynamic rebinding for inputs linked to REF outputs
        const TSValue* link_source = _view.link_source();
        const auto& path = _view.path();
        if (link_source && path.depth() > 0) {
            // Get the field index from the path (first element for bundle fields)
            size_t field_index = path.elements[0];
            // Try to get the TSRefTargetLink at the field index
            TSRefTargetLink* ref_link = const_cast<TSValue*>(link_source)->ref_link_at(field_index);
            if (ref_link) {
                const TSValue* target = ref_link->target_output();
                // Check for C++ target, element binding, OR Python output
                if (target || ref_link->is_element_binding() || ref_link->has_python_output()) {
                    // Use ref_link->view() which properly navigates to elements for element-based bindings
                    TSView target_view = ref_link->view();

                    // First check if modified - if not, return None (scalar delta semantics)
                    Node* node = _view.owning_node();
                    if (node && node->cached_evaluation_time_ptr()) {
                        engine_time_t eval_time = *node->cached_evaluation_time_ptr();

                        // Check modified_at for the ref_link
                        if (!ref_link->modified_at(eval_time)) {
                            return nb::none();  // Not modified this tick
                        }

                        // Check if a rebind occurred at this tick - if so, return full value as delta
                        // When REF target changes, all elements are "new" from the perspective of the input
                        engine_time_t rebind_time = ref_link->target_sample_time();
                        if (rebind_time == eval_time) {
                            // Rebind occurred this tick - return full value as delta
                            // For TSL/TSD/TSS: build delta dict with all elements
                            // For TSB: return regular delta (bundle fields don't change)
                            auto target_kind = target_view.ts_meta() ? target_view.ts_meta()->kind() : TSTypeKind::TS;
                            if (target_kind == TSTypeKind::TSL) {
                                // Build dict with all TSL elements
                                nb::dict result;
                                TSLView list_view = target_view.as_list();
                                size_t count = list_view.size();
                                for (size_t i = 0; i < count; ++i) {
                                    TSView elem = list_view.element(i);
                                    result[nb::int_(i)] = elem.to_python();
                                }
                                return result;
                            } else if (target_kind == TSTypeKind::TSD) {
                                // For TSD, compute proper delta using previous target from TSRefTargetLink
                                nb::object new_value = target_view.to_python();  // frozendict

                                // Get the previous target from TSRefTargetLink
                                const TSValue* prev_target = ref_link->prev_target_output();
                                if (prev_target) {
                                    // Get the previous target's value
                                    TSView prev_view = prev_target->view();
                                    nb::object old_value = prev_view.to_python();  // frozendict

                                    // Get REMOVE sentinel
                                    auto REMOVE = nb::module_::import_("hgraph._types._tsd_type").attr("REMOVE");

                                    // Compute delta = new_value vs old_value
                                    nb::dict delta_dict;

                                    // Items in new: add with their values
                                    for (auto item : new_value) {
                                        nb::object key = nb::cast<nb::object>(item);
                                        delta_dict[key] = new_value[key];
                                    }
                                    // Items in old but not in new: add as REMOVE
                                    for (auto item : old_value) {
                                        nb::object key = nb::cast<nb::object>(item);
                                        // Check if key exists in new_value using Python's 'in' operator
                                        bool in_new = false;
                                        for (auto new_item : new_value) {
                                            if (nb::cast<nb::object>(new_item).equal(key)) {
                                                in_new = true;
                                                break;
                                            }
                                        }
                                        if (!in_new) {
                                            delta_dict[key] = REMOVE;
                                        }
                                    }

                                    // Clear the previous target after use (consumed)
                                    ref_link->clear_prev_target();

                                    // Return as frozendict for consistency with Python TSD.delta_value
                                    auto frozendict_type = nb::module_::import_("frozendict").attr("frozendict");
                                    return frozendict_type(delta_dict);
                                }

                                // No previous target - return all as added (first time binding)
                                return new_value;
                            } else if (target_kind == TSTypeKind::TSS) {
                                // For TSS, compute proper delta using previous target from TSRefTargetLink
                                nb::object new_value = target_view.to_python();  // frozenset
                                auto PythonSetDelta = nb::module_::import_("hgraph._impl._types._tss").attr("PythonSetDelta");

                                // Get the previous target from TSRefTargetLink
                                // This was stored during rebind_target() before the target changed
                                const TSValue* prev_target = ref_link->prev_target_output();
                                if (prev_target) {
                                    // Get the previous target's value
                                    TSView prev_view = prev_target->view();
                                    nb::object old_value = prev_view.to_python();  // frozenset

                                    // Compute delta = new_value vs old_value
                                    nb::set added_set;
                                    nb::set removed_set;

                                    // Items in new but not in old are added
                                    for (auto item : new_value) {
                                        if (!nb::cast<nb::frozenset>(old_value).contains(item)) {
                                            added_set.add(item);
                                        }
                                    }
                                    // Items in old but not in new are removed
                                    for (auto item : old_value) {
                                        if (!nb::cast<nb::frozenset>(new_value).contains(item)) {
                                            removed_set.add(item);
                                        }
                                    }

                                    // Clear the previous target after use (consumed)
                                    ref_link->clear_prev_target();

                                    return PythonSetDelta(nb::frozenset(added_set), nb::frozenset(removed_set));
                                }

                                // No previous target - return all as added (first time binding)
                                return PythonSetDelta(new_value, nb::frozenset());
                            } else {
                                // For scalars and other types, return value
                                return target_view.to_python();
                            }
                        }
                    }

                    return target_view.to_python_delta();
                }
            }

            // Check for TSLink with KEY_SET binding (TSD viewed as TSS)
            TSLink* link = const_cast<TSValue*>(link_source)->link_at(field_index);
            if (link && link->is_key_set_binding() && link->valid()) {
                // TSLink::view() returns a TSSView of the TSD's keys
                return link->view().to_python_delta();
            }
        }

        // Use view's delta conversion - handles Window types specially
        return _view.to_python_delta();
    }

    nb::object PyTimeSeriesInput::owning_node() const {
        Node* n = _view.owning_node();
        return n ? wrap_node(n->shared_from_this()) : nb::none();
    }

    nb::object PyTimeSeriesInput::owning_graph() const {
        Node* n = _view.owning_node();
        if (n && n->graph()) {
            return wrap_graph(n->graph()->shared_from_this());
        }
        return nb::none();
    }

    engine_time_t PyTimeSeriesInput::last_modified_time() const {
        return _view.last_modified_time();
    }

    nb::bool_ PyTimeSeriesInput::modified() const {
        Node* n = _view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            return nb::bool_(false);
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();

        // Check if we have a TSRefTargetLink that may have rebound
        const TSValue* link_source = _view.link_source();
        const auto& path = _view.path();
        if (link_source && path.depth() > 0) {
            size_t field_index = path.elements[0];
            TSRefTargetLink* ref_link = const_cast<TSValue*>(link_source)->ref_link_at(field_index);
            if (ref_link) {
                // Use TSRefTargetLink's modified_at which considers rebind notifications
                return nb::bool_(ref_link->modified_at(eval_time));
            }
        }

        // Fall back to view's modified check
        return nb::bool_(_view.modified_at(eval_time));
    }

    nb::bool_ PyTimeSeriesInput::valid() const {
        // If we have a bound output from REF binding, check its validity
        if (_bound_output) {
            return nb::bool_(_bound_output->ts_valid());
        }

        // Check if we have a TSRefTargetLink that may have rebound
        const TSValue* link_source = _view.link_source();
        const auto& path = _view.path();
        if (link_source && path.depth() > 0) {
            size_t field_index = path.elements[0];
            TSRefTargetLink* ref_link = const_cast<TSValue*>(link_source)->ref_link_at(field_index);
            if (ref_link && ref_link->valid()) {
                // Get the current target view from ref_link
                TSView target_view = ref_link->view();
                return nb::bool_(target_view.ts_valid());
            }
        }

        return nb::bool_(_view.ts_valid());
    }

    nb::bool_ PyTimeSeriesInput::all_valid() const {
        return nb::bool_(_view.all_valid());
    }

    nb::bool_ PyTimeSeriesInput::is_reference() const {
        const TSMeta* meta = _view.ts_meta();
        return nb::bool_(meta && meta->is_reference());
    }

    nb::bool_ PyTimeSeriesInput::active() const {
        // View-based inputs are always "active" in terms of being ready to read
        return nb::bool_(true);
    }

    void PyTimeSeriesInput::make_active() {
        // No-op for view-based inputs
    }

    void PyTimeSeriesInput::make_passive() {
        // No-op for view-based inputs
    }

    nb::bool_ PyTimeSeriesInput::bound() const {
        // For passthrough inputs (used by valid operator), track explicit binding state
        return nb::bool_(_explicit_bound);
    }

    void PyTimeSeriesInput::bind_output(nb::object output) {
        // Track explicit binding state for passthrough inputs
        // This is called by the valid operator when binding a REF's output to a passthrough input
        _explicit_bound = true;
    }

    void PyTimeSeriesInput::un_bind_output() {
        // Track explicit binding state for passthrough inputs
        // This is called by the valid operator when a REF's value changes
        _explicit_bound = false;
        _bound_output = nullptr;
    }

    nb::bool_ PyTimeSeriesInput::has_peer() const {
        // View-based inputs don't have peer connections
        return nb::bool_(false);
    }

    void PyTimeSeriesInput::set_bound_output(const TSValue* output) {
        _bound_output = output;
        _explicit_bound = (output != nullptr);
    }

    const TSValue* PyTimeSeriesInput::bound_output() const {
        return _bound_output;
    }

    void PyTimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesInput, PyTimeSeriesType>(m, "TimeSeriesInput")
            // Common time-series properties
            .def_prop_ro("owning_node", &PyTimeSeriesInput::owning_node)
            .def_prop_ro("owning_graph", &PyTimeSeriesInput::owning_graph)
            .def_prop_ro("value", &PyTimeSeriesInput::value)
            .def_prop_ro("delta_value", &PyTimeSeriesInput::delta_value)
            .def_prop_ro("modified", &PyTimeSeriesInput::modified)
            .def_prop_ro("valid", &PyTimeSeriesInput::valid)
            .def_prop_ro("all_valid", &PyTimeSeriesInput::all_valid)
            .def_prop_ro("last_modified_time", &PyTimeSeriesInput::last_modified_time)
            .def("is_reference", &PyTimeSeriesInput::is_reference)
            // Input-specific methods
            .def_prop_ro("bound", &PyTimeSeriesInput::bound)
            .def_prop_ro("active", &PyTimeSeriesInput::active)
            .def("make_active", &PyTimeSeriesInput::make_active)
            .def("make_passive", &PyTimeSeriesInput::make_passive)
            .def("bind_output", &PyTimeSeriesInput::bind_output)
            .def("un_bind_output", &PyTimeSeriesInput::un_bind_output)
            .def_prop_ro("has_peer", &PyTimeSeriesInput::has_peer);
    }
}  // namespace hgraph
