#include "hgraph/api/python/wrapper_factory.h"

#include <fmt/format.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/time_series/ts_link.h>
#include <hgraph/types/time_series/ts_ref_target_link.h>

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
        const TSMeta* meta = _view.ts_meta();

        // For TSD element wrappers: check if the element key still exists in the parent TSD
        if (has_element_key()) {
            const TSValue* root = _view.root();
            if (root && root->ts_meta() && root->ts_meta()->kind() == TSTypeKind::TSD) {
                TSDView dict = root->view().as_dict();
                if (dict.valid()) {
                    // Check if the key exists in the TSD
                    const TSDTypeMeta* dict_meta = dict.dict_meta();
                    const value::TypeMeta* key_schema = dict_meta->key_type();

                    if (key_schema && key_schema->ops) {
                        // Create temp key storage and convert from Python
                        std::vector<char> temp_key(key_schema->size);
                        void* key_ptr = temp_key.data();

                        bool key_valid = false;
                        try {
                            if (key_schema->ops->construct) {
                                key_schema->ops->construct(key_ptr, key_schema);
                            }
                            if (key_schema->ops->from_python) {
                                key_schema->ops->from_python(key_ptr, _element_key, key_schema);
                            }

                            // Check if key exists
                            value::ConstMapView map_view = dict.value_view().as_map();
                            value::ConstValueView key_view(key_ptr, key_schema);
                            auto slot_idx = map_view.find_index(key_view);
                            key_valid = slot_idx.has_value();

                            if (key_schema->ops->destruct) {
                                key_schema->ops->destruct(key_ptr, key_schema);
                            }
                        } catch (const std::exception& e) {
                            if (key_schema->ops->destruct) {
                                key_schema->ops->destruct(key_ptr, key_schema);
                            }
                            return nb::bool_(false);
                        }

                        if (!key_valid) {
                            return nb::bool_(false);
                        }
                    }
                }
            }
        }

        // For REF types, check if the TimeSeriesReference has a valid bound target
        if (meta && meta->kind() == TSTypeKind::REF) {
            const void* data = _view.value_view().data();
            if (data) {
                const TimeSeriesReference* ref = static_cast<const TimeSeriesReference*>(data);
                if (ref->is_bound()) {
                    // Check if the bound target is valid
                    if (ref->is_python_bound()) {
                        try {
                            nb::object py_output = ref->python_output();
                            if (py_output.is_valid() && !py_output.is_none()) {
                                nb::object valid_attr = py_output.attr("valid");
                                return nb::bool_(nb::cast<bool>(valid_attr));
                            }
                        } catch (...) {
                            // Fall through to default
                        }
                    } else if (ref->is_view_bound()) {
                        const TSValue* target = ref->view_output();
                        if (target) {
                            return nb::bool_(target->ts_valid());
                        }
                    }
                }
                // Reference exists but is not bound or target is invalid
                return nb::bool_(false);
            }
        }

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

    void PyTimeSeriesOutput::bind_output(nb::object output) {
        // For REF outputs, this is used to bind a Python output object.
        // Note: We do NOT store the TimeSeriesReference in the view's value storage because:
        // 1. REFTypeMeta::value_schema() returns the REFERENCED type's schema, not TimeSeriesReference
        // 2. The storage is allocated for the referenced type's value (e.g., int for REF[TS[int]])
        // 3. Casting that storage to TimeSeriesReference* causes undefined behavior
        //
        // For REF outputs, the binding should be handled through the TSValue's ref_cache mechanism
        // or through the TSRefTargetLink subscription system for inputs.

        const TSMeta* meta = _view.ts_meta();
        if (!meta || meta->kind() != TSTypeKind::REF) {
            // Not a REF type - this is a no-op
            return;
        }

        // Store in ref_cache if we have a root TSValue
        const TSValue* root = _view.root();
        if (root) {
            nb::object ref_obj = TimeSeriesReference::make_python_bound(output).is_empty()
                                     ? nb::none()
                                     : nb::cast(TimeSeriesReference::make_python_bound(output));
            const_cast<TSValue*>(root)->set_ref_cache(ref_obj);
        }

        // Mark as modified
        Node* node = _view.owning_node();
        if (node && node->graph()) {
            engine_time_t current_time = node->graph()->evaluation_time();
            _view.notify_modified(current_time);
        }
    }

    void PyTimeSeriesOutput::make_active() {
        // For REF outputs, this affects scheduling
        // In the view-based system, this is typically a no-op since scheduling
        // is handled differently. The Python layer may handle this.
    }

    void PyTimeSeriesOutput::make_passive() {
        // For REF outputs, this affects scheduling
        // In the view-based system, this is typically a no-op.
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
            // REF-specific methods (for TSD[K, REF[...]] outputs)
            .def("bind_output", &PyTimeSeriesOutput::bind_output)
            .def("make_active", &PyTimeSeriesOutput::make_active)
            .def("make_passive", &PyTimeSeriesOutput::make_passive)
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

        // If we have a bound Python output (from bind_output for REF inputs),
        // return a TimeSeriesReference wrapping it
        if (_bound_py_output.is_valid() && !_bound_py_output.is_none()) {
            // Use BoundTimeSeriesReference which can wrap any object with value/valid/modified
            auto ref_module = nb::module_::import_("hgraph._impl._types._ref");
            auto bound_ref_class = ref_module.attr("BoundTimeSeriesReference");
            return bound_ref_class(_bound_py_output);
        }

        // For REF types, check the underlying TimeSeriesReference value
        // This handles TSD element refs where binding is stored in the value
        if (meta && meta->kind() == TSTypeKind::REF) {
            const void* data = _view.value_view().data();
            if (data) {
                const TimeSeriesReference* ref = static_cast<const TimeSeriesReference*>(data);
                if (ref->is_bound()) {
                    if (ref->is_python_bound()) {
                        // Return a BoundTimeSeriesReference wrapping the Python output
                        auto ref_module = nb::module_::import_("hgraph._impl._types._ref");
                        auto bound_ref_class = ref_module.attr("BoundTimeSeriesReference");
                        return bound_ref_class(ref->python_output());
                    }
                    // For other bound types, fall through to view's to_python
                }
            }
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
                        bool modified = ref_link->modified_at(eval_time);
                        if (!modified) {
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

                    nb::object result = target_view.to_python_delta();
                    return result;
                } else {
                }
            } else {
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

        // Check if the binding was modified this tick (bind_output was called)
        // This is critical for REF inputs like _ref in tsd_get_item_default
        if (_binding_modified_time == eval_time) {
            return nb::bool_(true);
        }

        // If we have a bound Python output (e.g., CppKeySetOutputWrapper),
        // check its modified status - this handles virtual outputs
        if (_bound_py_output.is_valid() && !_bound_py_output.is_none()) {
            try {
                if (nb::hasattr(_bound_py_output, "modified")) {
                    return nb::bool_(nb::cast<bool>(_bound_py_output.attr("modified")));
                }
            } catch (...) {
                // Fall through to normal check
            }
        }

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

        // If we have a bound Python output object (from bind_output for REF inputs),
        // check its valid property
        if (_bound_py_output.is_valid() && !_bound_py_output.is_none()) {
            try {
                nb::object valid_attr = _bound_py_output.attr("valid");
                return nb::bool_(nb::cast<bool>(valid_attr));
            } catch (...) {
                // If the object doesn't have a valid attribute, fall through
            }
        }

        // Check if we have a TSRefTargetLink that may have rebound
        // This handles wired REF inputs where binding is done through the link system
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

        // For REF types, check the underlying TimeSeriesReference value
        // This handles TSD element refs where bind_output was called explicitly
        const TSMeta* meta = _view.ts_meta();
        if (meta && meta->kind() == TSTypeKind::REF) {
            const void* data = _view.value_view().data();
            if (data) {
                const TimeSeriesReference* ref = static_cast<const TimeSeriesReference*>(data);
                if (ref->is_bound()) {
                    if (ref->is_python_bound()) {
                        try {
                            nb::object py_output = ref->python_output();
                            if (py_output.is_valid() && !py_output.is_none()) {
                                nb::object valid_attr = py_output.attr("valid");
                                return nb::bool_(nb::cast<bool>(valid_attr));
                            }
                        } catch (...) {
                            // Fall through
                        }
                    } else if (ref->is_view_bound()) {
                        const TSValue* target = ref->view_output();
                        if (target) {
                            return nb::bool_(target->ts_valid());
                        }
                    }
                }
                // For TSD element refs without link, check if bound via bind_output
                // If not bound, fall through to view check rather than returning false
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

        // Store the Python object for modification tracking
        // This is important for virtual outputs like CppKeySetOutputWrapper
        // that need to propagate their modified() status
        _bound_py_output = output;

        // Track when the binding changed - this makes modified() return true for this tick
        // This is critical for _ref.modified checks in tsd_get_item_default
        Node* binding_node = _view.owning_node();
        if (binding_node && binding_node->graph()) {
            _binding_modified_time = binding_node->graph()->evaluation_time();
        }

        // Note: We do NOT store the TimeSeriesReference in the view's value storage because:
        // 1. REFTypeMeta::value_schema() returns the REFERENCED type's schema, not TimeSeriesReference
        // 2. The storage is allocated for the referenced type's value (e.g., int for REF[TS[int]])
        // 3. Casting that storage to TimeSeriesReference* causes undefined behavior
        //
        // Instead, we store in ref_cache which is checked by TSRefTargetLink::notify()

        // CRITICAL: For passthrough REF inputs, we need to set ref_cache on the NODE'S OUTPUT,
        // not on the input's view root. The downstream TSRefTargetLink is subscribed to the
        // node's output TSValue, so that's where the ref_cache needs to be updated.
        //
        // Architecture:
        // - Node A (tsd_get_item_default): has REF output and passthrough REF input (_ref)
        // - Node B (downstream): input with TSRefTargetLink bound to Node A's output
        // - When _ref.bind_output(x) is called, we need to update Node A's output's ref_cache
        // - TSRefTargetLink on Node B will then read from Node A's output's ref_cache

        // Get the owning node to find its output TSValue
        Node* node = _view.owning_node();
        if (!node) {
            const TSValue* root = _view.root();
            if (root) {
                node = root->owning_node();
            }
        }

        // For passthrough REF inputs, the node's output should be a REF type
        // Set ref_cache on the node's output TSValue
        const TSValue* ref_output_ts = nullptr;
        if (node && node->ts_output()) {
            const TSMeta* output_meta = node->ts_output()->ts_meta();
            if (output_meta && output_meta->kind() == TSTypeKind::REF) {
                ref_output_ts = node->ts_output();
            }
        }

        // If no node output found, try to find via TSRefTargetLink
        if (!ref_output_ts) {
            const TSValue* link_source = _view.link_source();
            const auto& path = _view.path();

            if (link_source && path.depth() > 0) {
                size_t field_index = path.elements[0];
                TSRefTargetLink* ref_link = const_cast<TSValue*>(link_source)->ref_link_at(field_index);
                if (ref_link) {
                    ref_output_ts = ref_link->ref_output();
                }
            }
        }

        // Final fallback to _view.root()
        if (!ref_output_ts) {
            ref_output_ts = _view.root();
        }

        if (ref_output_ts) {
            // Create a C++ TimeSeriesReference wrapping the Python output
            // NOTE: Only create once - avoid double evaluation
            TimeSeriesReference temp_ref = TimeSeriesReference::make_python_bound(output);
            nb::object ref_obj = temp_ref.is_empty()
                                     ? nb::none()
                                     : nb::cast(temp_ref);
            const_cast<TSValue*>(ref_output_ts)->set_ref_cache(ref_obj);
        }

        // Notify the output's overlay if we have one (for proper notification propagation)
        if (ref_output_ts) {
            if (auto* output_overlay = ref_output_ts->overlay()) {
                if (node && node->graph()) {
                    engine_time_t current_time = node->graph()->evaluation_time();
                    const_cast<TSOverlayStorage*>(output_overlay)->mark_modified(current_time);
                }
            }
        }
    }

    void PyTimeSeriesInput::un_bind_output() {
        // Track explicit binding state for passthrough inputs
        // This is called by the valid operator when a REF's value changes
        _explicit_bound = false;
        _bound_output = nullptr;
        _bound_py_output = nb::object();  // Clear the Python reference
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
