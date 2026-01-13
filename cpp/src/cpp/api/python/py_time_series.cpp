#include "hgraph/api/python/wrapper_factory.h"

#include <fmt/format.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/graph.h>
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
        // TODO: Check if this is a REF type via TSMeta
        return nb::bool_(false);
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
            .def("copy_from_input", &PyTimeSeriesOutput::copy_from_input);
    }

    // ========== PyTimeSeriesInput implementation (view-only) ==========

    PyTimeSeriesInput::PyTimeSeriesInput(TSView view)
        : _view(view) {}

    nb::object PyTimeSeriesInput::value() const {
        return _view.to_python();
    }

    nb::object PyTimeSeriesInput::delta_value() const {
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
        std::cerr << "[DEBUG PyTimeSeriesInput::delta_value] link_source=" << link_source
                  << " _view.ts_meta_kind=" << (_view.ts_meta() ? static_cast<int>(_view.ts_meta()->kind()) : -1)
                  << " path_size=" << path.depth()
                  << std::endl;
        if (link_source && path.depth() > 0) {
            // Get the field index from the path (first element for bundle fields)
            size_t field_index = path.elements[0];
            std::cerr << "[DEBUG PyTimeSeriesInput::delta_value] field_index=" << field_index << std::endl;
            // Try to get the TSRefTargetLink at the field index
            TSRefTargetLink* ref_link = const_cast<TSValue*>(link_source)->ref_link_at(field_index);
            std::cerr << "[DEBUG PyTimeSeriesInput::delta_value] ref_link=" << (ref_link ? "yes" : "no")
                      << std::endl;
            if (ref_link) {
                const TSValue* target = ref_link->target_output();
                std::cerr << "[DEBUG PyTimeSeriesInput::delta_value] target=" << target
                          << " is_element_binding=" << ref_link->is_element_binding()
                          << std::endl;
                if (target || ref_link->is_element_binding()) {
                    // Use ref_link->view() which properly navigates to elements for element-based bindings
                    TSView target_view = ref_link->view();
                    std::cerr << "[DEBUG PyTimeSeriesInput::delta_value] target_view kind="
                              << (target_view.ts_meta() ? static_cast<int>(target_view.ts_meta()->kind()) : -1)
                              << std::endl;

                    // Check if a rebind occurred at this tick - if so, return full value as delta
                    // When REF target changes, all elements are "new" from the perspective of the input
                    Node* node = _view.owning_node();
                    if (node && node->cached_evaluation_time_ptr()) {
                        engine_time_t eval_time = *node->cached_evaluation_time_ptr();
                        engine_time_t rebind_time = ref_link->target_sample_time();
                        std::cerr << "[DEBUG PyTimeSeriesInput::delta_value] eval_time=" << eval_time
                                  << " rebind_time=" << rebind_time << std::endl;
                        if (rebind_time == eval_time) {
                            // Rebind occurred this tick - return full value as delta
                            // For TSL/TSD/TSS: build delta dict with all elements
                            // For TSB: return regular delta (bundle fields don't change)
                            std::cerr << "[DEBUG PyTimeSeriesInput::delta_value] rebind this tick - returning full delta"
                                      << std::endl;
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
                                // For TSD, return all key-value pairs
                                return target_view.to_python();  // TSD already returns dict
                            } else {
                                // For scalars and other types, return value
                                return target_view.to_python();
                            }
                        }
                    }

                    return target_view.to_python_delta();
                }
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
        if (n && n->cached_evaluation_time_ptr()) {
            engine_time_t eval_time = *n->cached_evaluation_time_ptr();
            return nb::bool_(_view.modified_at(eval_time));
        }
        return nb::bool_(false);
    }

    nb::bool_ PyTimeSeriesInput::valid() const {
        // If we have a bound output from REF binding, check its validity
        if (_bound_output) {
            return nb::bool_(_bound_output->ts_valid());
        }
        return nb::bool_(_view.ts_valid());
    }

    nb::bool_ PyTimeSeriesInput::all_valid() const {
        return nb::bool_(_view.all_valid());
    }

    nb::bool_ PyTimeSeriesInput::is_reference() const {
        // TODO: Check if this is a REF type via TSMeta
        return nb::bool_(false);
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
