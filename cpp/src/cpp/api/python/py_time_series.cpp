#include "hgraph/api/python/wrapper_factory.h"

#include <fmt/format.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/graph.h>

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
        // View-based inputs are always "bound" to their data
        return nb::bool_(true);
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
            .def("make_passive", &PyTimeSeriesInput::make_passive);
    }
}  // namespace hgraph
