#include <hgraph/api/python/py_time_series.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/value/python_conversion.h>

namespace hgraph
{
    // =========================================================================
    // PyTimeSeriesType - Base class implementation
    // =========================================================================

    PyTimeSeriesType::PyTimeSeriesType(node_s_ptr node, const TimeSeriesTypeMeta* meta)
        : _node(std::move(node)), _meta(meta) {}

    nb::object PyTimeSeriesType::owning_node() const {
        return _node ? wrap_node(_node) : nb::none();
    }

    nb::object PyTimeSeriesType::owning_graph() const {
        if (!_node) return nb::none();
        auto g = _node->graph();
        return g ? wrap_graph(g->shared_from_this()) : nb::none();
    }

    nb::bool_ PyTimeSeriesType::has_parent_or_node() const {
        return nb::bool_(_node != nullptr);
    }

    nb::bool_ PyTimeSeriesType::has_owning_node() const {
        return nb::bool_(_node != nullptr);
    }

    nb::bool_ PyTimeSeriesType::is_reference() const {
        return nb::bool_(_meta && _meta->ts_kind == TimeSeriesKind::REF);
    }

    void PyTimeSeriesType::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesType>(m, "TimeSeriesType")
            .def_prop_ro("owning_node", &PyTimeSeriesType::owning_node)
            .def_prop_ro("owning_graph", &PyTimeSeriesType::owning_graph)
            .def_prop_ro("value", &PyTimeSeriesType::value)
            .def_prop_ro("delta_value", &PyTimeSeriesType::delta_value)
            .def_prop_ro("modified", &PyTimeSeriesType::modified)
            .def_prop_ro("valid", &PyTimeSeriesType::valid)
            .def_prop_ro("all_valid", &PyTimeSeriesType::all_valid)
            .def_prop_ro("last_modified_time", &PyTimeSeriesType::last_modified_time)
            .def("is_reference", &PyTimeSeriesType::is_reference);
    }

    // =========================================================================
    // PyTimeSeriesOutput - Output wrapper implementation
    // =========================================================================

    PyTimeSeriesOutput::PyTimeSeriesOutput(node_s_ptr node, ts::TSOutputView view, ts::TSOutput* output, const TimeSeriesTypeMeta* meta)
        : PyTimeSeriesType(std::move(node), meta), _view(std::move(view)), _output(output) {}

    nb::object PyTimeSeriesOutput::value() const {
        if (!_view.valid()) return nb::none();
        // Use the value type's to_python conversion
        auto ts_value_view = _view.value_view();
        if (!ts_value_view.valid()) return nb::none();
        // Get the underlying ValueView which has the data() method
        auto vv = ts_value_view.value_view();
        return value::value_to_python(vv.data(), ts_value_view.schema());
    }

    nb::object PyTimeSeriesOutput::delta_value() const {
        // For now, delta_value returns the same as value for simple types
        // TODO: Implement proper delta tracking for collections
        return value();
    }

    engine_time_t PyTimeSeriesOutput::last_modified_time() const {
        return _view.last_modified_time();
    }

    nb::bool_ PyTimeSeriesOutput::modified() const {
        if (!_node) return nb::bool_(false);
        auto eval_time = _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        return nb::bool_(_view.modified_at(eval_time));
    }

    nb::bool_ PyTimeSeriesOutput::valid() const {
        return nb::bool_(_view.valid() && _view.has_value());
    }

    nb::bool_ PyTimeSeriesOutput::all_valid() const {
        // For simple values, all_valid is same as valid
        // TODO: For collections, check all elements
        return valid();
    }

    nb::object PyTimeSeriesOutput::parent_output() const {
        // V2 views don't track parent - return None
        // TODO: Could potentially navigate via path if needed
        return nb::none();
    }

    nb::bool_ PyTimeSeriesOutput::has_parent_output() const {
        return nb::bool_(false);
    }

    void PyTimeSeriesOutput::apply_result(nb::object value) {
        if (value.is_none()) return;
        set_value(std::move(value));
    }

    void PyTimeSeriesOutput::set_value(nb::object py_value) {
        if (!_view.valid() || !_meta) return;

        auto eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        auto& ts_value_view = _view.value_view();

        if (py_value.is_none()) {
            _view.mark_invalid();
            return;
        }

        // Use the value type's from_python conversion
        auto* schema = ts_value_view.schema();
        if (schema && schema->ops && schema->ops->from_python) {
            // Get the underlying ValueView which has the data() method
            auto vv = ts_value_view.value_view();
            schema->ops->from_python(vv.data(), py_value.ptr(), schema);
            _view.mark_modified(eval_time);
        }
    }

    void PyTimeSeriesOutput::copy_from_output(const PyTimeSeriesOutput& other) {
        // TODO: Implement efficient copy between outputs
        set_value(other.value());
    }

    void PyTimeSeriesOutput::copy_from_input(const PyTimeSeriesInput& input) {
        // TODO: Implement efficient copy from input
        set_value(input.value());
    }

    void PyTimeSeriesOutput::clear() {
        _view.mark_invalid();
    }

    void PyTimeSeriesOutput::invalidate() {
        _view.mark_invalid();
    }

    bool PyTimeSeriesOutput::can_apply_result(nb::object value) {
        // For simple values, we can always apply if view is valid
        return _view.valid();
    }

    void PyTimeSeriesOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesOutput, PyTimeSeriesType>(m, "TimeSeriesOutput")
            .def_prop_ro("parent_output", &PyTimeSeriesOutput::parent_output)
            .def_prop_ro("has_parent_output", &PyTimeSeriesOutput::has_parent_output)
            .def_prop_rw("value", &PyTimeSeriesOutput::value, &PyTimeSeriesOutput::set_value, nb::arg("value").none())
            .def("can_apply_result", &PyTimeSeriesOutput::can_apply_result)
            .def("apply_result", &PyTimeSeriesOutput::apply_result, nb::arg("value").none())
            .def("invalidate", &PyTimeSeriesOutput::invalidate)
            .def("copy_from_output", &PyTimeSeriesOutput::copy_from_output)
            .def("copy_from_input", &PyTimeSeriesOutput::copy_from_input);
    }

    // =========================================================================
    // PyTimeSeriesInput - Input wrapper implementation
    // =========================================================================

    PyTimeSeriesInput::PyTimeSeriesInput(node_s_ptr node, ts::TSInputView view, ts::TSInput* input, const TimeSeriesTypeMeta* meta)
        : PyTimeSeriesType(std::move(node), meta), _view(std::move(view)), _input(input) {}

    nb::object PyTimeSeriesInput::value() const {
        if (!_view.valid()) return nb::none();
        auto value_view = _view.value_view();
        if (!value_view.valid()) return nb::none();
        return value::value_to_python(value_view.data(), value_view.schema());
    }

    nb::object PyTimeSeriesInput::delta_value() const {
        // For now, delta_value returns the same as value for simple types
        return value();
    }

    engine_time_t PyTimeSeriesInput::last_modified_time() const {
        return _view.last_modified_time();
    }

    nb::bool_ PyTimeSeriesInput::modified() const {
        if (!_node) return nb::bool_(false);
        auto eval_time = _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        return nb::bool_(_view.modified_at(eval_time));
    }

    nb::bool_ PyTimeSeriesInput::valid() const {
        return nb::bool_(_view.valid() && _view.has_value());
    }

    nb::bool_ PyTimeSeriesInput::all_valid() const {
        return valid();
    }

    nb::object PyTimeSeriesInput::parent_input() const {
        // V2 views don't track parent
        return nb::none();
    }

    nb::bool_ PyTimeSeriesInput::has_parent_input() const {
        return nb::bool_(false);
    }

    nb::bool_ PyTimeSeriesInput::active() const {
        return nb::bool_(_input && _input->active());
    }

    void PyTimeSeriesInput::make_active() {
        if (_input) _input->make_active();
    }

    void PyTimeSeriesInput::make_passive() {
        if (_input) _input->make_passive();
    }

    nb::bool_ PyTimeSeriesInput::bound() const {
        return nb::bool_(_input && _input->bound());
    }

    nb::bool_ PyTimeSeriesInput::has_peer() const {
        // TODO: Implement peer tracking if needed
        return nb::bool_(false);
    }

    nb::object PyTimeSeriesInput::output() const {
        // TODO: Would need to wrap the bound output
        return nb::none();
    }

    nb::bool_ PyTimeSeriesInput::has_output() const {
        return nb::bool_(_input && _input->bound());
    }

    nb::bool_ PyTimeSeriesInput::bind_output(nb::object output_) {
        // TODO: Implement binding - needs unwrap of output
        return nb::bool_(false);
    }

    void PyTimeSeriesInput::un_bind_output(bool unbind_refs) {
        if (_input) _input->unbind_output();
    }

    nb::object PyTimeSeriesInput::reference_output() const {
        // TODO: Implement for REF types
        return nb::none();
    }

    nb::object PyTimeSeriesInput::get_input(size_t index) const {
        // TODO: Implement for collection types
        return nb::none();
    }

    void PyTimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesInput, PyTimeSeriesType>(m, "TimeSeriesInput")
            .def_prop_ro("parent_input", &PyTimeSeriesInput::parent_input)
            .def_prop_ro("has_parent_input", &PyTimeSeriesInput::has_parent_input)
            .def_prop_ro("bound", &PyTimeSeriesInput::bound)
            .def_prop_ro("has_peer", &PyTimeSeriesInput::has_peer)
            .def_prop_ro("output", &PyTimeSeriesInput::output)
            .def_prop_ro("reference_output", &PyTimeSeriesInput::reference_output)
            .def_prop_ro("active", &PyTimeSeriesInput::active)
            .def("__getitem__", &PyTimeSeriesInput::get_input, "index"_a)
            .def("bind_output", &PyTimeSeriesInput::bind_output, "output"_a)
            .def("un_bind_output", &PyTimeSeriesInput::un_bind_output, "unbind_refs"_a = false)
            .def("make_active", &PyTimeSeriesInput::make_active)
            .def("make_passive", &PyTimeSeriesInput::make_passive);
    }

}  // namespace hgraph
