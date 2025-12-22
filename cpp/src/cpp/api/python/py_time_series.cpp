#include <hgraph/api/python/py_time_series.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/value/python_conversion.h>
#include <hgraph/types/time_series/delta_view_python.h>
#include <hgraph/api/python/ts_python_helpers.h>

namespace hgraph
{
    // =========================================================================
    // PyTimeSeriesType - Base class implementation
    // =========================================================================

    PyTimeSeriesType::PyTimeSeriesType(node_s_ptr node, const TSMeta* meta)
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
        return nb::bool_(_meta && _meta->ts_kind == TSKind::REF);
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

    PyTimeSeriesOutput::PyTimeSeriesOutput(node_s_ptr node, value::TSView view, ts::TSOutput* output, const TSMeta* meta)
        : PyTimeSeriesType(std::move(node), meta), _view(std::move(view)), _output(output) {}

    nb::object PyTimeSeriesOutput::value() const {
        if (!_view.valid()) return nb::none();
        // Get the underlying value view which has the data() method
        auto vv = _view.value_view();
        if (!vv.valid()) return nb::none();
        auto* schema = _view.value_schema();
        return value::value_to_python(vv.data(), schema);
    }

    nb::object PyTimeSeriesOutput::delta_value() const {
        if (!_node) return nb::none();

        // IMPORTANT: Use fresh view from _output when available to ensure consistency
        // with set_python_value which also uses output->view(). Using the stored _view
        // can result in tracker mismatch when the output was modified through set_python_value.
        value::TSView view_to_use;
        if (_output) {
            view_to_use = _output->view();
        } else {
            view_to_use = _view;  // Fallback for field wrappers (no _output)
        }

        if (!view_to_use.valid()) return nb::none();

        auto eval_time = _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;

        if (!view_to_use.modified_at(eval_time)) return nb::none();

        // For collection types (TSD, TSL, TSS), check if there's a cached delta
        // These types don't have native C++ storage, so we cache the Python result
        if (_output && _meta) {
            auto ts_kind = _meta->ts_kind;
            if (ts_kind == TSKind::TSD ||
                ts_kind == TSKind::TSL ||
                ts_kind == TSKind::TSS) {
                // Try to get cached delta (cleared at tick end by callback)
                auto cached = ts::get_cached_delta(_output);
                if (!cached.is_none()) {
                    return cached;
                }
            }
        }

        // Fall back to DeltaView-based conversion for types with C++ storage
        auto delta = view_to_use.delta_view(eval_time);
        return ts::delta_to_python(delta);
    }

    engine_time_t PyTimeSeriesOutput::last_modified_time() const {
        // Use fresh view from _output when available for consistency
        value::TSView view_to_use = _output ? _output->view() : _view;
        return view_to_use.last_modified_time();
    }

    nb::bool_ PyTimeSeriesOutput::modified() const {
        if (!_node) return nb::bool_(false);
        // Use fresh view from _output when available for consistency
        value::TSView view_to_use = _output ? _output->view() : _view;
        auto eval_time = _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        return nb::bool_(view_to_use.modified_at(eval_time));
    }

    nb::bool_ PyTimeSeriesOutput::valid() const {
        // Use fresh view from _output when available for consistency
        value::TSView view_to_use = _output ? _output->view() : _view;
        return nb::bool_(view_to_use.valid() && view_to_use.has_value());
    }

    nb::bool_ PyTimeSeriesOutput::all_valid() const {
        // For simple values, all_valid is same as valid
        // TODO: For collections, check all elements
        return valid();
    }

    nb::object PyTimeSeriesOutput::parent_output() const {
        // Views don't track parent - return None
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

        if (py_value.is_none()) {
            _view.mark_invalid();
            return;
        }

        // _view is already a TimeSeriesValueView - use it directly
        auto* schema = _view.schema();
        if (schema && schema->ops && schema->ops->from_python) {
            // Get the underlying ValueView which has the data() method
            auto vv = _view.value_view();
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

    PyTimeSeriesInput::PyTimeSeriesInput(node_s_ptr node, ts::TSInputView view, ts::TSInput* input, const TSMeta* meta)
        : PyTimeSeriesType(std::move(node), meta), _view(std::move(view)), _input(input) {}

    PyTimeSeriesInput::PyTimeSeriesInput(node_s_ptr node, ts::TSInputView view, const TSMeta* meta)
        : PyTimeSeriesType(std::move(node), meta), _view(std::move(view)), _input(nullptr) {}

    PyTimeSeriesInput::PyTimeSeriesInput(node_s_ptr node, ts::TSInputView view, ts::TSInput* root_input, size_t field_index, const TSMeta* meta)
        : PyTimeSeriesType(std::move(node), meta), _view(std::move(view)), _input(nullptr), _root_input(root_input), _field_index(field_index) {}

    nb::object PyTimeSeriesInput::value() const {
        // The view points to the AccessStrategy and fetches fresh data on each call
        // This works for both direct inputs and field wrappers
        if (!_view.valid()) {
            return nb::none();
        }

        // Special case for REF inputs: return the TimeSeriesReference from the bound REF output
        // This preserves path information and empty reference state
        if (_meta && _meta->ts_kind == TSKind::REF) {
            // Try ref_bound_output() first (for RefObserver case - non-REF input to REF output)
            ts::TSOutput* output_to_check = _view.ref_bound_output();

            // For direct REF-to-REF binding, check if bound_output is a REF output
            if (!output_to_check) {
                auto* bound = _view.bound_output();
                // Only use bound_output if it's actually a REF output
                if (bound && bound->ts_kind() == TSKind::REF) {
                    output_to_check = bound;
                }
            }

            if (output_to_check) {
                auto cached = ts::get_cached_delta(output_to_check);
                if (!cached.is_none()) {
                    return cached;
                }
            }

            // Fall back to creating TimeSeriesReference from bound view
            auto* source = _view.source();
            if (source) {
                auto bound_view = source->bound_view();
                if (bound_view.valid()) {
                    auto ref = TimeSeriesReference::make(bound_view);
                    return nb::cast(ref);
                }
            }
            return nb::none();
        }

        auto value_view = _view.value_view();
        if (!value_view.valid()) return nb::none();
        // Get schema from meta if available, fall back to value_view schema
        auto* schema = _meta ? _meta->value_schema() : value_view.schema();
        return value::value_to_python(value_view.data(), schema);
    }

    nb::object PyTimeSeriesInput::delta_value() const {
        if (!_node) return nb::none();
        auto eval_time = _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;

        // The view fetches fresh data from the strategy
        if (!_view.valid()) {
            return nb::none();
        }
        if (!_view.modified_at(eval_time)) {
            return nb::none();
        }

        // For collection types (TSD, TSL, TSS), check if there's a cached delta
        // on the bound output. These types don't have native C++ storage.
        if (_meta) {
            auto ts_kind = _meta->ts_kind;
            if (ts_kind == TSKind::TSD ||
                ts_kind == TSKind::TSL ||
                ts_kind == TSKind::TSS) {
                // Get the bound output and check its cache
                auto* bound_output = _view.bound_output();
                if (bound_output) {
                    auto cached = ts::get_cached_delta(bound_output);
                    if (!cached.is_none()) {
                        return cached;
                    }
                }
            }
        }

        // Fall back to DeltaView-based conversion for types with C++ storage
        auto delta = _view.delta_view(eval_time);
        return ts::delta_to_python(delta);
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
        // Views don't track parent
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
        else if (_root_input && _field_index != SIZE_MAX) {
            // For bindable field wrappers, unbind through the root input
            auto bindable = _root_input->element(_field_index);
            if (bindable.valid()) {
                bindable.bind(value::TSView());  // Bind to invalid view to unbind
            }
        }
    }

    void PyTimeSeriesInput::bind_output_view(value::TSView view) {
        if (_input) {
            // Direct input - use bind_output
            _input->bind_output(view);
        } else if (_root_input && _field_index != SIZE_MAX) {
            // Check if already bound to this output to avoid creating strategies every tick
            auto current_view = _view;
            if (current_view.valid()) {
                auto* bound = current_view.bound_output();
                if (bound && view.valid()) {
                    // Compare output pointers - if same, skip rebinding
                    auto* target = view.root_output();
                    if (bound == target) {
                        return;  // Already bound to this output
                    }
                }
            }

            // Bindable field wrapper - use TSInputBindableView
            auto bindable = _root_input->element(_field_index);
            if (bindable.valid()) {
                bindable.bind(view);

                // Update our view to point to the newly bound strategy
                // The view we have might be stale - get fresh view after binding
                auto root_view = _root_input->view();
                if (root_view.valid()) {
                    _view = root_view.element(_field_index);
                }
            }
        }
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
