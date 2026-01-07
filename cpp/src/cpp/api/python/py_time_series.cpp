#include "hgraph/api/python/wrapper_factory.h"
#include "hgraph/types/time_series_type.h"

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ref.h>

namespace hgraph
{
    nb::object PyTimeSeriesType::owning_node() const {
        if (!has_impl()) {
            // View-based instances don't support graph navigation yet
            return nb::none();
        }
        auto n = _impl->owning_node();
        return n ? wrap_node(n->shared_from_this()) : nb::none();
    }

    nb::object PyTimeSeriesType::owning_graph() const {
        if (!has_impl()) {
            // View-based instances don't support graph navigation yet
            return nb::none();
        }
        auto g = _impl->owning_graph();
        return g ? wrap_graph(g->shared_from_this()) : nb::none();
    }

    nb::bool_ PyTimeSeriesType::has_parent_or_node() const {
        if (!has_impl()) return nb::bool_(false);
        return nb::bool_(_impl->has_parent_or_node());
    }

    nb::bool_ PyTimeSeriesType::has_owning_node() const {
        if (!has_impl()) return nb::bool_(false);
        return nb::bool_(_impl->has_owning_node());
    }

    nb::object PyTimeSeriesType::value() const {
        // Base implementation uses impl if available
        if (has_impl()) {
            return _impl->py_value();
        }
        // Phase 0 note: view-based wrappers must override required legacy surfaces.
        // See `ts_design_docs/Value_TSValue_MIGRATION_PLAN.md` Phase 0 checklist.
        throw std::runtime_error("PyTimeSeriesType::value() called on view-only instance - override in derived class");
    }

    nb::object PyTimeSeriesType::delta_value() const {
        if (!has_impl()) {
            // View-based instances don't support delta_value yet.
            // Phase 0 note: delta capability is part of the required parity surface.
            // See `ts_design_docs/Value_TSValue_MIGRATION_PLAN.md` Phase 0 checklist.
            return nb::none();
        }
        return _impl->py_delta_value();
    }

    engine_time_t PyTimeSeriesType::last_modified_time() const {
        if (!has_impl()) {
            return MIN_DT;  // View-based should override this
        }
        return _impl->last_modified_time();
    }

    nb::bool_ PyTimeSeriesType::valid() const {
        if (!has_impl()) return nb::bool_(false);  // View-based should override
        return nb::bool_(_impl->valid());
    }

    nb::bool_ PyTimeSeriesType::all_valid() const {
        if (!has_impl()) return nb::bool_(false);  // View-based should override
        return nb::bool_(_impl->all_valid());
    }

    nb::bool_ PyTimeSeriesType::is_reference() const {
        if (!has_impl()) return nb::bool_(false);
        return nb::bool_(_impl->is_reference());
    }

    nb::bool_ PyTimeSeriesType::modified() const {
        if (!has_impl()) return nb::bool_(false);  // View-based should override
        return nb::bool_(_impl->modified());
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
            // .def("re_parent", static_cast<void (PyTimeSeriesType::*)(const Node::ptr &)>(&PyTimeSeriesType::re_parent))
            // .def("re_parent", static_cast<void (PyTimeSeriesType::*)(const ptr &)>(&PyTimeSeriesType::re_parent))
            .def("is_reference", &PyTimeSeriesType::is_reference);
        //.def("has_reference", &PyTimeSeriesType::has_reference)
    }

    PyTimeSeriesType::PyTimeSeriesType(api_ptr impl) : _impl{std::move(impl)} {}

    control_block_ptr PyTimeSeriesType::control_block() const { return _impl.control_block(); }

    // ========== PyTimeSeriesOutput view-based implementation ==========

    PyTimeSeriesOutput::PyTimeSeriesOutput(TSMutableView view)
        : PyTimeSeriesType(api_ptr{}), _view(view) {}

    TSMutableView PyTimeSeriesOutput::view() const {
        if (!_view.has_value()) {
            throw std::runtime_error("PyTimeSeriesOutput::view() called on non-view-based instance");
        }
        return *_view;
    }

    nb::object PyTimeSeriesOutput::value() const {
        if (_view.has_value()) {
            return _view->to_python();
        }
        // Use the underlying impl's py_value() for ApiPtr-based instances
        return impl()->py_value();
    }

    void PyTimeSeriesOutput::set_value(nb::object value) {
        if (_view.has_value()) {
            // Phase 0 note: view-based mutation currently bypasses engine-time semantics.
            // - No timestamp/modified-time update is performed here.
            // - Legacy outputs treat `None` as invalidate; this path currently forwards to `from_python`.
            // See `ts_design_docs/Value_TSValue_MIGRATION_PLAN.md` Phase 0 checklist.
            _view->from_python(value);
        } else {
            // Use the underlying impl's py_set_value() for ApiPtr-based instances
            impl()->py_set_value(value);
        }
    }

    nb::object PyTimeSeriesOutput::parent_output() const {
        if (_view.has_value()) {
            return nb::none();  // View-based instances don't have graph navigation
        }
        return impl()->parent_output() ? wrap_output(impl()->parent_output()) : nb::none();
    }

    nb::bool_ PyTimeSeriesOutput::has_parent_output() const {
        if (_view.has_value()) {
            return nb::bool_(false);  // View-based instances don't have graph navigation
        }
        return nb::bool_(impl()->has_parent_output());
    }

    void PyTimeSeriesOutput::apply_result(nb::object value) {
        if (_view.has_value()) {
            // Phase 0 note: legacy behavior treats `None` as invalidate.
            // This view-based path currently ignores `None` (no-op) and does not update modification time.
            // See `ts_design_docs/Value_TSValue_MIGRATION_PLAN.md` Phase 0 checklist.
            if (!value.is_none()) {
                _view->from_python(value);
            }
            return;
        }
        impl()->apply_result(std::move(value));
    }

    void PyTimeSeriesOutput::copy_from_output(const PyTimeSeriesOutput &output) {
        if (_view.has_value()) {
            if (output.is_view_based()) {
                // Both are view-based - copy via TSView
                _view->copy_from(output.view());
            } else {
                // Copy from ApiPtr-based output - get the Python value and convert
                _view->from_python(output.value());
            }
            return;
        }
        impl()->copy_from_output(*unwrap_output(output));
    }

    void PyTimeSeriesOutput::copy_from_input(const PyTimeSeriesInput &input) {
        if (_view.has_value()) {
            if (input.is_view_based()) {
                // Both are view-based - copy via TSView
                _view->copy_from(input.view());
            } else {
                // Copy from ApiPtr-based input - get the Python value and convert
                _view->from_python(input.value());
            }
            return;
        }
        impl()->copy_from_input(*unwrap_input(input));
    }

    void PyTimeSeriesOutput::clear() {
        if (_view.has_value()) {
            _view->invalidate_ts();
            return;
        }
        impl()->clear();
    }

    void PyTimeSeriesOutput::invalidate() {
        if (_view.has_value()) {
            _view->invalidate_ts();
            return;
        }
        impl()->invalidate();
    }

    bool PyTimeSeriesOutput::can_apply_result(nb::object value) {
        if (_view.has_value()) {
            // View-based instances can always apply a result (no queue logic)
            return true;
        }
        return impl()->can_apply_result(std::move(value));
    }


    void PyTimeSeriesOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesOutput, PyTimeSeriesType>(m, "TimeSeriesOutput")
            .def_prop_ro("parent_output", &PyTimeSeriesOutput::parent_output)
            .def_prop_ro("has_parent_output", &PyTimeSeriesOutput::has_parent_output)
            .def_prop_rw("value", &PyTimeSeriesOutput::value, &PyTimeSeriesOutput::set_value, nb::arg("value").none())
            .def("can_apply_result", &PyTimeSeriesOutput::can_apply_result)
            .def("apply_result", &PyTimeSeriesOutput::apply_result, nb::arg("value").none())
            .def("clear", &PyTimeSeriesOutput::clear)
            .def("invalidate", &PyTimeSeriesOutput::invalidate)
            // .def("mark_invalid", &PyTimeSeriesOutput::mark_invalid)
            // .def("mark_modified", static_cast<void (PyTimeSeriesOutput::*)()>(&PyTimeSeriesOutput::mark_modified))
            // .def("mark_modified", static_cast<void (PyTimeSeriesOutput::*)(engine_time_t)>(&PyTimeSeriesOutput::mark_modified))
            // .def("subscribe", &PyTimeSeriesOutput::subscribe)
            // .def("unsubscribe", &PyTimeSeriesOutput::un_subscribe)
            .def("copy_from_output", &PyTimeSeriesOutput::copy_from_output)
            .def("copy_from_input", &PyTimeSeriesOutput::copy_from_input);
    }

    TimeSeriesOutput *PyTimeSeriesOutput::impl() const { return static_cast_impl<TimeSeriesOutput>(); }

    // ========== PyTimeSeriesInput view-based implementation ==========

    PyTimeSeriesInput::PyTimeSeriesInput(TSView view)
        : PyTimeSeriesType(api_ptr{}), _view(view) {}

    TSView PyTimeSeriesInput::view() const {
        if (!_view.has_value()) {
            throw std::runtime_error("PyTimeSeriesInput::view() called on non-view-based instance");
        }
        return *_view;
    }

    nb::object PyTimeSeriesInput::value() const {
        if (_view.has_value()) {
            return _view->to_python();
        }
        // Use the underlying impl's py_value() for ApiPtr-based instances
        return impl()->py_value();
    }

    nb::object PyTimeSeriesInput::parent_input() const {
        if (_view.has_value()) {
            return nb::none();  // View-based instances don't have graph navigation
        }
        return impl()->parent_input() ? wrap_input(impl()->parent_input()) : nb::none();
    }

    nb::bool_ PyTimeSeriesInput::has_parent_input() const {
        if (_view.has_value()) {
            return nb::bool_(false);  // View-based instances don't have graph navigation
        }
        return nb::bool_(impl()->has_parent_input());
    }

    nb::bool_ PyTimeSeriesInput::active() const {
        if (_view.has_value()) {
            return nb::bool_(true);  // View-based instances are always "active"
        }
        return nb::bool_(impl()->active());
    }

    void PyTimeSeriesInput::make_active() {
        if (_view.has_value()) return;  // No-op for view-based
        impl()->make_active();
    }

    void PyTimeSeriesInput::make_passive() {
        if (_view.has_value()) return;  // No-op for view-based
        impl()->make_passive();
    }

    nb::bool_ PyTimeSeriesInput::bound() const {
        if (_view.has_value()) {
            return nb::bool_(true);  // View-based instances are always "bound" (to their data)
        }
        return nb::bool_(impl()->bound());
    }

    nb::bool_ PyTimeSeriesInput::has_peer() const {
        if (_view.has_value()) {
            return nb::bool_(false);  // View-based instances don't have peers
        }
        return nb::bool_(impl()->has_peer());
    }

    nb::object PyTimeSeriesInput::output() const {
        if (_view.has_value()) {
            return nb::none();  // View-based instances don't have output binding
        }
        return wrap_output(impl()->output());
    }

    nb::bool_ PyTimeSeriesInput::has_output() const {
        if (_view.has_value()) {
            return nb::bool_(false);  // View-based instances don't have output binding
        }
        return nb::bool_(impl()->has_output());
    }

    nb::bool_ PyTimeSeriesInput::bind_output(nb::object output_) {
        if (_view.has_value()) {
            throw std::runtime_error("bind_output not supported for view-based instances");
        }
        return nb::bool_(impl()->bind_output(unwrap_output(output_)));
    }

    void PyTimeSeriesInput::un_bind_output(bool unbind_refs) {
        if (_view.has_value()) {
            throw std::runtime_error("un_bind_output not supported for view-based instances");
        }
        return impl()->un_bind_output(unbind_refs);
    }

    nb::object PyTimeSeriesInput::reference_output() const {
        if (_view.has_value()) {
            return nb::none();  // View-based instances don't have reference outputs
        }
        return wrap_output(impl()->reference_output());
    }

    nb::object PyTimeSeriesInput::get_input(size_t index) const {
        if (_view.has_value()) {
            throw std::runtime_error("get_input not supported for view-based instances");
        }
        return wrap_input(impl()->get_input(index));
    }

    void PyTimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesInput, PyTimeSeriesType>(m, "TimeSeriesInput")
            .def_prop_ro("value", &PyTimeSeriesInput::value)
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

    TimeSeriesInput *PyTimeSeriesInput::impl() const { return static_cast_impl<TimeSeriesInput>(); }
}  // namespace hgraph
