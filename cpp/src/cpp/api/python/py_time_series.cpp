#include "hgraph/api/python/wrapper_factory.h"
#include "hgraph/types/time_series_type.h"

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>

namespace hgraph
{
    // ========== PyTimeSeriesType Implementation ==========

    nb::object PyTimeSeriesType::owning_node() const {
        if (has_view()) {
            // View-based: get node from the ShortPath
            auto node_ptr = view().short_path().node();
            return node_ptr ? wrap_node(node_ptr->shared_from_this()) : nb::none();
        }
        auto n = _impl->owning_node();
        return n ? wrap_node(n->shared_from_this()) : nb::none();
    }

    nb::object PyTimeSeriesType::owning_graph() const {
        if (has_view()) {
            // View-based: get graph from node->graph() back-pointer
            auto node_ptr = view().short_path().node();
            if (node_ptr) {
                auto graph_ptr = node_ptr->graph();
                return graph_ptr ? wrap_graph(graph_ptr->shared_from_this()) : nb::none();
            }
            return nb::none();
        }
        auto g = _impl->owning_graph();
        return g ? wrap_graph(g->shared_from_this()) : nb::none();
    }

    nb::bool_ PyTimeSeriesType::has_parent_or_node() const {
        if (has_view()) {
            // View-based: check if path has node
            return nb::bool_(view().short_path().node() != nullptr);
        }
        return nb::bool_(_impl->has_parent_or_node());
    }

    nb::bool_ PyTimeSeriesType::has_owning_node() const {
        if (has_view()) {
            return nb::bool_(view().short_path().node() != nullptr);
        }
        return nb::bool_(_impl->has_owning_node());
    }

    nb::object PyTimeSeriesType::value() const {
        if (has_view()) {
            return view().to_python();
        }
        return _impl->py_value();
    }

    nb::object PyTimeSeriesType::delta_value() const {
        if (has_view()) {
            return view().delta_to_python();
        }
        return _impl->py_delta_value();
    }

    engine_time_t PyTimeSeriesType::last_modified_time() const {
        if (has_view()) {
            return view().last_modified_time();
        }
        return _impl->last_modified_time();
    }

    nb::bool_ PyTimeSeriesType::valid() const {
        if (has_view()) {
            return nb::bool_(view().valid());
        }
        return nb::bool_(_impl->valid());
    }

    nb::bool_ PyTimeSeriesType::all_valid() const {
        if (has_view()) {
            return nb::bool_(view().all_valid());
        }
        return nb::bool_(_impl->all_valid());
    }

    nb::bool_ PyTimeSeriesType::is_reference() const {
        if (has_view()) {
            return nb::bool_(view().ts_meta()->kind == TSKind::REF);
        }
        return nb::bool_(_impl->is_reference());
    }

    nb::bool_ PyTimeSeriesType::modified() const {
        if (has_view()) {
            return nb::bool_(view().modified());
        }
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

    // Legacy constructor - uses ApiPtr
    PyTimeSeriesType::PyTimeSeriesType(api_ptr impl) : _impl{std::move(impl)} {}

    // New view-based constructor
    PyTimeSeriesType::PyTimeSeriesType(TSView view) : view_{std::move(view)} {}

    control_block_ptr PyTimeSeriesType::control_block() const { return _impl.control_block(); }

    // ========== PyTimeSeriesOutput Implementation ==========

    PyTimeSeriesOutput::PyTimeSeriesOutput(TSOutputView view)
        : PyTimeSeriesType(view.ts_view())
        , output_view_{std::move(view)} {}

    nb::object PyTimeSeriesOutput::parent_output() const {
        if (has_output_view()) {
            const ShortPath& path = output_view().short_path();
            if (path.is_root()) {
                return nb::none();  // No parent - this is root
            }

            // Get parent path (removes last index)
            ShortPath parent_path = path.parent();

            // Get TSOutput for navigation (const_cast safe: we're creating a new view, not modifying)
            TSOutput* output = const_cast<TSOutput*>(output_view().output());
            if (!output) {
                return nb::none();
            }

            // Get root view from TSOutput's native value
            engine_time_t current_time = output_view().current_time();
            TSView parent_view = output->native_value().ts_view(current_time);

            // Navigate through parent indices
            for (size_t idx : parent_path.indices()) {
                parent_view = parent_view[idx];
            }

            // Set the path on the view
            parent_view.view_data().path = parent_path;

            // Create and return wrapped TSOutputView
            TSOutputView parent_output_view(std::move(parent_view), output);
            return wrap_output_view(parent_output_view);
        }
        return impl()->parent_output() ? wrap_output(impl()->parent_output()) : nb::none();
    }

    nb::bool_ PyTimeSeriesOutput::has_parent_output() const {
        if (has_output_view()) {
            // View-based: has parent if path is not root (has indices)
            return nb::bool_(!output_view().short_path().is_root());
        }
        return nb::bool_(impl()->has_parent_output());
    }

    void PyTimeSeriesOutput::apply_result(nb::object value) {
        if (has_output_view()) {
            if (!value.is_none()) {
                output_view().from_python(value);
            }
            return;
        }
        impl()->apply_result(std::move(value));
    }

    void PyTimeSeriesOutput::set_value(nb::object value) {
        if (has_output_view()) {
            if (value.is_none()) {
                output_view().invalidate();
            } else {
                output_view().from_python(value);
            }
            return;
        }
        impl()->py_set_value(std::move(value));
    }

    void PyTimeSeriesOutput::copy_from_output(const PyTimeSeriesOutput &output) {
        if (has_output_view()) {
            // View-based: copy the value
            if (output.has_output_view()) {
                output_view().set_value(output.output_view().value());
            } else {
                // Mixed mode: source is legacy, target is view
                output_view().from_python(output.value());
            }
            return;
        }
        impl()->copy_from_output(*unwrap_output(output));
    }

    void PyTimeSeriesOutput::copy_from_input(const PyTimeSeriesInput &input) {
        if (has_output_view()) {
            // View-based: copy the value from input
            if (input.has_view()) {
                output_view().set_value(input.view().value());
            } else {
                // Mixed mode: source is legacy, target is view
                output_view().from_python(input.value());
            }
            return;
        }
        impl()->copy_from_input(*unwrap_input(input));
    }

    void PyTimeSeriesOutput::clear() {
        if (has_output_view()) {
            output_view().invalidate();
            return;
        }
        impl()->clear();
    }

    void PyTimeSeriesOutput::invalidate() {
        if (has_output_view()) {
            output_view().invalidate();
            return;
        }
        impl()->invalidate();
    }

    bool PyTimeSeriesOutput::can_apply_result(nb::object value) {
        if (has_output_view()) {
            // View-based: always can apply (validation done at set time)
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

    // ========== PyTimeSeriesInput Implementation ==========

    PyTimeSeriesInput::PyTimeSeriesInput(TSInputView view)
        : PyTimeSeriesType(view.ts_view())
        , input_view_{std::move(view)} {}

    nb::object PyTimeSeriesInput::parent_input() const {
        if (has_input_view()) {
            const ShortPath& path = input_view().short_path();
            if (path.is_root()) {
                return nb::none();  // No parent - this is root
            }

            // Get parent path (removes last index)
            ShortPath parent_path = path.parent();

            // Get TSInput for navigation (const_cast safe: we're creating a new view, not modifying)
            TSInput* input = const_cast<TSInput*>(input_view().input());
            if (!input) {
                return nb::none();
            }

            // Get root view from TSInput
            engine_time_t current_time = input_view().current_time();
            TSInputView root_view = input->view(current_time);

            // Navigate through parent indices
            TSInputView parent_view = root_view;
            for (size_t idx : parent_path.indices()) {
                parent_view = parent_view[idx];
            }

            // The path is already set by navigation, but ensure it's correct
            parent_view.ts_view().view_data().path = parent_path;

            // Return wrapped TSInputView
            return wrap_input_view(parent_view);
        }
        return impl()->parent_input() ? wrap_input(impl()->parent_input()) : nb::none();
    }

    nb::bool_ PyTimeSeriesInput::has_parent_input() const {
        if (has_input_view()) {
            // View-based: has parent if path is not root (has indices)
            return nb::bool_(!input_view().short_path().is_root());
        }
        return nb::bool_(impl()->has_parent_input());
    }

    nb::bool_ PyTimeSeriesInput::active() const {
        if (has_input_view()) {
            return nb::bool_(input_view().active());
        }
        return nb::bool_(impl()->active());
    }

    void PyTimeSeriesInput::make_active() {
        if (has_input_view()) {
            input_view().make_active();
            return;
        }
        impl()->make_active();
    }

    void PyTimeSeriesInput::make_passive() {
        if (has_input_view()) {
            input_view().make_passive();
            return;
        }
        impl()->make_passive();
    }

    nb::bool_ PyTimeSeriesInput::bound() const {
        if (has_input_view()) {
            return nb::bool_(input_view().is_bound());
        }
        return nb::bool_(impl()->bound());
    }

    nb::bool_ PyTimeSeriesInput::has_peer() const {
        if (has_input_view()) {
            // Peer means bound and NOT via REF indirection
            if (!input_view().is_bound()) {
                return nb::bool_(false);
            }
            // Get the schema of what we're bound to
            const TSMeta* bound_meta = input_view().ts_meta();
            if (!bound_meta) {
                return nb::bool_(false);
            }
            // Peer = bound and not a REF type
            return nb::bool_(bound_meta->kind != TSKind::REF);
        }
        return nb::bool_(impl()->has_peer());
    }

    nb::object PyTimeSeriesInput::output() const {
        if (has_input_view()) {
            TSOutput* bound = input_view().bound_output();
            if (!bound) {
                return nb::none();
            }
            // Create output view with current time and input's schema
            engine_time_t current_time = input_view().current_time();
            const TSMeta* schema = input_view().ts_meta();
            TSOutputView out_view = bound->view(current_time, schema);
            return wrap_output_view(out_view);
        }
        return wrap_output(impl()->output());
    }

    nb::bool_ PyTimeSeriesInput::has_output() const {
        if (has_input_view()) {
            return nb::bool_(input_view().is_bound());
        }
        return nb::bool_(impl()->has_output());
    }

    nb::bool_ PyTimeSeriesInput::bind_output(nb::object output_) {
        if (has_input_view()) {
            // Get the output view from the PyTimeSeriesOutput wrapper
            if (nb::isinstance<PyTimeSeriesOutput>(output_)) {
                auto& py_output = nb::cast<PyTimeSeriesOutput&>(output_);
                if (py_output.has_output_view()) {
                    TSOutputView& out_view = py_output.output_view();
                    input_view_.value().bind(out_view);
                    return nb::bool_(true);
                }
            }
            // If we can't get a view from the output, fall through to impl mode
            // This shouldn't happen in pure view mode, but handle gracefully
            throw std::runtime_error("bind_output requires view-based output when input is view-based");
        }
        return nb::bool_(impl()->bind_output(unwrap_output(output_)));
    }

    void PyTimeSeriesInput::un_bind_output(bool unbind_refs) {
        if (has_input_view()) {
            input_view().unbind();
            return;
        }
        return impl()->un_bind_output(unbind_refs);
    }

    nb::object PyTimeSeriesInput::reference_output() const {
        if (has_input_view()) {
            // Check if bound to a REF type
            const TSMeta* bound_meta = input_view().ts_meta();
            if (!bound_meta || bound_meta->kind != TSKind::REF) {
                return nb::none();
            }
            // Bound to REF - return the bound output
            TSOutput* bound = input_view().bound_output();
            if (!bound) {
                return nb::none();
            }
            engine_time_t current_time = input_view().current_time();
            TSOutputView out_view = bound->view(current_time, bound_meta);
            return wrap_output_view(out_view);
        }
        return wrap_output(impl()->reference_output());
    }

    nb::object PyTimeSeriesInput::get_input(size_t index) const {
        if (has_input_view()) {
            // Navigate to child and wrap
            TSInputView child_view = input_view()[index];
            return wrap_input_view(child_view);
        }
        return wrap_input(impl()->get_input(index));
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

    TimeSeriesInput *PyTimeSeriesInput::impl() const { return static_cast_impl<TimeSeriesInput>(); }
}  // namespace hgraph
