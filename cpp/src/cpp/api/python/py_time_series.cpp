#include "hgraph/api/python/wrapper_factory.h"

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/link_target.h>

namespace hgraph
{
    // ========== PyTimeSeriesType Implementation ==========

    PyTimeSeriesType::PyTimeSeriesType(TSView view) : view_{std::move(view)} {}

    nb::object PyTimeSeriesType::owning_node() const {
        auto node_ptr = view().short_path().node();
        return node_ptr ? wrap_node(node_ptr->shared_from_this()) : nb::none();
    }

    nb::object PyTimeSeriesType::owning_graph() const {
        auto node_ptr = view().short_path().node();
        if (node_ptr) {
            auto graph_ptr = node_ptr->graph();
            return graph_ptr ? wrap_graph(graph_ptr->shared_from_this()) : nb::none();
        }
        return nb::none();
    }

    nb::bool_ PyTimeSeriesType::has_parent_or_node() const {
        return nb::bool_(view().short_path().node() != nullptr);
    }

    nb::bool_ PyTimeSeriesType::has_owning_node() const {
        return nb::bool_(view().short_path().node() != nullptr);
    }

    nb::object PyTimeSeriesType::value() const {
        return view().to_python();
    }

    nb::object PyTimeSeriesType::delta_value() const {
        return view().delta_to_python();
    }

    engine_time_t PyTimeSeriesType::last_modified_time() const {
        return view().last_modified_time();
    }

    nb::bool_ PyTimeSeriesType::valid() const {
        return nb::bool_(view().valid());
    }

    nb::bool_ PyTimeSeriesType::all_valid() const {
        return nb::bool_(view().all_valid());
    }

    nb::bool_ PyTimeSeriesType::is_reference() const {
        return nb::bool_(view().ts_meta()->kind == TSKind::REF);
    }

    nb::bool_ PyTimeSeriesType::modified() const {
        return nb::bool_(view().modified());
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

    // ========== PyTimeSeriesOutput Implementation ==========

    PyTimeSeriesOutput::PyTimeSeriesOutput(TSOutputView view)
        : PyTimeSeriesType(view.ts_view())
        , output_view_{std::move(view)} {}

    TSOutputView& PyTimeSeriesOutput::output_view() {
        auto* out = output_view_.output();
        if (out && out->is_forwarded()) {
            // Refresh ViewData pointers in-place from forwarded_target.
            // This handles the case where forwarded_target was set after the wrapper
            // was constructed (e.g., TsdMapNode::wire_graph sets forwarded_target
            // on the inner graph's output stub after node start).
            auto& ft = out->forwarded_target();
            auto& vd = output_view_.ts_view().view_data();
            vd.value_data = ft.value_data;
            vd.time_data = ft.time_data;
            vd.observer_data = ft.observer_data;
            vd.delta_data = ft.delta_data;
            vd.link_data = ft.link_data;
            vd.ops = ft.ops;
            vd.meta = ft.meta;
        }
        return output_view_;
    }

    const TSOutputView& PyTimeSeriesOutput::output_view() const {
        return const_cast<PyTimeSeriesOutput*>(this)->output_view();
    }

    nb::object PyTimeSeriesOutput::parent_output() const {
        const ShortPath& path = output_view().short_path();
        if (path.is_root()) {
            return nb::none();
        }

        ShortPath parent_path = path.parent();

        TSOutput* output = const_cast<TSOutput*>(output_view().output());
        if (!output) {
            return nb::none();
        }

        engine_time_t current_time = output_view().current_time();
        TSView parent_view = output->native_value().ts_view(current_time);

        for (size_t idx : parent_path.indices()) {
            parent_view = parent_view[idx];
        }

        parent_view.view_data().path = parent_path;

        TSOutputView parent_output_view(std::move(parent_view), output);
        return wrap_output_view(parent_output_view);
    }

    nb::bool_ PyTimeSeriesOutput::has_parent_output() const {
        return nb::bool_(!output_view().short_path().is_root());
    }

    void PyTimeSeriesOutput::apply_result(nb::object value) {
        if (!value.is_none()) {
            output_view().from_python(value);
        }
    }

    void PyTimeSeriesOutput::set_value(nb::object value) {
        if (value.is_none()) {
            output_view().invalidate();
        } else {
            output_view().from_python(value);
        }
    }

    void PyTimeSeriesOutput::copy_from_output(const PyTimeSeriesOutput &output) {
        output_view().set_value(output.output_view().value());
    }

    void PyTimeSeriesOutput::copy_from_input(const PyTimeSeriesInput &input) {
        output_view().set_value(input.view().value());
    }

    void PyTimeSeriesOutput::clear() {
        output_view().invalidate();
    }

    void PyTimeSeriesOutput::invalidate() {
        output_view().invalidate();
    }

    bool PyTimeSeriesOutput::can_apply_result(nb::object value) {
        return true;
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
            .def("copy_from_output", &PyTimeSeriesOutput::copy_from_output)
            .def("copy_from_input", &PyTimeSeriesOutput::copy_from_input);
    }

    // ========== PyTimeSeriesInput Implementation ==========

    PyTimeSeriesInput::PyTimeSeriesInput(TSInputView view)
        : PyTimeSeriesType(view.ts_view())
        , input_view_{std::move(view)} {}

    nb::object PyTimeSeriesInput::parent_input() const {
        const ShortPath& path = input_view().short_path();
        if (path.is_root()) {
            return nb::none();
        }

        ShortPath parent_path = path.parent();

        TSInput* input = const_cast<TSInput*>(input_view().input());
        if (!input) {
            return nb::none();
        }

        engine_time_t current_time = input_view().current_time();
        TSInputView root_view = input->view(current_time);

        TSInputView parent_view = root_view;
        for (size_t idx : parent_path.indices()) {
            parent_view = parent_view[idx];
        }

        parent_view.ts_view().view_data().path = parent_path;

        return wrap_input_view(parent_view);
    }

    nb::bool_ PyTimeSeriesInput::has_parent_input() const {
        return nb::bool_(!input_view().short_path().is_root());
    }

    nb::bool_ PyTimeSeriesInput::active() const {
        return nb::bool_(input_view().active());
    }

    void PyTimeSeriesInput::make_active() {
        input_view().make_active();
    }

    void PyTimeSeriesInput::make_passive() {
        input_view().make_passive();
    }

    nb::bool_ PyTimeSeriesInput::bound() const {
        return nb::bool_(input_view().is_bound());
    }

    nb::bool_ PyTimeSeriesInput::has_peer() const {
        // Dispatch through ts_ops to check peered state in the link structure
        const auto& vd = input_view().ts_view().view_data();
        if (vd.ops && vd.ops->is_peered) {
            return nb::bool_(vd.ops->is_peered(vd));
        }
        return nb::bool_(false);
    }

    nb::object PyTimeSeriesInput::output() const {
        // First try the view's stored bound_output (set during bind on same view)
        TSOutput* bound = input_view().bound_output();

        // If not available (transient view), recover from link structure
        if (!bound) {
            const auto& vd = input_view().ts_view().view_data();
            if (vd.uses_link_target && vd.link_data) {
                auto* lt = static_cast<const LinkTarget*>(vd.link_data);
                if (lt->is_linked && lt->target_path.node()) {
                    bound = lt->target_path.node()->ts_output();
                }
            }
        }

        if (!bound) {
            return nb::none();
        }
        engine_time_t current_time = input_view().current_time();
        const TSMeta* schema = input_view().ts_meta();
        TSOutputView out_view = bound->view(current_time, schema);
        return wrap_output_view(out_view);
    }

    nb::bool_ PyTimeSeriesInput::has_output() const {
        return nb::bool_(input_view().is_bound());
    }

    nb::bool_ PyTimeSeriesInput::bind_output(nb::object output_) {
        if (nb::isinstance<PyTimeSeriesOutput>(output_)) {
            auto& py_output = nb::cast<PyTimeSeriesOutput&>(output_);
            TSOutputView& out_view = py_output.output_view();
            // TSInputView::bind() delegates to ts_ops::bind which sets peered on LinkTarget
            input_view_.bind(out_view);
            // Return peered state from link structure
            const auto& vd = input_view().ts_view().view_data();
            bool peered = vd.ops && vd.ops->is_peered && vd.ops->is_peered(vd);
            return nb::bool_(peered);
        }
        throw std::runtime_error("bind_output requires a PyTimeSeriesOutput with view");
    }

    void PyTimeSeriesInput::un_bind_output(bool unbind_refs) {
        input_view().unbind();
    }

    nb::object PyTimeSeriesInput::reference_output() const {
        const TSMeta* bound_meta = input_view().ts_meta();
        if (!bound_meta || bound_meta->kind != TSKind::REF) {
            return nb::none();
        }
        TSOutput* bound = input_view().bound_output();
        // If not available, recover from link structure
        if (!bound) {
            const auto& vd = input_view().ts_view().view_data();
            if (vd.uses_link_target && vd.link_data) {
                auto* lt = static_cast<const LinkTarget*>(vd.link_data);
                if (lt->is_linked && lt->target_path.node()) {
                    bound = lt->target_path.node()->ts_output();
                }
            }
        }
        if (!bound) {
            return nb::none();
        }
        engine_time_t current_time = input_view().current_time();
        TSOutputView out_view = bound->view(current_time, bound_meta);
        return wrap_output_view(out_view);
    }

    nb::object PyTimeSeriesInput::get_input(size_t index) const {
        TSInputView child_view = input_view()[index];
        return wrap_input_view(child_view);
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
