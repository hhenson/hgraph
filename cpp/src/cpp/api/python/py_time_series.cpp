#include "hgraph/api/python/wrapper_factory.h"

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_output.h>

#include <optional>
#include <stdexcept>
#include <utility>

namespace hgraph
{
namespace
{
    bool has_parent(const ShortPath &path) { return !path.indices.empty(); }

    bool input_kind_requires_bound_validity(const TSInputView& input_view) {
        const TSMeta* meta = input_view.ts_meta();
        const TSKind kind = meta != nullptr ? meta->kind : input_view.as_ts_view().kind();
        return kind == TSKind::TSValue || kind == TSKind::SIGNAL;
    }

    std::optional<ViewData> resolve_bound_target_view_data(const TSInputView &input_view) {
        const ViewData &vd = input_view.as_ts_view().view_data();
        if (!vd.uses_link_target || vd.link_data == nullptr) {
            return std::nullopt;
        }

        const auto *lt = static_cast<const LinkTarget *>(vd.link_data);
        if (lt == nullptr || !lt->is_linked) {
            return std::nullopt;
        }

        ViewData target{};
        target.path = lt->target_path;
        target.value_data = lt->value_data;
        target.time_data = lt->time_data;
        target.observer_data = lt->observer_data;
        target.delta_data = lt->delta_data;
        target.link_data = lt->link_data;
        target.link_observer_registry = vd.link_observer_registry;
        target.sampled = vd.sampled;
        target.uses_link_target = false;
        target.projection = ViewProjection::NONE;
        target.ops = lt->ops;
        target.meta = lt->meta;
        return target;
    }
}  // namespace

    // ========== PyTimeSeriesType Implementation ==========

    PyTimeSeriesType::PyTimeSeriesType(TSView view) : view_{std::move(view)} {}

    nb::object PyTimeSeriesType::owning_node() const {
        auto *node_ptr = view().short_path().node;
        return node_ptr ? wrap_node(node_ptr->shared_from_this()) : nb::none();
    }

    nb::object PyTimeSeriesType::owning_graph() const {
        auto *node_ptr = view().short_path().node;
        if (!node_ptr) {
            return nb::none();
        }
        auto *graph_ptr = node_ptr->graph();
        return graph_ptr ? wrap_graph(graph_ptr->shared_from_this()) : nb::none();
    }

    nb::bool_ PyTimeSeriesType::has_parent_or_node() const {
        const auto &path = view().short_path();
        return nb::bool_(path.node != nullptr || has_parent(path));
    }

    nb::bool_ PyTimeSeriesType::has_owning_node() const {
        return nb::bool_(view().short_path().node != nullptr);
    }

    nb::object PyTimeSeriesType::value() const {
        if (auto* input = dynamic_cast<const PyTimeSeriesInput*>(this)) {
            return input->input_view().as_ts_view().to_python();
        }
        if (auto* output = dynamic_cast<const PyTimeSeriesOutput*>(this)) {
            return output->output_view().as_ts_view().to_python();
        }
        return view().to_python();
    }

    nb::object PyTimeSeriesType::delta_value() const {
        if (auto* input = dynamic_cast<const PyTimeSeriesInput*>(this)) {
            return input->input_view().as_ts_view().delta_to_python();
        }
        if (auto* output = dynamic_cast<const PyTimeSeriesOutput*>(this)) {
            return output->output_view().as_ts_view().delta_to_python();
        }
        return view().delta_to_python();
    }

    engine_time_t PyTimeSeriesType::last_modified_time() const {
        if (auto* input = dynamic_cast<const PyTimeSeriesInput*>(this)) {
            if (input_kind_requires_bound_validity(input->input_view()) &&
                !input->input_view().is_bound()) {
                return MIN_DT;
            }
            return input->input_view().as_ts_view().last_modified_time();
        }
        if (auto* output = dynamic_cast<const PyTimeSeriesOutput*>(this)) {
            return output->output_view().as_ts_view().last_modified_time();
        }
        return view().last_modified_time();
    }

    nb::bool_ PyTimeSeriesType::valid() const {
        if (auto* input = dynamic_cast<const PyTimeSeriesInput*>(this)) {
            if (input_kind_requires_bound_validity(input->input_view()) &&
                !input->input_view().is_bound()) {
                return nb::bool_(false);
            }
            return nb::bool_(input->input_view().as_ts_view().valid());
        }
        if (auto* output = dynamic_cast<const PyTimeSeriesOutput*>(this)) {
            return nb::bool_(output->output_view().as_ts_view().valid());
        }
        return nb::bool_(view().valid());
    }

    nb::bool_ PyTimeSeriesType::all_valid() const {
        if (auto* input = dynamic_cast<const PyTimeSeriesInput*>(this)) {
            if (input_kind_requires_bound_validity(input->input_view()) &&
                !input->input_view().is_bound()) {
                return nb::bool_(false);
            }
            return nb::bool_(input->input_view().as_ts_view().all_valid());
        }
        if (auto* output = dynamic_cast<const PyTimeSeriesOutput*>(this)) {
            return nb::bool_(output->output_view().as_ts_view().all_valid());
        }
        return nb::bool_(view().all_valid());
    }

    nb::bool_ PyTimeSeriesType::is_reference() const {
        const auto *meta = view().ts_meta();
        return nb::bool_(meta != nullptr && meta->kind == TSKind::REF);
    }

    nb::bool_ PyTimeSeriesType::modified() const {
        if (auto* input = dynamic_cast<const PyTimeSeriesInput*>(this)) {
            if (input_kind_requires_bound_validity(input->input_view()) &&
                !input->input_view().is_bound()) {
                return nb::bool_(false);
            }
            return nb::bool_(input->input_view().as_ts_view().modified());
        }
        if (auto* output = dynamic_cast<const PyTimeSeriesOutput*>(this)) {
            return nb::bool_(output->output_view().as_ts_view().modified());
        }
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
        : PyTimeSeriesType(view.as_ts_view())
        , output_view_(std::move(view)) {}

    nb::object PyTimeSeriesOutput::parent_output() const {
        const ShortPath &path = output_view().short_path();
        if (!has_parent(path) || path.node == nullptr) {
            return nb::none();
        }

        TSOutputView root = path.node->output(output_view().current_time());
        if (!root) {
            return nb::none();
        }

        TSView parent = root.as_ts_view();
        for (size_t i = 0; i + 1 < path.indices.size(); ++i) {
            parent = parent.child_at(path.indices[i]);
        }
        return wrap_output_view(TSOutputView(nullptr, std::move(parent)));
    }

    nb::bool_ PyTimeSeriesOutput::has_parent_output() const {
        return nb::bool_(has_parent(output_view().short_path()));
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
        output_view().copy_from_output(output.output_view());
    }

    void PyTimeSeriesOutput::copy_from_input(const PyTimeSeriesInput &input) {
        output_view().copy_from_input(input.input_view());
    }

    void PyTimeSeriesOutput::clear() { output_view().invalidate(); }

    void PyTimeSeriesOutput::invalidate() { output_view().invalidate(); }

    bool PyTimeSeriesOutput::can_apply_result(nb::object value) {
        (void)value;
        return static_cast<bool>(output_view());
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
        : PyTimeSeriesType(view.as_ts_view())
        , input_view_(std::move(view)) {}

    nb::object PyTimeSeriesInput::parent_input() const {
        const ShortPath &path = input_view().short_path();
        if (!has_parent(path) || path.node == nullptr) {
            return nb::none();
        }

        TSInputView root = path.node->input(input_view().current_time());
        if (!root) {
            return nb::none();
        }

        TSView parent = root.as_ts_view();
        for (size_t i = 0; i + 1 < path.indices.size(); ++i) {
            parent = parent.child_at(path.indices[i]);
        }
        return wrap_input_view(TSInputView(nullptr, std::move(parent)));
    }

    nb::bool_ PyTimeSeriesInput::has_parent_input() const {
        return nb::bool_(has_parent(input_view().short_path()));
    }

    nb::bool_ PyTimeSeriesInput::active() const { return nb::bool_(input_view().active()); }

    void PyTimeSeriesInput::make_active() { input_view().make_active(); }

    void PyTimeSeriesInput::make_passive() { input_view().make_passive(); }

    nb::bool_ PyTimeSeriesInput::bound() const { return nb::bool_(input_view().is_bound()); }

    nb::bool_ PyTimeSeriesInput::has_peer() const {
        // Input-level default peer semantics match Python: bound implies peer.
        return nb::bool_(input_view().is_bound());
    }

    nb::object PyTimeSeriesInput::output() const {
        auto target = resolve_bound_target_view_data(input_view());
        if (!target.has_value()) {
            return nb::none();
        }
        return wrap_output_view(TSOutputView(nullptr, TSView(*target, input_view().current_time())));
    }

    nb::bool_ PyTimeSeriesInput::has_output() const { return nb::bool_(input_view().is_bound()); }

    nb::bool_ PyTimeSeriesInput::bind_output(nb::object output_) {
        if (!nb::isinstance<PyTimeSeriesOutput>(output_)) {
            throw std::runtime_error("bind_output requires a TimeSeriesOutput instance");
        }

        auto &py_output = nb::cast<PyTimeSeriesOutput &>(output_);
        input_view().bind(py_output.output_view());
        return nb::bool_(input_view().is_bound());
    }

    void PyTimeSeriesInput::un_bind_output(bool unbind_refs) {
        (void)unbind_refs;
        input_view().unbind();
    }

    nb::object PyTimeSeriesInput::reference_output() const {
        const auto *meta = input_view().ts_meta();
        if (meta == nullptr || meta->kind != TSKind::REF) {
            return nb::none();
        }
        return output();
    }

    nb::object PyTimeSeriesInput::get_input(size_t index) const {
        if (auto list = input_view().try_as_list(); list.has_value()) {
            return wrap_input_view(list->at(index));
        }
        if (auto bundle = input_view().try_as_bundle(); bundle.has_value()) {
            return wrap_input_view(bundle->at(index));
        }
        throw nb::index_error();
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
