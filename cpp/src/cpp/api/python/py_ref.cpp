#include <hgraph/api/python/py_ref.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_view.h>

#include <fmt/format.h>

namespace hgraph
{
namespace
{
    engine_time_t resolve_bound_view_current_time(const ViewData& vd) {
        node_ptr owner = vd.path.node;
        if (owner == nullptr) {
            return MIN_DT;
        }

        if (const engine_time_t* et = owner->cached_evaluation_time_ptr(); et != nullptr) {
            if (*et != MIN_DT) {
                return *et;
            }
        }

        graph_ptr g = owner->graph();
        if (g == nullptr) {
            return MIN_DT;
        }

        const engine_time_t graph_time = g->evaluation_time();
        if (graph_time != MIN_DT) {
            return graph_time;
        }

        if (auto api = g->evaluation_engine_api(); api != nullptr) {
            return api->start_time();
        }
        return MIN_DT;
    }

    std::optional<ViewData> resolve_bound_target_view(const TSInputView &input_view) {
        const ViewData &source = input_view.as_ts_view().view_data();
        const TSMeta* meta = input_view.ts_meta();
        ViewData target{};
        if (meta != nullptr && meta->kind == TSKind::REF) {
            if (!resolve_direct_bound_view_data(source, target)) {
                return std::nullopt;
            }
            return target;
        }
        if (!resolve_bound_target_view_data(source, target)) {
            return std::nullopt;
        }

        return target;
    }

    TimeSeriesReference make_reference_from_input_view(const TSInputView& input_view) {
        const TSMeta* meta = input_view.ts_meta();
        if (meta != nullptr && meta->kind == TSKind::REF) {
            nb::object ref_obj = input_view.to_python();
            if (ref_obj.is_none()) {
                return TimeSeriesReference::make();
            }
            if (nb::isinstance<TimeSeriesReference>(ref_obj)) {
                return nb::cast<TimeSeriesReference>(ref_obj);
            }
            return TimeSeriesReference::make();
        }

        if (auto bound = resolve_bound_target_view(input_view); bound.has_value()) {
            return TimeSeriesReference::make(*bound);
        }

        if (auto list = input_view.try_as_list(); list.has_value()) {
            std::vector<TimeSeriesReference> refs;
            refs.reserve(list->count());
            for (size_t i = 0; i < list->count(); ++i) {
                refs.emplace_back(make_reference_from_input_view(list->at(i)));
            }
            return TimeSeriesReference::make(std::move(refs));
        }

        if (auto bundle = input_view.try_as_bundle(); bundle.has_value()) {
            std::vector<TimeSeriesReference> refs;
            refs.reserve(bundle->count());
            for (size_t i = 0; i < bundle->count(); ++i) {
                refs.emplace_back(make_reference_from_input_view(bundle->at(i)));
            }
            return TimeSeriesReference::make(std::move(refs));
        }

        return TimeSeriesReference::make();
    }
}  // namespace

    void ref_register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesReference>(m, "TimeSeriesReference")
            .def("__str__", &TimeSeriesReference::to_string)
            .def("__repr__", &TimeSeriesReference::to_string)
            .def(
                "__eq__",
                [](const TimeSeriesReference &self, nb::object other) {
                    if (other.is_none()) {
                        return false;
                    }
                    return nb::isinstance<TimeSeriesReference>(other) && self == nb::cast<TimeSeriesReference>(other);
                },
                nb::arg("other"), nb::is_operator())
            .def(
                "bind_input",
                [](TimeSeriesReference &self, PyTimeSeriesInput &ts_input) { self.bind_input(ts_input.input_view()); },
                "input_"_a)
            .def(
                "bind_input",
                [](TimeSeriesReference &self, TSInputView &ts_input) { self.bind_input(ts_input); },
                "input_"_a)
            .def_prop_ro("has_output", &TimeSeriesReference::has_output)
            .def_prop_ro("is_empty", &TimeSeriesReference::is_empty)
            .def_prop_ro("is_bound", &TimeSeriesReference::is_bound)
            .def_prop_ro("is_unbound", &TimeSeriesReference::is_unbound)
            .def_prop_ro("is_valid", &TimeSeriesReference::is_valid)
            .def_prop_ro("output", [](TimeSeriesReference &self) -> nb::object {
                if (const ViewData *bound = self.bound_view(); bound != nullptr) {
                    const engine_time_t current_time = resolve_bound_view_current_time(*bound);
                    TSView bound_view(*bound, current_time);
                    if (std::getenv("HGRAPH_DEBUG_REF_OUTPUT") != nullptr) {
                        const TSMeta* root_meta = bound->meta;
                        const TSMeta* cur_meta = bound_view.ts_meta();
                        const TSMeta* root_elem = (root_meta != nullptr && root_meta->kind == TSKind::TSD) ? root_meta->element_ts() : nullptr;
                        std::fprintf(stderr,
                                     "[ref_output] path=%s idx_count=%zu root_kind=%d root_elem_kind=%d cur_kind=%d current=%lld\n",
                                     bound->path.to_string().c_str(),
                                     bound->path.indices.size(),
                                     root_meta != nullptr ? static_cast<int>(root_meta->kind) : -1,
                                     root_elem != nullptr ? static_cast<int>(root_elem->kind) : -1,
                                     cur_meta != nullptr ? static_cast<int>(cur_meta->kind) : -1,
                                     static_cast<long long>(current_time.time_since_epoch().count()));
                    }
                    return wrap_output_view(TSOutputView(nullptr, std::move(bound_view)));
                }
                return nb::none();
            })
            .def_prop_ro("items", [](TimeSeriesReference &self) -> nb::object {
                return self.is_unbound() ? nb::cast(self.items()) : nb::none();
            })
            .def("__getitem__", [](TimeSeriesReference &self, size_t ndx) { return self[ndx]; })
            .def_static(
                "make",
                [](nb::object ts, nb::object items) -> TimeSeriesReference {
                    if (!ts.is_none()) {
                        if (nb::isinstance<PyTimeSeriesOutput>(ts)) {
                            auto &py_output = nb::cast<PyTimeSeriesOutput &>(ts);
                            return TimeSeriesReference::make(py_output.output_view().as_ts_view().view_data());
                        }
                        if (nb::isinstance<PyTimeSeriesReferenceInput>(ts)) {
                            auto &py_input = nb::cast<PyTimeSeriesReferenceInput &>(ts);
                            return nb::cast<TimeSeriesReference>(py_input.ref_value());
                        }
                        if (nb::isinstance<PyTimeSeriesInput>(ts)) {
                            auto &py_input = nb::cast<PyTimeSeriesInput &>(ts);
                            auto out_obj = py_input.output();
                            if (nb::isinstance<PyTimeSeriesOutput>(out_obj)) {
                                auto &out = nb::cast<PyTimeSeriesOutput &>(out_obj);
                                return TimeSeriesReference::make(out.output_view().as_ts_view().view_data());
                            }
                            return make_reference_from_input_view(py_input.input_view());
                        }
                    } else if (!items.is_none()) {
                        return TimeSeriesReference::make(nb::cast<std::vector<TimeSeriesReference>>(items));
                    }
                    return TimeSeriesReference::make();
                },
                "ts"_a = nb::none(), "from_items"_a = nb::none());

        // PyTS wrapper classes registration
        PyTimeSeriesReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesReferenceInput::register_with_nanobind(m);
        PyTimeSeriesValueReferenceInput::register_with_nanobind(m);
        PyTimeSeriesListReferenceInput::register_with_nanobind(m);
        PyTimeSeriesBundleReferenceInput::register_with_nanobind(m);
        PyTimeSeriesDictReferenceInput::register_with_nanobind(m);
        PyTimeSeriesSetReferenceInput::register_with_nanobind(m);
        PyTimeSeriesWindowReferenceInput::register_with_nanobind(m);
        PyTimeSeriesValueReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesListReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesBundleReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesDictReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesSetReferenceOutput::register_with_nanobind(m);
        PyTimeSeriesWindowReferenceOutput::register_with_nanobind(m);
    }

    // Base REF Output constructor
    PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput(TSOutputView view)
        : PyTimeSeriesOutput(std::move(view)) {}

    nb::str PyTimeSeriesReferenceOutput::to_string() const {
        auto value = output_view().to_python();
        auto v = fmt::format("TimeSeriesReferenceOutput@{:p}[{}]", static_cast<const void *>(&output_view()),
                             value.is_none() ? "None" : nb::cast<std::string>(nb::str(value)));
        return nb::str(v.c_str());
    }

    nb::str PyTimeSeriesReferenceOutput::to_repr() const {
        return to_string();
    }

    void PyTimeSeriesReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesReferenceOutput, PyTimeSeriesOutput>(m, "TimeSeriesReferenceOutput")
            .def("__str__", &PyTimeSeriesReferenceOutput::to_string)
            .def("__repr__", &PyTimeSeriesReferenceOutput::to_repr)
            .def("items", [](const PyTimeSeriesReferenceOutput& self) -> nb::object {
                nb::list out;
                const TSView& base_view = self.output_view().as_ts_view();
                const TSMeta* meta = base_view.ts_meta();
                while (meta != nullptr && meta->kind == TSKind::REF) {
                    meta = meta->element_ts();
                }
                if (meta == nullptr || meta->kind != TSKind::TSB || meta->fields() == nullptr) {
                    return out;
                }

                for (size_t i = 0; i < meta->field_count(); ++i) {
                    TSView child = base_view.child_at(i);
                    if (!child) {
                        continue;
                    }
                    const char* field_name = meta->fields()[i].name;
                    if (field_name == nullptr) {
                        continue;
                    }
                    out.append(nb::make_tuple(
                        nb::str(field_name),
                        wrap_output_view(TSOutputView(nullptr, std::move(child)))));
                }
                return out;
            })
            .def("__getitem__", [](const PyTimeSeriesReferenceOutput& self, const nb::object& key) -> nb::object {
                const TSView& base_view = self.output_view().as_ts_view();
                const TSMeta* meta = base_view.ts_meta();
                while (meta != nullptr && meta->kind == TSKind::REF) {
                    meta = meta->element_ts();
                }

                TSView child{};

                if (nb::isinstance<nb::int_>(key)) {
                    child = base_view.child_at(nb::cast<size_t>(key));
                } else if (nb::isinstance<nb::str>(key)) {
                    if (meta == nullptr || meta->kind != TSKind::TSB || meta->fields() == nullptr) {
                        throw nb::key_error();
                    }
                    const std::string field_name = nb::cast<std::string>(key);
                    bool found = false;
                    size_t field_index = 0;
                    for (size_t i = 0; i < meta->field_count(); ++i) {
                        const char* name = meta->fields()[i].name;
                        if (name != nullptr && field_name == name) {
                            found = true;
                            field_index = i;
                            break;
                        }
                    }
                    if (!found) {
                        throw nb::key_error();
                    }
                    child = base_view.child_at(field_index);
                } else if (meta != nullptr && meta->kind == TSKind::TSD && meta->key_type() != nullptr) {
                    value::Value key_value(meta->key_type());
                    key_value.emplace();
                    meta->key_type()->ops().from_python(key_value.data(), key, meta->key_type());
                    child = base_view.child_by_key(key_value.view());
                } else {
                    throw nb::key_error();
                }

                if (!child) {
                    throw nb::key_error();
                }
                return wrap_output_view(TSOutputView(nullptr, std::move(child)));
            }, "key"_a, nb::keep_alive<0, 1>());
    }

    // Base REF Input constructor
    PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput(TSInputView view)
        : PyTimeSeriesInput(std::move(view)) {}

    nb::object PyTimeSeriesReferenceInput::ref_value() const {
        // For TS->REF (non-peered), value is a bound reference to the target TS output.
        // For REF->REF (peered), value is the bound REF output payload itself.
        if (auto target = resolve_bound_target_view(input_view()); target.has_value()) {
            TSView target_view(*target, input_view().current_time());
            const TSMeta* target_meta = target_view.ts_meta();
            if (target_meta != nullptr && target_meta->kind != TSKind::REF) {
                return nb::cast(TimeSeriesReference::make(*target));
            }

            if (target_meta != nullptr && target_meta->kind == TSKind::REF && target->ops != nullptr) {
                nb::object target_value = target->ops->to_python(*target);
                if (target_value.is_none()) {
                    return nb::cast(TimeSeriesReference::make());
                }
                if (nb::isinstance<TimeSeriesReference>(target_value)) {
                    return target_value;
                }
                throw std::runtime_error("TimeSeriesReferenceInput.value expected REF payload from bound REF output");
            }
        }

        // Non-peered REF[TSB]/REF[TSL] can be driven by child binds without a
        // root bind target. TSView::value() materializes the composite REF payload.
        value::View synthesized = input_view().as_ts_view().value();
        if (synthesized.valid()) {
            nb::object synthesized_obj = synthesized.to_python();
            if (nb::isinstance<TimeSeriesReference>(synthesized_obj)) {
                return synthesized_obj;
            }
        }

        nb::object value_obj = input_view().to_python();
        if (value_obj.is_none()) {
            return nb::cast(TimeSeriesReference::make());
        }
        if (!nb::isinstance<TimeSeriesReference>(value_obj)) {
            throw std::runtime_error("TimeSeriesReferenceInput.value expected a TimeSeriesReference payload");
        }
        return value_obj;
    }

    nb::str PyTimeSeriesReferenceInput::to_string() const {
        auto v = fmt::format("TimeSeriesReferenceInput@{:p}[{}]",
                             static_cast<const void *>(&input_view()),
                             input_view().valid() ? "valid" : "invalid");
        return nb::str(v.c_str());
    }

    nb::str PyTimeSeriesReferenceInput::to_repr() const { return to_string(); }

    void PyTimeSeriesReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesReferenceInput, PyTimeSeriesInput>(m, "TimeSeriesReferenceInput")
            .def_prop_ro("value", &PyTimeSeriesReferenceInput::ref_value)
            .def_prop_ro("delta_value", &PyTimeSeriesReferenceInput::ref_value)
            .def("__str__", &PyTimeSeriesReferenceInput::to_string)
            .def("__repr__", &PyTimeSeriesReferenceInput::to_repr);
    }

    // Specialized Reference Input classes - nanobind registration only
    void PyTimeSeriesValueReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesValueReferenceInput");
    }

    void PyTimeSeriesListReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesListReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesListReferenceInput");
    }

    void PyTimeSeriesBundleReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesBundleReferenceInput");
    }

    void PyTimeSeriesDictReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesDictReferenceInput");
    }

    void PyTimeSeriesSetReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesSetReferenceInput");
    }

    void PyTimeSeriesWindowReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesWindowReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesWindowReferenceInput");
    }

    // Specialized Reference Output classes - nanobind registration only
    void PyTimeSeriesValueReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesValueReferenceOutput");
    }

    void PyTimeSeriesListReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesListReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesListReferenceOutput");
    }

    void PyTimeSeriesBundleReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesBundleReferenceOutput");
    }

    void PyTimeSeriesDictReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesDictReferenceOutput");
    }

    void PyTimeSeriesSetReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesSetReferenceOutput");
    }

    void PyTimeSeriesWindowReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesWindowReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesWindowReferenceOutput");
    }

}  // namespace hgraph
