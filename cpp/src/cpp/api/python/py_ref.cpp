#include <hgraph/api/python/py_ref.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/link_target.h>
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

        if (auto api = g->evaluation_engine_api(); api != nullptr) {
            return api->start_time();
        }

        return g->evaluation_time();
    }

    std::optional<ViewData> resolve_non_ref_target(const TSInputView &input_view) {
        const ViewData &vd = input_view.as_ts_view().view_data();
        if (!vd.uses_link_target || vd.link_data == nullptr) {
            return std::nullopt;
        }

        const auto *lt = static_cast<const LinkTarget *>(vd.link_data);
        if (lt == nullptr || !lt->is_linked || lt->meta == nullptr || lt->meta->kind == TSKind::REF) {
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
                    return wrap_output_view(TSOutputView(nullptr, TSView(*bound, resolve_bound_view_current_time(*bound))));
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

                            if (auto list = py_input.input_view().try_as_list(); list.has_value()) {
                                std::vector<TimeSeriesReference> refs;
                                refs.reserve(list->count());
                                for (size_t i = 0; i < list->count(); ++i) {
                                    refs.emplace_back(TimeSeriesReference::make(list->at(i).as_ts_view().view_data()));
                                }
                                return TimeSeriesReference::make(std::move(refs));
                            }

                            if (auto bundle = py_input.input_view().try_as_bundle(); bundle.has_value()) {
                                std::vector<TimeSeriesReference> refs;
                                refs.reserve(bundle->count());
                                for (size_t i = 0; i < bundle->count(); ++i) {
                                    refs.emplace_back(TimeSeriesReference::make(bundle->at(i).as_ts_view().view_data()));
                                }
                                return TimeSeriesReference::make(std::move(refs));
                            }
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
            .def("__repr__", &PyTimeSeriesReferenceOutput::to_repr);
    }

    // Base REF Input constructor
    PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput(TSInputView view)
        : PyTimeSeriesInput(std::move(view)) {}

    nb::object PyTimeSeriesReferenceInput::ref_value() const {
        if (auto target = resolve_non_ref_target(input_view()); target.has_value()) {
            return nb::cast(TimeSeriesReference::make(*target));
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
