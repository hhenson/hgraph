#include <hgraph/api/python/py_ref.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/ts_indexed.h>

#include <utility>

namespace hgraph
{

    void ref_register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesReference>(m, "TimeSeriesReference")
            .def("__str__", &TimeSeriesReference::to_string)
            .def("__repr__", &TimeSeriesReference::to_string)
            .def(nb::self == nb::self)
            .def(nb::self != nb::self)
            .def(
                "bind_input",
                [](TimeSeriesReference &self, PyTimeSeriesInput &ts_input) {
                    auto input_ = unwrap_input(ts_input);
                    if (input_) {
                        self.bind_input(*input_);
                    } else {
                        throw std::runtime_error("Cannot bind to null input");
                    }
                },
                "input_"_a)
            .def_prop_ro("has_output", &TimeSeriesReference::has_output)
            .def_prop_ro("is_empty", &TimeSeriesReference::is_empty)
            .def_prop_ro("is_bound", &TimeSeriesReference::is_bound)
            .def_prop_ro("is_unbound", &TimeSeriesReference::is_unbound)
            .def_prop_ro("is_valid", &TimeSeriesReference::is_valid)
            .def_prop_ro("output", [](TimeSeriesReference &self) { 
                auto out = self.output();
                if (!out) return nb::none();
                // Use the simpler wrap_output overload that gets control block from the output itself
                return wrap_output(out.get());
            })
            .def_prop_ro("items", &TimeSeriesReference::items)
            .def("__getitem__", &TimeSeriesReference::operator[])
            .def_static(
                "make",
                [](nb::object ts, nb::object items) -> TimeSeriesReference {
                    if (!ts.is_none()) {
                        if (nb::isinstance<PyTimeSeriesOutput>(ts)) {
                            // unwrap_output now returns shared_ptr directly
                            auto output_shared = unwrap_output(ts);
                            if (!output_shared) { throw std::runtime_error("Invalid output"); }
                            return TimeSeriesReference::make(output_shared);
                        }
                        if (nb::isinstance<PyTimeSeriesReferenceInput>(ts))
                            return unwrap_input_as<TimeSeriesReferenceInput>(ts)->value();
                        if (nb::isinstance<PyTimeSeriesInput>(ts)) {
                            auto ts_input = unwrap_input(ts);
                            if (ts_input->has_peer()) {
                                auto output = ts_input->output();
                                return TimeSeriesReference::make(output);
                            }
                            // Deal with list of inputs
                            std::vector<TimeSeriesReference> items_list;
                            auto ts_ndx = std::dynamic_pointer_cast<IndexedTimeSeriesInput>(ts_input);
                            if (!ts_ndx) { throw std::runtime_error("Expected IndexedTimeSeriesInput"); }
                            items_list.reserve(ts_ndx->size());
                            for (auto &ts_ptr : ts_ndx->values()) {
                                auto ref_input = std::dynamic_pointer_cast<TimeSeriesReferenceInput>(ts_ptr);
                                items_list.emplace_back(ref_input ? ref_input->value() : TimeSeriesReference::make());
                            }
                            return TimeSeriesReference::make(items_list);
                        }
                        // We may wish to raise an exception here?
                    } else if (!items.is_none()) {
                        auto items_list = nb::cast<std::vector<TimeSeriesReference>>(items);
                        return TimeSeriesReference::make(items_list);
                    }
                    return TimeSeriesReference::make();
                },
                "ts"_a = nb::none(), "from_items"_a = nb::none());

        // PyTS wrapper classes registration - left unregistered for now
        // These may require additional setup in wrapper_factory or elsewhere
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

    nb::str PyTimeSeriesReferenceOutput::to_string() const {
        auto impl_{impl()};
        auto v{fmt::format("TimeSeriesReferenceOutput@{:p}[{}]", static_cast<const void *>(impl_),
                           impl_->has_value() ? impl_->value().to_string() : "None")};
        return nb::str(v.c_str());
    }

    nb::str PyTimeSeriesReferenceOutput::to_repr() const {
        auto impl_{impl()};
        auto v{fmt::format("TimeSeriesReferenceOutput@{:p}[{}]", static_cast<const void *>(impl_),
                           impl_->has_value() ? impl_->value().to_string() : "None")};
        return nb::str(v.c_str());
    }

    void PyTimeSeriesReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesReferenceOutput, PyTimeSeriesOutput>(m, "TimeSeriesReferenceOutput")
            // .def("observe_reference", &TimeSeriesReferenceOutput::observe_reference, "input"_a,
            //      "Register an input as observing this reference value")
            // .def("stop_observing_reference", &TimeSeriesReferenceOutput::stop_observing_reference, "input"_a,
            //      "Unregister an input from observing this reference value")
            // .def_prop_ro(
            //     "reference_observers_count", [](const TimeSeriesReferenceOutput &self) {
            //         return self._reference_observers.size();
            //     },
            //     "Number of inputs observing this reference value")
            .def("__str__", &PyTimeSeriesReferenceOutput::to_string)
            .def("__repr__", &PyTimeSeriesReferenceOutput::to_repr);
    }

    TimeSeriesReferenceOutput *PyTimeSeriesReferenceOutput::impl() const { return static_cast_impl<TimeSeriesReferenceOutput>().get(); }

    nb::str PyTimeSeriesReferenceInput::to_string() const {
        auto       *impl_{impl()};
        std::string value_str = "None";
        if (impl_->has_value()) {
            value_str = impl_->raw_value()->to_string();
        } else if (impl_->has_output()) {
            value_str = "bound";
        } else if (!impl_->items().empty()) {
            value_str = fmt::format("{} items", impl_->items().size());
        }
        return nb::str(fmt::format("TimeSeriesReferenceInput@{:p}[{}]", static_cast<const void *>(impl_), value_str).c_str());
    }

    nb::str PyTimeSeriesReferenceInput::to_repr() const { return to_string(); }

    void PyTimeSeriesReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesReferenceInput, PyTimeSeriesInput>(m, "TimeSeriesReferenceInput")
            .def("__str__", &PyTimeSeriesReferenceInput::to_string)
            .def("__repr__", &PyTimeSeriesReferenceInput::to_repr);
    }

    TimeSeriesReferenceInput *PyTimeSeriesReferenceInput::impl() const { return static_cast_impl<TimeSeriesReferenceInput>().get(); }

    PyTimeSeriesValueReferenceInput::PyTimeSeriesValueReferenceInput(TimeSeriesValueReferenceInput *ref)
        : PyTimeSeriesReferenceInput(ref) {}

    PyTimeSeriesValueReferenceInput::PyTimeSeriesValueReferenceInput(TimeSeriesValueReferenceInput *ref,
                                                                     control_block_ptr              control_block)
        : PyTimeSeriesReferenceInput(ref, std::move(control_block)) {}

    void PyTimeSeriesValueReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesValueReferenceInput");
    }

    PyTimeSeriesListReferenceInput::PyTimeSeriesListReferenceInput(TimeSeriesListReferenceInput *ref)
        : PyTimeSeriesReferenceInput(ref) {}

    PyTimeSeriesListReferenceInput::PyTimeSeriesListReferenceInput(TimeSeriesListReferenceInput *ref,
                                                                   control_block_ptr              control_block)
        : PyTimeSeriesReferenceInput(ref, std::move(control_block)) {}

    size_t PyTimeSeriesListReferenceInput::size() const {
        return static_cast_impl<TimeSeriesListReferenceInput>()->size();
    }

    void PyTimeSeriesListReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesListReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesListReferenceInput")
            .def("__len__", &PyTimeSeriesListReferenceInput::size);
    }

    PyTimeSeriesBundleReferenceInput::PyTimeSeriesBundleReferenceInput(TimeSeriesBundleReferenceInput *ref)
        : PyTimeSeriesReferenceInput(ref) {}

    PyTimeSeriesBundleReferenceInput::PyTimeSeriesBundleReferenceInput(TimeSeriesBundleReferenceInput *ref,
                                                                       control_block_ptr                control_block)
        : PyTimeSeriesReferenceInput(ref, std::move(control_block)) {}

    nb::int_ PyTimeSeriesBundleReferenceInput::size() const {
        return nb::int_(static_cast_impl<TimeSeriesBundleReferenceInput>()->size());
    }

    void PyTimeSeriesBundleReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesBundleReferenceInput")
            .def("__len__", &PyTimeSeriesBundleReferenceInput::size);
    }

    PyTimeSeriesDictReferenceInput::PyTimeSeriesDictReferenceInput(TimeSeriesDictReferenceInput *ref)
        : PyTimeSeriesReferenceInput(ref) {}

    PyTimeSeriesDictReferenceInput::PyTimeSeriesDictReferenceInput(TimeSeriesDictReferenceInput *ref,
                                                                   control_block_ptr              control_block)
        : PyTimeSeriesReferenceInput(ref, std::move(control_block)) {}

    void PyTimeSeriesDictReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesDictReferenceInput");
    }

    PyTimeSeriesSetReferenceInput::PyTimeSeriesSetReferenceInput(TimeSeriesSetReferenceInput *ref)
        : PyTimeSeriesReferenceInput(ref) {}

    PyTimeSeriesSetReferenceInput::PyTimeSeriesSetReferenceInput(TimeSeriesSetReferenceInput *ref,
                                                                 control_block_ptr            control_block)
        : PyTimeSeriesReferenceInput(ref, std::move(control_block)) {}

    void PyTimeSeriesSetReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesSetReferenceInput");
    }

    PyTimeSeriesWindowReferenceInput::PyTimeSeriesWindowReferenceInput(TimeSeriesWindowReferenceInput *ref)
        : PyTimeSeriesReferenceInput(ref) {}

    PyTimeSeriesWindowReferenceInput::PyTimeSeriesWindowReferenceInput(TimeSeriesWindowReferenceInput *ref,
                                                                       control_block_ptr                control_block)
        : PyTimeSeriesReferenceInput(ref, std::move(control_block)) {}

    void PyTimeSeriesWindowReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesWindowReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesWindowReferenceInput");
    }

    PyTimeSeriesValueReferenceOutput::PyTimeSeriesValueReferenceOutput(TimeSeriesValueReferenceOutput *ref)
        : PyTimeSeriesReferenceOutput(ref) {}

    PyTimeSeriesValueReferenceOutput::PyTimeSeriesValueReferenceOutput(TimeSeriesValueReferenceOutput *ref,
                                                                       control_block_ptr                 control_block)
        : PyTimeSeriesReferenceOutput(ref, std::move(control_block)) {}

    void PyTimeSeriesValueReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesValueReferenceOutput");
    }

    PyTimeSeriesListReferenceOutput::PyTimeSeriesListReferenceOutput(TimeSeriesListReferenceOutput *ref)
        : PyTimeSeriesReferenceOutput(ref) {}

    PyTimeSeriesListReferenceOutput::PyTimeSeriesListReferenceOutput(TimeSeriesListReferenceOutput *ref,
                                                                     control_block_ptr                control_block)
        : PyTimeSeriesReferenceOutput(ref, std::move(control_block)) {}

    nb::int_ PyTimeSeriesListReferenceOutput::size() const {
        return nb::int_(static_cast_impl<TimeSeriesListReferenceOutput>()->size());
    }

    void PyTimeSeriesListReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesListReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesListReferenceOutput")
            .def("__len__", &PyTimeSeriesListReferenceOutput::size);
    }

    PyTimeSeriesBundleReferenceOutput::PyTimeSeriesBundleReferenceOutput(TimeSeriesBundleReferenceOutput *ref)
        : PyTimeSeriesReferenceOutput(ref) {}

    PyTimeSeriesBundleReferenceOutput::PyTimeSeriesBundleReferenceOutput(TimeSeriesBundleReferenceOutput *ref,
                                                                         control_block_ptr                  control_block)
        : PyTimeSeriesReferenceOutput(ref, std::move(control_block)) {}

    nb::int_ PyTimeSeriesBundleReferenceOutput::size() const {
        return nb::int_(static_cast_impl<TimeSeriesBundleReferenceOutput>()->size());
    }

    void PyTimeSeriesBundleReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesBundleReferenceOutput")
            .def("__len__", &PyTimeSeriesBundleReferenceOutput::size);
    }

    PyTimeSeriesDictReferenceOutput::PyTimeSeriesDictReferenceOutput(TimeSeriesDictReferenceOutput *ref)
        : PyTimeSeriesReferenceOutput(ref) {}

    PyTimeSeriesDictReferenceOutput::PyTimeSeriesDictReferenceOutput(TimeSeriesDictReferenceOutput *ref,
                                                                     control_block_ptr                control_block)
        : PyTimeSeriesReferenceOutput(ref, std::move(control_block)) {}

    void PyTimeSeriesDictReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesDictReferenceOutput");
    }

    PyTimeSeriesSetReferenceOutput::PyTimeSeriesSetReferenceOutput(TimeSeriesSetReferenceOutput *ref)
        : PyTimeSeriesReferenceOutput(ref) {}

    PyTimeSeriesSetReferenceOutput::PyTimeSeriesSetReferenceOutput(TimeSeriesSetReferenceOutput *ref,
                                                                   control_block_ptr              control_block)
        : PyTimeSeriesReferenceOutput(ref, std::move(control_block)) {}

    void PyTimeSeriesSetReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesSetReferenceOutput");
    }

    PyTimeSeriesWindowReferenceOutput::PyTimeSeriesWindowReferenceOutput(TimeSeriesWindowReferenceOutput *ref)
        : PyTimeSeriesReferenceOutput(ref) {}

    PyTimeSeriesWindowReferenceOutput::PyTimeSeriesWindowReferenceOutput(TimeSeriesWindowReferenceOutput *ref,
                                                                         control_block_ptr                 control_block)
        : PyTimeSeriesReferenceOutput(ref, std::move(control_block)) {}

    void PyTimeSeriesWindowReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesWindowReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesWindowReferenceOutput");
    }

}  // namespace hgraph