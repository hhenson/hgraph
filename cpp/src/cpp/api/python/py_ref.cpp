#include <hgraph/api/python/py_ref.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/ts_indexed.h>

#include <utility>

namespace hgraph
{

    void ref_register_with_nanobind(nb::module_ &m) {
        // ============================================================
        // TSReference - Value-stack time-series reference
        // ============================================================

        // PortType enum
        nb::enum_<PortType>(m, "PortType")
            .value("INPUT", PortType::INPUT)
            .value("OUTPUT", PortType::OUTPUT);

        // TSReference class
        nb::class_<TSReference>(m, "TSReference")
            .def(nb::init<>())  // Default constructor (EMPTY)
            .def(nb::init<const TSReference&>())  // Copy constructor
            .def_static("empty", &TSReference::empty, "Create an empty reference")
            .def("is_empty", &TSReference::is_empty)
            .def("is_peered", &TSReference::is_peered)
            .def("is_non_peered", &TSReference::is_non_peered)
            .def("has_output", &TSReference::has_output)
            .def_prop_ro("kind", &TSReference::kind)
            .def("size", &TSReference::size)
            .def("__str__", &TSReference::to_string)
            .def("__repr__", &TSReference::to_string)
            .def("__eq__", [](const TSReference& self, const TSReference& other) {
                return self == other;
            }, nb::is_operator())
            .def("to_fq", &TSReference::to_fq, "Convert to FQReference for serialization");

        // TSReference::Kind enum
        nb::enum_<TSReference::Kind>(m, "TSReferenceKind")
            .value("EMPTY", TSReference::Kind::EMPTY)
            .value("PEERED", TSReference::Kind::PEERED)
            .value("NON_PEERED", TSReference::Kind::NON_PEERED);

        // FQReference class
        nb::class_<FQReference>(m, "FQReference")
            .def(nb::init<>())  // Default constructor (EMPTY)
            .def_static("empty", &FQReference::empty, "Create an empty FQReference")
            .def_static("peered", &FQReference::peered,
                "node_id"_a, "port_type"_a, "indices"_a,
                "Create a peered FQReference")
            .def_static("non_peered", &FQReference::non_peered,
                "items"_a,
                "Create a non-peered FQReference")
            .def("is_empty", &FQReference::is_empty)
            .def("is_peered", &FQReference::is_peered)
            .def("is_non_peered", &FQReference::is_non_peered)
            .def("has_output", &FQReference::has_output)
            .def("is_valid", &FQReference::is_valid)
            .def_prop_ro("kind", [](const FQReference& self) { return self.kind; })
            .def_prop_ro("node_id", [](const FQReference& self) { return self.node_id; })
            .def_prop_ro("port_type", [](const FQReference& self) { return self.port_type; })
            .def_prop_ro("indices", [](const FQReference& self) { return self.indices; })
            .def_prop_ro("items", [](const FQReference& self) { return self.items; })
            .def("__str__", &FQReference::to_string)
            .def("__repr__", &FQReference::to_string)
            .def("__eq__", [](const FQReference& self, const FQReference& other) {
                return self == other;
            }, nb::is_operator());

        // ============================================================
        // TimeSeriesReference - Legacy output-pointer based reference
        // ============================================================

        nb::class_<TimeSeriesReference>(m, "TimeSeriesReference")
            .def("__str__", &TimeSeriesReference::to_string)
            .def("__repr__", &TimeSeriesReference::to_string)
            .def(
                "__eq__",
                [](const TimeSeriesReference &self, nb::object other) {
                    if (other.is_none()) { return false; }
                    if (nb::isinstance<TimeSeriesReference>(other)) {
                        return self == nb::cast<TimeSeriesReference>(other);
                    }
                    return false;
                },
                nb::arg("other"), nb::is_operator())
            .def(
                "bind_input",
                [](TimeSeriesReference &self, PyTimeSeriesInput &ts_input) {
                    auto input_{unwrap_input(ts_input)};
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
            .def_prop_ro("output", [](TimeSeriesReference &self) { return wrap_output(self.output()); })
            .def_prop_ro("items", &TimeSeriesReference::items)
            .def("__getitem__", &TimeSeriesReference::operator[])
            .def_static(
                "make",
                [](nb::object ts, nb::object items) -> TimeSeriesReference {
                    if (!ts.is_none()) {
                        if (nb::isinstance<PyTimeSeriesOutput>(ts))
                            return TimeSeriesReference::make(unwrap_output(ts));
                        if (nb::isinstance<PyTimeSeriesReferenceInput>(ts))
                            return unwrap_input_as<TimeSeriesReferenceInput>(ts)->value();
                        if (nb::isinstance<PyTimeSeriesInput>(ts)) {
                            auto ts_input = unwrap_input(ts);
                            if (ts_input->has_peer()) return TimeSeriesReference::make(ts_input->output());
                            // Deal with list of inputs
                            std::vector<TimeSeriesReference> items_list;
                            auto                             ts_ndx{std::dynamic_pointer_cast<IndexedTimeSeriesInput>(ts_input)};
                            items_list.reserve(ts_ndx->size());
                            for (auto &ts_ptr : ts_ndx->values()) {
                                auto ref_input{dynamic_cast<TimeSeriesReferenceInput *>(ts_ptr.get())};
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

    // Base REF Output constructors
    PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput(api_ptr impl)
        : PyTimeSeriesOutput(std::move(impl)) {}

    PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput(TSOutputView view)
        : PyTimeSeriesOutput(std::move(view)) {}

    nb::str PyTimeSeriesReferenceOutput::to_string() const {
        if (has_output_view()) {
            auto v = fmt::format("TimeSeriesReferenceOutput[view]");
            return nb::str(v.c_str());
        }
        auto impl_{impl()};
        auto v{fmt::format("TimeSeriesReferenceOutput@{:p}[{}]", static_cast<const void *>(impl_),
                           impl_->has_value() ? impl_->value().to_string() : "None")};
        return nb::str(v.c_str());
    }

    nb::str PyTimeSeriesReferenceOutput::to_repr() const {
        if (has_output_view()) {
            auto v = fmt::format("TimeSeriesReferenceOutput[view]");
            return nb::str(v.c_str());
        }
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

    TimeSeriesReferenceOutput *PyTimeSeriesReferenceOutput::impl() const { return static_cast_impl<TimeSeriesReferenceOutput>(); }

    // Base REF Input constructors
    PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput(api_ptr impl)
        : PyTimeSeriesInput(std::move(impl)) {}

    PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput(TSInputView view)
        : PyTimeSeriesInput(std::move(view)) {}

    nb::str PyTimeSeriesReferenceInput::to_string() const {
        if (has_input_view()) {
            return nb::str("TimeSeriesReferenceInput[view]");
        }
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

    TimeSeriesReferenceInput *PyTimeSeriesReferenceInput::impl() const { return static_cast_impl<TimeSeriesReferenceInput>(); }

    PyTimeSeriesValueReferenceInput::PyTimeSeriesValueReferenceInput(api_ptr impl)
        : PyTimeSeriesReferenceInput(std::move(impl)) {}

    void PyTimeSeriesValueReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesValueReferenceInput");
    }

    PyTimeSeriesListReferenceInput::PyTimeSeriesListReferenceInput(api_ptr impl)
        : PyTimeSeriesReferenceInput(std::move(impl)) {}

    size_t PyTimeSeriesListReferenceInput::size() const {
        return static_cast_impl<TimeSeriesListReferenceInput>()->size();
    }

    void PyTimeSeriesListReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesListReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesListReferenceInput")
            .def("__len__", &PyTimeSeriesListReferenceInput::size);
    }

    PyTimeSeriesBundleReferenceInput::PyTimeSeriesBundleReferenceInput(api_ptr impl)
        : PyTimeSeriesReferenceInput(std::move(impl)) {}


    nb::int_ PyTimeSeriesBundleReferenceInput::size() const {
        return nb::int_(static_cast_impl<TimeSeriesBundleReferenceInput>()->size());
    }

    void PyTimeSeriesBundleReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesBundleReferenceInput")
            .def("__len__", &PyTimeSeriesBundleReferenceInput::size);
    }

    PyTimeSeriesDictReferenceInput::PyTimeSeriesDictReferenceInput(api_ptr impl)
        : PyTimeSeriesReferenceInput(std::move(impl)) {}


    void PyTimeSeriesDictReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesDictReferenceInput");
    }

    PyTimeSeriesSetReferenceInput::PyTimeSeriesSetReferenceInput(api_ptr impl)
        : PyTimeSeriesReferenceInput(std::move(impl)) {}


    void PyTimeSeriesSetReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesSetReferenceInput");
    }

    PyTimeSeriesWindowReferenceInput::PyTimeSeriesWindowReferenceInput(api_ptr impl)
        : PyTimeSeriesReferenceInput(std::move(impl)) {}


    void PyTimeSeriesWindowReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesWindowReferenceInput, PyTimeSeriesReferenceInput>(m, "TimeSeriesWindowReferenceInput");
    }

    PyTimeSeriesValueReferenceOutput::PyTimeSeriesValueReferenceOutput(api_ptr impl)
        : PyTimeSeriesReferenceOutput(std::move(impl)) {}


    void PyTimeSeriesValueReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesValueReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesValueReferenceOutput");
    }

    PyTimeSeriesListReferenceOutput::PyTimeSeriesListReferenceOutput(api_ptr impl)
        : PyTimeSeriesReferenceOutput(std::move(impl)) {}


    nb::int_ PyTimeSeriesListReferenceOutput::size() const {
        return nb::int_(static_cast_impl<TimeSeriesListReferenceOutput>()->size());
    }

    void PyTimeSeriesListReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesListReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesListReferenceOutput")
            .def("__len__", &PyTimeSeriesListReferenceOutput::size);
    }

    PyTimeSeriesBundleReferenceOutput::PyTimeSeriesBundleReferenceOutput(api_ptr impl)
        : PyTimeSeriesReferenceOutput(std::move(impl)) {}


    nb::int_ PyTimeSeriesBundleReferenceOutput::size() const {
        return nb::int_(static_cast_impl<TimeSeriesBundleReferenceOutput>()->size());
    }

    void PyTimeSeriesBundleReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesBundleReferenceOutput")
            .def("__len__", &PyTimeSeriesBundleReferenceOutput::size);
    }

    PyTimeSeriesDictReferenceOutput::PyTimeSeriesDictReferenceOutput(api_ptr impl)
        : PyTimeSeriesReferenceOutput(std::move(impl)) {}


    void PyTimeSeriesDictReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesDictReferenceOutput");
    }

    PyTimeSeriesSetReferenceOutput::PyTimeSeriesSetReferenceOutput(api_ptr impl)
        : PyTimeSeriesReferenceOutput(std::move(impl)) {}


    void PyTimeSeriesSetReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesSetReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesSetReferenceOutput");
    }

    PyTimeSeriesWindowReferenceOutput::PyTimeSeriesWindowReferenceOutput(api_ptr impl)
        : PyTimeSeriesReferenceOutput(std::move(impl)) {}


    void PyTimeSeriesWindowReferenceOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesWindowReferenceOutput, PyTimeSeriesReferenceOutput>(m, "TimeSeriesWindowReferenceOutput");
    }

}  // namespace hgraph