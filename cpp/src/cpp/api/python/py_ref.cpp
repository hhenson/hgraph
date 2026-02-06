#include <hgraph/api/python/py_ref.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/ts_reference_ops.h>
#include <hgraph/types/node.h>

#include <utility>

namespace hgraph
{

    void ref_register_with_nanobind(nb::module_ &m) {
        // ============================================================
        // TSReference - Value-stack time-series reference
        // ============================================================

        // Note: PortType enum is registered in py_ts_input_output.cpp

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
            .def("to_fq", &TSReference::to_fq, "Convert to FQReference for serialization")
            // Factory method compatible with Python TimeSeriesReference.make() API
            // Returns TSReference (path-based) - the new system
            .def_static(
                "make",
                [](nb::object ts, nb::object from_items) -> TSReference {
                    // Case 1: No arguments - return empty reference
                    if (ts.is_none() && from_items.is_none()) {
                        return TSReference::empty();
                    }

                    // Case 2: from_items is provided - create NON_PEERED
                    if (!from_items.is_none()) {
                        std::vector<TSReference> items;
                        for (auto item : from_items) {
                            if (nb::isinstance<TSReference>(item)) {
                                items.push_back(nb::cast<TSReference>(item));
                            } else {
                                items.push_back(TSReference::empty());
                            }
                        }
                        return TSReference::non_peered(std::move(items));
                    }

                    // Case 3: ts is a TSReference directly - return it
                    if (nb::isinstance<TSReference>(ts)) {
                        return nb::cast<TSReference>(ts);
                    }

                    // Case 4: ts is an output - extract ShortPath
                    if (nb::isinstance<PyTimeSeriesOutput>(ts)) {
                        auto& py_output = nb::cast<PyTimeSeriesOutput&>(ts);
                        const ShortPath& path = py_output.output_view().short_path();
                        return TSReference::peered(path);
                    }

                    // Case 5: ts is a REF input - extract TSReference from its value
                    if (nb::isinstance<PyTimeSeriesReferenceInput>(ts)) {
                        auto& py_input = nb::cast<PyTimeSeriesReferenceInput&>(ts);
                        auto input_view = py_input.input_view();
                        if (input_view.valid()) {
                            // Get the TSReference value from the REF input
                            auto val_view = input_view.value();
                            return val_view.as<TSReference>();
                        }
                        throw std::runtime_error("TSReference.make: REF input is not valid");
                    }

                    // Case 6: ts is a regular input with peer - wrap the bound output
                    if (nb::isinstance<PyTimeSeriesInput>(ts)) {
                        auto& py_input = nb::cast<PyTimeSeriesInput&>(ts);
                        const auto& input_view = py_input.input_view();
                        if (input_view.is_bound()) {
                            TSOutput* bound_output = input_view.bound_output();
                            if (bound_output) {
                                ShortPath path = bound_output->root_path();
                                return TSReference::peered(std::move(path));
                            }
                        }
                        throw std::runtime_error("TSReference.make: Input is not bound");
                    }

                    // Fallback: empty reference
                    return TSReference::empty();
                },
                "ts"_a = nb::none(), "from_items"_a = nb::none(),
                "Create a TSReference from a time-series input/output or from items")
            // ABC-compatible properties to match Python TimeSeriesReference
            .def_prop_ro("is_valid", [](const TSReference& self) {
                return !self.is_empty();
            })
            .def_prop_ro("is_bound", [](const TSReference& self) {
                return self.is_peered();
            })
            .def_prop_ro("is_unbound", [](const TSReference& self) {
                return self.is_non_peered();
            })
            .def_prop_ro("items", [](const TSReference& self) -> nb::object {
                if (!self.is_non_peered()) {
                    return nb::none();
                }
                return nb::cast(self.items());
            })
            .def("__getitem__", [](const TSReference& self, size_t idx) {
                return self[idx];
            });

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
        auto v = fmt::format("TimeSeriesReferenceOutput[view]");
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

    nb::str PyTimeSeriesReferenceInput::to_string() const {
        return nb::str("TimeSeriesReferenceInput[view]");
    }

    nb::str PyTimeSeriesReferenceInput::to_repr() const { return to_string(); }

    void PyTimeSeriesReferenceInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesReferenceInput, PyTimeSeriesInput>(m, "TimeSeriesReferenceInput")
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
