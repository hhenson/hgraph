#include <hgraph/api/python/py_ref.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/ts_input.h>

namespace hgraph
{
    // TimeSeriesReferenceOutput - stub for Python compatibility
    // In the new system, reference outputs are just PyTimeSeriesOutput with REF metadata
    struct PyTimeSeriesReferenceOutput : PyTimeSeriesOutput {
        using PyTimeSeriesOutput::PyTimeSeriesOutput;
    };

    // TimeSeriesReferenceInput - stub for Python compatibility
    struct PyTimeSeriesReferenceInput : PyTimeSeriesInput {
        using PyTimeSeriesInput::PyTimeSeriesInput;
    };

    // Helper function to bind input to a TimeSeriesReference
    static void bind_input_impl(const TimeSeriesReference& ref, PyTimeSeriesInput& py_input) {
        if (ref.is_empty()) {
            // Empty reference - unbind the input
            py_input.un_bind_output(false);
            return;
        }

        if (ref.is_bound()) {
            // Resolve the reference to get the output view
            auto view = ref.resolve();
            if (!view.valid()) {
                // Invalid view - unbind
                py_input.un_bind_output(false);
                return;
            }

            // Check if this input can be bound
            if (!py_input.can_bind()) {
                return;  // Not a bindable input
            }

            // Use bind_output_view which handles both direct inputs and field wrappers
            py_input.bind_output_view(view);
            return;
        }

        // UNBOUND - this would be for composite references
        // For now, not implemented - would need to iterate items
    }

    // TimeSeriesReference Python bindings
    static void register_time_series_reference(nb::module_ &m) {
        nb::class_<TimeSeriesReference>(m, "TimeSeriesReference")
            .def("__str__", &TimeSeriesReference::to_string)
            .def("__repr__", &TimeSeriesReference::to_string)
            // Use std::optional with nb::arg().none() to allow None as input
            .def("__eq__", [](const TimeSeriesReference &self, std::optional<TimeSeriesReference> other) {
                if (!other.has_value()) {
                    return false;  // None comparison
                }
                return self == other.value();
            }, nb::arg("other").none())
            .def("__ne__", [](const TimeSeriesReference &self, std::optional<TimeSeriesReference> other) {
                if (!other.has_value()) {
                    return true;  // None comparison
                }
                return !(self == other.value());
            }, nb::arg("other").none())
            .def_prop_ro("has_output", &TimeSeriesReference::has_output)
            .def_prop_ro("is_empty", &TimeSeriesReference::is_empty)
            .def_prop_ro("is_bound", &TimeSeriesReference::is_bound)
            .def_prop_ro("is_unbound", &TimeSeriesReference::is_unbound)
            .def_prop_ro("is_valid", &TimeSeriesReference::is_valid)
            .def_prop_ro("output", [](TimeSeriesReference &self) -> nb::object {
                if (!self.has_output()) return nb::none();
                // Return the raw output pointer (navigation through views)
                auto* output = self.output_ptr();
                if (!output) return nb::none();
                // TODO: wrap output when Python wrappers are complete
                return nb::none();
            })
            .def_prop_ro("items", &TimeSeriesReference::items)
            .def("__getitem__", &TimeSeriesReference::operator[])
            .def("bind_input", [](const TimeSeriesReference& self, PyTimeSeriesInput& input) {
                bind_input_impl(self, input);
            }, "input_"_a)
            .def_static(
                "make",
                [](nb::object ts, nb::object items) -> TimeSeriesReference {
                    if (!ts.is_none()) {
                        // TODO: handle output/input creation when views are complete
                        throw std::runtime_error("TimeSeriesReference::make from output not yet implemented");
                    } else if (!items.is_none()) {
                        auto items_list = nb::cast<std::vector<TimeSeriesReference>>(items);
                        return TimeSeriesReference::make(items_list);
                    }
                    return TimeSeriesReference::make();
                },
                "ts"_a = nb::none(), "from_items"_a = nb::none());
    }

    void ref_register_with_nanobind(nb::module_ &m) {
        // Register the TimeSeriesReference value type
        register_time_series_reference(m);

        // Register stub classes for Python compatibility
        // These are subclasses of the base time-series types for isinstance() checks
        nb::class_<PyTimeSeriesReferenceOutput, PyTimeSeriesOutput>(m, "TimeSeriesReferenceOutput");
        nb::class_<PyTimeSeriesReferenceInput, PyTimeSeriesInput>(m, "TimeSeriesReferenceInput");
    }

}  // namespace hgraph
