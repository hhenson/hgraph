#include <hgraph/api/python/py_ref.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/ts_indexed.h>

namespace hgraph
{

    void register_time_series_reference_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesReference>(m, "TimeSeriesReference")
            .def("__str__", &TimeSeriesReference::to_string)
            .def("__repr__", &TimeSeriesReference::to_string)
            .def(
                "bind_input",
                [](TimeSeriesReference &self, PyTimeSeriesInput &ts_input) {
                    auto input_{unwrap_input(ts_input)};
                    if (input_ != nullptr) {
                        self.bind_input(*unwrap_input(ts_input));
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
            .def_static("make", nb::overload_cast<>(&TimeSeriesReference::make))
            .def_static("make", nb::overload_cast<time_series_output_ptr>(&TimeSeriesReference::make))
            .def_static("make", nb::overload_cast<std::vector<TimeSeriesReference>>(&TimeSeriesReference::make))
            .def_static(
                "make",
                [](nb::object ts, nb::object items) -> TimeSeriesReference {
                    if (!ts.is_none()) {
                        if (nb::isinstance<TimeSeriesOutput>(ts))
                            return TimeSeriesReference::make(nb::cast<TimeSeriesOutput::ptr>(ts));
                        if (nb::isinstance<TimeSeriesReferenceInput>(ts))
                            return nb::cast<TimeSeriesReferenceInput::ptr>(ts)->value();
                        if (nb::isinstance<TimeSeriesInput>(ts)) {
                            auto ts_input = nb::cast<TimeSeriesInput::ptr>(ts);
                            if (ts_input->has_peer()) return TimeSeriesReference::make(ts_input->output());
                            // Deal with list of inputs
                            std::vector<TimeSeriesReference> items_list;
                            auto                             ts_ndx{dynamic_cast<IndexedTimeSeriesInput *>(ts_input.get())};
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
    }
}  // namespace hgraph