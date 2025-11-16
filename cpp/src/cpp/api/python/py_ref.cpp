#include <hgraph/api/python/py_ref.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/ref.h>

namespace hgraph
{
    void PyTimeSeriesReference::bind_input(nb::object &ts_input) const {
        _impl->bind_input(*unwrap_input(ts_input));
    }

    void PyTimeSeriesReference::register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesReference>(m, "TimeSeriesReference")
            .def("__str__", &PyTimeSeriesReference::to_string)
            .def("__repr__", &PyTimeSeriesReference::to_string)
            .def("bind_input", &PyTimeSeriesReference::bind_input)
            .def_prop_ro("has_output", &PyTimeSeriesReference::has_output)
            .def_prop_ro("is_empty", &PyTimeSeriesReference::is_empty)
            .def_prop_ro("is_valid", &PyTimeSeriesReference::is_valid)
            .def_static(
                "make",
                [](nb::object ts, nb::object items) -> ptr {
                    if (not ts.is_none()) {
                        if (nb::isinstance<TimeSeriesOutput>(ts)) return make(nb::cast<TimeSeriesOutput::ptr>(ts));
                        if (nb::isinstance<PyTimeSeriesReferenceInput>(ts))
                            return nb::cast<PyTimeSeriesReferenceInput::ptr>(ts)->value();
                        if (nb::isinstance<TimeSeriesInput>(ts)) {
                            auto ts_input = nb::cast<TimeSeriesInput::ptr>(ts);
                            if (ts_input->has_peer()) return make(ts_input->output());
                            // Deal with list of inputs
                            std::vector<ptr> items_list;
                            auto             ts_ndx{dynamic_cast<IndexedTimeSeriesInput *>(ts_input.get())};
                            items_list.reserve(ts_ndx->size());
                            for (auto &ts_ptr : ts_ndx->values()) {
                                auto ref_input{dynamic_cast<PyTimeSeriesReferenceInput *>(ts_ptr.get())};
                                items_list.emplace_back(ref_input ? ref_input->value() : nullptr);
                            }
                            return make(items_list);
                        }
                        // We may wish to raise an exception here?
                    } else if (not items.is_none()) {
                        auto items_list = nb::cast<std::vector<ptr>>(items);
                        return make(items_list);
                    }
                    return make();
                },
                "ts"_a = nb::none(), "from_items"_a = nb::none());

        nb::class_<EmptyTimeSeriesReference, PyTimeSeriesReference>(m, "EmptyTimeSeriesReference");

        nb::class_<BoundTimeSeriesReference, PyTimeSeriesReference>(m, "BoundTimeSeriesReference")
            .def_prop_ro("output", &BoundTimeSeriesReference::output);

        nb::class_<UnBoundTimeSeriesReference, PyTimeSeriesReference>(m, "UnBoundTimeSeriesReference")
            .def_prop_ro("items", &UnBoundTimeSeriesReference::items)
            .def("__getitem__", [](UnBoundTimeSeriesReference &self, size_t index) -> PyTimeSeriesReference::ptr {
                const auto &items = self.items();
                if (index >= items.size()) { throw std::out_of_range("Index out of range"); }
                return items[index];
            });
    }

    PyTimeSeriesReference::PyTimeSeriesReference(api_ptr impl) : _impl{std::move(impl)} {}

}  // namespace hgraph