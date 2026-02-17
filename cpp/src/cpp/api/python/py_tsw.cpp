#include <hgraph/api/python/py_tsw.h>
#include <hgraph/types/value/queue_ops.h>

#include <vector>

namespace hgraph
{
    // ========== PyTimeSeriesFixedWindowOutput ==========

    PyTimeSeriesFixedWindowOutput::PyTimeSeriesFixedWindowOutput(TSOutputView view)
        : PyTimeSeriesOutput(std::move(view)) {}

    nb::object PyTimeSeriesFixedWindowOutput::value_times() const {
        auto wv = output_view().as_window();
        const engine_time_t *times = wv.value_times();
        size_t count = wv.value_times_count();
        if (!times || count == 0) {
            return nb::none();
        }
        std::vector<engine_time_t> result(times, times + count);
        return nb::cast(result);
    }

    engine_time_t PyTimeSeriesFixedWindowOutput::first_modified_time() const {
        return output_view().as_window().first_modified_time();
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::size() const {
        return nb::int_(output_view().as_window().size());
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::min_size() const {
        return nb::int_(output_view().as_window().min_size());
    }

    nb::bool_ PyTimeSeriesFixedWindowOutput::has_removed_value() const {
        return nb::bool_(output_view().as_window().has_removed_value());
    }

    nb::object PyTimeSeriesFixedWindowOutput::removed_value() const {
        auto wv = output_view().as_window();
        if (!wv.has_removed_value()) {
            return nb::none();
        }
        value::View rv = wv.as_ts_view().as_window().removed_value();
        return rv.valid() ? rv.to_python() : nb::none();
    }

    nb::int_ PyTimeSeriesFixedWindowOutput::len() const {
        return nb::int_(output_view().as_window().length());
    }

    // ========== PyTimeSeriesTimeWindowOutput ==========

    PyTimeSeriesTimeWindowOutput::PyTimeSeriesTimeWindowOutput(TSOutputView view)
        : PyTimeSeriesOutput(std::move(view)) {}

    nb::object PyTimeSeriesTimeWindowOutput::value_times() const {
        auto wv = output_view().as_window();
        const engine_time_t *times = wv.value_times();
        size_t count = wv.value_times_count();
        if (!times || count == 0) {
            return nb::none();
        }
        std::vector<engine_time_t> result(times, times + count);
        return nb::cast(result);
    }

    engine_time_t PyTimeSeriesTimeWindowOutput::first_modified_time() const {
        return output_view().as_window().first_modified_time();
    }

    nb::object PyTimeSeriesTimeWindowOutput::size() const {
        return nb::cast(engine_time_delta_t{static_cast<int64_t>(output_view().as_window().size())});
    }

    nb::object PyTimeSeriesTimeWindowOutput::min_size() const {
        return nb::cast(engine_time_delta_t{static_cast<int64_t>(output_view().as_window().min_size())});
    }

    nb::bool_ PyTimeSeriesTimeWindowOutput::has_removed_value() const {
        return nb::bool_(output_view().as_window().has_removed_value());
    }

    nb::object PyTimeSeriesTimeWindowOutput::removed_value() const {
        auto wv = output_view().as_window();
        if (!wv.has_removed_value()) {
            return nb::none();
        }

        value::View rv = wv.as_ts_view().as_window().removed_value();
        if (!rv.valid()) {
            return nb::none();
        }

        // Duration windows remove a queue of values.
        auto *queue = static_cast<const value::QueueStorage *>(rv.data());
        if (queue == nullptr || queue->size() == 0) {
            return nb::none();
        }

        const auto *rq_schema = rv.schema();
        const auto *elem_type = output_view().ts_meta() != nullptr ? output_view().ts_meta()->value_type : nullptr;
        if (rq_schema == nullptr || elem_type == nullptr) {
            return nb::none();
        }

        nb::tuple result = nb::steal<nb::tuple>(PyTuple_New(static_cast<Py_ssize_t>(queue->size())));
        for (size_t i = 0; i < queue->size(); ++i) {
            const void *elem = value::QueueOps::get_element_ptr_const(queue, i, rq_schema);
            nb::object py_elem = elem_type->ops().to_python(elem, elem_type);
            PyTuple_SET_ITEM(result.ptr(), static_cast<Py_ssize_t>(i), py_elem.release().ptr());
        }
        return result;
    }

    nb::int_ PyTimeSeriesTimeWindowOutput::len() const {
        return nb::int_(output_view().as_window().length());
    }

    // ========== PyTimeSeriesWindowInput ==========

    PyTimeSeriesWindowInput::PyTimeSeriesWindowInput(TSInputView view)
        : PyTimeSeriesInput(std::move(view)) {}

    nb::object PyTimeSeriesWindowInput::value_times() const {
        auto wv = input_view().as_window();
        const engine_time_t *times = wv.value_times();
        size_t count = wv.value_times_count();
        if (!times || count == 0) {
            return nb::none();
        }
        std::vector<engine_time_t> result(times, times + count);
        return nb::cast(result);
    }

    engine_time_t PyTimeSeriesWindowInput::first_modified_time() const {
        return input_view().as_window().first_modified_time();
    }

    nb::bool_ PyTimeSeriesWindowInput::has_removed_value() const {
        return nb::bool_(input_view().as_window().has_removed_value());
    }

    nb::object PyTimeSeriesWindowInput::removed_value() const {
        auto wv = input_view().as_window();
        if (!wv.has_removed_value()) {
            return nb::none();
        }

        value::View rv = wv.as_ts_view().as_window().removed_value();
        return rv.valid() ? rv.to_python() : nb::none();
    }

    nb::int_ PyTimeSeriesWindowInput::len() const {
        return nb::int_(input_view().as_window().length());
    }

    // ========== Nanobind Registration ==========

    void tsw_register_with_nanobind(nb::module_ &m) {
        // Fixed-size window output
        nb::class_<PyTimeSeriesFixedWindowOutput, PyTimeSeriesOutput>(m, "TimeSeriesFixedWindowOutput")
            .def_prop_ro("value_times", &PyTimeSeriesFixedWindowOutput::value_times)
            .def_prop_ro("first_modified_time", &PyTimeSeriesFixedWindowOutput::first_modified_time)
            .def_prop_ro("size", &PyTimeSeriesFixedWindowOutput::size)
            .def_prop_ro("min_size", &PyTimeSeriesFixedWindowOutput::min_size)
            .def_prop_ro("has_removed_value", &PyTimeSeriesFixedWindowOutput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesFixedWindowOutput::removed_value)
            .def("__len__", &PyTimeSeriesFixedWindowOutput::len);

        // Time-based window output
        nb::class_<PyTimeSeriesTimeWindowOutput, PyTimeSeriesOutput>(m, "TimeSeriesTimeWindowOutput")
            .def_prop_ro("value_times", &PyTimeSeriesTimeWindowOutput::value_times)
            .def_prop_ro("first_modified_time", &PyTimeSeriesTimeWindowOutput::first_modified_time)
            .def_prop_ro("size", &PyTimeSeriesTimeWindowOutput::size)
            .def_prop_ro("min_size", &PyTimeSeriesTimeWindowOutput::min_size)
            .def_prop_ro("has_removed_value", &PyTimeSeriesTimeWindowOutput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesTimeWindowOutput::removed_value)
            .def("__len__", &PyTimeSeriesTimeWindowOutput::len);

        // Window input (works with both fixed and time-based outputs)
        nb::class_<PyTimeSeriesWindowInput, PyTimeSeriesInput>(m, "TimeSeriesWindowInput")
            .def_prop_ro("value_times", &PyTimeSeriesWindowInput::value_times)
            .def_prop_ro("first_modified_time", &PyTimeSeriesWindowInput::first_modified_time)
            .def_prop_ro("has_removed_value", &PyTimeSeriesWindowInput::has_removed_value)
            .def_prop_ro("removed_value", &PyTimeSeriesWindowInput::removed_value)
            .def("__len__", &PyTimeSeriesWindowInput::len);
    }

}  // namespace hgraph
