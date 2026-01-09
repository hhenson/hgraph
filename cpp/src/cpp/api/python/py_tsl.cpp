#include <hgraph/api/python/py_tsl.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <fmt/format.h>

namespace hgraph
{
    // ============================================================
    // Helper function for delta_value (shared by Input and Output)
    // ============================================================

    static nb::object tsl_compute_delta_value(const TSView& view) {
        TSLView list = view.as_list();
        Node* n = view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            throw std::runtime_error("delta_value requires node context with evaluation time");
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();
        nb::dict result;
        for (size_t i = 0; i < list.size(); ++i) {
            TSView elem = list.element(i);
            // Python: {i: ts.delta_value for i, ts in enumerate(self._ts_values) if ts.modified}
            if (elem.modified_at(eval_time)) {
                nb::object wrapped = wrap_input_view(elem);
                nb::object delta = nb::getattr(wrapped, "delta_value");
                result[nb::int_(i)] = delta;
            }
        }
        return result;
    }

    // ============================================================
    // PyTimeSeriesListOutput
    // ============================================================

    PyTimeSeriesListOutput::PyTimeSeriesListOutput(TSMutableView view)
        : PyTimeSeriesOutput(view) {}

    nb::object PyTimeSeriesListOutput::iter() const {
        return nb::iter(values());
    }

    nb::object PyTimeSeriesListOutput::get_item(const nb::handle &key) const {
        if (nb::isinstance<nb::int_>(key)) {
            TSLView list = _view.as_list();
            size_t index = nb::cast<size_t>(key);
            TSView elem = list.element(index);
            return wrap_input_view(elem);
        }
        throw std::runtime_error("Invalid key type for TimeSeriesListOutput");
    }

    nb::object PyTimeSeriesListOutput::keys() const {
        TSLView list = _view.as_list();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            result.append(nb::int_(i));
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::values() const {
        TSLView list = _view.as_list();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            result.append(wrap_input_view(list.element(i)));
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::valid_keys() const {
        TSLView list = _view.as_list();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            TSView elem = list.element(i);
            if (elem.ts_valid()) {
                result.append(nb::int_(i));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::modified_keys() const {
        TSLView list = _view.as_list();
        Node* n = _view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            return nb::list();
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            TSView elem = list.element(i);
            if (elem.modified_at(eval_time)) {
                result.append(nb::int_(i));
            }
        }
        return result;
    }

    nb::int_ PyTimeSeriesListOutput::len() const {
        TSLView list = _view.as_list();
        return nb::int_(list.size());
    }

    nb::object PyTimeSeriesListOutput::items() const {
        TSLView list = _view.as_list();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            nb::tuple item = nb::make_tuple(nb::int_(i), wrap_input_view(list.element(i)));
            result.append(item);
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::valid_values() const {
        TSLView list = _view.as_list();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            TSView elem = list.element(i);
            if (elem.ts_valid()) {
                result.append(wrap_input_view(elem));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::valid_items() const {
        TSLView list = _view.as_list();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            TSView elem = list.element(i);
            if (elem.ts_valid()) {
                nb::tuple item = nb::make_tuple(nb::int_(i), wrap_input_view(elem));
                result.append(item);
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::modified_values() const {
        TSLView list = _view.as_list();
        Node* n = _view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            return nb::list();
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            TSView elem = list.element(i);
            if (elem.modified_at(eval_time)) {
                result.append(wrap_input_view(elem));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::modified_items() const {
        TSLView list = _view.as_list();
        Node* n = _view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            return nb::list();
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            TSView elem = list.element(i);
            if (elem.modified_at(eval_time)) {
                nb::tuple item = nb::make_tuple(nb::int_(i), wrap_input_view(elem));
                result.append(item);
            }
        }
        return result;
    }

    bool PyTimeSeriesListOutput::empty() const {
        TSLView list = _view.as_list();
        return list.size() == 0;
    }

    nb::object PyTimeSeriesListOutput::delta_value() const {
        return tsl_compute_delta_value(_view);
    }

    nb::str PyTimeSeriesListOutput::py_str() {
        TSLView list = _view.as_list();
        auto str = fmt::format("TimeSeriesListOutput@{:p}[size={}, valid={}]",
            static_cast<const void*>(_view.value_view().data()),
            list.size(),
            _view.ts_valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesListOutput::py_repr() {
        return py_str();
    }

    // ============================================================
    // PyTimeSeriesListInput
    // ============================================================

    PyTimeSeriesListInput::PyTimeSeriesListInput(TSView view)
        : PyTimeSeriesInput(view) {}

    nb::object PyTimeSeriesListInput::iter() const {
        return nb::iter(values());
    }

    nb::object PyTimeSeriesListInput::get_item(const nb::handle &key) const {
        if (nb::isinstance<nb::int_>(key)) {
            TSLView list = _view.as_list();
            size_t index = nb::cast<size_t>(key);
            TSView elem = list.element(index);
            return wrap_input_view(elem);
        }
        throw std::runtime_error("Invalid key type for TimeSeriesListInput");
    }

    nb::object PyTimeSeriesListInput::keys() const {
        TSLView list = _view.as_list();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            result.append(nb::int_(i));
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::values() const {
        TSLView list = _view.as_list();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            result.append(wrap_input_view(list.element(i)));
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::valid_keys() const {
        TSLView list = _view.as_list();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            TSView elem = list.element(i);
            if (elem.ts_valid()) {
                result.append(nb::int_(i));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::modified_keys() const {
        TSLView list = _view.as_list();
        Node* n = _view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            return nb::list();
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            TSView elem = list.element(i);
            if (elem.modified_at(eval_time)) {
                result.append(nb::int_(i));
            }
        }
        return result;
    }

    nb::int_ PyTimeSeriesListInput::len() const {
        TSLView list = _view.as_list();
        return nb::int_(list.size());
    }

    nb::object PyTimeSeriesListInput::items() const {
        TSLView list = _view.as_list();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            nb::tuple item = nb::make_tuple(nb::int_(i), wrap_input_view(list.element(i)));
            result.append(item);
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::valid_values() const {
        TSLView list = _view.as_list();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            TSView elem = list.element(i);
            if (elem.ts_valid()) {
                result.append(wrap_input_view(elem));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::valid_items() const {
        TSLView list = _view.as_list();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            TSView elem = list.element(i);
            if (elem.ts_valid()) {
                nb::tuple item = nb::make_tuple(nb::int_(i), wrap_input_view(elem));
                result.append(item);
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::modified_values() const {
        TSLView list = _view.as_list();
        Node* n = _view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            return nb::list();
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            TSView elem = list.element(i);
            if (elem.modified_at(eval_time)) {
                result.append(wrap_input_view(elem));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::modified_items() const {
        TSLView list = _view.as_list();
        Node* n = _view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            return nb::list();
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();
        nb::list result;
        for (size_t i = 0; i < list.size(); ++i) {
            TSView elem = list.element(i);
            if (elem.modified_at(eval_time)) {
                nb::tuple item = nb::make_tuple(nb::int_(i), wrap_input_view(elem));
                result.append(item);
            }
        }
        return result;
    }

    bool PyTimeSeriesListInput::empty() const {
        TSLView list = _view.as_list();
        return list.size() == 0;
    }

    nb::object PyTimeSeriesListInput::delta_value() const {
        return tsl_compute_delta_value(_view);
    }

    nb::str PyTimeSeriesListInput::py_str() {
        TSLView list = _view.as_list();
        auto str = fmt::format("TimeSeriesListInput@{:p}[size={}, valid={}]",
            static_cast<const void*>(_view.value_view().data()),
            list.size(),
            _view.ts_valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesListInput::py_repr() {
        return py_str();
    }

    // ============================================================
    // Registration
    // ============================================================

    void tsl_register_with_nanobind(nb::module_ &m) {
        // Register TimeSeriesListOutput
        nb::class_<PyTimeSeriesListOutput, PyTimeSeriesOutput>(m, "TimeSeriesListOutput")
            .def("__getitem__", &PyTimeSeriesListOutput::get_item)
            .def("__iter__", &PyTimeSeriesListOutput::iter)
            .def("__len__", &PyTimeSeriesListOutput::len)
            .def_prop_ro("empty", &PyTimeSeriesListOutput::empty)
            .def_prop_ro("delta_value", &PyTimeSeriesListOutput::delta_value)
            .def("values", &PyTimeSeriesListOutput::values)
            .def("valid_values", &PyTimeSeriesListOutput::valid_values)
            .def("modified_values", &PyTimeSeriesListOutput::modified_values)
            .def("keys", &PyTimeSeriesListOutput::keys)
            .def("items", &PyTimeSeriesListOutput::items)
            .def("valid_keys", &PyTimeSeriesListOutput::valid_keys)
            .def("valid_items", &PyTimeSeriesListOutput::valid_items)
            .def("modified_keys", &PyTimeSeriesListOutput::modified_keys)
            .def("modified_items", &PyTimeSeriesListOutput::modified_items)
            .def("__str__", &PyTimeSeriesListOutput::py_str)
            .def("__repr__", &PyTimeSeriesListOutput::py_repr);

        // Register TimeSeriesListInput
        nb::class_<PyTimeSeriesListInput, PyTimeSeriesInput>(m, "TimeSeriesListInput")
            .def("__getitem__", &PyTimeSeriesListInput::get_item)
            .def("__iter__", &PyTimeSeriesListInput::iter)
            .def("__len__", &PyTimeSeriesListInput::len)
            .def_prop_ro("empty", &PyTimeSeriesListInput::empty)
            .def_prop_ro("delta_value", &PyTimeSeriesListInput::delta_value)
            .def("values", &PyTimeSeriesListInput::values)
            .def("valid_values", &PyTimeSeriesListInput::valid_values)
            .def("modified_values", &PyTimeSeriesListInput::modified_values)
            .def("keys", &PyTimeSeriesListInput::keys)
            .def("items", &PyTimeSeriesListInput::items)
            .def("valid_keys", &PyTimeSeriesListInput::valid_keys)
            .def("valid_items", &PyTimeSeriesListInput::valid_items)
            .def("modified_keys", &PyTimeSeriesListInput::modified_keys)
            .def("modified_items", &PyTimeSeriesListInput::modified_items)
            .def("__str__", &PyTimeSeriesListInput::py_str)
            .def("__repr__", &PyTimeSeriesListInput::py_repr);
    }

}  // namespace hgraph
