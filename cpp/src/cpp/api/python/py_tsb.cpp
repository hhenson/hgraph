#include <hgraph/api/python/py_tsb.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <fmt/format.h>

namespace hgraph
{
    // ============================================================
    // PyTimeSeriesBundleOutput
    // ============================================================

    PyTimeSeriesBundleOutput::PyTimeSeriesBundleOutput(TSMutableView view)
        : PyTimeSeriesOutput(view) {}

    nb::object PyTimeSeriesBundleOutput::iter() const {
        return nb::iter(keys());
    }

    nb::object PyTimeSeriesBundleOutput::get_item(const nb::handle &key) const {
        TSBView bundle = _view.as_bundle();
        if (nb::isinstance<nb::str>(key)) {
            std::string name = nb::cast<std::string>(key);
            if (bundle.has_field(name)) {
                TSView field = bundle.field(name);
                return wrap_input_view(field);
            }
            throw std::runtime_error(fmt::format("Field '{}' not found in bundle", name));
        }
        if (nb::isinstance<nb::int_>(key)) {
            size_t index = nb::cast<size_t>(key);
            TSView field = bundle.field(index);
            return wrap_input_view(field);
        }
        throw std::runtime_error("Invalid key type for TimeSeriesBundleOutput");
    }

    nb::object PyTimeSeriesBundleOutput::get_attr(const nb::handle &key) const {
        return get_item(key);
    }

    nb::bool_ PyTimeSeriesBundleOutput::contains(const nb::handle &key) const {
        if (nb::isinstance<nb::str>(key)) {
            TSBView bundle = _view.as_bundle();
            return nb::bool_(bundle.has_field(nb::cast<std::string>(key)));
        }
        return nb::bool_(false);
    }

    nb::object PyTimeSeriesBundleOutput::key_from_value(const nb::handle &value) const {
        // TODO: Implement key_from_value using view-based lookup
        return nb::none();
    }

    nb::object PyTimeSeriesBundleOutput::keys() const {
        TSBView bundle = _view.as_bundle();
        const TSBTypeMeta* meta = bundle.bundle_meta();
        nb::list result;
        for (size_t i = 0; i < meta->field_count(); ++i) {
            result.append(nb::str(meta->field(i).name.c_str()));
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::values() const {
        TSBView bundle = _view.as_bundle();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            result.append(wrap_input_view(bundle.field(i)));
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::valid_keys() const {
        TSBView bundle = _view.as_bundle();
        const TSBTypeMeta* meta = bundle.bundle_meta();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            TSView field = bundle.field(i);
            if (field.ts_valid()) {
                result.append(nb::str(meta->field(i).name.c_str()));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::valid_values() const {
        TSBView bundle = _view.as_bundle();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            TSView field = bundle.field(i);
            if (field.ts_valid()) {
                result.append(wrap_input_view(field));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::modified_keys() const {
        TSBView bundle = _view.as_bundle();
        const TSBTypeMeta* meta = bundle.bundle_meta();
        Node* n = _view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            return nb::list();
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            TSView field = bundle.field(i);
            if (field.modified_at(eval_time)) {
                result.append(nb::str(meta->field(i).name.c_str()));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::modified_values() const {
        TSBView bundle = _view.as_bundle();
        Node* n = _view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            return nb::list();
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            TSView field = bundle.field(i);
            if (field.modified_at(eval_time)) {
                result.append(wrap_input_view(field));
            }
        }
        return result;
    }

    nb::int_ PyTimeSeriesBundleOutput::len() const {
        TSBView bundle = _view.as_bundle();
        return nb::int_(bundle.field_count());
    }

    nb::bool_ PyTimeSeriesBundleOutput::empty() const {
        TSBView bundle = _view.as_bundle();
        return nb::bool_(bundle.field_count() == 0);
    }

    nb::object PyTimeSeriesBundleOutput::items() const {
        TSBView bundle = _view.as_bundle();
        const TSBTypeMeta* meta = bundle.bundle_meta();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            nb::tuple item = nb::make_tuple(
                nb::str(meta->field(i).name.c_str()),
                wrap_input_view(bundle.field(i))
            );
            result.append(item);
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::valid_items() const {
        TSBView bundle = _view.as_bundle();
        const TSBTypeMeta* meta = bundle.bundle_meta();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            TSView field = bundle.field(i);
            if (field.ts_valid()) {
                nb::tuple item = nb::make_tuple(
                    nb::str(meta->field(i).name.c_str()),
                    wrap_input_view(field)
                );
                result.append(item);
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::modified_items() const {
        TSBView bundle = _view.as_bundle();
        const TSBTypeMeta* meta = bundle.bundle_meta();
        Node* n = _view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            return nb::list();
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            TSView field = bundle.field(i);
            if (field.modified_at(eval_time)) {
                nb::tuple item = nb::make_tuple(
                    nb::str(meta->field(i).name.c_str()),
                    wrap_input_view(field)
                );
                result.append(item);
            }
        }
        return result;
    }

    nb::str PyTimeSeriesBundleOutput::py_str() {
        TSBView bundle = _view.as_bundle();
        auto str = fmt::format("TimeSeriesBundleOutput@{:p}[fields={}, valid={}]",
            static_cast<const void*>(_view.value_view().data()),
            bundle.field_count(),
            _view.ts_valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesBundleOutput::py_repr() {
        return py_str();
    }

    // ============================================================
    // PyTimeSeriesBundleInput
    // ============================================================

    PyTimeSeriesBundleInput::PyTimeSeriesBundleInput(TSView view)
        : PyTimeSeriesInput(view) {}

    nb::object PyTimeSeriesBundleInput::iter() const {
        return nb::iter(keys());
    }

    nb::object PyTimeSeriesBundleInput::get_item(const nb::handle &key) const {
        TSBView bundle = _view.as_bundle();
        if (nb::isinstance<nb::str>(key)) {
            std::string name = nb::cast<std::string>(key);
            if (bundle.has_field(name)) {
                TSView field = bundle.field(name);
                return wrap_input_view(field);
            }
            throw std::runtime_error(fmt::format("Field '{}' not found in bundle", name));
        }
        if (nb::isinstance<nb::int_>(key)) {
            size_t index = nb::cast<size_t>(key);
            TSView field = bundle.field(index);
            return wrap_input_view(field);
        }
        throw std::runtime_error("Invalid key type for TimeSeriesBundleInput");
    }

    nb::object PyTimeSeriesBundleInput::get_attr(const nb::handle &key) const {
        return get_item(key);
    }

    nb::bool_ PyTimeSeriesBundleInput::contains(const nb::handle &key) const {
        if (nb::isinstance<nb::str>(key)) {
            TSBView bundle = _view.as_bundle();
            return nb::bool_(bundle.has_field(nb::cast<std::string>(key)));
        }
        return nb::bool_(false);
    }

    nb::object PyTimeSeriesBundleInput::key_from_value(const nb::handle &value) const {
        // TODO: Implement key_from_value using view-based lookup
        return nb::none();
    }

    nb::object PyTimeSeriesBundleInput::keys() const {
        TSBView bundle = _view.as_bundle();
        const TSBTypeMeta* meta = bundle.bundle_meta();
        nb::list result;
        for (size_t i = 0; i < meta->field_count(); ++i) {
            result.append(nb::str(meta->field(i).name.c_str()));
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::values() const {
        TSBView bundle = _view.as_bundle();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            result.append(wrap_input_view(bundle.field(i)));
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::valid_keys() const {
        TSBView bundle = _view.as_bundle();
        const TSBTypeMeta* meta = bundle.bundle_meta();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            TSView field = bundle.field(i);
            if (field.ts_valid()) {
                result.append(nb::str(meta->field(i).name.c_str()));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::valid_values() const {
        TSBView bundle = _view.as_bundle();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            TSView field = bundle.field(i);
            if (field.ts_valid()) {
                result.append(wrap_input_view(field));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::modified_keys() const {
        TSBView bundle = _view.as_bundle();
        const TSBTypeMeta* meta = bundle.bundle_meta();
        Node* n = _view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            return nb::list();
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            TSView field = bundle.field(i);
            if (field.modified_at(eval_time)) {
                result.append(nb::str(meta->field(i).name.c_str()));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::modified_values() const {
        TSBView bundle = _view.as_bundle();
        Node* n = _view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            return nb::list();
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            TSView field = bundle.field(i);
            if (field.modified_at(eval_time)) {
                result.append(wrap_input_view(field));
            }
        }
        return result;
    }

    nb::int_ PyTimeSeriesBundleInput::len() const {
        TSBView bundle = _view.as_bundle();
        return nb::int_(bundle.field_count());
    }

    nb::bool_ PyTimeSeriesBundleInput::empty() const {
        TSBView bundle = _view.as_bundle();
        return nb::bool_(bundle.field_count() == 0);
    }

    nb::object PyTimeSeriesBundleInput::items() const {
        TSBView bundle = _view.as_bundle();
        const TSBTypeMeta* meta = bundle.bundle_meta();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            nb::tuple item = nb::make_tuple(
                nb::str(meta->field(i).name.c_str()),
                wrap_input_view(bundle.field(i))
            );
            result.append(item);
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::valid_items() const {
        TSBView bundle = _view.as_bundle();
        const TSBTypeMeta* meta = bundle.bundle_meta();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            TSView field = bundle.field(i);
            if (field.ts_valid()) {
                nb::tuple item = nb::make_tuple(
                    nb::str(meta->field(i).name.c_str()),
                    wrap_input_view(field)
                );
                result.append(item);
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::modified_items() const {
        TSBView bundle = _view.as_bundle();
        const TSBTypeMeta* meta = bundle.bundle_meta();
        Node* n = _view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            return nb::list();
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();
        nb::list result;
        for (size_t i = 0; i < bundle.field_count(); ++i) {
            TSView field = bundle.field(i);
            if (field.modified_at(eval_time)) {
                nb::tuple item = nb::make_tuple(
                    nb::str(meta->field(i).name.c_str()),
                    wrap_input_view(field)
                );
                result.append(item);
            }
        }
        return result;
    }

    nb::str PyTimeSeriesBundleInput::py_str() {
        TSBView bundle = _view.as_bundle();
        auto str = fmt::format("TimeSeriesBundleInput@{:p}[fields={}, valid={}]",
            static_cast<const void*>(_view.value_view().data()),
            bundle.field_count(),
            _view.ts_valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesBundleInput::py_repr() {
        return py_str();
    }

    // ============================================================
    // Registration
    // ============================================================

    void tsb_register_with_nanobind(nb::module_ &m) {
        // Register TimeSeriesBundleOutput
        nb::class_<PyTimeSeriesBundleOutput, PyTimeSeriesOutput>(m, "TimeSeriesBundleOutput")
            .def("__getitem__", &PyTimeSeriesBundleOutput::get_item)
            .def("__getattr__", &PyTimeSeriesBundleOutput::get_attr)
            .def("__iter__", &PyTimeSeriesBundleOutput::iter)
            .def("__len__", &PyTimeSeriesBundleOutput::len)
            .def("__contains__", &PyTimeSeriesBundleOutput::contains)
            .def("keys", &PyTimeSeriesBundleOutput::keys)
            .def("items", &PyTimeSeriesBundleOutput::items)
            .def("values", &PyTimeSeriesBundleOutput::values)
            .def("valid_keys", &PyTimeSeriesBundleOutput::valid_keys)
            .def("valid_values", &PyTimeSeriesBundleOutput::valid_values)
            .def("valid_items", &PyTimeSeriesBundleOutput::valid_items)
            .def("modified_keys", &PyTimeSeriesBundleOutput::modified_keys)
            .def("modified_values", &PyTimeSeriesBundleOutput::modified_values)
            .def("modified_items", &PyTimeSeriesBundleOutput::modified_items)
            .def("key_from_value", &PyTimeSeriesBundleOutput::key_from_value)
            .def_prop_ro("empty", &PyTimeSeriesBundleOutput::empty)
            .def_prop_ro("as_schema", [](nb::handle self) { return self; })
            .def("__str__", &PyTimeSeriesBundleOutput::py_str)
            .def("__repr__", &PyTimeSeriesBundleOutput::py_repr);

        // Register TimeSeriesBundleInput
        nb::class_<PyTimeSeriesBundleInput, PyTimeSeriesInput>(m, "TimeSeriesBundleInput")
            .def("__getitem__", &PyTimeSeriesBundleInput::get_item)
            .def("__getattr__", &PyTimeSeriesBundleInput::get_attr)
            .def("__iter__", &PyTimeSeriesBundleInput::iter)
            .def("__len__", &PyTimeSeriesBundleInput::len)
            .def("__contains__", &PyTimeSeriesBundleInput::contains)
            .def("keys", &PyTimeSeriesBundleInput::keys)
            .def("items", &PyTimeSeriesBundleInput::items)
            .def("values", &PyTimeSeriesBundleInput::values)
            .def("valid_keys", &PyTimeSeriesBundleInput::valid_keys)
            .def("valid_values", &PyTimeSeriesBundleInput::valid_values)
            .def("valid_items", &PyTimeSeriesBundleInput::valid_items)
            .def("modified_keys", &PyTimeSeriesBundleInput::modified_keys)
            .def("modified_values", &PyTimeSeriesBundleInput::modified_values)
            .def("modified_items", &PyTimeSeriesBundleInput::modified_items)
            .def("key_from_value", &PyTimeSeriesBundleInput::key_from_value)
            .def_prop_ro("empty", &PyTimeSeriesBundleInput::empty)
            .def_prop_ro("as_schema", [](nb::handle self) { return self; })
            .def("__str__", &PyTimeSeriesBundleInput::py_str)
            .def("__repr__", &PyTimeSeriesBundleInput::py_repr);
    }

}  // namespace hgraph
