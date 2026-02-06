#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_dict_view.h>
#include <hgraph/types/constants.h>
#include <hgraph/util/date_time.h>

namespace hgraph
{

    // Helper to convert Python key to value::Value using TSMeta
    static value::Value<> key_from_python_with_meta(const nb::object &key, const TSMeta* meta) {
        const auto *key_schema = meta->key_type;
        value::Value<> key_val(key_schema);
        key_schema->ops->from_python(key_val.data(), key, key_schema);
        return key_val;
    }

    // Helper to convert value::View key to Python
    static nb::object key_view_to_python(const value::View& key, const TSMeta* meta) {
        return meta->key_type->ops->to_python(key.data(), meta->key_type);
    }

    // ===== PyTimeSeriesDictOutput Implementation =====

    value::Value<> PyTimeSeriesDictOutput::key_from_python(const nb::object &key) const {
        return key_from_python_with_meta(key, view().ts_meta());
    }

    size_t PyTimeSeriesDictOutput::size() const {
        auto dict_view = view().as_dict();
        return dict_view.size();
    }

    nb::object PyTimeSeriesDictOutput::get_item(const nb::object &item) const {
        if (get_key_set_id().is(item)) { return key_set(); }
        auto dict_view = view().as_dict();
        auto key_val = key_from_python(item);
        TSView elem_view = dict_view.at(key_val.const_view());
        return wrap_output_view(TSOutputView(elem_view, nullptr));
    }

    nb::object PyTimeSeriesDictOutput::get(const nb::object &item, const nb::object &default_value) const {
        auto dict_view = view().as_dict();
        auto key_val = key_from_python(item);
        if (dict_view.contains(key_val.const_view())) {
            TSView elem_view = dict_view.at(key_val.const_view());
            return wrap_output_view(TSOutputView(elem_view, nullptr));
        }
        return default_value;
    }

    nb::object PyTimeSeriesDictOutput::get_or_create(const nb::object &key) {
        auto dict_view = output_view().ts_view().as_dict();
        auto key_val = key_from_python(key);
        TSView elem_view = dict_view.get_or_create(key_val.const_view());
        return wrap_output_view(TSOutputView(elem_view, nullptr));
    }

    void PyTimeSeriesDictOutput::create(const nb::object &item) {
        auto dict_view = output_view().ts_view().as_dict();
        auto key_val = key_from_python(item);
        dict_view.create(key_val.const_view());
    }

    nb::object PyTimeSeriesDictOutput::iter() const {
        return nb::iter(keys());
    }

    bool PyTimeSeriesDictOutput::contains(const nb::object &item) const {
        auto dict_view = view().as_dict();
        auto key_val = key_from_python(item);
        return dict_view.contains(key_val.const_view());
    }

    nb::object PyTimeSeriesDictOutput::key_set() const {
        auto dict_view = view().as_dict();
        TSSView tss_view = dict_view.key_set();
        // Wrap the TSSView as a TSOutputView (TSS is output-like for reading)
        // Use the parent view's current_time since TSSView doesn't expose it
        return wrap_output_view(TSOutputView(TSView(tss_view.view_data(), view().current_time()), nullptr));
    }

    nb::object PyTimeSeriesDictOutput::keys() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto key : dict_view.keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::values() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto it = dict_view.items().begin(); it != dict_view.items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::items() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto it = dict_view.items().begin(); it != dict_view.items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::modified_keys() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto key : dict_view.modified_keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::modified_values() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto it = dict_view.modified_items().begin(); it != dict_view.modified_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::modified_items() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto it = dict_view.modified_items().begin(); it != dict_view.modified_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    bool PyTimeSeriesDictOutput::was_modified(const nb::object &key) const {
        auto dict_view = view().as_dict();
        // Check if key is in modified slots
        auto key_val = key_from_python(key);
        // Check if key exists and if it's modified
        if (!dict_view.contains(key_val.const_view())) return false;
        // Get the value and check if it's modified
        TSView elem = dict_view.at(key_val.const_view());
        return elem.modified();
    }

    nb::object PyTimeSeriesDictOutput::valid_keys() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto it = dict_view.valid_items().begin(); it != dict_view.valid_items().end(); ++it) {
            result.append(key_view_to_python(it.key(), dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::valid_values() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto it = dict_view.valid_items().begin(); it != dict_view.valid_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::valid_items() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto it = dict_view.valid_items().begin(); it != dict_view.valid_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::added_keys() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto key : dict_view.added_keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::added_values() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto it = dict_view.added_items().begin(); it != dict_view.added_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::added_items() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto it = dict_view.added_items().begin(); it != dict_view.added_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    bool PyTimeSeriesDictOutput::has_added() const {
        auto dict_view = view().as_dict();
        return !dict_view.added_slots().empty();
    }

    bool PyTimeSeriesDictOutput::was_added(const nb::object &key) const {
        auto dict_view = view().as_dict();
        auto key_val = key_from_python(key);
        return dict_view.was_added(key_val.const_view());
    }

    nb::object PyTimeSeriesDictOutput::removed_keys() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto key : dict_view.removed_keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::removed_values() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto it = dict_view.removed_items().begin(); it != dict_view.removed_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::removed_items() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto it = dict_view.removed_items().begin(); it != dict_view.removed_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    bool PyTimeSeriesDictOutput::has_removed() const {
        auto dict_view = view().as_dict();
        return !dict_view.removed_slots().empty();
    }

    bool PyTimeSeriesDictOutput::was_removed(const nb::object &key) const {
        auto dict_view = view().as_dict();
        auto key_val = key_from_python(key);
        return dict_view.was_removed(key_val.const_view());
    }

    nb::object PyTimeSeriesDictOutput::key_from_value(const nb::object &value) const {
        // Not supported in view mode - would need additional infrastructure
        throw std::runtime_error("not implemented: PyTimeSeriesDictOutput::key_from_value");
    }

    nb::str PyTimeSeriesDictOutput::py_str() const {
        auto dict_view = view().as_dict();
        auto str = fmt::format("TimeSeriesDictOutput@{:p}[size={}, valid={}]",
                               static_cast<const void *>(&dict_view), dict_view.size(), dict_view.valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesDictOutput::py_repr() const {
        return py_str();
    }

    void PyTimeSeriesDictOutput::set_item(const nb::object &key, const nb::object &value) {
        auto dict_view = output_view().ts_view().as_dict();
        auto key_val = key_from_python(key);

        // Get element value type from TSMeta and convert Python value
        const TSMeta* meta = dict_view.meta();
        const value::TypeMeta* elem_value_type = meta->element_ts->value_type;
        value::Value<> value_val(elem_value_type);
        elem_value_type->ops->from_python(value_val.data(), value, elem_value_type);

        dict_view.set(key_val.const_view(), value_val.const_view());
    }

    void PyTimeSeriesDictOutput::del_item(const nb::object &key) {
        auto dict_view = output_view().ts_view().as_dict();
        auto key_val = key_from_python(key);
        dict_view.remove(key_val.const_view());
    }

    nb::object PyTimeSeriesDictOutput::pop(const nb::object &key, const nb::object &default_value) {
        throw std::runtime_error("not implemented: PyTimeSeriesDictOutput::pop");
    }

    nb::object PyTimeSeriesDictOutput::get_ref(const nb::object &key, const nb::object &requester) {
        throw std::runtime_error("not implemented: PyTimeSeriesDictOutput::get_ref");
    }

    void PyTimeSeriesDictOutput::release_ref(const nb::object &key, const nb::object &requester) {
        throw std::runtime_error("not implemented: PyTimeSeriesDictOutput::release_ref");
    }

    // ===== PyTimeSeriesDictInput Implementation =====

    value::Value<> PyTimeSeriesDictInput::key_from_python(const nb::object &key) const {
        return key_from_python_with_meta(key, input_view().ts_meta());
    }

    size_t PyTimeSeriesDictInput::size() const {
        auto dict_view = input_view().ts_view().as_dict();
        return dict_view.size();
    }

    nb::object PyTimeSeriesDictInput::get_item(const nb::object &item) const {
        if (get_key_set_id().is(item)) { return key_set(); }
        auto dict_view = input_view().ts_view().as_dict();
        auto key_val = key_from_python(item);
        TSView elem_view = dict_view.at(key_val.const_view());
        return wrap_input_view(TSInputView(elem_view, nullptr));
    }

    nb::object PyTimeSeriesDictInput::get(const nb::object &item, const nb::object &default_value) const {
        auto dict_view = input_view().ts_view().as_dict();
        auto key_val = key_from_python(item);
        if (dict_view.contains(key_val.const_view())) {
            TSView elem_view = dict_view.at(key_val.const_view());
            return wrap_input_view(TSInputView(elem_view, nullptr));
        }
        return default_value;
    }

    nb::object PyTimeSeriesDictInput::get_or_create(const nb::object &key) {
        throw std::runtime_error("not implemented: PyTimeSeriesDictInput::get_or_create");
    }

    void PyTimeSeriesDictInput::create(const nb::object &item) {
        throw std::runtime_error("not implemented: PyTimeSeriesDictInput::create");
    }

    nb::object PyTimeSeriesDictInput::iter() const {
        return nb::iter(keys());
    }

    bool PyTimeSeriesDictInput::contains(const nb::object &item) const {
        auto dict_view = input_view().ts_view().as_dict();
        auto key_val = key_from_python(item);
        return dict_view.contains(key_val.const_view());
    }

    nb::object PyTimeSeriesDictInput::key_set() const {
        auto dict_view = input_view().ts_view().as_dict();
        TSSView tss_view = dict_view.key_set();
        // Wrap the TSSView as a TSInputView (TSS is input-like for reading)
        // Use the parent view's current_time since TSSView doesn't expose it
        return wrap_input_view(TSInputView(TSView(tss_view.view_data(), input_view().current_time()), nullptr));
    }

    nb::object PyTimeSeriesDictInput::keys() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto key : dict_view.keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::values() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto it = dict_view.items().begin(); it != dict_view.items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::items() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto it = dict_view.items().begin(); it != dict_view.items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::modified_keys() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto key : dict_view.modified_keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::modified_values() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto it = dict_view.modified_items().begin(); it != dict_view.modified_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::modified_items() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto it = dict_view.modified_items().begin(); it != dict_view.modified_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    bool PyTimeSeriesDictInput::was_modified(const nb::object &key) const {
        auto dict_view = input_view().ts_view().as_dict();
        auto key_val = key_from_python(key);
        if (!dict_view.contains(key_val.const_view())) return false;
        TSView elem = dict_view.at(key_val.const_view());
        return elem.modified();
    }

    nb::object PyTimeSeriesDictInput::valid_keys() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto it = dict_view.valid_items().begin(); it != dict_view.valid_items().end(); ++it) {
            result.append(key_view_to_python(it.key(), dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::valid_values() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto it = dict_view.valid_items().begin(); it != dict_view.valid_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::valid_items() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto it = dict_view.valid_items().begin(); it != dict_view.valid_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::added_keys() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto key : dict_view.added_keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::added_values() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto it = dict_view.added_items().begin(); it != dict_view.added_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::added_items() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto it = dict_view.added_items().begin(); it != dict_view.added_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    bool PyTimeSeriesDictInput::has_added() const {
        auto dict_view = input_view().ts_view().as_dict();
        return !dict_view.added_slots().empty();
    }

    bool PyTimeSeriesDictInput::was_added(const nb::object &key) const {
        auto dict_view = input_view().ts_view().as_dict();
        auto key_val = key_from_python(key);
        return dict_view.was_added(key_val.const_view());
    }

    nb::object PyTimeSeriesDictInput::removed_keys() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto key : dict_view.removed_keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::removed_values() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto it = dict_view.removed_items().begin(); it != dict_view.removed_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::removed_items() const {
        auto dict_view = input_view().ts_view().as_dict();
        nb::list result;
        for (auto it = dict_view.removed_items().begin(); it != dict_view.removed_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    bool PyTimeSeriesDictInput::has_removed() const {
        auto dict_view = input_view().ts_view().as_dict();
        return !dict_view.removed_slots().empty();
    }

    bool PyTimeSeriesDictInput::was_removed(const nb::object &key) const {
        auto dict_view = input_view().ts_view().as_dict();
        auto key_val = key_from_python(key);
        return dict_view.was_removed(key_val.const_view());
    }

    nb::object PyTimeSeriesDictInput::key_from_value(const nb::object &value) const {
        // Not supported in view mode - would need additional infrastructure
        throw std::runtime_error("not implemented: PyTimeSeriesDictInput::key_from_value");
    }

    nb::str PyTimeSeriesDictInput::py_str() const {
        auto dict_view = input_view().ts_view().as_dict();
        auto str = fmt::format("TimeSeriesDictInput@{:p}[size={}, valid={}]",
                               static_cast<const void *>(&dict_view), dict_view.size(), dict_view.valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesDictInput::py_repr() const {
        return py_str();
    }

    void PyTimeSeriesDictInput::on_key_added(const nb::object &key) {
        throw std::runtime_error("not implemented: PyTimeSeriesDictInput::on_key_added");
    }

    void PyTimeSeriesDictInput::on_key_removed(const nb::object &key) {
        throw std::runtime_error("not implemented: PyTimeSeriesDictInput::on_key_removed");
    }

    // ===== Nanobind Registration =====

    void tsd_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictOutput, PyTimeSeriesOutput>(m, "TimeSeriesDictOutput")
            .def("__contains__", &PyTimeSeriesDictOutput::contains, "key"_a)
            .def("__getitem__", &PyTimeSeriesDictOutput::get_item, "key"_a)
            .def("__setitem__", &PyTimeSeriesDictOutput::set_item, "key"_a, "value"_a)
            .def("__delitem__", &PyTimeSeriesDictOutput::del_item, "key"_a)
            .def("__len__", &PyTimeSeriesDictOutput::size)
            .def("pop", &PyTimeSeriesDictOutput::pop, "key"_a, "default"_a = nb::none())
            .def("get", &PyTimeSeriesDictOutput::get, "key"_a, "default"_a = nb::none())
            .def("get_or_create", &PyTimeSeriesDictOutput::get_or_create, "key"_a)
            .def("create", &PyTimeSeriesDictOutput::create, "key"_a)
            .def("clear", &PyTimeSeriesDictOutput::clear)
            .def("__iter__", &PyTimeSeriesDictOutput::iter)
            .def("keys", &PyTimeSeriesDictOutput::keys)
            .def("values", &PyTimeSeriesDictOutput::values)
            .def("items", &PyTimeSeriesDictOutput::items)
            .def("valid_keys", &PyTimeSeriesDictOutput::valid_keys)
            .def("valid_values", &PyTimeSeriesDictOutput::valid_values)
            .def("valid_items", &PyTimeSeriesDictOutput::valid_items)
            .def("added_keys", &PyTimeSeriesDictOutput::added_keys)
            .def("added_values", &PyTimeSeriesDictOutput::added_values)
            .def("added_items", &PyTimeSeriesDictOutput::added_items)
            .def("was_added", &PyTimeSeriesDictOutput::was_added, "key"_a)
            .def_prop_ro("has_added", &PyTimeSeriesDictOutput::has_added)
            .def("modified_keys", &PyTimeSeriesDictOutput::modified_keys)
            .def("modified_values", &PyTimeSeriesDictOutput::modified_values)
            .def("modified_items", &PyTimeSeriesDictOutput::modified_items)
            .def("was_modified", &PyTimeSeriesDictOutput::was_modified, "key"_a)
            .def("removed_keys", &PyTimeSeriesDictOutput::removed_keys)
            .def("removed_values", &PyTimeSeriesDictOutput::removed_values)
            .def("removed_items", &PyTimeSeriesDictOutput::removed_items)
            .def("was_removed", &PyTimeSeriesDictOutput::was_removed, "key"_a)
            .def("key_from_value", &PyTimeSeriesDictOutput::key_from_value, "value"_a)
            .def_prop_ro("has_removed", &PyTimeSeriesDictOutput::has_removed)
            .def("get_ref", &PyTimeSeriesDictOutput::get_ref, "key"_a, "requester"_a)
            .def("release_ref", &PyTimeSeriesDictOutput::release_ref, "key"_a, "requester"_a)
            .def_prop_ro("key_set", &PyTimeSeriesDictOutput::key_set)
            .def("__str__", &PyTimeSeriesDictOutput::py_str)
            .def("__repr__", &PyTimeSeriesDictOutput::py_repr);

        nb::class_<PyTimeSeriesDictInput, PyTimeSeriesInput>(m, "TimeSeriesDictInput")
            .def("__contains__", &PyTimeSeriesDictInput::contains, "key"_a)
            .def("__getitem__", &PyTimeSeriesDictInput::get_item, "key"_a)
            .def("__len__", &PyTimeSeriesDictInput::size)
            .def("__iter__", &PyTimeSeriesDictInput::iter)
            .def("get", &PyTimeSeriesDictInput::get, "key"_a, "default"_a = nb::none())
            .def("get_or_create", &PyTimeSeriesDictInput::get_or_create, "key"_a)
            .def("create", &PyTimeSeriesDictInput::create, "key"_a)
            .def("keys", &PyTimeSeriesDictInput::keys)
            .def("values", &PyTimeSeriesDictInput::values)
            .def("items", &PyTimeSeriesDictInput::items)
            .def("valid_keys", &PyTimeSeriesDictInput::valid_keys)
            .def("valid_values", &PyTimeSeriesDictInput::valid_values)
            .def("valid_items", &PyTimeSeriesDictInput::valid_items)
            .def("added_keys", &PyTimeSeriesDictInput::added_keys)
            .def("added_values", &PyTimeSeriesDictInput::added_values)
            .def("added_items", &PyTimeSeriesDictInput::added_items)
            .def("was_added", &PyTimeSeriesDictInput::was_added, "key"_a)
            .def_prop_ro("has_added", &PyTimeSeriesDictInput::has_added)
            .def("modified_keys", &PyTimeSeriesDictInput::modified_keys)
            .def("modified_values", &PyTimeSeriesDictInput::modified_values)
            .def("modified_items", &PyTimeSeriesDictInput::modified_items)
            .def("was_modified", &PyTimeSeriesDictInput::was_modified, "key"_a)
            .def("removed_keys", &PyTimeSeriesDictInput::removed_keys)
            .def("removed_values", &PyTimeSeriesDictInput::removed_values)
            .def("removed_items", &PyTimeSeriesDictInput::removed_items)
            .def("was_removed", &PyTimeSeriesDictInput::was_removed, "key"_a)
            .def("on_key_added", &PyTimeSeriesDictInput::on_key_added, "key"_a)
            .def("on_key_removed", &PyTimeSeriesDictInput::on_key_removed, "key"_a)
            .def("key_from_value", &PyTimeSeriesDictInput::key_from_value, "value"_a)
            .def_prop_ro("has_removed", &PyTimeSeriesDictInput::has_removed)
            .def_prop_ro("key_set", &PyTimeSeriesDictInput::key_set)
            .def("__str__", &PyTimeSeriesDictInput::py_str)
            .def("__repr__", &PyTimeSeriesDictInput::py_repr);
    }
}  // namespace hgraph
