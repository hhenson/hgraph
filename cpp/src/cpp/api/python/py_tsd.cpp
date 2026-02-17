#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/py_ts_runtime_internal.h>
#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/constants.h>

#include <fmt/format.h>
#include <cstdio>
#include <cstdlib>
#include <type_traits>

namespace hgraph
{
namespace
{
    value::Value key_from_python_with_meta(const nb::object &key, const TSMeta *meta) {
        const auto *key_schema = meta != nullptr ? meta->key_type() : nullptr;
        if (key_schema == nullptr) {
            return {};
        }

        value::Value key_val(key_schema);
        key_val.emplace();
        key_schema->ops().from_python(key_val.data(), key, key_schema);
        return key_val;
    }

    nb::list tsd_keys_python(const TSView &view) {
        nb::list out;
        value::View value = view.value();
        if (!value.valid() || !value.is_map()) {
            return out;
        }
        for (value::View key : value.as_map().keys()) {
            out.append(key.to_python());
        }
        return out;
    }

    nb::list tsd_delta_keys_slot(const TSView &view, size_t tuple_index, bool expect_map) {
        nb::list out;
        value::View delta = view.delta_value();
        if (!delta.valid() || !delta.is_tuple()) {
            return out;
        }

        auto tuple = delta.as_tuple();
        if (tuple_index >= tuple.size()) {
            return out;
        }

        value::View slot = tuple.at(tuple_index);
        if (!slot.valid()) {
            return out;
        }

        if (expect_map) {
            if (!slot.is_map()) {
                return out;
            }
            for (value::View key : slot.as_map().keys()) {
                out.append(key.to_python());
            }
            return out;
        }

        if (!slot.is_set()) {
            return out;
        }
        for (value::View key : slot.as_set()) {
            out.append(key.to_python());
        }
        return out;
    }

    bool list_contains_py_object(const nb::list &list_obj, const nb::object &value) {
        return PySequence_Contains(list_obj.ptr(), value.ptr()) == 1;
    }

    template <typename DictViewT>
    nb::list dict_values_for_keys(DictViewT dict_view, const nb::list &keys) {
        nb::list out;
        const auto *meta = dict_view.ts_meta();
        for (const auto &key_item : keys) {
            nb::object key = nb::cast<nb::object>(key_item);
            auto key_val = key_from_python_with_meta(key, meta);
            if (key_val.schema() == nullptr) {
                continue;
            }
            auto child = dict_view.at_key(key_val.view());
            if (child) {
                if constexpr (std::is_same_v<DictViewT, TSDOutputView>) {
                    out.append(wrap_output_view(std::move(child)));
                } else {
                    out.append(wrap_input_view(std::move(child)));
                }
            }
        }
        return out;
    }

    template <typename DictViewT>
    nb::list dict_items_for_keys(DictViewT dict_view, const nb::list &keys) {
        nb::list out;
        const auto *meta = dict_view.ts_meta();
        for (const auto &key_item : keys) {
            nb::object key = nb::cast<nb::object>(key_item);
            auto key_val = key_from_python_with_meta(key, meta);
            if (key_val.schema() == nullptr) {
                continue;
            }
            auto child = dict_view.at_key(key_val.view());
            if (child) {
                if (std::getenv("HGRAPH_DEBUG_TSD_ITEMS") != nullptr) {
                    std::string key_s = nb::cast<std::string>(nb::repr(key));
                    std::fprintf(stderr,
                                 "[tsd_items] key=%s child_path=%s\n",
                                 key_s.c_str(),
                                 child.as_ts_view().short_path().to_string().c_str());
                }
                if constexpr (std::is_same_v<DictViewT, TSDOutputView>) {
                    out.append(nb::make_tuple(key, wrap_output_view(std::move(child))));
                } else {
                    out.append(nb::make_tuple(key, wrap_input_view(std::move(child))));
                }
            }
        }
        return out;
    }

    bool is_remove_marker(const nb::object &obj) {
        auto tsd_mod = nb::module_::import_("hgraph._types._tsd_type");
        return obj.is(tsd_mod.attr("REMOVE")) || obj.is(tsd_mod.attr("REMOVE_IF_EXISTS"));
    }

    bool is_remove_if_exists_marker(const nb::object &obj) {
        auto tsd_mod = nb::module_::import_("hgraph._types._tsd_type");
        return obj.is(tsd_mod.attr("REMOVE_IF_EXISTS"));
    }
}  // namespace

    // ===== PyTimeSeriesDictOutput Implementation =====

    value::Value PyTimeSeriesDictOutput::key_from_python(const nb::object &key) const {
        return key_from_python_with_meta(key, output_view().ts_meta());
    }

    size_t PyTimeSeriesDictOutput::size() const {
        return output_view().as_dict().count();
    }

    nb::object PyTimeSeriesDictOutput::get_item(const nb::object &item) const {
        if (get_key_set_id().is(item)) {
            return key_set();
        }
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            throw nb::key_error();
        }

        auto child = output_view().as_dict().at_key(key_val.view());
        if (!child) {
            throw nb::key_error();
        }
        return wrap_output_view(std::move(child));
    }

    nb::object PyTimeSeriesDictOutput::get(const nb::object &item, const nb::object &default_value) const {
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            return default_value;
        }
        auto child = output_view().as_dict().at_key(key_val.view());
        return child ? wrap_output_view(std::move(child)) : default_value;
    }

    nb::object PyTimeSeriesDictOutput::get_or_create(const nb::object &key) {
        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            return nb::none();
        }
        auto child = output_view().as_dict().create(key_val.view());
        return child ? wrap_output_view(std::move(child)) : nb::none();
    }

    void PyTimeSeriesDictOutput::create(const nb::object &item) {
        auto key_val = key_from_python(item);
        if (key_val.schema() != nullptr) {
            output_view().as_dict().create(key_val.view());
        }
    }

    nb::object PyTimeSeriesDictOutput::iter() const {
        return nb::iter(keys());
    }

    bool PyTimeSeriesDictOutput::contains(const nb::object &item) const {
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            return false;
        }
        value::View value = view().value();
        return value.valid() && value.is_map() && value.as_map().contains(key_val.view());
    }

    nb::object PyTimeSeriesDictOutput::key_set() const {
        TSOutputView key_set = output_view();
        key_set.as_ts_view().view_data().projection = ViewProjection::TSD_KEY_SET;
        return nb::cast(PyTimeSeriesSetOutput(std::move(key_set)));
    }

    nb::object PyTimeSeriesDictOutput::keys() const {
        return tsd_keys_python(output_view().as_ts_view());
    }

    nb::object PyTimeSeriesDictOutput::values() const {
        return dict_values_for_keys(output_view().as_dict(), nb::cast<nb::list>(keys()));
    }

    nb::object PyTimeSeriesDictOutput::items() const {
        return dict_items_for_keys(output_view().as_dict(), nb::cast<nb::list>(keys()));
    }

    nb::object PyTimeSeriesDictOutput::modified_keys() const {
        return tsd_delta_keys_slot(output_view().as_ts_view(), 0, true);
    }

    nb::object PyTimeSeriesDictOutput::modified_values() const {
        return dict_values_for_keys(output_view().as_dict(), nb::cast<nb::list>(modified_keys()));
    }

    nb::object PyTimeSeriesDictOutput::modified_items() const {
        return dict_items_for_keys(output_view().as_dict(), nb::cast<nb::list>(modified_keys()));
    }

    bool PyTimeSeriesDictOutput::was_modified(const nb::object &key) const {
        return list_contains_py_object(nb::cast<nb::list>(modified_keys()), key);
    }

    nb::object PyTimeSeriesDictOutput::valid_keys() const {
        nb::list out;
        auto dict = output_view().as_dict();
        for (const auto &key_item : nb::cast<nb::list>(keys())) {
            nb::object key = nb::cast<nb::object>(key_item);
            auto key_val = key_from_python(key);
            if (key_val.schema() == nullptr) {
                continue;
            }
            auto child = dict.at_key(key_val.view());
            if (child && child.valid()) {
                out.append(key);
            }
        }
        return out;
    }

    nb::object PyTimeSeriesDictOutput::valid_values() const {
        return dict_values_for_keys(output_view().as_dict(), nb::cast<nb::list>(valid_keys()));
    }

    nb::object PyTimeSeriesDictOutput::valid_items() const {
        return dict_items_for_keys(output_view().as_dict(), nb::cast<nb::list>(valid_keys()));
    }

    nb::object PyTimeSeriesDictOutput::added_keys() const {
        return tsd_delta_keys_slot(output_view().as_ts_view(), 1, false);
    }

    nb::object PyTimeSeriesDictOutput::added_values() const {
        return dict_values_for_keys(output_view().as_dict(), nb::cast<nb::list>(added_keys()));
    }

    nb::object PyTimeSeriesDictOutput::added_items() const {
        return dict_items_for_keys(output_view().as_dict(), nb::cast<nb::list>(added_keys()));
    }

    bool PyTimeSeriesDictOutput::has_added() const {
        return nb::len(nb::cast<nb::list>(added_keys())) > 0;
    }

    bool PyTimeSeriesDictOutput::was_added(const nb::object &key) const {
        return list_contains_py_object(nb::cast<nb::list>(added_keys()), key);
    }

    nb::object PyTimeSeriesDictOutput::removed_keys() const {
        return tsd_delta_keys_slot(output_view().as_ts_view(), 2, false);
    }

    nb::object PyTimeSeriesDictOutput::removed_values() const {
        nb::list out;
        for (const auto &key_item : nb::cast<nb::list>(removed_keys())) {
            (void)key_item;
            out.append(nb::none());
        }
        return out;
    }

    nb::object PyTimeSeriesDictOutput::removed_items() const {
        nb::list out;
        for (const auto &key_item : nb::cast<nb::list>(removed_keys())) {
            nb::object key = nb::cast<nb::object>(key_item);
            out.append(nb::make_tuple(key, nb::none()));
        }
        return out;
    }

    bool PyTimeSeriesDictOutput::has_removed() const {
        return nb::len(nb::cast<nb::list>(removed_keys())) > 0;
    }

    bool PyTimeSeriesDictOutput::was_removed(const nb::object &key) const {
        return list_contains_py_object(nb::cast<nb::list>(removed_keys()), key);
    }

    nb::object PyTimeSeriesDictOutput::key_from_value(const nb::object &value) const {
        auto *wrapped = nb::inst_ptr<PyTimeSeriesOutput>(value);
        if (wrapped == nullptr) {
            return nb::none();
        }
        const auto &target = wrapped->output_view().short_path();

        auto dict = output_view().as_dict();
        for (const auto &key_item : nb::cast<nb::list>(keys())) {
            nb::object key = nb::cast<nb::object>(key_item);
            auto key_val = key_from_python(key);
            if (key_val.schema() == nullptr) {
                continue;
            }
            auto child = dict.at_key(key_val.view());
            if (!child) {
                continue;
            }
            const auto &path = child.short_path();
            if (path.node == target.node && path.port_type == target.port_type && path.indices == target.indices) {
                return key;
            }
        }
        return nb::none();
    }

    nb::str PyTimeSeriesDictOutput::py_str() const {
        auto str = fmt::format("TimeSeriesDictOutput@{:p}[size={}, valid={}]",
                               static_cast<const void *>(&output_view()), size(), output_view().valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesDictOutput::py_repr() const {
        return py_str();
    }

    void PyTimeSeriesDictOutput::set_item(const nb::object &key, const nb::object &value) {
        if (is_remove_marker(value)) {
            auto key_val = key_from_python(key);
            if (key_val.schema() == nullptr) {
                return;
            }

            auto dict = output_view().as_dict();
            if (is_remove_if_exists_marker(value)) {
                dict.remove(key_val.view());
                return;
            }

            if (!dict.remove(key_val.view())) {
                throw nb::key_error();
            }
            return;
        }

        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            throw nb::key_error();
        }
        auto child = output_view().as_dict().create(key_val.view());
        if (!child) {
            throw std::runtime_error("Failed to create TSD output child");
        }
        child.from_python(value);
    }

    void PyTimeSeriesDictOutput::del_item(const nb::object &key) {
        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            throw nb::key_error();
        }
        if (!output_view().as_dict().remove(key_val.view())) {
            throw nb::key_error();
        }
    }

    nb::object PyTimeSeriesDictOutput::pop(const nb::object &key, const nb::object &default_value) {
        if (!contains(key)) {
            return default_value;
        }
        nb::object out = get_item(key);
        del_item(key);
        return out;
    }

    nb::object PyTimeSeriesDictOutput::get_ref(const nb::object &key, const nb::object &requester) {
        TSOutputView base = output_view();
        TSOutputView out = runtime_tsd_get_ref_output(base, key, requester);
        return out ? wrap_output_view(std::move(out)) : nb::none();
    }

    void PyTimeSeriesDictOutput::release_ref(const nb::object &key, const nb::object &requester) {
        TSOutputView base = output_view();
        runtime_tsd_release_ref_output(base, key, requester);
    }

    // ===== PyTimeSeriesDictInput Implementation =====

    value::Value PyTimeSeriesDictInput::key_from_python(const nb::object &key) const {
        return key_from_python_with_meta(key, input_view().ts_meta());
    }

    size_t PyTimeSeriesDictInput::size() const {
        return input_view().as_dict().count();
    }

    nb::object PyTimeSeriesDictInput::get_item(const nb::object &item) const {
        if (get_key_set_id().is(item)) {
            return key_set();
        }
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            throw nb::key_error();
        }
        auto child = input_view().as_dict().at_key(key_val.view());
        if (!child) {
            throw nb::key_error();
        }
        return wrap_input_view(std::move(child));
    }

    nb::object PyTimeSeriesDictInput::get(const nb::object &item, const nb::object &default_value) const {
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            return default_value;
        }
        auto child = input_view().as_dict().at_key(key_val.view());
        return child ? wrap_input_view(std::move(child)) : default_value;
    }

    nb::object PyTimeSeriesDictInput::get_or_create(const nb::object &key) {
        auto value = get(key, nb::none());
        if (!value.is_none()) {
            return value;
        }
        create(key);
        return get(key, nb::none());
    }

    void PyTimeSeriesDictInput::create(const nb::object &item) {
        on_key_added(item);
    }

    nb::object PyTimeSeriesDictInput::iter() const {
        return nb::iter(keys());
    }

    bool PyTimeSeriesDictInput::contains(const nb::object &item) const {
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            return false;
        }
        value::View value = view().value();
        return value.valid() && value.is_map() && value.as_map().contains(key_val.view());
    }

    nb::object PyTimeSeriesDictInput::key_set() const {
        TSInputView key_set = input_view();
        key_set.as_ts_view().view_data().projection = ViewProjection::TSD_KEY_SET;
        return nb::cast(PyTimeSeriesSetInput(std::move(key_set)));
    }

    nb::object PyTimeSeriesDictInput::keys() const {
        return tsd_keys_python(input_view().as_ts_view());
    }

    nb::object PyTimeSeriesDictInput::values() const {
        return dict_values_for_keys(input_view().as_dict(), nb::cast<nb::list>(keys()));
    }

    nb::object PyTimeSeriesDictInput::items() const {
        return dict_items_for_keys(input_view().as_dict(), nb::cast<nb::list>(keys()));
    }

    nb::object PyTimeSeriesDictInput::modified_keys() const {
        return tsd_delta_keys_slot(input_view().as_ts_view(), 0, true);
    }

    nb::object PyTimeSeriesDictInput::modified_values() const {
        return dict_values_for_keys(input_view().as_dict(), nb::cast<nb::list>(modified_keys()));
    }

    nb::object PyTimeSeriesDictInput::modified_items() const {
        return dict_items_for_keys(input_view().as_dict(), nb::cast<nb::list>(modified_keys()));
    }

    bool PyTimeSeriesDictInput::was_modified(const nb::object &key) const {
        return list_contains_py_object(nb::cast<nb::list>(modified_keys()), key);
    }

    nb::object PyTimeSeriesDictInput::valid_keys() const {
        nb::list out;
        auto dict = input_view().as_dict();
        for (const auto &key_item : nb::cast<nb::list>(keys())) {
            nb::object key = nb::cast<nb::object>(key_item);
            auto key_val = key_from_python(key);
            if (key_val.schema() == nullptr) {
                continue;
            }
            auto child = dict.at_key(key_val.view());
            if (child && child.valid()) {
                out.append(key);
            }
        }
        return out;
    }

    nb::object PyTimeSeriesDictInput::valid_values() const {
        return dict_values_for_keys(input_view().as_dict(), nb::cast<nb::list>(valid_keys()));
    }

    nb::object PyTimeSeriesDictInput::valid_items() const {
        return dict_items_for_keys(input_view().as_dict(), nb::cast<nb::list>(valid_keys()));
    }

    nb::object PyTimeSeriesDictInput::added_keys() const {
        return tsd_delta_keys_slot(input_view().as_ts_view(), 1, false);
    }

    nb::object PyTimeSeriesDictInput::added_values() const {
        return dict_values_for_keys(input_view().as_dict(), nb::cast<nb::list>(added_keys()));
    }

    nb::object PyTimeSeriesDictInput::added_items() const {
        return dict_items_for_keys(input_view().as_dict(), nb::cast<nb::list>(added_keys()));
    }

    bool PyTimeSeriesDictInput::has_added() const {
        return nb::len(nb::cast<nb::list>(added_keys())) > 0;
    }

    bool PyTimeSeriesDictInput::was_added(const nb::object &key) const {
        return list_contains_py_object(nb::cast<nb::list>(added_keys()), key);
    }

    nb::object PyTimeSeriesDictInput::removed_keys() const {
        return tsd_delta_keys_slot(input_view().as_ts_view(), 2, false);
    }

    nb::object PyTimeSeriesDictInput::removed_values() const {
        nb::list out;
        for (const auto &key_item : nb::cast<nb::list>(removed_keys())) {
            (void)key_item;
            out.append(nb::none());
        }
        return out;
    }

    nb::object PyTimeSeriesDictInput::removed_items() const {
        nb::list out;
        for (const auto &key_item : nb::cast<nb::list>(removed_keys())) {
            nb::object key = nb::cast<nb::object>(key_item);
            out.append(nb::make_tuple(key, nb::none()));
        }
        return out;
    }

    bool PyTimeSeriesDictInput::has_removed() const {
        return nb::len(nb::cast<nb::list>(removed_keys())) > 0;
    }

    bool PyTimeSeriesDictInput::was_removed(const nb::object &key) const {
        return list_contains_py_object(nb::cast<nb::list>(removed_keys()), key);
    }

    nb::object PyTimeSeriesDictInput::key_from_value(const nb::object &value) const {
        auto *wrapped = nb::inst_ptr<PyTimeSeriesInput>(value);
        if (wrapped == nullptr) {
            return nb::none();
        }
        const auto &target = wrapped->input_view().short_path();

        auto dict = input_view().as_dict();
        for (const auto &key_item : nb::cast<nb::list>(keys())) {
            nb::object key = nb::cast<nb::object>(key_item);
            auto key_val = key_from_python(key);
            if (key_val.schema() == nullptr) {
                continue;
            }
            auto child = dict.at_key(key_val.view());
            if (!child) {
                continue;
            }
            const auto &path = child.short_path();
            if (path.node == target.node && path.port_type == target.port_type && path.indices == target.indices) {
                return key;
            }
        }
        return nb::none();
    }

    nb::str PyTimeSeriesDictInput::py_str() const {
        auto str = fmt::format("TimeSeriesDictInput@{:p}[size={}, valid={}]",
                               static_cast<const void *>(&input_view()), size(), input_view().valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesDictInput::py_repr() const {
        return py_str();
    }

    void PyTimeSeriesDictInput::on_key_added(const nb::object &key) {
        if (!input_view().is_bound()) {
            return;
        }

        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            return;
        }

        auto child_input = input_view().as_dict().at_key(key_val.view());
        if (!child_input) {
            return;
        }

        auto out_obj = output();
        if (!nb::isinstance<PyTimeSeriesOutput>(out_obj)) {
            return;
        }

        auto &py_output = nb::cast<PyTimeSeriesOutput &>(out_obj);
        if (py_output.output_view().as_ts_view().kind() != TSKind::TSD) {
            return;
        }

        auto child_output = py_output.output_view().as_dict().at_key(key_val.view());
        if (!child_output) {
            return;
        }

        child_input.bind(child_output);
        if (input_view().active()) {
            child_input.make_active();
        }
    }

    void PyTimeSeriesDictInput::on_key_removed(const nb::object &key) {
        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            return;
        }

        auto child_input = input_view().as_dict().at_key(key_val.view());
        if (!child_input) {
            return;
        }

        if (child_input.active()) {
            child_input.make_passive();
        }
        child_input.unbind();
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
