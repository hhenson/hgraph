#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/py_ts_runtime_internal.h>
#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/time_series/ts_ops.h>

#include <fmt/format.h>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <type_traits>
#include <vector>

namespace hgraph
{
namespace
{
    template <typename Range>
    nb::list wrap_keys(Range&& keys) {
        nb::list out;
        for (auto key : keys) {
            out.append(key.to_python());
        }
        return out;
    }

    template <typename Range>
    nb::list wrap_output_values(Range&& values) {
        nb::list out;
        for (auto child : values) {
            out.append(wrap_output_view(std::move(child)));
        }
        return out;
    }

    template <typename Range>
    nb::list wrap_input_values(Range&& values) {
        nb::list out;
        for (auto child : values) {
            out.append(wrap_input_view(std::move(child)));
        }
        return out;
    }

    template <typename Range>
    nb::list wrap_output_items(Range&& items) {
        nb::list out;
        for (auto entry : items) {
            out.append(nb::make_tuple(entry.first.to_python(), wrap_output_view(std::move(entry.second))));
        }
        return out;
    }

    template <typename Range>
    nb::list wrap_input_items(Range&& items) {
        nb::list out;
        for (auto entry : items) {
            out.append(nb::make_tuple(entry.first.to_python(), wrap_input_view(std::move(entry.second))));
        }
        return out;
    }
}  // namespace

    // ===== PyTimeSeriesDictOutput Implementation =====

    value::Value PyTimeSeriesDictOutput::key_from_python(const nb::object &key) const {
        return tsd_key_from_python(key, output_view().ts_meta());
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
        auto child = output_view().as_dict().get(key_val.view());
        return child ? wrap_output_view(std::move(child)) : default_value;
    }

    nb::object PyTimeSeriesDictOutput::get_or_create(const nb::object &key) {
        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            return nb::none();
        }
        auto child = output_view().as_dict().get_or_create(key_val.view());
        return child ? wrap_output_view(std::move(child)) : nb::none();
    }

    void PyTimeSeriesDictOutput::create(const nb::object &item) {
        auto key_val = key_from_python(item);
        if (key_val.schema() != nullptr) {
            (void)output_view().as_dict().create(key_val.view());
        }
    }

    void PyTimeSeriesDictOutput::clear() {
        auto dict = output_view().as_dict();
        value::View current = output_view().as_ts_view().value();
        if (!current.valid() || !current.is_map()) {
            return;
        }

        std::vector<value::Value> keys;
        keys.reserve(current.as_map().size());
        for (value::View key : current.as_map().keys()) {
            keys.emplace_back(key.clone());
        }

        for (const auto& key : keys) {
            dict.remove(key.view());
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
        return output_view().as_dict().contains(key_val.view());
    }

    nb::object PyTimeSeriesDictOutput::key_set() const {
        return nb::cast(PyTimeSeriesSetOutput(output_view().as_dict().key_set()));
    }

    nb::object PyTimeSeriesDictOutput::keys() const {
        return wrap_keys(output_view().as_dict().keys());
    }

    nb::object PyTimeSeriesDictOutput::values() const {
        return wrap_output_values(output_view().as_dict().values());
    }

    nb::object PyTimeSeriesDictOutput::items() const {
        return wrap_output_items(output_view().as_dict().items());
    }

    nb::object PyTimeSeriesDictOutput::delta_value() const {
        return output_view().delta_to_python();
    }

    nb::object PyTimeSeriesDictOutput::modified_keys() const {
        return wrap_keys(output_view().as_dict().modified_keys());
    }

    nb::object PyTimeSeriesDictOutput::modified_values() const {
        return wrap_output_values(output_view().as_dict().modified_values());
    }

    nb::object PyTimeSeriesDictOutput::modified_items() const {
        return wrap_output_items(output_view().as_dict().modified_items());
    }

    bool PyTimeSeriesDictOutput::was_modified(const nb::object &key) const {
        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            return false;
        }
        return output_view().as_dict().was_modified(key_val.view());
    }

    nb::object PyTimeSeriesDictOutput::valid_keys() const {
        return wrap_keys(output_view().as_dict().valid_keys());
    }

    nb::object PyTimeSeriesDictOutput::valid_values() const {
        return wrap_output_values(output_view().as_dict().valid_values());
    }

    nb::object PyTimeSeriesDictOutput::valid_items() const {
        return wrap_output_items(output_view().as_dict().valid_items());
    }

    nb::object PyTimeSeriesDictOutput::added_keys() const {
        return wrap_keys(output_view().as_dict().added_keys());
    }

    nb::object PyTimeSeriesDictOutput::added_values() const {
        return wrap_output_values(output_view().as_dict().added_values());
    }

    nb::object PyTimeSeriesDictOutput::added_items() const {
        return wrap_output_items(output_view().as_dict().added_items());
    }

    bool PyTimeSeriesDictOutput::has_added() const {
        return output_view().as_dict().has_added();
    }

    bool PyTimeSeriesDictOutput::was_added(const nb::object &key) const {
        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            return false;
        }
        return output_view().as_dict().was_added(key_val.view());
    }

    nb::object PyTimeSeriesDictOutput::removed_keys() const {
        return wrap_keys(output_view().as_dict().removed_keys());
    }

    nb::object PyTimeSeriesDictOutput::removed_values() const {
        return wrap_output_values(output_view().as_dict().removed_values());
    }

    nb::object PyTimeSeriesDictOutput::removed_items() const {
        return wrap_output_items(output_view().as_dict().removed_items());
    }

    bool PyTimeSeriesDictOutput::has_removed() const {
        return output_view().as_dict().has_removed();
    }

    bool PyTimeSeriesDictOutput::was_removed(const nb::object &key) const {
        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            return false;
        }
        return output_view().as_dict().was_removed(key_val.view());
    }

    nb::object PyTimeSeriesDictOutput::key_from_value(const nb::object &value) const {
        auto *wrapped = nb::inst_ptr<PyTimeSeriesOutput>(value);
        if (wrapped == nullptr) {
            return nb::none();
        }

        const auto key = output_view().as_dict().key_for_child(wrapped->output_view());
        if (!key.has_value()) {
            return nb::none();
        }
        return key->to_python();
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
        if (ts_python_is_remove_marker(value)) {
            auto key_val = key_from_python(key);
            if (key_val.schema() == nullptr) {
                return;
            }

            auto dict = output_view().as_dict();
            if (ts_python_is_remove_if_exists_marker(value)) {
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
        TSOutputView out = output_view().as_dict().get_ref(key, requester);
        return out ? wrap_output_view(std::move(out)) : nb::none();
    }

    void PyTimeSeriesDictOutput::release_ref(const nb::object &key, const nb::object &requester) {
        output_view().as_dict().release_ref(key, requester);
    }

    // ===== PyTimeSeriesDictInput Implementation =====

    value::Value PyTimeSeriesDictInput::key_from_python(const nb::object &key) const {
        return tsd_key_from_python(key, input_view().ts_meta());
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
            if (std::getenv("HGRAPH_DEBUG_TSD_INPUT_CREATE") != nullptr) {
                value::View local = input_view().as_ts_view().value();
                const value::TypeMeta* map_key = (local.valid() && local.is_map()) ? local.as_map().key_type() : nullptr;
                value::View local_nav{};
                const ViewData& vd = input_view().as_ts_view().view_data();
                auto* value_root = static_cast<const value::Value*>(vd.value_data);
                if (value_root != nullptr && value_root->has_value()) {
                    local_nav = value_root->view();
                    for (size_t index : vd.path.indices) {
                        if (!local_nav.valid() || !local_nav.is_tuple()) {
                            local_nav = {};
                            break;
                        }
                        auto tuple = local_nav.as_tuple();
                        if (index >= tuple.size()) {
                            local_nav = {};
                            break;
                        }
                        local_nav = tuple.at(index);
                    }
                }
                const value::TypeMeta* nav_map_key = (local_nav.valid() && local_nav.is_map())
                                                         ? local_nav.as_map().key_type()
                                                         : nullptr;
                std::fprintf(stderr,
                             "[tsd_input.get_item] miss key=%s path=%s bound=%d local_valid=%d local_is_map=%d local_size=%zu key_schema=%p map_key=%p local_nav_valid=%d local_nav_is_map=%d local_nav_size=%zu local_nav_map_key=%p\n",
                             nb::cast<std::string>(nb::str(item)).c_str(),
                             input_view().short_path().to_string().c_str(),
                             input_view().is_bound() ? 1 : 0,
                             local.valid() ? 1 : 0,
                             (local.valid() && local.is_map()) ? 1 : 0,
                             (local.valid() && local.is_map()) ? local.as_map().size() : 0UL,
                             static_cast<const void*>(key_val.schema()),
                             static_cast<const void*>(map_key),
                             local_nav.valid() ? 1 : 0,
                             (local_nav.valid() && local_nav.is_map()) ? 1 : 0,
                             (local_nav.valid() && local_nav.is_map()) ? local_nav.as_map().size() : 0UL,
                             static_cast<const void*>(nav_map_key));
            }
            throw nb::key_error();
        }
        return wrap_input_view(std::move(child));
    }

    nb::object PyTimeSeriesDictInput::get(const nb::object &item, const nb::object &default_value) const {
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            return default_value;
        }
        auto child = input_view().as_dict().get(key_val.view());
        return child ? wrap_input_view(std::move(child)) : default_value;
    }

    nb::object PyTimeSeriesDictInput::get_or_create(const nb::object &key) {
        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            return nb::none();
        }
        auto child = input_view().as_dict().get_or_create(key_val.view());
        return child ? wrap_input_view(std::move(child)) : nb::none();
    }

    void PyTimeSeriesDictInput::create(const nb::object &item) {
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            return;
        }

        TSView child = input_view().as_ts_view().as_dict().create(key_val.view());
        if (std::getenv("HGRAPH_DEBUG_TSD_INPUT_CREATE") != nullptr) {
            value::View local = input_view().as_ts_view().value();
            const value::TypeMeta* key_type = (local.valid() && local.is_map()) ? local.as_map().key_type() : nullptr;
            const value::TypeMeta* value_type = (local.valid() && local.is_map()) ? local.as_map().value_type() : nullptr;
            const bool contains = local.valid() && local.is_map() && local.as_map().contains(key_val.view());
            const bool debug_dispatch = std::getenv("HGRAPH_DEBUG_TSD_CREATE_DISPATCH") != nullptr;
            std::fprintf(stderr,
                         "[tsd_input.create] key=%s child=%d bound=%d local_valid=%d local_is_map=%d local_size=%zu key_schema=%p map_key=%p value_type=%p contains=%d dbg_dispatch=%d\n",
                         nb::cast<std::string>(nb::str(item)).c_str(),
                         child ? 1 : 0,
                         input_view().is_bound() ? 1 : 0,
                         local.valid() ? 1 : 0,
                         (local.valid() && local.is_map()) ? 1 : 0,
                         (local.valid() && local.is_map()) ? local.as_map().size() : 0UL,
                         static_cast<const void*>(key_val.schema()),
                         static_cast<const void*>(key_type),
                         static_cast<const void*>(value_type),
                         contains ? 1 : 0,
                         debug_dispatch ? 1 : 0);
        }

    }

    nb::object PyTimeSeriesDictInput::iter() const {
        return nb::iter(keys());
    }

    bool PyTimeSeriesDictInput::contains(const nb::object &item) const {
        auto key_val = key_from_python(item);
        if (key_val.schema() == nullptr) {
            return false;
        }
        return input_view().as_dict().contains(key_val.view());
    }

    nb::object PyTimeSeriesDictInput::key_set() const {
        return nb::cast(PyTimeSeriesSetInput(input_view().as_dict().key_set()));
    }

    nb::object PyTimeSeriesDictInput::keys() const {
        return wrap_keys(input_view().as_dict().keys());
    }

    nb::object PyTimeSeriesDictInput::values() const {
        return wrap_input_values(input_view().as_dict().values());
    }

    nb::object PyTimeSeriesDictInput::items() const {
        return wrap_input_items(input_view().as_dict().items());
    }

    nb::object PyTimeSeriesDictInput::delta_value() const {
        return input_view().as_ts_view().delta_to_python();
    }

    nb::object PyTimeSeriesDictInput::modified_keys() const {
        return wrap_keys(input_view().as_dict().modified_keys());
    }

    nb::object PyTimeSeriesDictInput::modified_values() const {
        return wrap_input_values(input_view().as_dict().modified_values());
    }

    nb::object PyTimeSeriesDictInput::modified_items() const {
        return wrap_input_items(input_view().as_dict().modified_items());
    }

    bool PyTimeSeriesDictInput::was_modified(const nb::object &key) const {
        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            return false;
        }
        return input_view().as_dict().was_modified(key_val.view());
    }

    nb::object PyTimeSeriesDictInput::valid_keys() const {
        return wrap_keys(input_view().as_dict().valid_keys());
    }

    nb::object PyTimeSeriesDictInput::valid_values() const {
        return wrap_input_values(input_view().as_dict().valid_values());
    }

    nb::object PyTimeSeriesDictInput::valid_items() const {
        return wrap_input_items(input_view().as_dict().valid_items());
    }

    nb::object PyTimeSeriesDictInput::added_keys() const {
        return wrap_keys(input_view().as_dict().added_keys());
    }

    nb::object PyTimeSeriesDictInput::added_values() const {
        return wrap_input_values(input_view().as_dict().added_values());
    }

    nb::object PyTimeSeriesDictInput::added_items() const {
        return wrap_input_items(input_view().as_dict().added_items());
    }

    bool PyTimeSeriesDictInput::has_added() const {
        return input_view().as_dict().has_added();
    }

    bool PyTimeSeriesDictInput::was_added(const nb::object &key) const {
        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            return false;
        }
        return input_view().as_dict().was_added(key_val.view());
    }

    nb::object PyTimeSeriesDictInput::removed_keys() const {
        return wrap_keys(input_view().as_dict().removed_keys());
    }

    nb::object PyTimeSeriesDictInput::removed_values() const {
        return wrap_input_values(input_view().as_dict().removed_values());
    }

    nb::object PyTimeSeriesDictInput::removed_items() const {
        return wrap_input_items(input_view().as_dict().removed_items());
    }

    bool PyTimeSeriesDictInput::has_removed() const {
        return input_view().as_dict().has_removed();
    }

    bool PyTimeSeriesDictInput::was_removed(const nb::object &key) const {
        auto key_val = key_from_python(key);
        if (key_val.schema() == nullptr) {
            return false;
        }
        return input_view().as_dict().was_removed(key_val.view());
    }

    nb::object PyTimeSeriesDictInput::key_from_value(const nb::object &value) const {
        auto *wrapped = nb::inst_ptr<PyTimeSeriesInput>(value);
        if (wrapped == nullptr) {
            return nb::none();
        }

        const auto key = input_view().as_dict().key_for_child(wrapped->input_view());
        if (!key.has_value()) {
            return nb::none();
        }
        return key->to_python();
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
        (void)input_view().as_ts_view().as_dict().remove(key_val.view());
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
