#include "ts_ops_internal.h"

namespace hgraph {

namespace {

bool is_empty_mapping_payload(const nb::object& value_obj) {
    if (value_obj.is_none()) {
        return false;
    }
    if (nb::isinstance<nb::dict>(value_obj)) {
        return nb::len(value_obj) == 0;
    }

    nb::object items = nb::getattr(value_obj, "items", nb::none());
    if (items.is_none() || PyCallable_Check(items.ptr()) == 0) {
        return false;
    }

    nb::object iter_items = items();
    if (iter_items.is_none()) {
        return false;
    }

    Py_ssize_t size = PyObject_Length(iter_items.ptr());
    if (size < 0) {
        PyErr_Clear();
        return false;
    }
    return size == 0;
}

}  // namespace

void tsd_remove_empty_mapping_payloads(nb::dict& delta_out) {
    if (PyDict_Size(delta_out.ptr()) <= 0) {
        return;
    }

    nb::list keys_to_remove;
    for (const auto& kv : delta_out) {
        nb::object key_obj = nb::cast<nb::object>(kv.first);
        nb::object value_obj = nb::cast<nb::object>(kv.second);
        if (is_empty_mapping_payload(value_obj)) {
            keys_to_remove.append(key_obj);
        }
    }

    for (const auto& key_item : keys_to_remove) {
        nb::object key_obj = nb::cast<nb::object>(key_item);
        PyDict_DelItem(delta_out.ptr(), key_obj.ptr());
    }
}

void tsd_update_visible_key_history_from_delta(const ViewData& data,
                                               const TSMeta* current,
                                               const nb::dict& delta_out,
                                               engine_time_t current_time) {
    if (PyDict_Size(delta_out.ptr()) <= 0 || current == nullptr) {
        return;
    }

    const value::TypeMeta* key_type = current->key_type();
    if (key_type == nullptr) {
        return;
    }

    nb::object remove_marker = get_remove();
    nb::object remove_if_exists_marker = get_remove_if_exists();
    for (const auto& kv : delta_out) {
        nb::object py_key = nb::cast<nb::object>(kv.first);
        nb::object py_value = nb::cast<nb::object>(kv.second);

        value::Value key_value(key_type);
        key_value.emplace();
        try {
            key_type->ops().from_python(key_value.data(), py_key, key_type);
        } catch (...) {
            continue;
        }

        const View key_view = key_value.view();
        if (py_value.is(remove_marker) || py_value.is(remove_if_exists_marker)) {
            clear_tsd_visible_key_history(data, key_view);
        } else {
            mark_tsd_visible_key_history(data, key_view, current_time);
        }
    }
}

}  // namespace hgraph
