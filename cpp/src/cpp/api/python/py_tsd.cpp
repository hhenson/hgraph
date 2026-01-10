#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/ts_overlay_storage.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <fmt/format.h>

namespace hgraph
{
    // Helper to get current evaluation time from a view
    static engine_time_t get_tsd_current_time(const TSView& view) {
        Node* node = view.owning_node();
        if (!node) return MIN_DT;
        graph_ptr graph = node->graph();
        if (!graph) return MIN_DT;
        return graph->evaluation_time();
    }
    // ===== PyTimeSeriesDictOutput Implementation =====

    PyTimeSeriesDictOutput::PyTimeSeriesDictOutput(TSMutableView view)
        : PyTimeSeriesOutput(view) {}

    size_t PyTimeSeriesDictOutput::size() const {
        TSDView dict = _view.as_dict();
        return dict.size();
    }

    nb::object PyTimeSeriesDictOutput::get_item(const nb::object &item) const {
        throw std::runtime_error("PyTimeSeriesDictOutput::get_item not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::get(const nb::object &item, const nb::object &default_value) const {
        throw std::runtime_error("PyTimeSeriesDictOutput::get not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::get_or_create(const nb::object &key) {
        throw std::runtime_error("PyTimeSeriesDictOutput::get_or_create not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesDictOutput::create(const nb::object &item) {
        throw std::runtime_error("PyTimeSeriesDictOutput::create not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::iter() const {
        return nb::iter(keys());
    }

    bool PyTimeSeriesDictOutput::contains(const nb::object &item) const {
        TSDView dict = _view.as_dict();
        // Use O(1) contains lookup via the backing store
        return dict.contains_python(item);
    }

    nb::object PyTimeSeriesDictOutput::key_set() const {
        // TODO: Return a TimeSeriesSetInput wrapper for the key set
        throw std::runtime_error("PyTimeSeriesDictOutput::key_set not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::keys() const {
        TSDView dict = _view.as_dict();
        nb::list result;
        for (const auto& key : dict.keys()) {
            result.append(key.to_python());
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::values() const {
        TSDView dict = _view.as_dict();
        nb::list result;
        for (const auto& val : dict.ts_values()) {
            // Wrap each TSView as a Python wrapper
            result.append(wrap_input_view(val));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::items() const {
        TSDView dict = _view.as_dict();
        nb::list result;
        for (const auto& [key, val] : dict.items()) {
            nb::tuple pair = nb::make_tuple(key.to_python(), wrap_input_view(val));
            result.append(pair);
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::modified_keys() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::modified_keys not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::modified_values() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::modified_values not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::modified_items() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::modified_items not yet implemented for view-based wrappers");
    }

    bool PyTimeSeriesDictOutput::was_modified(const nb::object &key) const {
        throw std::runtime_error("PyTimeSeriesDictOutput::was_modified not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::valid_keys() const {
        TSDView dict = _view.as_dict();
        nb::list result;
        for (const auto& key : dict.valid_keys()) {
            result.append(key.to_python());
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::valid_values() const {
        TSDView dict = _view.as_dict();
        nb::list result;
        for (const auto& val : dict.valid_values()) {
            result.append(wrap_input_view(val));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::valid_items() const {
        TSDView dict = _view.as_dict();
        nb::list result;
        for (const auto& [key, val] : dict.valid_items()) {
            nb::tuple pair = nb::make_tuple(key.to_python(), wrap_input_view(val));
            result.append(pair);
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::added_keys() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        // Get delta view (need non-const for lazy cleanup)
        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        nb::list result;
        if (delta.valid()) {
            for (const auto& key : delta.added_keys()) {
                result.append(key.to_python());
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::added_values() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        nb::list result;
        if (delta.valid()) {
            // Get added values as raw ConstValueView and convert to Python
            for (const auto& val : delta.added_values()) {
                result.append(val.to_python());
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::added_items() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        nb::list result;
        if (delta.valid()) {
            auto keys = delta.added_keys();
            auto vals = delta.added_values();
            for (size_t i = 0; i < keys.size() && i < vals.size(); ++i) {
                nb::tuple pair = nb::make_tuple(keys[i].to_python(), vals[i].to_python());
                result.append(pair);
            }
        }
        return result;
    }

    bool PyTimeSeriesDictOutput::has_added() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        return delta.valid() && delta.has_added();
    }

    bool PyTimeSeriesDictOutput::was_added(const nb::object &key) const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        // Use view-layer method with time-check and C++ equality
        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        return mut_dict->was_added_python(key, current_time);
    }

    nb::object PyTimeSeriesDictOutput::removed_keys() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        nb::list result;
        if (delta.valid()) {
            for (const auto& key : delta.removed_keys()) {
                result.append(key.to_python());
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::removed_values() const {
        // MapDeltaView doesn't have removed_values() - removed values aren't stored
        // Return empty list for now
        return nb::list();
    }

    nb::object PyTimeSeriesDictOutput::removed_items() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        nb::list result;
        if (delta.valid()) {
            // We only have removed keys, not values
            for (const auto& key : delta.removed_keys()) {
                nb::tuple pair = nb::make_tuple(key.to_python(), nb::none());
                result.append(pair);
            }
        }
        return result;
    }

    bool PyTimeSeriesDictOutput::has_removed() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        return delta.valid() && delta.has_removed();
    }

    bool PyTimeSeriesDictOutput::was_removed(const nb::object &key) const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        // Use view-layer method with time-check and C++ equality
        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        return mut_dict->was_removed_python(key, current_time);
    }

    nb::object PyTimeSeriesDictOutput::key_from_value(const nb::object &value) const {
        return nb::none();
    }

    // value() and delta_value() are inherited from base - view layer handles TSD specifics

    nb::str PyTimeSeriesDictOutput::py_str() const {
        TSDView dict = _view.as_dict();
        auto str = fmt::format("TimeSeriesDictOutput@{:p}[size={}, valid={}]",
                               static_cast<const void*>(_view.value_view().data()),
                               dict.size(), _view.ts_valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesDictOutput::py_repr() const {
        return py_str();
    }

    void PyTimeSeriesDictOutput::set_item(const nb::object &key, const nb::object &value) {
        throw std::runtime_error("PyTimeSeriesDictOutput::set_item not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesDictOutput::del_item(const nb::object &key) {
        throw std::runtime_error("PyTimeSeriesDictOutput::del_item not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::pop(const nb::object &key, const nb::object &default_value) {
        throw std::runtime_error("PyTimeSeriesDictOutput::pop not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::get_ref(const nb::object &key, const nb::object &requester) {
        throw std::runtime_error("PyTimeSeriesDictOutput::get_ref not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesDictOutput::release_ref(const nb::object &key, const nb::object &requester) {
        throw std::runtime_error("PyTimeSeriesDictOutput::release_ref not yet implemented for view-based wrappers");
    }

    // ===== PyTimeSeriesDictInput Implementation =====

    PyTimeSeriesDictInput::PyTimeSeriesDictInput(TSView view)
        : PyTimeSeriesInput(view) {}

    size_t PyTimeSeriesDictInput::size() const {
        TSDView dict = _view.as_dict();
        return dict.size();
    }

    nb::object PyTimeSeriesDictInput::get_item(const nb::object &item) const {
        throw std::runtime_error("PyTimeSeriesDictInput::get_item not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::get(const nb::object &item, const nb::object &default_value) const {
        throw std::runtime_error("PyTimeSeriesDictInput::get not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::get_or_create(const nb::object &key) {
        throw std::runtime_error("PyTimeSeriesDictInput::get_or_create not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesDictInput::create(const nb::object &item) {
        throw std::runtime_error("PyTimeSeriesDictInput::create not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::iter() const {
        return nb::iter(keys());
    }

    bool PyTimeSeriesDictInput::contains(const nb::object &item) const {
        TSDView dict = _view.as_dict();
        // Use O(1) contains lookup via the backing store
        return dict.contains_python(item);
    }

    nb::object PyTimeSeriesDictInput::key_set() const {
        // TODO: Return a TimeSeriesSetInput wrapper for the key set
        throw std::runtime_error("PyTimeSeriesDictInput::key_set not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::keys() const {
        TSDView dict = _view.as_dict();
        nb::list result;
        for (const auto& key : dict.keys()) {
            result.append(key.to_python());
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::values() const {
        TSDView dict = _view.as_dict();
        nb::list result;
        for (const auto& val : dict.ts_values()) {
            result.append(wrap_input_view(val));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::items() const {
        TSDView dict = _view.as_dict();
        nb::list result;
        for (const auto& [key, val] : dict.items()) {
            nb::tuple pair = nb::make_tuple(key.to_python(), wrap_input_view(val));
            result.append(pair);
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::modified_keys() const {
        throw std::runtime_error("PyTimeSeriesDictInput::modified_keys not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::modified_values() const {
        throw std::runtime_error("PyTimeSeriesDictInput::modified_values not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::modified_items() const {
        throw std::runtime_error("PyTimeSeriesDictInput::modified_items not yet implemented for view-based wrappers");
    }

    bool PyTimeSeriesDictInput::was_modified(const nb::object &key) const {
        throw std::runtime_error("PyTimeSeriesDictInput::was_modified not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::valid_keys() const {
        TSDView dict = _view.as_dict();
        nb::list result;
        for (const auto& key : dict.valid_keys()) {
            result.append(key.to_python());
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::valid_values() const {
        TSDView dict = _view.as_dict();
        nb::list result;
        for (const auto& val : dict.valid_values()) {
            result.append(wrap_input_view(val));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::valid_items() const {
        TSDView dict = _view.as_dict();
        nb::list result;
        for (const auto& [key, val] : dict.valid_items()) {
            nb::tuple pair = nb::make_tuple(key.to_python(), wrap_input_view(val));
            result.append(pair);
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::added_keys() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        nb::list result;
        if (delta.valid()) {
            for (const auto& key : delta.added_keys()) {
                result.append(key.to_python());
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::added_values() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        nb::list result;
        if (delta.valid()) {
            // Get added values as raw ConstValueView and convert to Python
            for (const auto& val : delta.added_values()) {
                result.append(val.to_python());
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::added_items() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        nb::list result;
        if (delta.valid()) {
            auto keys = delta.added_keys();
            auto vals = delta.added_values();
            for (size_t i = 0; i < keys.size() && i < vals.size(); ++i) {
                nb::tuple pair = nb::make_tuple(keys[i].to_python(), vals[i].to_python());
                result.append(pair);
            }
        }
        return result;
    }

    bool PyTimeSeriesDictInput::has_added() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        return delta.valid() && delta.has_added();
    }

    bool PyTimeSeriesDictInput::was_added(const nb::object &key) const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        // Use view-layer method with time-check and C++ equality
        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        return mut_dict->was_added_python(key, current_time);
    }

    nb::object PyTimeSeriesDictInput::removed_keys() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        nb::list result;
        if (delta.valid()) {
            for (const auto& key : delta.removed_keys()) {
                result.append(key.to_python());
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::removed_values() const {
        // MapDeltaView doesn't have removed_values() - removed values aren't stored
        // Return empty list for now
        return nb::list();
    }

    nb::object PyTimeSeriesDictInput::removed_items() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        nb::list result;
        if (delta.valid()) {
            // We only have removed keys, not values
            for (const auto& key : delta.removed_keys()) {
                nb::tuple pair = nb::make_tuple(key.to_python(), nb::none());
                result.append(pair);
            }
        }
        return result;
    }

    bool PyTimeSeriesDictInput::has_removed() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        return delta.valid() && delta.has_removed();
    }

    bool PyTimeSeriesDictInput::was_removed(const nb::object &key) const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);

        // Use view-layer method with time-check and C++ equality
        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        return mut_dict->was_removed_python(key, current_time);
    }

    nb::object PyTimeSeriesDictInput::key_from_value(const nb::object &value) const {
        return nb::none();
    }

    // value() and delta_value() are inherited from base - view layer handles TSD specifics

    nb::str PyTimeSeriesDictInput::py_str() const {
        TSDView dict = _view.as_dict();
        auto str = fmt::format("TimeSeriesDictInput@{:p}[size={}, valid={}]",
                               static_cast<const void*>(_view.value_view().data()),
                               dict.size(), _view.ts_valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesDictInput::py_repr() const {
        return py_str();
    }

    void PyTimeSeriesDictInput::on_key_added(const nb::object &key) {
        throw std::runtime_error("PyTimeSeriesDictInput::on_key_added not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesDictInput::on_key_removed(const nb::object &key) {
        throw std::runtime_error("PyTimeSeriesDictInput::on_key_removed not yet implemented for view-based wrappers");
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
            // value and delta_value are inherited from base class (uses view layer dispatch)
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
            // value and delta_value are inherited from base class (uses view layer dispatch)
            .def_prop_ro("key_set", &PyTimeSeriesDictInput::key_set)
            .def("__str__", &PyTimeSeriesDictInput::py_str)
            .def("__repr__", &PyTimeSeriesDictInput::py_repr);
    }
}  // namespace hgraph
