#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/ts_overlay_storage.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <fmt/format.h>
#include <optional>

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
        // Return a C++ wrapper that provides TSS output interface for TSD keys
        // The wrapper stores a copy of the view directly, making it independent of this Python wrapper's lifetime
        auto* wrapper = new CppKeySetOutputWrapper(_view);
        return nb::cast(wrapper, nb::rv_policy::take_ownership);
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
        // For view-based wrappers, we simply return the element at the given key wrapped as an output.
        // Python's bind_output will create a TimeSeriesReference.make(output) from it.
        // Note: requester parameter is for reference counting which is not implemented in view-based system.

        TSDView dict = _view.as_dict();
        if (!dict.valid()) {
            return nb::none();
        }

        // Get key schema and convert Python key to value
        const TSDTypeMeta* dict_meta = dict.dict_meta();
        const value::TypeMeta* key_schema = dict_meta->key_type();

        if (!key_schema || !key_schema->ops) {
            throw std::runtime_error("PyTimeSeriesDictOutput::get_ref: invalid key schema");
        }

        // Create temp key storage and convert from Python
        std::vector<char> temp_key(key_schema->size);
        void* key_ptr = temp_key.data();

        if (key_schema->ops->construct) {
            key_schema->ops->construct(key_ptr, key_schema);
        }

        try {
            if (key_schema->ops->from_python) {
                key_schema->ops->from_python(key_ptr, key, key_schema);
            }
        } catch (...) {
            if (key_schema->ops->destruct) {
                key_schema->ops->destruct(key_ptr, key_schema);
            }
            throw std::runtime_error("PyTimeSeriesDictOutput::get_ref: failed to convert key from Python");
        }

        // Find the slot index for this key
        value::ConstMapView map_view = dict.value_view().as_map();
        value::ConstValueView key_view(key_ptr, key_schema);
        auto slot_idx = map_view.find_index(key_view);

        // Clean up key
        if (key_schema->ops->destruct) {
            key_schema->ops->destruct(key_ptr, key_schema);
        }

        if (!slot_idx) {
            throw std::out_of_range("PyTimeSeriesDictOutput::get_ref: key not found");
        }

        // Get the value view at this slot
        value::ConstValueView value_view = map_view.value_at(*slot_idx);
        const TSMeta* value_ts_type = dict_meta->value_ts_type();

        // Get value overlay if available (for mutable access)
        TSOverlayStorage* value_ov = nullptr;
        if (auto* map_ov = dict.map_overlay()) {
            value_ov = map_ov->value_overlay(*slot_idx);
        }

        // Create a mutable view for the element (using const_cast since underlying is mutable)
        TSMutableView elem_view(const_cast<void*>(value_view.data()), value_ts_type, value_ov);

        // Return wrapped as output
        return wrap_output_view(elem_view);
    }

    void PyTimeSeriesDictOutput::release_ref(const nb::object &key, const nb::object &requester) {
        // For view-based wrappers, release_ref is a no-op.
        // Reference counting is not implemented in the view-based system.
        // The view will remain valid until the underlying TSD is modified.
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
        // Return a C++ wrapper that provides TSS input interface for TSD keys
        // The wrapper stores a copy of the view directly, making it independent of this Python wrapper's lifetime
        auto* wrapper = new CppKeySetInputWrapper(_view);
        return nb::cast(wrapper, nb::rv_policy::take_ownership);
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

    // ===== CppKeySetOutputWrapper Implementation =====

    CppKeySetOutputWrapper::CppKeySetOutputWrapper(TSMutableView view)
        : _view(view), _is_empty_output_cache(nullptr) {}

    nb::object CppKeySetOutputWrapper::value() const {
        // Return current keys as frozenset
        TSDView dict = _view.as_dict();
        nb::set key_set;
        for (const auto& key : dict.keys()) {
            key_set.add(key.to_python());
        }
        return nb::frozenset(key_set);
    }

    nb::object CppKeySetOutputWrapper::delta_value() const {
        // For TSS, delta_value equals value
        return value();
    }

    bool CppKeySetOutputWrapper::valid() const {
        // The key_set is always valid because it's a property of the TSD that
        // always exists. It always has a value (the current set of keys, even if empty).
        // This matches Python where the key_set output is a TimeSeriesSetOutput
        // that's always considered valid as a property of the TSD.
        return true;
    }

    bool CppKeySetOutputWrapper::modified() const {
        return _view.modified();
    }

    nb::object CppKeySetOutputWrapper::last_modified_time() const {
        return nb::cast(_view.last_modified_time());
    }

    nb::object CppKeySetOutputWrapper::added() const {
        // Return added keys as frozenset
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);
        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        nb::set key_set;
        if (delta.valid()) {
            for (const auto& key : delta.added_keys()) {
                key_set.add(key.to_python());
            }
        }
        return nb::frozenset(key_set);
    }

    nb::object CppKeySetOutputWrapper::removed() const {
        // Return removed keys as frozenset
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);
        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        nb::set key_set;
        if (delta.valid()) {
            for (const auto& key : delta.removed_keys()) {
                key_set.add(key.to_python());
            }
        }
        return nb::frozenset(key_set);
    }

    bool CppKeySetOutputWrapper::was_added(const nb::object& item) const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);
        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        return mut_dict->was_added_python(item, current_time);
    }

    bool CppKeySetOutputWrapper::was_removed(const nb::object& item) const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);
        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        return mut_dict->was_removed_python(item, current_time);
    }

    size_t CppKeySetOutputWrapper::size() const {
        TSDView dict = _view.as_dict();
        return dict.size();
    }

    bool CppKeySetOutputWrapper::contains(const nb::object& item) const {
        TSDView dict = _view.as_dict();
        return dict.contains_python(item);
    }

    nb::object CppKeySetOutputWrapper::values() const {
        // Same as value() for TSS
        return value();
    }

    nb::object CppKeySetOutputWrapper::is_empty_output() {
        if (!_is_empty_output_cache) {
            _is_empty_output_cache = std::make_shared<CppKeySetIsEmptyOutput>(_view);
        }
        return nb::cast(_is_empty_output_cache.get(), nb::rv_policy::reference);
    }

    nb::object CppKeySetOutputWrapper::owning_node() const {
        Node* node = _view.owning_node();
        if (!node) return nb::none();
        return wrap_node(node->shared_from_this());
    }

    nb::object CppKeySetOutputWrapper::owning_graph() const {
        Node* node = _view.owning_node();
        if (!node) return nb::none();
        graph_ptr graph = node->graph();
        if (!graph) return nb::none();
        return wrap_graph(graph->shared_from_this());
    }

    nb::str CppKeySetOutputWrapper::py_str() const {
        auto str = fmt::format("CppKeySetOutputWrapper[size={}, valid={}]", size(), valid());
        return nb::str(str.c_str());
    }

    nb::str CppKeySetOutputWrapper::py_repr() const {
        return py_str();
    }

    // ===== CppKeySetIsEmptyOutput Implementation =====

    CppKeySetIsEmptyOutput::CppKeySetIsEmptyOutput(TSMutableView view)
        : _view(view), _last_empty_state(std::nullopt),
          _last_check_time(MIN_DT), _cached_modified(false) {}

    bool CppKeySetIsEmptyOutput::value() const {
        TSDView dict = _view.as_dict();
        return dict.size() == 0;
    }

    bool CppKeySetIsEmptyOutput::delta_value() const {
        return value();
    }

    bool CppKeySetIsEmptyOutput::valid() const {
        // The is_empty output is always valid because it's derived from the key_set
        // which always exists. It always has a valid boolean value (whether the set is empty).
        return true;
    }

    bool CppKeySetIsEmptyOutput::modified() {
        // Get current time to avoid recalculating on multiple calls per tick
        engine_time_t current_time = _view.last_modified_time();

        // If we already computed modified for this tick, return cached result
        if (_last_check_time == current_time && _last_empty_state.has_value()) {
            return _cached_modified;
        }

        // Update check time
        _last_check_time = current_time;

        // Check if the empty state changed
        TSDView dict = _view.as_dict();
        bool current_empty = (dict.size() == 0);

        // Handle first value - need to emit regardless of TSD modification
        if (!_last_empty_state.has_value()) {
            _last_empty_state = current_empty;
            _cached_modified = true;
            return true;
        }

        // For subsequent ticks, only report modified if TSD was modified AND value changed
        if (!_view.modified()) {
            _cached_modified = false;
            return false;
        }

        bool previous_empty = *_last_empty_state;
        _last_empty_state = current_empty;

        // Modified only if the empty state actually changed
        _cached_modified = (current_empty != previous_empty);
        return _cached_modified;
    }

    nb::object CppKeySetIsEmptyOutput::last_modified_time() const {
        return nb::cast(_view.last_modified_time());
    }

    bool CppKeySetIsEmptyOutput::all_valid() const {
        return valid();
    }

    nb::object CppKeySetIsEmptyOutput::owning_node() const {
        Node* node = _view.owning_node();
        if (!node) return nb::none();
        return wrap_node(node->shared_from_this());
    }

    nb::object CppKeySetIsEmptyOutput::owning_graph() const {
        Node* node = _view.owning_node();
        if (!node) return nb::none();
        graph_ptr graph = node->graph();
        if (!graph) return nb::none();
        return wrap_graph(graph->shared_from_this());
    }

    nb::str CppKeySetIsEmptyOutput::py_str() const {
        auto str = fmt::format("CppKeySetIsEmptyOutput[value={}, valid={}]", value(), valid());
        return nb::str(str.c_str());
    }

    nb::str CppKeySetIsEmptyOutput::py_repr() const {
        return py_str();
    }

    // ===== CppKeySetInputWrapper Implementation =====

    CppKeySetInputWrapper::CppKeySetInputWrapper(TSView view)
        : _view(view) {}

    nb::object CppKeySetInputWrapper::value() const {
        // Return current keys as frozenset
        TSDView dict = _view.as_dict();
        nb::set key_set;
        for (const auto& key : dict.keys()) {
            key_set.add(key.to_python());
        }
        return nb::frozenset(key_set);
    }

    nb::object CppKeySetInputWrapper::delta_value() const {
        return value();
    }

    bool CppKeySetInputWrapper::valid() const {
        return _view.ts_valid();
    }

    bool CppKeySetInputWrapper::modified() const {
        return _view.modified();
    }

    nb::object CppKeySetInputWrapper::last_modified_time() const {
        return nb::cast(_view.last_modified_time());
    }

    nb::object CppKeySetInputWrapper::added() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);
        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        nb::set key_set;
        if (delta.valid()) {
            for (const auto& key : delta.added_keys()) {
                key_set.add(key.to_python());
            }
        }
        return nb::frozenset(key_set);
    }

    nb::object CppKeySetInputWrapper::removed() const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);
        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        MapDeltaView delta = mut_dict->delta_view(current_time);

        nb::set key_set;
        if (delta.valid()) {
            for (const auto& key : delta.removed_keys()) {
                key_set.add(key.to_python());
            }
        }
        return nb::frozenset(key_set);
    }

    bool CppKeySetInputWrapper::was_added(const nb::object& item) const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);
        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        return mut_dict->was_added_python(item, current_time);
    }

    bool CppKeySetInputWrapper::was_removed(const nb::object& item) const {
        TSDView dict = _view.as_dict();
        engine_time_t current_time = get_tsd_current_time(_view);
        TSDView* mut_dict = const_cast<TSDView*>(&dict);
        return mut_dict->was_removed_python(item, current_time);
    }

    size_t CppKeySetInputWrapper::size() const {
        TSDView dict = _view.as_dict();
        return dict.size();
    }

    bool CppKeySetInputWrapper::contains(const nb::object& item) const {
        TSDView dict = _view.as_dict();
        return dict.contains_python(item);
    }

    nb::object CppKeySetInputWrapper::values() const {
        return value();
    }

    bool CppKeySetInputWrapper::all_valid() const {
        return valid();
    }

    bool CppKeySetInputWrapper::bound() const {
        // For a view-based input, check if it's bound by looking at the bound flag
        // In the view-based system, binding is tracked differently
        return _view.ts_valid();  // Simplified - valid means bound for passthrough inputs
    }

    bool CppKeySetInputWrapper::has_peer() const {
        // For view-based input, check if there's a peer connection
        return _view.ts_valid();  // Simplified
    }

    nb::object CppKeySetInputWrapper::output() const {
        // For key_set input wrappers, we need to return the output side's key_set
        // Create a CppKeySetOutputWrapper for the output view
        // Note: For inputs, we need to get the output side - this requires bound_output tracking
        // For now, return a new output wrapper with the same view (cast to mutable)
        TSMutableView mut_view = TSMutableView(const_cast<void*>(_view.value_view().data()),
                                                _view.ts_meta(), static_cast<TSValue*>(nullptr));
        auto* wrapper = new CppKeySetOutputWrapper(mut_view);
        return nb::cast(wrapper, nb::rv_policy::take_ownership);
    }

    nb::object CppKeySetInputWrapper::owning_node() const {
        Node* node = _view.owning_node();
        if (!node) return nb::none();
        return wrap_node(node->shared_from_this());
    }

    nb::object CppKeySetInputWrapper::owning_graph() const {
        Node* node = _view.owning_node();
        if (!node) return nb::none();
        graph_ptr graph = node->graph();
        if (!graph) return nb::none();
        return wrap_graph(graph->shared_from_this());
    }

    nb::str CppKeySetInputWrapper::py_str() const {
        auto str = fmt::format("CppKeySetInputWrapper[size={}, valid={}]", size(), valid());
        return nb::str(str.c_str());
    }

    nb::str CppKeySetInputWrapper::py_repr() const {
        return py_str();
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

        // Register CppKeySetOutputWrapper - TSS output interface for TSD keys
        nb::class_<CppKeySetOutputWrapper>(m, "CppKeySetOutputWrapper")
            .def_prop_ro("value", &CppKeySetOutputWrapper::value)
            .def_prop_ro("delta_value", &CppKeySetOutputWrapper::delta_value)
            .def_prop_ro("valid", &CppKeySetOutputWrapper::valid)
            .def_prop_ro("modified", &CppKeySetOutputWrapper::modified)
            .def_prop_ro("last_modified_time", &CppKeySetOutputWrapper::last_modified_time)
            .def("added", &CppKeySetOutputWrapper::added)
            .def("removed", &CppKeySetOutputWrapper::removed)
            .def("was_added", &CppKeySetOutputWrapper::was_added, "item"_a)
            .def("was_removed", &CppKeySetOutputWrapper::was_removed, "item"_a)
            .def("__len__", &CppKeySetOutputWrapper::size)
            .def("__contains__", &CppKeySetOutputWrapper::contains, "item"_a)
            .def("values", &CppKeySetOutputWrapper::values)
            .def("is_empty_output", &CppKeySetOutputWrapper::is_empty_output)
            .def_prop_ro("owning_node", &CppKeySetOutputWrapper::owning_node)
            .def_prop_ro("owning_graph", &CppKeySetOutputWrapper::owning_graph)
            .def("__str__", &CppKeySetOutputWrapper::py_str)
            .def("__repr__", &CppKeySetOutputWrapper::py_repr);

        // Register CppKeySetIsEmptyOutput - TS[bool] output for is_empty
        nb::class_<CppKeySetIsEmptyOutput>(m, "CppKeySetIsEmptyOutput")
            .def_prop_ro("value", &CppKeySetIsEmptyOutput::value)
            .def_prop_ro("delta_value", &CppKeySetIsEmptyOutput::delta_value)
            .def_prop_ro("valid", &CppKeySetIsEmptyOutput::valid)
            .def_prop_ro("modified", &CppKeySetIsEmptyOutput::modified)
            .def_prop_ro("last_modified_time", &CppKeySetIsEmptyOutput::last_modified_time)
            .def_prop_ro("all_valid", &CppKeySetIsEmptyOutput::all_valid)
            .def_prop_ro("owning_node", &CppKeySetIsEmptyOutput::owning_node)
            .def_prop_ro("owning_graph", &CppKeySetIsEmptyOutput::owning_graph)
            .def("__str__", &CppKeySetIsEmptyOutput::py_str)
            .def("__repr__", &CppKeySetIsEmptyOutput::py_repr);

        // Register CppKeySetInputWrapper - TSS input interface for TSD keys
        nb::class_<CppKeySetInputWrapper>(m, "CppKeySetInputWrapper")
            .def_prop_ro("value", &CppKeySetInputWrapper::value)
            .def_prop_ro("delta_value", &CppKeySetInputWrapper::delta_value)
            .def_prop_ro("valid", &CppKeySetInputWrapper::valid)
            .def_prop_ro("modified", &CppKeySetInputWrapper::modified)
            .def_prop_ro("last_modified_time", &CppKeySetInputWrapper::last_modified_time)
            .def("added", &CppKeySetInputWrapper::added)
            .def("removed", &CppKeySetInputWrapper::removed)
            .def("was_added", &CppKeySetInputWrapper::was_added, "item"_a)
            .def("was_removed", &CppKeySetInputWrapper::was_removed, "item"_a)
            .def("__len__", &CppKeySetInputWrapper::size)
            .def("__contains__", &CppKeySetInputWrapper::contains, "item"_a)
            .def("values", &CppKeySetInputWrapper::values)
            .def_prop_ro("all_valid", &CppKeySetInputWrapper::all_valid)
            .def_prop_ro("bound", &CppKeySetInputWrapper::bound)
            .def_prop_ro("has_peer", &CppKeySetInputWrapper::has_peer)
            .def_prop_ro("output", &CppKeySetInputWrapper::output)
            .def_prop_ro("owning_node", &CppKeySetInputWrapper::owning_node)
            .def_prop_ro("owning_graph", &CppKeySetInputWrapper::owning_graph)
            .def("__str__", &CppKeySetInputWrapper::py_str)
            .def("__repr__", &CppKeySetInputWrapper::py_repr);
    }
}  // namespace hgraph
