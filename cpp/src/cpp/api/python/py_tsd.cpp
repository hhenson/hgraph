#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/py_ts.h>
#include <hgraph/api/python/py_tsb.h>
#include <hgraph/api/python/py_tsl.h>
#include <hgraph/api/python/py_tss.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/value/dict_type.h>
#include <hgraph/types/value/python_conversion.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/constants.h>

namespace hgraph
{
    // Helper to create output wrapper from a TSView for dict entry access
    static nb::object create_tsd_output_wrapper_from_view(
            const node_s_ptr& node,
            value::TSView view,
            const TSMeta* meta) {
        if (!view.valid()) return nb::none();

        if (!meta) {
            return nb::cast(PyTimeSeriesOutput(node, std::move(view), nullptr, meta));
        }

        switch (meta->ts_kind) {
            case TSKind::TS:
                return nb::cast(PyTimeSeriesValueOutput(node, std::move(view), nullptr, meta));
            case TSKind::TSB:
                return nb::cast(PyTimeSeriesBundleOutput(node, std::move(view), nullptr, meta));
            case TSKind::TSL:
                return nb::cast(PyTimeSeriesListOutput(node, std::move(view), nullptr, meta));
            case TSKind::TSS:
                return nb::cast(PyTimeSeriesSetOutput(node, std::move(view), nullptr, meta));
            case TSKind::TSD:
                return nb::cast(PyTimeSeriesDictOutput(node, std::move(view), nullptr, meta));
            case TSKind::REF:
            default:
                return nb::cast(PyTimeSeriesOutput(node, std::move(view), nullptr, meta));
        }
    }

    // Helper to create input wrapper from a TSInputView for dict entry access
    static nb::object create_tsd_input_wrapper_from_view(
            const node_s_ptr& node,
            ts::TSInputView view,
            const TSMeta* meta) {
        if (!view.valid()) return nb::none();

        if (!meta) {
            return nb::cast(PyTimeSeriesInput(node, std::move(view), meta));
        }

        switch (meta->ts_kind) {
            case TSKind::TS:
                return nb::cast(PyTimeSeriesValueInput(node, std::move(view), nullptr, meta));
            case TSKind::TSB:
                return nb::cast(PyTimeSeriesBundleInput(node, std::move(view), nullptr, meta));
            case TSKind::TSL:
                return nb::cast(PyTimeSeriesListInput(node, std::move(view), nullptr, meta));
            case TSKind::TSS:
                return nb::cast(PyTimeSeriesSetInput(node, std::move(view), nullptr, meta));
            case TSKind::TSD:
                return nb::cast(PyTimeSeriesDictInput(node, std::move(view), nullptr, meta));
            case TSKind::REF:
            default:
                return nb::cast(PyTimeSeriesInput(node, std::move(view), meta));
        }
    }
    // PyTimeSeriesDictOutput implementations
    nb::object PyTimeSeriesDictOutput::get_item(const nb::object &item) const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::none();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::none();

        // Convert Python key to C++ storage for lookup
        auto* key_type = tsd_meta->key_type;
        std::vector<char> key_storage(key_type->size);
        key_type->ops->construct(key_storage.data(), key_type);
        value::value_from_python(key_storage.data(), item, key_type);

        // Use the view's entry() method to navigate to the value
        auto& mutable_view = const_cast<value::TSView&>(_view);
        auto entry_view = mutable_view.entry(value::ConstValueView(key_storage.data(), key_type));

        // Destruct the temporary key storage
        if (key_type->ops->destruct) {
            key_type->ops->destruct(key_storage.data(), key_type);
        }

        if (!entry_view.valid()) {
            throw std::out_of_range("Key not found in TSD");
        }

        return create_tsd_output_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type);
    }

    nb::object PyTimeSeriesDictOutput::get(const nb::object &item, const nb::object &default_value) const {
        try {
            return get_item(item);
        } catch (const std::out_of_range&) {
            return default_value;
        }
    }

    void PyTimeSeriesDictOutput::set_item(const nb::object &key, const nb::object &value) {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return;

        if (!_view.valid() || _view.kind() != value::TypeKind::Dict) return;

        auto* key_type = tsd_meta->key_type;
        auto* value_ts_type = tsd_meta->value_ts_type;
        auto* value_schema = value_ts_type ? value_ts_type->value_schema() : tsd_meta->dict_value_type;
        if (!value_schema) return;

        auto eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;

        // Convert key to C++ using Value for proper lifecycle
        value::Value key_value(key_type);
        if (key_type->ops && key_type->ops->from_python) {
            key_type->ops->from_python(key_value.data(), key.ptr(), key_type);
        }

        // Convert value to C++ using Value for proper lifecycle
        value::Value val_value(value_schema);
        if (value_schema->ops && value_schema->ops->from_python) {
            value_schema->ops->from_python(val_value.data(), value.ptr(), value_schema);
        }

        // Get dict storage and insert
        auto* storage = static_cast<value::DictStorage*>(_view.value_view().data());
        auto [is_new_key, idx] = storage->insert(key_value.data(), val_value.data());

        // Update tracker based on whether key is new or existing
        if (is_new_key) {
            _view.tracker().mark_dict_key_added(idx, eval_time);
        } else {
            _view.tracker().mark_dict_value_modified(idx, eval_time);
        }

        _view.mark_modified(eval_time);
    }

    void PyTimeSeriesDictOutput::del_item(const nb::object &key) {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return;

        if (!_view.valid() || _view.kind() != value::TypeKind::Dict) return;

        auto* key_type = tsd_meta->key_type;
        auto eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;

        // Convert key to C++ using Value for proper lifecycle management
        value::Value key_value(key_type);
        if (key_type->ops && key_type->ops->from_python) {
            key_type->ops->from_python(key_value.data(), key.ptr(), key_type);
        }

        // Get the dict storage
        auto* storage = static_cast<value::DictStorage*>(_view.value_view().data());

        // Find key's index before removal for tracker update
        auto opt_index = storage->find_index(key_value.data());
        if (!opt_index) return;  // Key not in dict

        size_t index = *opt_index;

        // Check if key was added this tick - handle add-then-remove cancellation
        bool was_added_this_tick = _view.tracker().dict_key_added_at(index, eval_time);

        if (was_added_this_tick) {
            // Add-then-remove same tick: cancel out, don't record as removed
            _view.tracker().remove_dict_entry_tracking(index);
        } else {
            // Key existed before tick: record for delta access
            _view.tracker().record_dict_key_removal(key_value.data(), eval_time);
        }

        // Remove from storage
        storage->remove(key_value.data());

        // Mark the dict as modified
        _view.mark_modified(eval_time);
    }

    nb::object PyTimeSeriesDictOutput::pop(const nb::object &key, const nb::object &default_value) {
        // TODO: Implement pop via view
        return default_value;
    }

    nb::object PyTimeSeriesDictOutput::get_or_create(const nb::object &key) {
        // TODO: Implement get_or_create via view
        return nb::none();
    }

    void PyTimeSeriesDictOutput::create(const nb::object &item) {
        // TODO: Implement create via view
    }

    nb::object PyTimeSeriesDictOutput::get_ref(const nb::object &key, const nb::object &requester) {
        // TODO: Implement get_ref via view
        return nb::none();
    }

    void PyTimeSeriesDictOutput::release_ref(const nb::object &key, const nb::object &requester) {
        // TODO: Implement release_ref via view
    }

    nb::object PyTimeSeriesDictOutput::key_from_value(const nb::object &value) const {
        // TODO: Implement key_from_value via view
        return nb::none();
    }

    bool PyTimeSeriesDictOutput::contains(const nb::object &item) const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return false;

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return false;

        // Convert Python key to C++ storage for lookup
        auto* key_type = tsd_meta->key_type;
        std::vector<char> key_storage(key_type->size);
        key_type->ops->construct(key_storage.data(), key_type);
        value::value_from_python(key_storage.data(), item, key_type);

        // Check if key exists in dict storage
        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        bool result = storage->contains(key_storage.data());

        // Destruct the temporary key storage
        if (key_type->ops->destruct) {
            key_type->ops->destruct(key_storage.data(), key_type);
        }

        return result;
    }

    size_t PyTimeSeriesDictOutput::size() const {
        return view().dict_size();
    }

    nb::object PyTimeSeriesDictOutput::iter() const {
        return nb::iter(keys());
    }

    nb::object PyTimeSeriesDictOutput::key_set() const {
        // Return a TSS view to the internal key set
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta || !tsd_meta->key_set_meta()) {
            // Fallback to frozenset if metadata not available
            return nb::frozenset(keys());
        }

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) {
            return nb::frozenset(keys());
        }

        // Get the key_set TSView from the TSD view
        auto& mutable_view = const_cast<value::TSView&>(_view);
        auto key_set_view = mutable_view.key_set();
        if (!key_set_view.valid()) {
            return nb::frozenset(keys());
        }

        // Create a PyTimeSeriesSetOutput wrapper for the key_set
        // The key_set shares the node and uses the TSSTypeMeta
        return nb::cast(PyTimeSeriesSetOutput(_node, std::move(key_set_view), nullptr,
                                               tsd_meta->key_set_meta()));
    }

    nb::object PyTimeSeriesDictOutput::keys() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        nb::list result;
        for (auto kv : *storage) {
            nb::object py_key = value::value_to_python(kv.key.ptr, key_type);
            result.append(py_key);
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::values() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (auto kv : *storage) {
            // Navigate to each entry
            auto entry_view = mutable_view.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid()) {
                result.append(create_tsd_output_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::items() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (auto kv : *storage) {
            nb::object py_key = value::value_to_python(kv.key.ptr, key_type);
            auto entry_view = mutable_view.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid()) {
                nb::object py_value = create_tsd_output_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type);
                result.append(nb::make_tuple(py_key, py_value));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::valid_keys() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (auto kv : *storage) {
            auto entry_view = mutable_view.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid() && entry_view.has_value()) {
                nb::object py_key = value::value_to_python(kv.key.ptr, key_type);
                result.append(py_key);
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::valid_values() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (auto kv : *storage) {
            auto entry_view = mutable_view.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid() && entry_view.has_value()) {
                result.append(create_tsd_output_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::valid_items() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (auto kv : *storage) {
            auto entry_view = mutable_view.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid() && entry_view.has_value()) {
                nb::object py_key = value::value_to_python(kv.key.ptr, key_type);
                nb::object py_value = create_tsd_output_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type);
                result.append(nb::make_tuple(py_key, py_value));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::modified_keys() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (auto kv : *storage) {
            auto entry_view = mutable_view.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid() && entry_view.modified_at(eval_time)) {
                nb::object py_key = value::value_to_python(kv.key.ptr, key_type);
                result.append(py_key);
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::modified_values() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (auto kv : *storage) {
            auto entry_view = mutable_view.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid() && entry_view.modified_at(eval_time)) {
                result.append(create_tsd_output_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::modified_items() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (auto kv : *storage) {
            auto entry_view = mutable_view.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid() && entry_view.modified_at(eval_time)) {
                nb::object py_key = value::value_to_python(kv.key.ptr, key_type);
                nb::object py_value = create_tsd_output_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type);
                result.append(nb::make_tuple(py_key, py_value));
            }
        }
        return result;
    }

    bool PyTimeSeriesDictOutput::was_modified(const nb::object &key) const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return false;

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return false;

        auto* key_type = tsd_meta->key_type;
        std::vector<char> key_storage(key_type->size);
        key_type->ops->construct(key_storage.data(), key_type);
        value::value_from_python(key_storage.data(), key, key_type);

        auto& mutable_view = const_cast<value::TSView&>(_view);
        auto entry_view = mutable_view.entry(value::ConstValueView(key_storage.data(), key_type));

        if (key_type->ops->destruct) {
            key_type->ops->destruct(key_storage.data(), key_type);
        }

        if (!entry_view.valid()) return false;
        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        return entry_view.modified_at(eval_time);
    }

    nb::object PyTimeSeriesDictOutput::added_keys() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid()) return nb::list();

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        auto tracker = v.tracker();

        nb::list result;
        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());

        // Iterate through all entries and check which were added at current time
        size_t idx = 0;
        for (auto kv : *storage) {
            if (tracker.dict_key_added_at(idx, eval_time)) {
                nb::object py_key = value::value_to_python(kv.key.ptr, tsd_meta->key_type);
                result.append(py_key);
            }
            ++idx;
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::added_values() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid()) return nb::list();

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        auto tracker = v.tracker();

        nb::list result;
        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());

        size_t idx = 0;
        for (auto kv : *storage) {
            if (tracker.dict_key_added_at(idx, eval_time)) {
                auto entry_view = v.entry(value::ConstValueView(kv.key.ptr, tsd_meta->key_type));
                result.append(create_tsd_output_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type));
            }
            ++idx;
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::added_items() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid()) return nb::list();

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        auto tracker = v.tracker();

        nb::list result;
        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());

        size_t idx = 0;
        for (auto kv : *storage) {
            if (tracker.dict_key_added_at(idx, eval_time)) {
                nb::object py_key = value::value_to_python(kv.key.ptr, tsd_meta->key_type);
                auto entry_view = v.entry(value::ConstValueView(kv.key.ptr, tsd_meta->key_type));
                nb::tuple item = nb::make_tuple(py_key, create_tsd_output_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type));
                result.append(item);
            }
            ++idx;
        }
        return result;
    }

    bool PyTimeSeriesDictOutput::has_added() const {
        auto v = view();
        if (!v.valid()) return false;

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        auto tracker = v.tracker();
        return tracker.dict_added_count(eval_time) > 0;
    }

    bool PyTimeSeriesDictOutput::was_added(const nb::object &key) const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return false;

        auto v = view();
        if (!v.valid()) return false;

        // Convert key to C++ storage
        auto* key_type = tsd_meta->key_type;
        std::vector<char> key_storage(key_type->size);
        key_type->ops->construct(key_storage.data(), key_type);
        value::value_from_python(key_storage.data(), key, key_type);

        // Find index
        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto idx = storage->find_index(key_storage.data());

        if (key_type->ops->destruct) {
            key_type->ops->destruct(key_storage.data(), key_type);
        }

        if (!idx) return false;

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        auto tracker = v.tracker();
        return tracker.dict_key_added_at(*idx, eval_time);
    }

    nb::object PyTimeSeriesDictOutput::removed_keys() const {
        auto v = view();
        if (!v.valid()) return nb::list();

        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto tracker = v.tracker();
        size_t removed_count = tracker.dict_removed_count();

        // Get storage for key type info
        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = storage->keys().element_type();

        nb::list result;
        for (size_t i = 0; i < removed_count; ++i) {
            const void* key_ptr = tracker.dict_removed_key(i);
            if (key_ptr) {
                nb::object py_key = value::value_to_python(key_ptr, key_type);
                result.append(py_key);
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::removed_values() const {
        // Removed keys are stored in tracker's removed storage, values are gone
        // Return empty list to match expected semantics
        return nb::list();
    }

    nb::object PyTimeSeriesDictOutput::removed_items() const {
        // Return keys only with None values (values were removed)
        auto v = view();
        if (!v.valid()) return nb::list();

        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto tracker = v.tracker();
        size_t removed_count = tracker.dict_removed_count();

        // Get storage for key type info
        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = storage->keys().element_type();

        nb::list result;
        for (size_t i = 0; i < removed_count; ++i) {
            const void* key_ptr = tracker.dict_removed_key(i);
            if (key_ptr) {
                nb::object py_key = value::value_to_python(key_ptr, key_type);
                nb::tuple item = nb::make_tuple(py_key, nb::none());
                result.append(item);
            }
        }
        return result;
    }

    bool PyTimeSeriesDictOutput::has_removed() const {
        auto v = view();
        if (!v.valid()) return false;

        auto tracker = v.tracker();
        return tracker.dict_removed_count() > 0;
    }

    bool PyTimeSeriesDictOutput::was_removed(const nb::object &key) const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return false;

        auto v = view();
        if (!v.valid()) return false;

        // Convert key to C++ storage
        auto* key_type = tsd_meta->key_type;
        std::vector<char> key_storage(key_type->size);
        key_type->ops->construct(key_storage.data(), key_type);
        value::value_from_python(key_storage.data(), key, key_type);

        // Check if key was removed using tracker's was_key_removed
        auto tracker = v.tracker();
        bool found = tracker.dict_was_key_removed(key_storage.data());

        if (key_type->ops->destruct) {
            key_type->ops->destruct(key_storage.data(), key_type);
        }

        return found;
    }

    nb::str PyTimeSeriesDictOutput::py_str() const {
        return nb::str("TSD{...}");
    }

    nb::str PyTimeSeriesDictOutput::py_repr() const {
        return py_str();
    }

    void PyTimeSeriesDictOutput::set_value(nb::object py_value) {
        if (!_view.valid() || !_meta) return;

        auto eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;

        if (py_value.is_none()) {
            _view.mark_invalid();
            return;
        }

        // Check if it's a dict-like object
        if (!nb::isinstance<nb::dict>(py_value) && !nb::hasattr(py_value, "items")) {
            // Fall back to base class behavior for non-dict types
            PyTimeSeriesOutput::set_value(std::move(py_value));
            return;
        }

        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) {
            PyTimeSeriesOutput::set_value(std::move(py_value));
            return;
        }

        if (_view.kind() != value::TypeKind::Dict) {
            PyTimeSeriesOutput::set_value(std::move(py_value));
            return;
        }

        // Get type metadata
        auto* key_type = tsd_meta->key_type;
        auto* value_ts_type = tsd_meta->value_ts_type;
        auto* value_schema = value_ts_type ? value_ts_type->value_schema() : tsd_meta->dict_value_type;
        if (!value_schema) {
            PyTimeSeriesOutput::set_value(std::move(py_value));
            return;
        }

        // Get REMOVE and REMOVE_IF_EXISTS sentinels
        nb::object remove_sentinel = get_remove();
        nb::object remove_if_exists_sentinel = get_remove_if_exists();

        // Iterate through the dict and handle each key-value pair
        nb::object items_obj;
        if (nb::isinstance<nb::dict>(py_value)) {
            items_obj = nb::cast<nb::dict>(py_value).attr("items")();
        } else {
            items_obj = py_value.attr("items")();
        }

        // Check if the dict is empty - we still need to mark as modified
        bool is_empty = (nb::len(items_obj) == 0);
        if (is_empty && !_view.has_value()) {
            _view.mark_modified(eval_time);
            return;
        }

        // Get dict storage
        auto* storage = static_cast<value::DictStorage*>(_view.value_view().data());
        bool had_changes = false;

        for (auto item : items_obj) {
            nb::tuple kv = nb::cast<nb::tuple>(item);
            nb::object key = kv[0];
            nb::object val = kv[1];

            // Skip None values
            if (val.is_none()) {
                continue;
            }

            // Check for REMOVE sentinels using 'is' comparison
            if (val.is(remove_sentinel)) {
                del_item(key);
                had_changes = true;
            } else if (val.is(remove_if_exists_sentinel)) {
                if (contains(key)) {
                    del_item(key);
                    had_changes = true;
                }
            } else {
                // Normal value - insert or update entry using core storage with tracker

                // Convert key to C++ using Value for proper lifecycle
                value::Value key_value(key_type);
                if (key_type->ops && key_type->ops->from_python) {
                    key_type->ops->from_python(key_value.data(), key.ptr(), key_type);
                }

                // Convert value to C++ using Value for proper lifecycle
                value::Value val_value(value_schema);
                if (value_schema->ops && value_schema->ops->from_python) {
                    value_schema->ops->from_python(val_value.data(), val.ptr(), value_schema);
                }

                // Insert into storage and get index info
                auto [is_new_key, idx] = storage->insert(key_value.data(), val_value.data());

                // Update tracker based on whether key is new or existing
                if (is_new_key) {
                    _view.tracker().mark_dict_key_added(idx, eval_time);
                } else {
                    _view.tracker().mark_dict_value_modified(idx, eval_time);
                }

                had_changes = true;
            }
        }

        // Mark as modified if there were changes
        if (had_changes) {
            _view.mark_modified(eval_time);
        }
    }

    void PyTimeSeriesDictOutput::apply_result(nb::object value) {
        if (value.is_none()) return;
        set_value(std::move(value));
    }

    // PyTimeSeriesDictInput implementations
    nb::object PyTimeSeriesDictInput::get_item(const nb::object &item) const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::none();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::none();

        // Convert Python key to C++ storage for lookup
        auto* key_type = tsd_meta->key_type;
        std::vector<char> key_storage(key_type->size);
        key_type->ops->construct(key_storage.data(), key_type);
        value::value_from_python(key_storage.data(), item, key_type);

        // Use the view's entry() method to navigate to the value
        auto entry_view = v.entry(value::ConstValueView(key_storage.data(), key_type));

        // Destruct the temporary key storage
        if (key_type->ops->destruct) {
            key_type->ops->destruct(key_storage.data(), key_type);
        }

        if (!entry_view.valid()) {
            throw std::out_of_range("Key not found in TSD");
        }

        return create_tsd_input_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type);
    }

    nb::object PyTimeSeriesDictInput::get(const nb::object &item, const nb::object &default_value) const {
        try {
            return get_item(item);
        } catch (const std::out_of_range&) {
            return default_value;
        }
    }

    nb::object PyTimeSeriesDictInput::get_or_create(const nb::object &key) {
        // TODO: Implement get_or_create for inputs
        return nb::none();
    }

    void PyTimeSeriesDictInput::create(const nb::object &item) {
        // TODO: Implement create for inputs
    }

    void PyTimeSeriesDictInput::on_key_added(const nb::object &key) {
        // TODO: Implement on_key_added callback
    }

    void PyTimeSeriesDictInput::on_key_removed(const nb::object &key) {
        // TODO: Implement on_key_removed callback
    }

    nb::object PyTimeSeriesDictInput::key_from_value(const nb::object &value) const {
        // TODO: Implement key_from_value
        return nb::none();
    }

    bool PyTimeSeriesDictInput::contains(const nb::object &item) const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return false;

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return false;

        // Convert Python key to C++ storage for lookup
        auto* key_type = tsd_meta->key_type;
        std::vector<char> key_storage(key_type->size);
        key_type->ops->construct(key_storage.data(), key_type);
        value::value_from_python(key_storage.data(), item, key_type);

        // Check if key exists in dict storage
        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        bool result = storage->contains(key_storage.data());

        // Destruct the temporary key storage
        if (key_type->ops->destruct) {
            key_type->ops->destruct(key_storage.data(), key_type);
        }

        return result;
    }

    size_t PyTimeSeriesDictInput::size() const {
        return view().dict_size();
    }

    nb::object PyTimeSeriesDictInput::iter() const {
        return nb::iter(keys());
    }

    nb::object PyTimeSeriesDictInput::key_set() const {
        // Return a TSS view to the internal key set
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta || !tsd_meta->key_set_meta()) {
            // Fallback to frozenset if metadata not available
            return nb::frozenset(keys());
        }

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) {
            return nb::frozenset(keys());
        }

        // For inputs, get the bound output and access its key_set view
        auto* bound = v.bound_output();
        if (!bound) {
            return nb::frozenset(keys());
        }

        // Get the output's view and navigate to key_set
        auto output_view = bound->view();
        auto key_set_view = output_view.key_set();
        if (!key_set_view.valid()) {
            return nb::frozenset(keys());
        }

        // Create a PyTimeSeriesSetOutput wrapper for the key_set (read-only access)
        // Note: inputs expose the bound output's key_set, which is read-only
        return nb::cast(PyTimeSeriesSetOutput(_node, std::move(key_set_view), nullptr,
                                               tsd_meta->key_set_meta()));
    }

    nb::object PyTimeSeriesDictInput::keys() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        nb::list result;
        for (auto kv : *storage) {
            nb::object py_key = value::value_to_python(kv.key.ptr, key_type);
            result.append(py_key);
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::values() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        nb::list result;
        for (auto kv : *storage) {
            auto entry_view = v.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid()) {
                result.append(create_tsd_input_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::items() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        nb::list result;
        for (auto kv : *storage) {
            nb::object py_key = value::value_to_python(kv.key.ptr, key_type);
            auto entry_view = v.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid()) {
                nb::object py_value = create_tsd_input_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type);
                result.append(nb::make_tuple(py_key, py_value));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::valid_keys() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        nb::list result;
        for (auto kv : *storage) {
            auto entry_view = v.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid() && entry_view.has_value()) {
                nb::object py_key = value::value_to_python(kv.key.ptr, key_type);
                result.append(py_key);
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::valid_values() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        nb::list result;
        for (auto kv : *storage) {
            auto entry_view = v.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid() && entry_view.has_value()) {
                result.append(create_tsd_input_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::valid_items() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        nb::list result;
        for (auto kv : *storage) {
            auto entry_view = v.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid() && entry_view.has_value()) {
                nb::object py_key = value::value_to_python(kv.key.ptr, key_type);
                nb::object py_value = create_tsd_input_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type);
                result.append(nb::make_tuple(py_key, py_value));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::modified_keys() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        for (auto kv : *storage) {
            auto entry_view = v.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid() && entry_view.modified_at(eval_time)) {
                nb::object py_key = value::value_to_python(kv.key.ptr, key_type);
                result.append(py_key);
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::modified_values() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        for (auto kv : *storage) {
            auto entry_view = v.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid() && entry_view.modified_at(eval_time)) {
                result.append(create_tsd_input_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::modified_items() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return nb::list();

        auto* storage = static_cast<const value::DictStorage*>(v.value_view().data());
        auto* key_type = tsd_meta->key_type;

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        for (auto kv : *storage) {
            auto entry_view = v.entry(value::ConstValueView(kv.key.ptr, key_type));
            if (entry_view.valid() && entry_view.modified_at(eval_time)) {
                nb::object py_key = value::value_to_python(kv.key.ptr, key_type);
                nb::object py_value = create_tsd_input_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type);
                result.append(nb::make_tuple(py_key, py_value));
            }
        }
        return result;
    }

    bool PyTimeSeriesDictInput::was_modified(const nb::object &key) const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return false;

        auto v = view();
        if (!v.valid() || v.kind() != value::TypeKind::Dict) return false;

        auto* key_type = tsd_meta->key_type;
        std::vector<char> key_storage(key_type->size);
        key_type->ops->construct(key_storage.data(), key_type);
        value::value_from_python(key_storage.data(), key, key_type);

        auto entry_view = v.entry(value::ConstValueView(key_storage.data(), key_type));

        if (key_type->ops->destruct) {
            key_type->ops->destruct(key_storage.data(), key_type);
        }

        if (!entry_view.valid()) return false;
        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        return entry_view.modified_at(eval_time);
    }

    nb::object PyTimeSeriesDictInput::added_keys() const {
        // Delegate to bound output's added_keys
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid()) return nb::list();

        auto* bound = v.bound_output();
        if (!bound) return nb::list();

        auto output_view = bound->view();
        if (!output_view.valid()) return nb::list();

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        auto tracker = output_view.tracker();

        nb::list result;
        auto* storage = static_cast<const value::DictStorage*>(output_view.value_view().data());

        size_t idx = 0;
        for (auto kv : *storage) {
            if (tracker.dict_key_added_at(idx, eval_time)) {
                nb::object py_key = value::value_to_python(kv.key.ptr, tsd_meta->key_type);
                result.append(py_key);
            }
            ++idx;
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::added_values() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid()) return nb::list();

        auto* bound = v.bound_output();
        if (!bound) return nb::list();

        auto output_view = bound->view();
        if (!output_view.valid()) return nb::list();

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        auto tracker = output_view.tracker();

        nb::list result;
        auto* storage = static_cast<const value::DictStorage*>(output_view.value_view().data());

        size_t idx = 0;
        for (auto kv : *storage) {
            if (tracker.dict_key_added_at(idx, eval_time)) {
                auto entry_view = v.entry(value::ConstValueView(kv.key.ptr, tsd_meta->key_type));
                result.append(create_tsd_input_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type));
            }
            ++idx;
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::added_items() const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto v = view();
        if (!v.valid()) return nb::list();

        auto* bound = v.bound_output();
        if (!bound) return nb::list();

        auto output_view = bound->view();
        if (!output_view.valid()) return nb::list();

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        auto tracker = output_view.tracker();

        nb::list result;
        auto* storage = static_cast<const value::DictStorage*>(output_view.value_view().data());

        size_t idx = 0;
        for (auto kv : *storage) {
            if (tracker.dict_key_added_at(idx, eval_time)) {
                nb::object py_key = value::value_to_python(kv.key.ptr, tsd_meta->key_type);
                auto entry_view = v.entry(value::ConstValueView(kv.key.ptr, tsd_meta->key_type));
                nb::tuple item = nb::make_tuple(py_key, create_tsd_input_wrapper_from_view(_node, std::move(entry_view), tsd_meta->value_ts_type));
                result.append(item);
            }
            ++idx;
        }
        return result;
    }

    bool PyTimeSeriesDictInput::has_added() const {
        auto v = view();
        if (!v.valid()) return false;

        auto* bound = v.bound_output();
        if (!bound) return false;

        auto output_view = bound->view();
        if (!output_view.valid()) return false;

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        auto tracker = output_view.tracker();
        return tracker.dict_added_count(eval_time) > 0;
    }

    bool PyTimeSeriesDictInput::was_added(const nb::object &key) const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return false;

        auto v = view();
        if (!v.valid()) return false;

        auto* bound = v.bound_output();
        if (!bound) return false;

        auto output_view = bound->view();
        if (!output_view.valid()) return false;

        // Convert key to C++ storage
        auto* key_type = tsd_meta->key_type;
        std::vector<char> key_storage(key_type->size);
        key_type->ops->construct(key_storage.data(), key_type);
        value::value_from_python(key_storage.data(), key, key_type);

        // Find index
        auto* storage = static_cast<const value::DictStorage*>(output_view.value_view().data());
        auto idx = storage->find_index(key_storage.data());

        if (key_type->ops->destruct) {
            key_type->ops->destruct(key_storage.data(), key_type);
        }

        if (!idx) return false;

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        auto tracker = output_view.tracker();
        return tracker.dict_key_added_at(*idx, eval_time);
    }

    nb::object PyTimeSeriesDictInput::removed_keys() const {
        // For inputs, removed_keys requires accessing delta info from bound output
        auto v = view();
        if (!v.valid()) return nb::list();

        auto* bound = v.bound_output();
        if (!bound) return nb::list();

        auto output_view = bound->view();
        if (!output_view.valid()) return nb::list();

        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto tracker = output_view.tracker();
        size_t removed_count = tracker.dict_removed_count();

        // Get storage from output view for key type info
        auto* storage = static_cast<const value::DictStorage*>(output_view.value_view().data());
        auto* key_type = storage->keys().element_type();

        nb::list result;
        for (size_t i = 0; i < removed_count; ++i) {
            const void* key_ptr = tracker.dict_removed_key(i);
            if (key_ptr) {
                nb::object py_key = value::value_to_python(key_ptr, key_type);
                result.append(py_key);
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::removed_values() const {
        // Removed keys are stored in tracker's removed storage, values are gone
        // Return empty list to match expected semantics
        return nb::list();
    }

    nb::object PyTimeSeriesDictInput::removed_items() const {
        // For inputs, return removed keys with None values
        auto v = view();
        if (!v.valid()) return nb::list();

        auto* bound = v.bound_output();
        if (!bound) return nb::list();

        auto output_view = bound->view();
        if (!output_view.valid()) return nb::list();

        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return nb::list();

        auto tracker = output_view.tracker();
        size_t removed_count = tracker.dict_removed_count();

        // Get storage from output view for key type info
        auto* storage = static_cast<const value::DictStorage*>(output_view.value_view().data());
        auto* key_type = storage->keys().element_type();

        nb::list result;
        for (size_t i = 0; i < removed_count; ++i) {
            const void* key_ptr = tracker.dict_removed_key(i);
            if (key_ptr) {
                nb::object py_key = value::value_to_python(key_ptr, key_type);
                // Value was removed - return (key, None)
                result.append(nb::make_tuple(py_key, nb::none()));
            }
        }
        return result;
    }

    bool PyTimeSeriesDictInput::has_removed() const {
        auto v = view();
        if (!v.valid()) return false;

        auto* bound = v.bound_output();
        if (!bound) return false;

        auto output_view = bound->view();
        if (!output_view.valid()) return false;

        auto tracker = output_view.tracker();
        return tracker.dict_removed_count() > 0;
    }

    bool PyTimeSeriesDictInput::was_removed(const nb::object &key) const {
        auto* tsd_meta = dynamic_cast<const TSDTypeMeta*>(_meta);
        if (!tsd_meta) return false;

        auto v = view();
        if (!v.valid()) return false;

        auto* bound = v.bound_output();
        if (!bound) return false;

        auto output_view = bound->view();
        if (!output_view.valid()) return false;

        // Convert key to C++ storage
        auto* key_type = tsd_meta->key_type;
        std::vector<char> key_storage(key_type->size);
        key_type->ops->construct(key_storage.data(), key_type);
        value::value_from_python(key_storage.data(), key, key_type);

        // Check if key was removed using tracker's was_key_removed
        auto tracker = output_view.tracker();
        bool found = tracker.dict_was_key_removed(key_storage.data());

        if (key_type->ops->destruct) {
            key_type->ops->destruct(key_storage.data(), key_type);
        }

        return found;
    }

    nb::str PyTimeSeriesDictInput::py_str() const {
        return nb::str("TSD{...}");
    }

    nb::str PyTimeSeriesDictInput::py_repr() const {
        return py_str();
    }

    void tsd_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictOutput, PyTimeSeriesOutput>(m, "TimeSeriesDictOutput")
            .def("__contains__", &PyTimeSeriesDictOutput::contains)
            .def("__getitem__", &PyTimeSeriesDictOutput::get_item)
            .def("__setitem__", &PyTimeSeriesDictOutput::set_item)
            .def("__delitem__", &PyTimeSeriesDictOutput::del_item)
            .def("__len__", [](const PyTimeSeriesDictOutput& self) { return self.size(); })
            .def("__iter__", &PyTimeSeriesDictOutput::iter)
            .def("get", &PyTimeSeriesDictOutput::get)
            .def("get_or_create", &PyTimeSeriesDictOutput::get_or_create)
            .def("create", &PyTimeSeriesDictOutput::create)
            .def("pop", &PyTimeSeriesDictOutput::pop)
            .def("get_ref", &PyTimeSeriesDictOutput::get_ref)
            .def("release_ref", &PyTimeSeriesDictOutput::release_ref)
            .def("key_from_value", &PyTimeSeriesDictOutput::key_from_value)
            .def_prop_ro("key_set", &PyTimeSeriesDictOutput::key_set)
            .def("keys", &PyTimeSeriesDictOutput::keys)
            .def("values", &PyTimeSeriesDictOutput::values)
            .def("items", &PyTimeSeriesDictOutput::items)
            .def("valid_keys", &PyTimeSeriesDictOutput::valid_keys)
            .def("valid_values", &PyTimeSeriesDictOutput::valid_values)
            .def("valid_items", &PyTimeSeriesDictOutput::valid_items)
            .def("modified_keys", &PyTimeSeriesDictOutput::modified_keys)
            .def("modified_values", &PyTimeSeriesDictOutput::modified_values)
            .def("modified_items", &PyTimeSeriesDictOutput::modified_items)
            .def("was_modified", &PyTimeSeriesDictOutput::was_modified)
            .def("added_keys", &PyTimeSeriesDictOutput::added_keys)
            .def("added_values", &PyTimeSeriesDictOutput::added_values)
            .def("added_items", &PyTimeSeriesDictOutput::added_items)
            .def_prop_ro("has_added", &PyTimeSeriesDictOutput::has_added)
            .def("was_added", &PyTimeSeriesDictOutput::was_added)
            .def("removed_keys", &PyTimeSeriesDictOutput::removed_keys)
            .def("removed_values", &PyTimeSeriesDictOutput::removed_values)
            .def("removed_items", &PyTimeSeriesDictOutput::removed_items)
            .def_prop_ro("has_removed", &PyTimeSeriesDictOutput::has_removed)
            .def("was_removed", &PyTimeSeriesDictOutput::was_removed)
            .def("__str__", &PyTimeSeriesDictOutput::py_str)
            .def("__repr__", &PyTimeSeriesDictOutput::py_repr)
            // Override value property to use TSD-aware set_value that handles REMOVE
            .def_prop_rw("value",
                [](const PyTimeSeriesDictOutput& self) { return self.PyTimeSeriesOutput::value(); },
                &PyTimeSeriesDictOutput::set_value,
                nb::arg("value").none())
            .def("apply_result", &PyTimeSeriesDictOutput::apply_result, nb::arg("value").none());

        nb::class_<PyTimeSeriesDictInput, PyTimeSeriesInput>(m, "TimeSeriesDictInput")
            .def("__contains__", &PyTimeSeriesDictInput::contains)
            .def("__getitem__", &PyTimeSeriesDictInput::get_item)
            .def("__len__", [](const PyTimeSeriesDictInput& self) { return self.size(); })
            .def("__iter__", &PyTimeSeriesDictInput::iter)
            .def("get", &PyTimeSeriesDictInput::get)
            .def("get_or_create", &PyTimeSeriesDictInput::get_or_create)
            .def("create", &PyTimeSeriesDictInput::create)
            .def("on_key_added", &PyTimeSeriesDictInput::on_key_added)
            .def("on_key_removed", &PyTimeSeriesDictInput::on_key_removed)
            .def("key_from_value", &PyTimeSeriesDictInput::key_from_value)
            .def_prop_ro("key_set", &PyTimeSeriesDictInput::key_set)
            .def("keys", &PyTimeSeriesDictInput::keys)
            .def("values", &PyTimeSeriesDictInput::values)
            .def("items", &PyTimeSeriesDictInput::items)
            .def("valid_keys", &PyTimeSeriesDictInput::valid_keys)
            .def("valid_values", &PyTimeSeriesDictInput::valid_values)
            .def("valid_items", &PyTimeSeriesDictInput::valid_items)
            .def("modified_keys", &PyTimeSeriesDictInput::modified_keys)
            .def("modified_values", &PyTimeSeriesDictInput::modified_values)
            .def("modified_items", &PyTimeSeriesDictInput::modified_items)
            .def("was_modified", &PyTimeSeriesDictInput::was_modified)
            .def("added_keys", &PyTimeSeriesDictInput::added_keys)
            .def("added_values", &PyTimeSeriesDictInput::added_values)
            .def("added_items", &PyTimeSeriesDictInput::added_items)
            .def_prop_ro("has_added", &PyTimeSeriesDictInput::has_added)
            .def("was_added", &PyTimeSeriesDictInput::was_added)
            .def("removed_keys", &PyTimeSeriesDictInput::removed_keys)
            .def("removed_values", &PyTimeSeriesDictInput::removed_values)
            .def("removed_items", &PyTimeSeriesDictInput::removed_items)
            .def_prop_ro("has_removed", &PyTimeSeriesDictInput::has_removed)
            .def("was_removed", &PyTimeSeriesDictInput::was_removed)
            .def("__str__", &PyTimeSeriesDictInput::py_str)
            .def("__repr__", &PyTimeSeriesDictInput::py_repr);
    }

}  // namespace hgraph
