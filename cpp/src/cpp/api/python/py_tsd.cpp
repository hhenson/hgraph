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
        // TODO: Implement set_item via view - requires set_python_value on entry
    }

    void PyTimeSeriesDictOutput::del_item(const nb::object &key) {
        // TODO: Implement del_item via view
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
        // Returns a frozenset of keys
        return nb::frozenset(keys());
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
        // TODO: Implement added_keys via delta tracking
        return nb::list();
    }

    nb::object PyTimeSeriesDictOutput::added_values() const {
        // TODO: Implement added_values via delta tracking
        return nb::list();
    }

    nb::object PyTimeSeriesDictOutput::added_items() const {
        // TODO: Implement added_items via delta tracking
        return nb::list();
    }

    bool PyTimeSeriesDictOutput::has_added() const {
        // TODO: Implement has_added via delta tracking
        return false;
    }

    bool PyTimeSeriesDictOutput::was_added(const nb::object &key) const {
        // TODO: Implement was_added via delta tracking
        return false;
    }

    nb::object PyTimeSeriesDictOutput::removed_keys() const {
        // TODO: Implement removed_keys via delta tracking
        return nb::list();
    }

    nb::object PyTimeSeriesDictOutput::removed_values() const {
        // TODO: Implement removed_values via delta tracking
        return nb::list();
    }

    nb::object PyTimeSeriesDictOutput::removed_items() const {
        // TODO: Implement removed_items via delta tracking
        return nb::list();
    }

    bool PyTimeSeriesDictOutput::has_removed() const {
        // TODO: Implement has_removed via delta tracking
        return false;
    }

    bool PyTimeSeriesDictOutput::was_removed(const nb::object &key) const {
        // TODO: Implement was_removed via delta tracking
        return false;
    }

    nb::str PyTimeSeriesDictOutput::py_str() const {
        return nb::str("TSD{...}");
    }

    nb::str PyTimeSeriesDictOutput::py_repr() const {
        return py_str();
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
        return nb::frozenset(keys());
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
        // TODO: Implement added_keys via delta tracking
        return nb::list();
    }

    nb::object PyTimeSeriesDictInput::added_values() const {
        // TODO: Implement added_values via delta tracking
        return nb::list();
    }

    nb::object PyTimeSeriesDictInput::added_items() const {
        // TODO: Implement added_items via delta tracking
        return nb::list();
    }

    bool PyTimeSeriesDictInput::has_added() const {
        // TODO: Implement has_added via delta tracking
        return false;
    }

    bool PyTimeSeriesDictInput::was_added(const nb::object &key) const {
        // TODO: Implement was_added via delta tracking
        return false;
    }

    nb::object PyTimeSeriesDictInput::removed_keys() const {
        // TODO: Implement removed_keys via delta tracking
        return nb::list();
    }

    nb::object PyTimeSeriesDictInput::removed_values() const {
        // TODO: Implement removed_values via delta tracking
        return nb::list();
    }

    nb::object PyTimeSeriesDictInput::removed_items() const {
        // TODO: Implement removed_items via delta tracking
        return nb::list();
    }

    bool PyTimeSeriesDictInput::has_removed() const {
        // TODO: Implement has_removed via delta tracking
        return false;
    }

    bool PyTimeSeriesDictInput::was_removed(const nb::object &key) const {
        // TODO: Implement was_removed via delta tracking
        return false;
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
            .def("__repr__", &PyTimeSeriesDictOutput::py_repr);

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
