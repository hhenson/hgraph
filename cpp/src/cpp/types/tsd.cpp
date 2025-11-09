#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsd.h>
#include <hgraph/util/string_utils.h>

#include <fmt/format.h>
#include <nanobind/nanobind.h>

namespace hgraph
{
    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::apply_result(nb::object value) {
        // Ensure any Python API interaction occurs under the GIL and protect against exceptions
        if (value.is_none()) { return; }
        py_set_value(value);
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::can_apply_result(nb::object result) {
        if (result.is_none()) { return true; }
        if (!nb::cast<bool>(nb::bool_(result))) { return true; }

        auto remove           = get_remove();
        auto remove_if_exists = get_remove_if_exists();

        auto items = (nb::isinstance<nb::dict>(result) ? nb::iter(nb::cast<nb::dict>(result).items())
                      : nb::hasattr(result, "items")   ? nb::iter(nb::getattr(result, "items")())
                                                       : nb::iter(result));
        for (const auto &pair : items) {
            auto k  = pair[0];
            auto v_ = pair[1];
            if (v_.is_none()) { continue; }
            auto k_ = nb::cast<T_Key>(k);
            if (v_.is(remove) || v_.is(remove_if_exists)) {
                if (v_.is(remove_if_exists) && !contains(k_)) { continue; }
                if (was_modified(k_)) { return false; }
            } else {
                if (was_removed(k_)) { return false; }
                if (contains(k_)) {
                    if (!operator[](k_)->can_apply_result(nb::borrow(v_))) { return false; }
                }
            }
        }
        return true;
    }

    template <typename T_Key>
    nb::object TimeSeriesDictOutput_T<T_Key>::py_get(const nb::object &item, const nb::object &default_value) const {
        auto key{nb::cast<T_Key>(item)};
        // Do NOT call operator[] here on a const object; the const overload uses at() and will throw if missing.
        // Python dict.get semantics require returning the default when the key is absent.
        auto it = _ts_values.find(key);
        if (it != _ts_values.end()) {
            // Return existing value directly
            return nb::cast(it->second.get());
        }
        return default_value;
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::py_create(const nb::object &item) {
        _create(nb::cast<T_Key>(item));
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_iter() { return py_keys(); }

    template <typename T_Key>
    void TimeSeriesDictOutput_T<T_Key>::mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) {
        if (last_modified_time() < modified_time) { _modified_items.clear(); }

        if (&child != &key_set()) {
            auto key{key_from_value(&child)};
            _key_updated(key);
        }

        BaseTimeSeriesOutput::mark_child_modified(child, modified_time);
    }

    template <typename T_Key> const typename TimeSeriesDictOutput_T<T_Key>::map_type &TimeSeriesDictOutput_T<T_Key>::value() const {
        return _ts_values;
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::remove_value(const key_type &key, bool raise_if_not_found) {
        auto it{_ts_values.find(key)};
        if (it == _ts_values.end()) {
            if (raise_if_not_found) {
                std::string key_str{to_string(key)};
                std::string err_msg = "TSD[" + std::string(typeid(T_Key).name()) + "] Key '" + key_str + "' does not exist";
                throw nb::key_error(err_msg.c_str());
            }
            return;
        }

        bool was_added = key_set_t().was_added(key);
        key_set_t().remove(key);

        for (auto &observer : _key_observers) { observer->on_key_removed(key); }

        auto item{it->second};
        _ts_values.erase(it);
        // Is this wise clearing the item if we are going to track the remove? What if we need the last known value?
        item->clear();
        if (!was_added) { _removed_items.emplace(key, item); }
        // Note: TSS key_set handles all added/removed tracking via key_set_t().remove()
        _remove_key_value(key, item);
        _ref_ts_feature.update(key);
        _modified_items.erase(key);

        // Schedule cleanup notification only once per evaluation cycle
        auto et = owning_graph()->evaluation_clock()->evaluation_time();

        if (_last_cleanup_time < et) {
            _last_cleanup_time = et;
            owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([this]() { _clear_key_changes(); });
        }
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::_clear_key_tracking() { _ts_values_to_keys.clear(); }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::_add_key_value(const key_type &key, const value_type &value) {
        // Need to cast away const and upcast to interface type for map key
        _ts_values_to_keys.insert({const_cast<TimeSeriesOutput*>(static_cast<const TimeSeriesOutput*>(value.get())), key});
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::_key_updated(const key_type &key) {
        auto it{_ts_values.find(key)};
        if (it != _ts_values.end()) {
            _modified_items[key] = it->second;  // Use operator[] instead of insert to ensure update
        } else {
            // In the output we have noted that removed keys can still update, so we can't raise an exception here
        }
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::_remove_key_value(const key_type &key, const value_type &value) {
        // Need to cast away const and upcast to interface type for map key
        _ts_values_to_keys.erase(const_cast<TimeSeriesOutput*>(static_cast<const TimeSeriesOutput*>(value.get())));
    }

    template <typename T_Key>
    TimeSeriesDictOutput_T<T_Key>::TimeSeriesDictOutput_T(const node_ptr &parent, output_builder_ptr ts_builder,
                                                          output_builder_ptr ts_ref_builder)
        : TimeSeriesDictOutput(parent), _key_set{new TimeSeriesSetOutput_T<T_Key>(this)}, _ts_builder{std::move(ts_builder)},
          _ts_ref_builder{std::move(ts_ref_builder)},
          _ref_ts_feature{this,
                          _ts_ref_builder,
                          [](const TimeSeriesOutput &output, TimeSeriesOutput &result_output, const key_type &key) {
                              auto &output_t{dynamic_cast<const TimeSeriesDictOutput_T<T_Key> &>(output)};
                              auto  it = output_t._ts_values.find(key);
                              if (it != output_t._ts_values.end()) {
                                  auto r{TimeSeriesReference::make(it->second)};
                                  auto r_val{nb::cast(r)};
                                  result_output.apply_result(r_val);
                              } else {
                                  // Key removed: propagate empty reference and mark modified to match Python semantics
                                  auto r{TimeSeriesReference::make()};
                                  auto r_val{nb::cast(r)};
                                  result_output.apply_result(r_val);
                              }
                          },
                          {}} {
        _key_set->re_parent(this);
    }

    template <typename T_Key>
    TimeSeriesDictOutput_T<T_Key>::TimeSeriesDictOutput_T(const time_series_type_ptr &parent, output_builder_ptr ts_builder,
                                                          output_builder_ptr ts_ref_builder)
        : TimeSeriesDictOutput(static_cast<const TimeSeriesType::ptr &>(parent)), _key_set{new TimeSeriesSetOutput_T<T_Key>(this)},
          _ts_builder{std::move(ts_builder)}, _ts_ref_builder{std::move(ts_ref_builder)},
          _ref_ts_feature{this,
                          _ts_ref_builder,
                          [](const TimeSeriesOutput &output, TimeSeriesOutput &result_output, const key_type &key) {
                              auto &output_t{dynamic_cast<const TimeSeriesDictOutput_T<T_Key> &>(output)};
                              auto  it = output_t._ts_values.find(key);
                              if (it != output_t._ts_values.end()) {
                                  auto r{TimeSeriesReference::make(it->second)};
                                  auto r_val{nb::cast(r)};
                                  result_output.apply_result(r_val);
                              } else {
                                  // Key removed: propagate empty reference and mark modified to match Python semantics
                                  auto r{TimeSeriesReference::make()};
                                  auto r_val{nb::cast(r)};
                                  result_output.apply_result(r_val);
                              }
                          },
                          {}} {
        _key_set->re_parent(this);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::py_set_value(nb::object value) {
        if (value.is_none()) {
            invalidate();
            return;
        }
        if (!valid() and !nb::bool_(value)) {
            // If we are not valid, then an empty value can be used to force modified
            key_set().mark_modified();
            return;
        }

        if (!valid()) {
            key_set().mark_modified();  // Even if we tick an empty set, we still need to mark this as modified
        }
        auto item_attr = nb::getattr(value, "items", nb::none());
        auto remove{get_remove()};
        auto remove_if_exists{get_remove_if_exists()};

        // Python semantics for this layer: treat both mappings and iterables as deltas.
        // Operators (convert/collect/combine) are responsible for emitting REMOVE entries when needed.
        nb::iterator items = item_attr.is_none() ? nb::iter(value) : nb::iter(item_attr());
        for (const auto &kv : items) {
            auto k_ = nb::cast<T_Key>(kv[0]);
            auto v  = kv[1];
            if (v.is_none()) { continue; }
            if (v.is(remove) || v.is(remove_if_exists)) {
                if (contains(k_)) {
                    erase(k_);
                } else {
                    // Python semantics: REMOVE on missing -> KeyError; REMOVE_IF_EXISTS on missing -> no-op
                    if (v.is(remove)) {
                        std::string msg = "TSD key not found for REMOVE: " + to_string(k_);
                        throw nb::key_error(msg.c_str());
                    }  // else REMOVE_IF_EXISTS: do nothing
                }
            } else {
                _get_or_create(k_)->py_set_value(v);
            }
        }
    }

    template <typename T_Key> nb::object TimeSeriesDictOutput_T<T_Key>::py_value() const {
        auto v{nb::dict()};
        for (const auto &[key, value] : _ts_values) {
            if (value->valid()) { v[nb::cast(key)] = value->py_value(); }
        }
        // Return frozendict snapshot of all valid items (mirror Python `_tsd.py:value`)
        return get_frozendict()(v);
    }

    template <typename T_Key> nb::object TimeSeriesDictOutput_T<T_Key>::py_delta_value() const {
        auto delta_value{nb::dict()};
        for (const auto &[key, value] : _ts_values) {
            if (value->modified() && value->valid()) { delta_value[nb::cast(key)] = value->py_delta_value(); }
        }
        if (!_removed_items.empty()) {
            auto removed{get_remove()};
            for (const auto &[key, _] : _removed_items) { delta_value[nb::cast(key)] = removed; }
        }
        // Return frozendict of modified-valid entries plus removed keys (mirror Python `_tsd.py:delta_value`)
        return get_frozendict()(delta_value);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::clear() {
        key_set().clear();
        for (auto &[_, value] : _ts_values) { value->clear(); }

        _removed_items.clear();
        std::swap(_removed_items, _ts_values);
        _clear_key_tracking();
        _ref_ts_feature.update_all(std::views::keys(_removed_items).begin(), std::views::keys(_removed_items).end());
        _modified_items.clear();

        for (auto &observer : _key_observers) {
            for (const auto &[key, _] : _removed_items) { observer->on_key_removed(key); }
        }
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::invalidate() {
        for (auto &[_, value] : _ts_values) { value->invalidate(); }
        mark_invalid();
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::copy_from_output(const TimeSeriesOutput &output) {
        auto       &other = dynamic_cast<const TimeSeriesDictOutput_T<T_Key> &>(output);
        const auto &key_set_value{key_set_t().value()};
        const auto &other_key_set_value{other.key_set_t().value()};

        std::vector<T_Key> to_remove;
        for (const auto &key : key_set_value) {
            if (other_key_set_value.contains(key)) { to_remove.push_back(key); }
        }
        for (const auto &k : to_remove) { erase(k); }
        for (const auto &[k, v] : other._ts_values) { _get_or_create(k)->copy_from_output(*v); }
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::copy_from_input(const TimeSeriesInput &input) {
        auto       &dict_input = dynamic_cast<const TimeSeriesDictInput_T<T_Key> &>(input);
        const auto &key_set_value{key_set_t().value()};
        const auto &other_key_set_value{dict_input.key_set_t().value()};

        // Remove keys that are in output but NOT in input (matching Python: self.key_set.value - input.key_set.value)
        std::vector<T_Key> to_remove;
        for (const auto &key : key_set_value) {
            if (!other_key_set_value.contains(key)) { to_remove.push_back(key); }
        }
        for (const auto &k : to_remove) { erase(k); }
        for (const auto &[k, v_input] : dict_input.value()) { _get_or_create(k)->copy_from_input(*v_input); }
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::has_added() const { return !added_keys().empty(); }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::has_removed() const { return !_removed_items.empty(); }

    template <typename T_Key> size_t TimeSeriesDictOutput_T<T_Key>::size() const { return _ts_values.size(); }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::py_contains(const nb::object &item) const {
        return contains(nb::cast<T_Key>(item));
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::contains(const key_type &item) const {
        return _ts_values.find(item) != _ts_values.end();
    }

    template <typename T_Key> nb::object TimeSeriesDictOutput_T<T_Key>::py_get_item(const nb::object &item) const {
        auto KET_SET_ID = nb::module_::import_("hgraph").attr("KEY_SET_ID");
        if (KET_SET_ID.is(item)) { return nb::cast(_key_set); }
        auto  k  = nb::cast<T_Key>(item);
        auto  ts = operator[](k);
        auto *py = ts->self_py();
        if (py) return nb::borrow(py);
        // Return value directly
        return nb::cast(ts.get());
    }

    template <typename T_Key> nb::object TimeSeriesDictOutput_T<T_Key>::py_get_or_create(const nb::object &key) {
        auto ts = _get_or_create(nb::cast<T_Key>(key));
        return nb::cast(ts.get());
    }

    template <typename T_Key>
    typename TimeSeriesDictOutput_T<T_Key>::ts_type_ptr TimeSeriesDictOutput_T<T_Key>::operator[](const key_type &item) {
        return _get_or_create(item);
    }

    template <typename T_Key>
    typename TimeSeriesDictOutput_T<T_Key>::ts_type_ptr TimeSeriesDictOutput_T<T_Key>::operator[](const key_type &item) const {
        return _ts_values.at(item);
    }

    template <typename T_Key>
    typename TimeSeriesDictOutput_T<T_Key>::const_item_iterator TimeSeriesDictOutput_T<T_Key>::begin() const {
        return _ts_values.begin();
    }

    template <typename T_Key> typename TimeSeriesDictOutput_T<T_Key>::item_iterator TimeSeriesDictOutput_T<T_Key>::begin() {
        return _ts_values.begin();
    }

    template <typename T_Key>
    typename TimeSeriesDictOutput_T<T_Key>::const_item_iterator TimeSeriesDictOutput_T<T_Key>::end() const {
        return _ts_values.end();
    }

    template <typename T_Key> typename TimeSeriesDictOutput_T<T_Key>::item_iterator TimeSeriesDictOutput_T<T_Key>::end() {
        return _ts_values.end();
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_keys() const {
        // Return keys from _ts_values map, not from key_set (which only contains current tick's modifications)
        // This matches Python behavior: __iter__ returns iter(self._ts_values)
        return nb::make_key_iterator(nb::type<map_type>(), "KeyIterator", begin(), end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_values() const {
        return nb::make_value_iterator(nb::type<map_type>(), "ValueIterator", begin(), end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_items() const {
        return nb::make_iterator(nb::type<map_type>(), "ItemIterator", begin(), end());
    }

    template <typename T_Key>
    const typename TimeSeriesDictOutput_T<T_Key>::map_type &TimeSeriesDictOutput_T<T_Key>::modified_items() const {
        return modified() ? _modified_items : _empty;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_modified_keys() const {
        const auto &_modified{modified_items()};
        return nb::make_key_iterator(nb::type<map_type>(), "ModifiedKeyIterator", _modified.begin(), _modified.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_modified_values() const {
        const auto &_modified{modified_items()};
        return nb::make_value_iterator(nb::type<map_type>(), "ModifiedValueIterator", _modified.begin(), _modified.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_modified_items() const {
        const auto &_modified{modified_items()};
        return nb::make_iterator(nb::type<map_type>(), "ModifiedItemIterator", _modified.begin(), _modified.end());
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::py_was_modified(const nb::object &key) const {
        return was_modified(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::was_modified(const key_type &key) const {
        return _modified_items.find(key) != _modified_items.end();
    }

    template <typename T_Key> auto TimeSeriesDictOutput_T<T_Key>::valid_items() const {
        return _ts_values | std::views::filter([](const auto &item) { return item.second->valid(); });
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_valid_keys() const {
        auto valid_items_ = valid_items();
        return nb::make_key_iterator(nb::type<map_type>(), "ValidKeyIterator", valid_items_.begin(), valid_items_.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_valid_values() const {
        auto valid_items_ = valid_items();
        return nb::make_value_iterator(nb::type<map_type>(), "ValidValueIterator", valid_items_.begin(), valid_items_.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_valid_items() const {
        auto valid_items_ = valid_items();
        return nb::make_iterator(nb::type<map_type>(), "ValidItemIterator", valid_items_.begin(), valid_items_.end());
    }

    template <typename T_Key>
    const typename TimeSeriesDictOutput_T<T_Key>::k_set_type &TimeSeriesDictOutput_T<T_Key>::added_keys() const {
        // Delegate to key_set.added() to get the actual added keys from the underlying TSS
        return key_set_t().added();
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_added_keys() const {
        auto const &_keys{added_keys()};
        return nb::make_iterator(nb::type<k_set_type>(), "AddedKeyIterator", _keys.begin(), _keys.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_added_values() const {
        nb::list l{};
        for (const auto &k : added_keys()) { l.append(nb::cast(operator[](k).get())); }
        return nb::iter(l);
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_added_items() const {
        nb::dict d{};
        for (const auto &k : added_keys()) { d[nb::cast(k)] = (nb::cast(operator[](k).get())); }
        return nb::iter(d);
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::py_was_added(const nb::object &key) const {
        return was_added(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::was_added(const key_type &key) const {
        auto const &_keys{added_keys()};
        return _keys.find(key) != _keys.end();
    }

    template <typename T_Key>
    const typename TimeSeriesDictOutput_T<T_Key>::map_type &TimeSeriesDictOutput_T<T_Key>::removed_items() const {
        return _removed_items;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_removed_keys() const {
        const auto &_removed{removed_items()};
        return nb::make_key_iterator(nb::type<map_type>(), "RemovedKeyIterator", _removed.begin(), _removed.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_removed_values() const {
        const auto &_removed{removed_items()};
        return nb::make_value_iterator(nb::type<map_type>(), "RemovedValueIterator", _removed.begin(), _removed.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictOutput_T<T_Key>::py_removed_items() const {
        const auto &_removed{removed_items()};
        return nb::make_iterator(nb::type<map_type>(), "RemovedItemIterator", _removed.begin(), _removed.end());
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::py_was_removed(const nb::object &key) const {
        return was_removed(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::was_removed(const key_type &key) const {
        return _removed_items.find(key) != _removed_items.end();
    }

    template <typename T_Key> nb::object TimeSeriesDictOutput_T<T_Key>::py_key_set() const { return nb::cast(_key_set); }

    template <typename T_Key> TimeSeriesSetOutput &TimeSeriesDictOutput_T<T_Key>::key_set() {
        return key_set_t();
    }

    template <typename T_Key>
    const TimeSeriesSetOutput &TimeSeriesDictOutput_T<T_Key>::key_set() const {
        return key_set_t();
    }

    template <typename T_Key>
    TimeSeriesSetOutput_T<typename TimeSeriesDictOutput_T<T_Key>::key_type> &TimeSeriesDictOutput_T<T_Key>::key_set_t() {
        return *_key_set;
    }

    template <typename T_Key>
    const TimeSeriesSetOutput_T<typename TimeSeriesDictOutput_T<T_Key>::key_type> &
    TimeSeriesDictOutput_T<T_Key>::key_set_t() const {
        return *_key_set;
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::py_set_item(const nb::object &key, const nb::object &value) {
        auto ts{operator[](nb::cast<T_Key>(key))};
        ts->apply_result(value);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::py_del_item(const nb::object &key) {
        erase(nb::cast<T_Key>(key));
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::erase(const key_type &key) { remove_value(key, false); }

    template <typename T_Key>
    nb::object TimeSeriesDictOutput_T<T_Key>::py_pop(const nb::object &key, const nb::object &default_value) {
        nb::object value{};
        auto       k = nb::cast<T_Key>(key);
        if (auto it = _ts_values.find(k); it != _ts_values.end()) {
            value = it->second->py_value();
            remove_value(k, false);
        }
        if (!value.is_valid()) { value = default_value; }
        return value;
    }

    template <typename T_Key>
    nb::object TimeSeriesDictOutput_T<T_Key>::py_get_ref(const nb::object &key, const nb::object &requester) {
        return nb::cast(get_ref(nb::cast<key_type>(key), static_cast<const void *>(requester.ptr())));
    }

    template <typename T_Key>
    void TimeSeriesDictOutput_T<T_Key>::py_release_ref(const nb::object &key, const nb::object &requester) {
        release_ref(nb::cast<T_Key>(key), static_cast<const void *>(requester.ptr()));
    }

    template <typename T_Key>
    time_series_output_ptr TimeSeriesDictOutput_T<T_Key>::get_ref(const key_type &key, const void *requester) {
        return _ref_ts_feature.create_or_increment(key, requester);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::release_ref(const key_type &key, const void *requester) {
        _ref_ts_feature.release(key, requester);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::_dispose() {
        // Release all removed items first
        for (auto &[_, value] : _removed_items) { _ts_builder->release_instance(value); }
        _removed_items.clear();

        // Release all current values
        for (auto &[_, value] : _ts_values) { _ts_builder->release_instance(value); }
        _ts_values.clear();
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::_clear_key_changes() {
        // Release removed instances before clearing
        for (auto &[_, value] : _removed_items) { _ts_builder->release_instance(value); }
        _removed_items.clear();
    }

    template <typename T_Key> TimeSeriesOutput::ptr TimeSeriesDictOutput_T<T_Key>::_get_or_create(const key_type &key) {
        if (_ts_values.find(key) == _ts_values.end()) { _create(key); }
        return _ts_values[key];
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::has_reference() const { return _ts_builder->has_reference(); }

    template <typename T_Key>
    const typename TimeSeriesDictOutput_T<T_Key>::key_type &
    TimeSeriesDictOutput_T<T_Key>::key_from_value(TimeSeriesOutput *value) const {
        auto it = _ts_values_to_keys.find(value);
        if (it != _ts_values_to_keys.end()) { return it->second; }
        throw std::out_of_range("Value not found in TimeSeriesDictOutput");
    }

    template <typename T_Key>
    TimeSeriesDictInput_T<T_Key>::TimeSeriesDictInput_T(const node_ptr &parent, input_builder_ptr ts_builder)
        : TimeSeriesDictInput(parent), _key_set{new typename TimeSeriesDictInput_T<T_Key>::key_set_type{this}},
          _ts_builder{ts_builder} {}

    template <typename T_Key>
    TimeSeriesDictInput_T<T_Key>::TimeSeriesDictInput_T(const time_series_type_ptr &parent, input_builder_ptr ts_builder)
        : TimeSeriesDictInput(parent), _key_set{new typename TimeSeriesDictInput_T<T_Key>::key_set_type{this}},
          _ts_builder{ts_builder} {}

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::has_peer() const { return _has_peer; }

    template <typename T_Key>
    typename TimeSeriesDictInput_T<T_Key>::const_item_iterator TimeSeriesDictInput_T<T_Key>::begin() const {
        return const_cast<TimeSeriesDictInput_T *>(this)->begin();
    }

    template <typename T_Key> typename TimeSeriesDictInput_T<T_Key>::item_iterator TimeSeriesDictInput_T<T_Key>::begin() {
        return _ts_values.begin();
    }

    template <typename T_Key> typename TimeSeriesDictInput_T<T_Key>::const_item_iterator TimeSeriesDictInput_T<T_Key>::end() const {
        return const_cast<TimeSeriesDictInput_T *>(this)->end();
    }

    template <typename T_Key> typename TimeSeriesDictInput_T<T_Key>::item_iterator TimeSeriesDictInput_T<T_Key>::end() {
        return _ts_values.end();
    }

    template <typename T_Key> size_t TimeSeriesDictInput_T<T_Key>::size() const { return _ts_values.size(); }

    template <typename T_Key> const typename TimeSeriesDictInput_T<T_Key>::map_type &TimeSeriesDictInput_T<T_Key>::value() const {
        return _ts_values;
    }

    template <typename T_Key> nb::object TimeSeriesDictInput_T<T_Key>::py_value() const {
        auto v{nb::dict()};
        for (const auto &[key, value] : _ts_values) {
            if (value->valid()) { v[nb::cast(key)] = value->py_value(); }
        }
        // Return frozendict snapshot of all valid items to mirror Python `_tsd.py:value`
        return get_frozendict()(v);
    }

    template <typename T_Key> nb::object TimeSeriesDictInput_T<T_Key>::py_delta_value() const {
        auto delta{nb::dict()};
        // Build from currently modified and valid child inputs to avoid relying solely on observer-tracked state
        const auto &modified = modified_items();
        for (const auto &[key, value] : modified) {
            if (value->valid()) { delta[nb::cast(key)] = value->py_delta_value(); }
        }
        const auto &removed_{removed_items()};
        if (!removed_.empty()) {
            auto removed{get_remove()};
            for (const auto &[key, _] : removed_) {
                if (was_removed_valid(key)) {
                    // Check was_valid flag
                    delta[nb::cast(key)] = removed;
                }
            }
        }
        // Return frozendict to mirror Python `_tsd.py:delta_value`
        return get_frozendict()(delta);
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::py_contains(const nb::object &item) const {
        return contains(nb::cast<T_Key>(item));
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::contains(const key_type &item) const {
        return has_peer() ? key_set_t().contains(item) : _ts_values.contains(item);
    }

    template <typename T_Key>
    nb::object TimeSeriesDictInput_T<T_Key>::py_get(const nb::object &item, const nb::object &default_value) const {
        auto key{nb::cast<T_Key>(item)};
        if (contains(key)) { return nb::cast(operator[](key)); }
        return default_value;
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::py_create(const nb::object &item) {
        return _create(nb::cast<T_Key>(item));
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_iter() { return py_keys(); }

    template <typename T_Key> nb::object TimeSeriesDictInput_T<T_Key>::py_get_item(const nb::object &item) const {
        if (get_key_set_id().is(item)) { return nb::cast(const_cast<TimeSeriesDictInput_T *>(this)->key_set_t()); }
        return nb::cast(_ts_values.at(nb::cast<T_Key>(item)));
    }

    template <typename T_Key>
    TimeSeriesDictInput_T<T_Key>::value_type TimeSeriesDictInput_T<T_Key>::operator[](const key_type &item) const {
        return _ts_values.at(item);
    }

    template <typename T_Key>
    TimeSeriesDictInput_T<T_Key>::value_type TimeSeriesDictInput_T<T_Key>::operator[](const key_type &item) {
        return get_or_create(item);
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_keys() const {
        return nb::make_key_iterator(nb::type<map_type>(), "KeyIterator", _ts_values.begin(), _ts_values.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_values() const {
        return nb::make_value_iterator(nb::type<map_type>(), "ValueIterator", _ts_values.begin(), _ts_values.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_items() const {
        return nb::make_iterator(nb::type<map_type>(), "ItemIterator", _ts_values.begin(), _ts_values.end());
    }

    template <typename T_Key>
    const typename TimeSeriesDictInput_T<T_Key>::map_type &TimeSeriesDictInput_T<T_Key>::modified_items() const {
        _modified_items_cache.clear();
        if (sampled()) {
            // Return all valid items when sampled
            for (const auto &[key, value] : valid_items()) { _modified_items_cache.emplace(key, value); }
        } else if (has_peer()) {
            // When peered, only return items that are modified in the output
            for (const auto &[key, _] : output_t().modified_items()) {
                auto it = _ts_values.find(key);
                if (it != _ts_values.end()) { _modified_items_cache.emplace(key, it->second); }
            }
        } else if (active()) {
            // When active but not sampled or peered, only return cached modified items
            // during the current evaluation cycle
            if (last_modified_time() == owning_graph()->evaluation_clock()->evaluation_time()) {
                return _modified_items;
            } else {
                return empty_;  // Return empty set if not in current cycle
            }
        } else {
            // When not active, return all modified items
            for (const auto &[key, value] : _ts_values) {
                if (value->modified()) { _modified_items_cache.emplace(key, value); }
            }
        }
        return _modified_items_cache;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_modified_keys() const {
        const auto &items = modified_items();  // Ensure modified_items is populated first
        return nb::make_key_iterator(nb::type<map_type>(), "ModifiedKeyIterator", items.begin(), items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_modified_values() const {
        const auto &items = modified_items();
        return nb::make_value_iterator(nb::type<map_type>(), "ModifiedValueIterator", items.begin(), items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_modified_items() const {
        const auto &items = modified_items();
        return nb::make_iterator(nb::type<map_type>(), "ModifiedItemIterator", items.begin(), items.end());
    }

    template <typename T_Key> TimeSeriesSetInput &TimeSeriesDictInput_T<T_Key>::key_set() {
        return key_set_t();
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::py_was_modified(const nb::object &key) const {
        return was_modified(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::was_modified(const key_type &key) const {
        const auto &it{_ts_values.find(key)};
        return it != _ts_values.end() && it->second->modified();
    }

    template <typename T_Key>
    const typename TimeSeriesDictInput_T<T_Key>::map_type &TimeSeriesDictInput_T<T_Key>::valid_items() const {
        // Rebuild cache each call to ensure freshness; returns a reference to a member to ensure iterator lifetime safety.
        _valid_items_cache.clear();
        for (const auto &item : _ts_values) {
            if (item.second->valid()) { _valid_items_cache.insert(item); }
        }
        return _valid_items_cache;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_valid_keys() const {
        const auto &items{valid_items()};
        return nb::make_key_iterator(nb::type<map_type>(), "ValidKeyIterator", items.begin(), items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_valid_values() const {
        const auto &items{valid_items()};
        return nb::make_value_iterator(nb::type<map_type>(), "ValidValueIterator", items.begin(), items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_valid_items() const {
        const auto &items{valid_items()};
        return nb::make_iterator(nb::type<map_type>(), "ValidItemIterator", items.begin(), items.end());
    }

    template <typename T_Key>
    const typename TimeSeriesDictInput_T<T_Key>::map_type &TimeSeriesDictInput_T<T_Key>::added_items() const {
        // TODO: Try and ensure that we cache the result where possible
        _added_items_cache.clear();
        const auto &key_set{key_set_t()};
        for (const auto &k : key_set.added()) {
            // Check if key exists in _ts_values before accessing
            // During cleanup, keys might be in added set but not in _ts_values
            auto it = _ts_values.find(k);
            if (it != _ts_values.end()) {
                _added_items_cache.emplace(k, it->second);
            } else {
                // TODO: print out error message as this should never happen
            }
        }
        return _added_items_cache;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_added_keys() const {
        const auto &items = added_items();  // Ensure cache is populated
        return nb::make_key_iterator(nb::type<map_type>(), "AddedKeyIterator", items.begin(), items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_added_values() const {
        const auto &items = added_items();  // Ensure cache is populated
        return nb::make_value_iterator(nb::type<map_type>(), "AddedValueIterator", items.begin(), items.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_added_items() const {
        const auto &items = added_items();  // Ensure cache is populated
        return nb::make_iterator(nb::type<map_type>(), "AddedItemIterator", items.begin(), items.end());
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::has_added() const { return !key_set_t().added().empty(); }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::py_was_added(const nb::object &key) const {
        return was_added(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::was_added(const key_type &key) const {
        const auto &added{key_set_t().added()};
        return added.find(key) != added.end();
    }

    template <typename T_Key>
    const typename TimeSeriesDictInput_T<T_Key>::map_type &TimeSeriesDictInput_T<T_Key>::removed_items() const {
        _removed_item_cache.clear();
        for (const auto &key : key_set_t().removed()) {
            auto it{_removed_items.find(key)};
            if (it == _removed_items.end()) {
                // This really should not occur!
                throw std::runtime_error("Removed item not found in removed_cache");
                // continue;
            }
            // Python does a search inside of _ts_values to find a deleted key, but this seems rather odd to me.
            _removed_item_cache.emplace(key, it->second.first);
        }
        return _removed_item_cache;
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_removed_keys() const {
        auto const &removed_{removed_items()};
        return nb::make_key_iterator(nb::type<map_type>(), "RemovedKeyIterator", removed_.begin(), removed_.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_removed_values() const {
        auto const &removed_{removed_items()};
        return nb::make_value_iterator(nb::type<map_type>(), "RemovedValueIterator", removed_.begin(), removed_.end());
    }

    template <typename T_Key> nb::iterator TimeSeriesDictInput_T<T_Key>::py_removed_items() const {
        auto const &removed_{removed_items()};
        return nb::make_iterator(nb::type<map_type>(), "RemovedItemIterator", removed_.begin(), removed_.end());
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::has_removed() const { return !_removed_items.empty(); }

    template <typename T_Key> const TimeSeriesSetInput &TimeSeriesDictInput_T<T_Key>::key_set() const {
        return key_set_t();
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::on_key_added(const key_type &key) {
        auto value{get_or_create(key)};
        value->bind_output(output_t()[key].get());
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::on_key_removed(const key_type &key) {
        // Pop the value from _ts_values first (matching Python: self._ts_values.pop(key, None))
        auto it = _ts_values.find(key);
        if (it == _ts_values.end()) { return; }

        auto value{it->second};
        _ts_values.erase(it);  // Remove from _ts_values first
        _remove_key_value(key, value);

        register_clear_key_changes();
        auto was_valid = value->valid();

        if (value->parent_input().get() == this) {
            // This is our own input - deactivate and track for cleanup
            if (value->active()) { value->make_passive(); }
            _removed_items.insert({key, {value, was_valid}});
            auto it_{_modified_items.find(key)};
            if (it_ != _modified_items.end()) { _modified_items.erase(it_); }
            // if (!has_peer()) { value->un_bind_output(false); }
        } else {
            // This is a transplanted input - put it back and unbind it
            _ts_values.insert({key, value});
            _add_key_value(key, value);
            value->un_bind_output(true);  // unbind_refs=True
        }
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::was_removed(const key_type &key) const {
        return _removed_items.find(key) != _removed_items.end();
    }

    template <typename T_Key> nb::object TimeSeriesDictInput_T<T_Key>::py_key_set() const { return nb::cast(_key_set); }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::py_was_removed(const nb::object &key) const {
        return was_removed(nb::cast<T_Key>(key));
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::do_bind_output(time_series_output_ptr &value) {
        auto *value_output{dynamic_cast<TimeSeriesDictOutput_T<T_Key> *>(value.get())};

        // Peer when types match AND neither has references (matching Python logic)
        bool  peer = (is_same_type(value_output) || !(value_output->has_reference() || this->has_reference()));
        auto *output_key_set{&value_output->key_set_t()};

        key_set_t().bind_output(output_key_set);

        if (owning_node()->is_started() && has_output()) {
            output_t().remove_key_observer(this);
            _prev_output = {&output_t()};
            // TODO: check this will not enter again
            owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([this]() { this->reset_prev(); });
        }

        auto active_{active()};
        make_passive();  // Ensure we are unsubscribed from the old output while has_peer has the old value
        set_output(value_output);
        _has_peer = peer;

        if (active_) { make_active(); }

        // Call base implementation which will set _output and call make_active if needed
        // Note: Base calls make_passive first, but we already did that above with the OLD has_peer
        // Base then sets _output and calls make_active with the NEW has_peer (which we just set)
        BaseTimeSeriesInput::do_bind_output(value);

        if (!_ts_values.empty()) { register_clear_key_changes(); }

        for (const auto &key : key_set_t().values()) { on_key_added(key); }

        for (const auto &key : key_set_t().removed()) { on_key_removed(key); }

        value_output->add_key_observer(this);
        return peer;
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::do_un_bind_output(bool unbind_refs) {
        key_set_t().un_bind_output(unbind_refs);

        if (!_ts_values.empty()) {
            _removed_items.clear();
            for (const auto &[key, value] : _ts_values) { _removed_items.insert({key, {value, value->valid()}}); }
            _ts_values.clear();
            _clear_key_tracking();
            register_clear_key_changes();

            removed_map_type to_keep{};
            for (auto &[key, v] : _removed_items) {
                auto &[value, was_valid] = v;
                if (value->parent_input().get() != this) {
                    // Check for transplanted items, these do not get removed, but can be un-bound
                    value->un_bind_output(unbind_refs);
                    _ts_values.insert({key, value});
                    _add_key_value(key, value);
                } else {
                    to_keep.insert({key, {value, was_valid}});
                }
            }
            std::swap(_removed_items, to_keep);
        }
        // If we are un-binding then the output must exist by definition.
        output_t().remove_key_observer(this);
        if (has_peer()) {
            BaseTimeSeriesInput::do_un_bind_output(unbind_refs);
        } else {
            reset_output();
        }
    }

    template <typename T_Key>
    TimeSeriesSetInput_T<typename TimeSeriesDictInput_T<T_Key>::key_type> &TimeSeriesDictInput_T<T_Key>::key_set_t() {
        return *_key_set;
    }

    template <typename T_Key>
    const TimeSeriesSetInput_T<typename TimeSeriesDictInput_T<T_Key>::key_type> &TimeSeriesDictInput_T<T_Key>::key_set_t() const {
        return *_key_set;
    }

    template <typename T_Key>
    TimeSeriesDictOutput_T<typename TimeSeriesDictInput_T<T_Key>::key_type> &TimeSeriesDictInput_T<T_Key>::output_t() {
        return static_cast<TimeSeriesDictOutput_T<key_type> &>(*output());
    }

    template <typename T_Key>
    const TimeSeriesDictOutput_T<typename TimeSeriesDictInput_T<T_Key>::key_type> &TimeSeriesDictInput_T<T_Key>::output_t() const {
        return const_cast<TimeSeriesDictInput_T *>(this)->output_t();
    }

    template <typename T_Key>
    const typename TimeSeriesDictInput_T<T_Key>::key_type &
    TimeSeriesDictInput_T<T_Key>::key_from_value(TimeSeriesInput *value) const {
        auto it = _ts_values_to_keys.find(value);
        if (it != _ts_values_to_keys.end()) { return it->second; }
        throw std::runtime_error("key_from_value: value not found in _ts_values_to_keys");
    }

    template <typename T_Key>
    const typename TimeSeriesDictInput_T<T_Key>::key_type &TimeSeriesDictInput_T<T_Key>::key_from_value(value_type value) const {
        return key_from_value(value.get());
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::was_removed_valid(const key_type &key) const {
        auto it = _removed_items.find(key);
        if (it == _removed_items.end()) { return false; }
        return it->second.second;
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::reset_prev() { _prev_output = nullptr; }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::clear_key_changes() {
        _clear_key_changes_registered = false;

        // Guard against cleared node (matches Python: if self.owning_node is None)
        if (!has_owning_node()) { return; }

        // Release instances with deferred callback to ensure cleanup happens after all processing
        // Could this fall foul of clean-up ordering? Since key-set-removed could have already been cleaned up.
        for (auto &[key, value_pair] : _removed_items) {
            auto &[value, was_valid] = value_pair;
            // Capture by value to ensure the lambda has valid references
            auto builder  = _ts_builder;
            auto instance = value;
            owning_graph()->evaluation_engine_api()->add_after_evaluation_notification(
                [builder, instance]() { builder->release_instance(instance); });
            value->un_bind_output(true);
        }

        _removed_items.clear();
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::register_clear_key_changes() const {
        // This has side effects, but they are not directly impacting the behaviour of the class
        const_cast<TimeSeriesDictInput_T *>(this)->register_clear_key_changes();
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::register_clear_key_changes() {
        if (!_clear_key_changes_registered) {
            _clear_key_changes_registered = true;
            owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([this]() { clear_key_changes(); });
        }
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::_clear_key_tracking() { _ts_values_to_keys.clear(); }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::_add_key_value(const key_type &key, const value_type &value) {
        _ts_values_to_keys.insert({const_cast<TimeSeriesInput *>(value.get()), key});
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::_key_updated(const key_type &key) {
        auto it{_ts_values.find(key)};
        if (it != _ts_values.end()) {
            _modified_items[key] = it->second;  // Use operator[] instead of insert to ensure update
        } else {
            // If we cannot find the child there is a bug, let's rather catch this condition
            throw nb::key_error("Key not found in TSD");
        }
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::_remove_key_value(const key_type &key, const value_type &value) {
        _ts_values_to_keys.erase(const_cast<TimeSeriesInput *>(value.get()));  // Remove from reverse map
    }

    template <typename T_Key>
    TimeSeriesDictInput_T<T_Key>::value_type TimeSeriesDictInput_T<T_Key>::get_or_create(const key_type &key) {
        if (!_ts_values.contains(key)) { _create(key); }
        return _ts_values[key];
    }

    template <typename T_Key> nb::object TimeSeriesDictInput_T<T_Key>::py_get_or_create(const nb::object &key) {
        auto ts = get_or_create(nb::cast<T_Key>(key));
        return nb::cast(ts.get());
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::is_same_type(const TimeSeriesType *other) const {
        auto other_d = dynamic_cast<const TimeSeriesDictInput_T<key_type> *>(other);
        if (!other_d) { return false; }
        return _ts_builder->is_same_type(*other_d->_ts_builder);
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::has_reference() const { return _ts_builder->has_reference(); }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::make_active() {
        if (has_peer()) {
            TimeSeriesDictInput::make_active();
            // Reactivate transplanted inputs that might have been deactivated in make_passive()
            // This is an approximate solution but at this point the information about active state is lost
            for (auto &[_, value] : _ts_values) {
                // Check if this input was transplanted from another parent
                if (value->parent_input().get() != this) { value->make_active(); }
            }
        } else {
            set_active(true);
            key_set().make_active();
            for (auto &[_, value] : _ts_values) { value->make_active(); }
        }
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::make_passive() {
        if (has_peer()) {
            TimeSeriesDictInput::make_passive();
        } else {
            set_active(false);
            key_set().make_passive();
            for (auto &[_, value] : _ts_values) { value->make_passive(); }
        }
    }

    template <typename T_Key> bool TimeSeriesDictInput_T<T_Key>::modified() const {
        if (has_peer()) { return TimeSeriesDictInput::modified(); }
        if (active()) {
            auto et{owning_graph()->evaluation_clock()->evaluation_time()};
            return _last_modified_time == et || key_set_t().modified() || sample_time() == et;
        }
        return key_set_t().modified() ||
               std::any_of(_ts_values.begin(), _ts_values.end(), [](const auto &pair) { return pair.second->modified(); });
    }

    template <typename T_Key> engine_time_t TimeSeriesDictInput_T<T_Key>::last_modified_time() const {
        if (has_peer()) { return TimeSeriesDictInput::last_modified_time(); }
        if (active()) { return std::max(std::max(_last_modified_time, key_set_t().last_modified_time()), sample_time()); }
        auto max_e{std::max_element(_ts_values.begin(), _ts_values.end(), [](const auto &pair1, const auto &pair2) {
            return pair1.second->last_modified_time() < pair2.second->last_modified_time();
        })};
        return std::max(key_set_t().last_modified_time(), max_e == end() ? MIN_DT : max_e->second->last_modified_time());
    }

    template <typename T_Key>
    void TimeSeriesDictInput_T<T_Key>::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        if (_last_modified_time < modified_time) {
            _last_modified_time = modified_time;
            _modified_items.clear();
        }

        if (child != &key_set_t()) {
            // Child is not the key-set instance
            auto key{key_from_value(child)};
            _key_updated(key);
        }

        BaseTimeSeriesInput::notify_parent(this, modified_time);
    }

    template <typename T_Key> void TimeSeriesDictInput_T<T_Key>::_create(const key_type &key) {
        auto item{_ts_builder->make_instance(this)};
        // For non-peered inputs that are active, make the newly created item active too
        // This ensures proper notification chain for fast non-peer TSD scenarios
        if (!has_peer() and active()) { item->make_active(); }
        _ts_values.insert({key, item});
        _add_key_value(key, item);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::_create(const key_type &key) {
        key_set_t().add(key);  // This handles adding to the _added set in TSS
        auto item{_ts_builder->make_instance(this)};
        _ts_values.insert({key, item});
        _add_key_value(key, item);

        // If the key was removed in this cycle, clean up the removed tracking
        if (auto it = _removed_items.find(key); it != _removed_items.end()) {
            _ts_builder->release_instance(it->second);
            _removed_items.erase(it);
        }

        _ref_ts_feature.update(key);
        for (auto &observer : _key_observers) { observer->on_key_added(key); }

        auto et{owning_graph()->evaluation_clock()->evaluation_time()};
        if (_last_cleanup_time < et) {
            _last_cleanup_time = et;
            owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([this]() { _clear_key_changes(); });
        }
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::add_key_observer(TSDKeyObserver<key_type> *observer) {
        _key_observers.push_back(observer);
    }

    template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::remove_key_observer(TSDKeyObserver<key_type> *observer) {
        auto it = std::find(_key_observers.begin(), _key_observers.end(), observer);
        if (it != _key_observers.end()) {
            *it = std::move(_key_observers.back());
            _key_observers.pop_back();
        }
    }

    template <typename T_Key> bool TimeSeriesDictOutput_T<T_Key>::is_same_type(const TimeSeriesType *other) const {
        auto other_d = dynamic_cast<const TimeSeriesDictOutput_T<key_type> *>(other);
        if (!other_d) { return false; }
        return _ts_builder->is_same_type(*other_d->_ts_builder);
    }

    template struct TimeSeriesDictInput_T<bool>;
    template struct TimeSeriesDictInput_T<int64_t>;
    template struct TimeSeriesDictInput_T<double>;
    template struct TimeSeriesDictInput_T<engine_date_t>;
    template struct TimeSeriesDictInput_T<engine_time_t>;
    template struct TimeSeriesDictInput_T<engine_time_delta_t>;
    template struct TimeSeriesDictInput_T<nb::object>;

    using TSD_Bool      = TimeSeriesDictInput_T<bool>;
    using TSD_Int       = TimeSeriesDictInput_T<int64_t>;
    using TSD_Float     = TimeSeriesDictInput_T<double>;
    using TSD_Date      = TimeSeriesDictInput_T<engine_date_t>;
    using TSD_DateTime  = TimeSeriesDictInput_T<engine_time_t>;
    using TSD_TimeDelta = TimeSeriesDictInput_T<engine_time_delta_t>;
    using TSD_Object    = TimeSeriesDictInput_T<nb::object>;

    template struct TimeSeriesDictOutput_T<bool>;
    template struct TimeSeriesDictOutput_T<int64_t>;
    template struct TimeSeriesDictOutput_T<double>;
    template struct TimeSeriesDictOutput_T<engine_date_t>;
    template struct TimeSeriesDictOutput_T<engine_time_t>;
    template struct TimeSeriesDictOutput_T<engine_time_delta_t>;
    template struct TimeSeriesDictOutput_T<nb::object>;

    using TSD_OUT_Bool      = TimeSeriesDictOutput_T<bool>;
    using TSD_OUT_Int       = TimeSeriesDictOutput_T<int64_t>;
    using TSD_OUT_Float     = TimeSeriesDictOutput_T<double>;
    using TSD_OUT_Date      = TimeSeriesDictOutput_T<engine_date_t>;
    using TSD_OUT_DateTime  = TimeSeriesDictOutput_T<engine_time_t>;
    using TSD_OUT_TimeDelta = TimeSeriesDictOutput_T<engine_time_delta_t>;
    using TSD_OUT_Object    = TimeSeriesDictOutput_T<nb::object>;

    // template <typename T_Key> void TimeSeriesDictOutput_T<T_Key>::post_modify() { _post_modify(); }

    void tsd_register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesDictOutput, BaseTimeSeriesOutput>(m, "TimeSeriesDictOutput")
            .def("__contains__", &TimeSeriesDictOutput::py_contains, "key"_a)
            .def("__getitem__", &TimeSeriesDictOutput::py_get_item, "key"_a)
            .def("__setitem__", &TimeSeriesDictOutput::py_set_item, "key"_a, "value"_a)
            .def("__delitem__", &TimeSeriesDictOutput::py_del_item, "key"_a)
            .def("__len__", &TimeSeriesDictOutput::size)
            .def("pop", &TimeSeriesDictOutput::py_pop, "key"_a, "default"_a = nb::none())
            .def("get", &TimeSeriesDictOutput::py_get, "key"_a, "default"_a = nb::none())
            .def("get_or_create", &TimeSeriesDictOutput::py_get_or_create, "key"_a)
            .def("clear", &TimeSeriesDictOutput::clear)
            .def("__iter__", &TimeSeriesDictOutput::py_iter)
            .def("keys", &TimeSeriesDictOutput::py_keys)
            .def("values", &TimeSeriesDictOutput::py_values)
            .def("items", &TimeSeriesDictOutput::py_items)
            .def("valid_keys", &TimeSeriesDictOutput::py_valid_keys)
            .def("valid_values", &TimeSeriesDictOutput::py_valid_values)
            .def("valid_items", &TimeSeriesDictOutput::py_valid_items)
            .def("added_keys", &TimeSeriesDictOutput::py_added_keys)
            .def("added_values", &TimeSeriesDictOutput::py_added_values)
            .def("added_items", &TimeSeriesDictOutput::py_added_items)
            .def("was_added", &TimeSeriesDictOutput::py_was_added, "key"_a)
            .def_prop_ro("has_added", &TimeSeriesDictOutput::has_added)
            .def("modified_keys", &TimeSeriesDictOutput::py_modified_keys)
            .def("modified_values", &TimeSeriesDictOutput::py_modified_values)
            .def("modified_items", &TimeSeriesDictOutput::py_modified_items)
            .def("was_modified", &TimeSeriesDictOutput::py_was_modified, "key"_a)
            .def("removed_keys", &TimeSeriesDictOutput::py_removed_keys)
            .def("removed_values", &TimeSeriesDictOutput::py_removed_values)
            .def("removed_items", &TimeSeriesDictOutput::py_removed_items)
            .def("was_removed", &TimeSeriesDictOutput::py_was_removed, "key"_a)
            .def_prop_ro("has_removed", &TimeSeriesDictOutput::has_removed)
            .def(
                "get_ref",
                [](TimeSeriesDictOutput &self, const nb::object &key, const nb::object &requester) {
                    return self.py_get_ref(key, requester);
                },
                "key"_a, "requester"_a)
            .def(
                "release_ref",
                [](TimeSeriesDictOutput &self, const nb::object &key, const nb::object &requester) {
                    self.py_release_ref(key, requester);
                },
                "key"_a, "requester"_a)
            .def_prop_ro("key_set", &TimeSeriesDictOutput::py_key_set)
            .def("__str__",
                 [](const TimeSeriesDictOutput &self) {
                     return fmt::format("TimeSeriesDictOutput@{:p}[size={}, valid={}]", static_cast<const void *>(&self),
                                        self.size(), self.valid());
                 })
            .def("__repr__", [](const TimeSeriesDictOutput &self) {
                return fmt::format("TimeSeriesDictOutput@{:p}[size={}, valid={}]", static_cast<const void *>(&self), self.size(),
                                   self.valid());
            });

        nb::class_<TimeSeriesDictInput, BaseTimeSeriesInput>(m, "TimeSeriesDictInput")
            .def("__contains__", &TimeSeriesDictInput::py_contains, "key"_a)
            .def("__getitem__", &TimeSeriesDictInput::py_get_item, "key"_a)
            .def(
                "get",
                [](TimeSeriesDictInput &self, const nb::object &key, const nb::object &default_value) {
                    return self.py_contains(key) ? self.py_get_item(key) : default_value;
                },
                "key"_a, "default"_a = nb::none())
            .def("__len__", &TimeSeriesDictInput::size)
            .def("__iter__", &TimeSeriesDictInput::py_keys)
            .def("keys", &TimeSeriesDictInput::py_keys)
            .def("values", &TimeSeriesDictInput::py_values)
            .def("items", &TimeSeriesDictInput::py_items)
            .def("valid_keys", &TimeSeriesDictInput::py_valid_keys)
            .def("valid_values", &TimeSeriesDictInput::py_valid_values)
            .def("valid_items", &TimeSeriesDictInput::py_valid_items)
            .def("added_keys", &TimeSeriesDictInput::py_added_keys)
            .def("added_values", &TimeSeriesDictInput::py_added_values)
            .def("added_items", &TimeSeriesDictInput::py_added_items)
            .def("was_added", &TimeSeriesDictInput::py_was_added, "key"_a)
            .def_prop_ro("has_added", &TimeSeriesDictInput::has_added)
            .def("modified_keys", &TimeSeriesDictInput::py_modified_keys)
            .def("modified_values", &TimeSeriesDictInput::py_modified_values)
            .def("modified_items", &TimeSeriesDictInput::py_modified_items)
            .def("was_modified", &TimeSeriesDictInput::py_was_modified, "key"_a)
            .def("removed_keys", &TimeSeriesDictInput::py_removed_keys)
            .def("removed_values", &TimeSeriesDictInput::py_removed_values)
            .def("removed_items", &TimeSeriesDictInput::py_removed_items)
            .def("was_removed", &TimeSeriesDictInput::py_was_removed, "key"_a)
            .def_prop_ro("has_removed", &TimeSeriesDictInput::has_removed)
            .def_prop_ro("key_set", &TimeSeriesDictInput::py_key_set)
            .def("__str__",
                 [](const TimeSeriesDictInput &self) {
                     return fmt::format("TimeSeriesDictInput@{:p}[size={}, valid={}]", static_cast<const void *>(&self),
                                        self.size(), self.valid());
                 })
            .def("__repr__", [](const TimeSeriesDictInput &self) {
                return fmt::format("TimeSeriesDictInput@{:p}[size={}, valid={}]", static_cast<const void *>(&self), self.size(),
                                   self.valid());
            });

        nb::class_<TSD_OUT_Bool, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Bool");
        nb::class_<TSD_OUT_Int, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Int");
        nb::class_<TSD_OUT_Float, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Float");
        nb::class_<TSD_OUT_Date, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Date");
        nb::class_<TSD_OUT_DateTime, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_DateTime");
        nb::class_<TSD_OUT_TimeDelta, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_TimeDelta");
        nb::class_<TSD_OUT_Object, TimeSeriesDictOutput>(m, "TimeSeriesDictOutput_Object");

        nb::class_<TSD_Bool, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Bool")
            .def("_create", &TSD_Bool::_create)
            .def("on_key_removed", &TSD_Bool::on_key_removed);
        nb::class_<TSD_Int, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Int")
            .def("_create", &TSD_Int::_create)
            .def("on_key_removed", &TSD_Int::on_key_removed);
        nb::class_<TSD_Float, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Float")
            .def("_create", &TSD_Float::_create)
            .def("on_key_removed", &TSD_Float::on_key_removed);
        nb::class_<TSD_Date, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Date")
            .def("_create", &TSD_Date::_create)
            .def("on_key_removed", &TSD_Date::on_key_removed);
        nb::class_<TSD_DateTime, TimeSeriesDictInput>(m, "TimeSeriesDictInput_DateTime")
            .def("_create", &TSD_DateTime::_create)
            .def("on_key_removed", &TSD_DateTime::on_key_removed);
        nb::class_<TSD_TimeDelta, TimeSeriesDictInput>(m, "TimeSeriesDictInput_TimeDelta")
            .def("_create", &TSD_TimeDelta::_create)
            .def("on_key_removed", &TSD_TimeDelta::on_key_removed);
        nb::class_<TSD_Object, TimeSeriesDictInput>(m, "TimeSeriesDictInput_Object")
            .def("_create", &TSD_Object::_create)
            .def("on_key_removed", &TSD_Object::on_key_removed);
    }
}  // namespace hgraph