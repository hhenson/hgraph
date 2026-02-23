#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <hgraph/util/string_utils.h>

#include <fmt/format.h>
#include <nanobind/nanobind.h>
#include <hgraph/api/python/wrapper_factory.h>

namespace hgraph
{

    void TimeSeriesDictOutputImpl::apply_result(const nb::object& value) {
        // Ensure any Python API interaction occurs under the GIL and protect against exceptions
        if (value.is_none()) { return; }
        py_set_value(value);
    }

    bool TimeSeriesDictOutputImpl::can_apply_result(const nb::object& result) {
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
            // Convert Python key to Value using TypeOps
            value::Value key_val(_key_type);
            key_val.emplace();
            _key_type->ops().from_python(key_val.data(), k, _key_type);
            auto key_view = key_val.view();
            if (v_.is(remove) || v_.is(remove_if_exists)) {
                if (v_.is(remove_if_exists) && !contains(key_view)) { continue; }
                if (was_modified(key_view)) { return false; }
            } else {
                if (was_removed(key_view)) { return false; }
                if (contains(key_view)) {
                    if (!operator[](key_view)->can_apply_result(nb::borrow(v_))) { return false; }
                }
            }
        }
        return true;
    }

    void TimeSeriesDictOutputImpl::mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) {
        if (last_modified_time() < modified_time) {
            _modified_items.clear();
        }

        if (&child != &key_set()) {
            auto key{key_from_ts(&child)};
            _key_updated(key);
        }

        BaseTimeSeriesOutput::mark_child_modified(child, modified_time);
    }

    void TimeSeriesDictOutputImpl::remove_value(const value::View &key, bool raise_if_not_found) {
        auto it{_ts_values.find(key)};
        if (it == _ts_values.end()) {
            if (raise_if_not_found) {
                std::string err_msg = "TSD key does not exist";
                throw nb::key_error(err_msg.c_str());
            }
            return;
        }

        bool was_added_flag = key_set().was_added(key);
        auto item{it->second};
        // CRITICAL: Capture was_valid BEFORE clearing the item AND before notifying observers!
        // This allows INPUTs to query the OUTPUT for validity via was_removed_valid() during on_key_removed.
        // The cascading removal sequence means observers might be called from nested removals,
        // and they need to be able to query was_valid from ANY OUTPUT in the chain.
        bool item_was_valid = item->valid();
        // For reference outputs, check the REFERENCED output's validity, not the reference's binding status.
        // A REF output reports valid()=true when it's bound, but for REMOVE purposes we need to know
        // if the underlying output has a value - e.g., mesh fib(7) creates a REF that's bound but
        // the nested graph never completed so the referenced output has no value.
        if (auto ref_out = dynamic_cast<TimeSeriesReferenceOutput*>(item.get())) {
            if (ref_out->has_value()) {
                auto& ref = ref_out->value();
                if (ref.is_bound()) {
                    auto bound = ref.output();
                    if (bound && bound->valid()) {
                        // Referenced output is still valid - use that
                        item_was_valid = true;
                    } else if (bound && bound->last_modified_time() == MIN_DT) {
                        // Referenced output has last_modified_time == MIN_DT. This can mean:
                        // 1. It was NEVER written to (e.g., fib(7) never completed)
                        // 2. It was written to but has been CLEARED due to cascading cleanup
                        //
                        // Use the REF output's last_modified_time as a heuristic: if it was only
                        // modified at tick 1 (initial wiring), the bound output was never written to.
                        // If modified after tick 1, the bound output was likely written to and cleared.
                        auto ref_lmt = ref_out->last_modified_time();
                        if (ref_lmt <= engine_time_t{std::chrono::microseconds{1}}) {
                            item_was_valid = false;  // Never written to
                        } else {
                            // REF was modified after tick 1 - bound output was written to then cleared
                            item_was_valid = ref_out->valid();
                        }
                    } else {
                        // Bound output exists with lmt > MIN_DT but valid() is false
                        item_was_valid = false;
                    }
                } else {
                    item_was_valid = false;  // Not bound
                }
            } else {
                item_was_valid = false;  // No reference value means not valid
            }
        }

        // Store was_valid BEFORE notifying observers, so they can query it via was_removed_valid()
        if (!was_added_flag) {
            _removed_items.emplace(key.clone(), std::make_pair(item, item_was_valid));
        }

        // Check if the owning graph is stopping to avoid notification cascades
        // during teardown. This allows data cleanup while preventing crashes from
        // accessing partially stopped nodes.
        bool is_teardown = false;
        if (has_owning_node()) {
            auto g = owning_graph();
            is_teardown = (g != nullptr && g->is_stopping());
        }

        // Skip notifications during teardown to avoid accessing partially stopped state
        if (!is_teardown) {
            key_set().remove(key);
            for (auto &observer : _key_observers) { observer->on_key_removed(key); }
        }

        _ts_values.erase(it);
        item->clear();

        // Note: TSS key_set handles all added/removed tracking via key_set().remove()
        _remove_key_value(key, item);
        // Update feature extension using Value-based key
        _ref_ts_feature.update(key);
        // Erase from modified items using find-then-erase (transparent lookup + iterator erase)
        if (auto mod_it = _modified_items.find(key); mod_it != _modified_items.end()) {
            _modified_items.erase(mod_it);
        }

        // Schedule cleanup notification only once per evaluation cycle
        // Skip if no owning node or graph is stopping to avoid accessing partially torn-down state
        if (!has_owning_node()) {
            return;
        }
        auto g = owning_graph();
        if (g == nullptr || g->is_stopping()) {
            return;
        }
        auto et = g->evaluation_time();

        if (_last_cleanup_time < et) {
            _last_cleanup_time = et;
            auto weak_self = weak_from_this();
            g->evaluation_engine_api()->add_after_evaluation_notification([weak_self]() {
                if (auto self = weak_self.lock()) {
                    static_cast<TimeSeriesDictOutputImpl *>(self.get())->_clear_key_changes();
                }
            });
        }
    }

    void TimeSeriesDictOutputImpl::_clear_key_tracking() { _ts_values_to_keys.clear(); }

    void TimeSeriesDictOutputImpl::_add_key_value(const value::View &key, const value_type &value) {
        // Store Value key in reverse map
        _ts_values_to_keys.emplace(const_cast<TimeSeriesOutput *>(static_cast<const TimeSeriesOutput *>(value.get())),
                                   key.clone());
    }

    void TimeSeriesDictOutputImpl::_key_updated(const value::View &key) {
        auto it{_ts_values.find(key)};
        if (it != _ts_values.end()) {
            _modified_items.insert_or_assign(key.clone(), it->second);
        } else {
            // In the output we have noted that removed keys can still update, so we can't raise an exception here
        }
    }

    void TimeSeriesDictOutputImpl::_remove_key_value(const value::View &key, const value_type &value) {
        // Remove from reverse map
        _ts_values_to_keys.erase(const_cast<TimeSeriesOutput *>(static_cast<const TimeSeriesOutput *>(value.get())));
    }

    TimeSeriesDictOutputImpl::TimeSeriesDictOutputImpl(const node_ptr &parent, output_builder_s_ptr ts_builder,
                                                          output_builder_s_ptr ts_ref_builder, const value::TypeMeta* key_type)
        : TimeSeriesDictOutput(parent),
          _key_type{key_type},
          _key_set{arena_make_shared_as<TimeSeriesSetOutput, TimeSeriesOutput>(this, key_type)},
          _ts_builder{std::move(ts_builder)},
          _ts_ref_builder{std::move(ts_ref_builder)},
          _ref_ts_feature{this,
                          _ts_ref_builder,
                          key_type,
                          [](const TimeSeriesOutput &output, TimeSeriesOutput &result_output, const value::View &key) {
                              auto &output_t{dynamic_cast<const TimeSeriesDictOutputImpl &>(output)};
                              // Use key directly for lookup
                              auto it = output_t._ts_values.find(key);
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
    }

    TimeSeriesDictOutputImpl::TimeSeriesDictOutputImpl(time_series_output_ptr parent, output_builder_s_ptr ts_builder,
                                                          output_builder_s_ptr ts_ref_builder, const value::TypeMeta* key_type)
        : TimeSeriesDictOutput(parent),
          _key_type{key_type},
          _key_set{arena_make_shared_as<TimeSeriesSetOutput, TimeSeriesOutput>(this, key_type)},
          _ts_builder{std::move(ts_builder)}, _ts_ref_builder{std::move(ts_ref_builder)},
          _ref_ts_feature{this,
                          _ts_ref_builder,
                          key_type,
                          [](const TimeSeriesOutput &output, TimeSeriesOutput &result_output, const value::View &key) {
                              auto &output_t{dynamic_cast<const TimeSeriesDictOutputImpl &>(output)};
                              // Use key directly for lookup
                              auto it = output_t._ts_values.find(key);
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
    }

    void TimeSeriesDictOutputImpl::py_set_value(const nb::object& value) {
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
            // Convert Python key to Value using TypeOps
            value::Value key_val(_key_type);
            key_val.emplace();
            _key_type->ops().from_python(key_val.data(), kv[0], _key_type);
            auto key_view = key_val.view();
            auto v  = kv[1];
            if (v.is_none()) { continue; }
            if (v.is(remove) || v.is(remove_if_exists)) {
                if (contains(key_view)) {
                    erase(key_view);
                } else {
                    // Python semantics: REMOVE on missing -> KeyError; REMOVE_IF_EXISTS on missing -> no-op
                    if (v.is(remove)) {
                        std::string msg = "TSD key not found for REMOVE";
                        throw nb::key_error(msg.c_str());
                    }  // else REMOVE_IF_EXISTS: do nothing
                }
            } else {
                get_or_create(key_view)->py_set_value(v);
            }
        }
    }

    nb::object TimeSeriesDictOutputImpl::py_value() const {
        auto v{nb::dict()};
        for (const auto &[pv_key, value] : _ts_values) {
            if (value->valid()) {
                // Convert key to Python using TypeOps
                nb::object py_key = _key_type->ops().to_python(pv_key.view().data(), _key_type);
                v[py_key] = value->py_value();
            }
        }
        // Return frozendict snapshot of all valid items (mirror Python `_tsd.py:value`)
        return get_frozendict()(v);
    }

    nb::object TimeSeriesDictOutputImpl::py_delta_value() const {
        auto delta_value{nb::dict()};
        for (const auto &[pv_key, value] : _ts_values) {
            if (value->modified() && value->valid()) {
                // Convert key to Python using TypeOps
                nb::object py_key = _key_type->ops().to_python(pv_key.view().data(), _key_type);
                delta_value[py_key] = value->py_delta_value();
            }
        }
        if (!_removed_items.empty()) {
            auto removed{get_remove()};
            for (const auto &[pv_key, _] : _removed_items) {
                // Convert key to Python using TypeOps
                nb::object py_key = _key_type->ops().to_python(pv_key.view().data(), _key_type);
                delta_value[py_key] = removed;
            }
        }
        // Return frozendict of modified-valid entries plus removed keys (mirror Python `_tsd.py:delta_value`)
        return get_frozendict()(delta_value);
    }

    void TimeSeriesDictOutputImpl::clear() {
        key_set().clear();

        // Track items that were valid before clearing
        // Transfer _ts_values to _removed_items with was_valid=true (they were valid before clear)
        _removed_items.clear();
        for (auto &[pv_key, value] : _ts_values) {
            bool was_valid = value->valid();
            value->clear();
            // Clone the key since map keys are const
            _removed_items.emplace(pv_key.view().clone(), std::make_pair(value, was_valid));
        }
        _ts_values.clear();
        _clear_key_tracking();
        // Update feature outputs for removed keys using Value-based API
        for (const auto &[pv_key, _] : _removed_items) {
            _ref_ts_feature.update(pv_key.view());
        }
        _modified_items.clear();

        for (auto &observer : _key_observers) {
            for (const auto &[pv_key, _] : _removed_items) {
                observer->on_key_removed(pv_key.view());
            }
        }
    }

    void TimeSeriesDictOutputImpl::invalidate() {
        for (auto &[_, value] : _ts_values) { value->invalidate(); }
        mark_invalid();
    }

    void TimeSeriesDictOutputImpl::copy_from_output(const TimeSeriesOutput &output) {
        auto &other = dynamic_cast<const TimeSeriesDictOutputImpl &>(output);

        // Build list of keys to remove
        std::vector<value::Value> to_remove;
        for (auto elem : key_set().value_view()) {
            // Check if key is NOT in other's key set
            if (!other.key_set().contains(elem)) {
                to_remove.push_back(elem.clone());
            }
        }
        for (const auto &k : to_remove) { erase(k.view()); }
        for (const auto &[pv_key, v] : other._ts_values) {
            get_or_create(pv_key.view())->copy_from_output(*v);
        }
    }

    void TimeSeriesDictOutputImpl::copy_from_input(const TimeSeriesInput &input) {
        auto &dict_input = dynamic_cast<const TimeSeriesDictInputImpl &>(input);

        // Remove keys that are in output but NOT in input (matching Python: self.key_set.value - input.key_set.value)
        std::vector<value::Value> to_remove;
        for (auto elem : key_set().value_view()) {
            // Check if key is NOT in input's key set
            if (!dict_input.key_set().contains(elem)) {
                to_remove.push_back(elem.clone());
            }
        }
        for (const auto &k : to_remove) { erase(k.view()); }
        // Iterate Value-keyed map
        for (const auto &[pv_key, v_input] : dict_input.value()) {
            get_or_create(pv_key.view())->copy_from_input(*v_input);
        }
    }

    bool TimeSeriesDictOutputImpl::has_added() const { return !key_set().added_view().empty(); }

    bool TimeSeriesDictOutputImpl::has_removed() const { return !_removed_items.empty(); }

    size_t TimeSeriesDictOutputImpl::size() const { return _ts_values.size(); }

    TimeSeriesDictOutputImpl::value_type TimeSeriesDictOutputImpl::operator[](const value::View &key) {
        return get_or_create(key);
    }

    TimeSeriesDictOutputImpl::value_type TimeSeriesDictOutputImpl::operator[](const value::View &key) const {
        auto it = _ts_values.find(key);
        if (it == _ts_values.end()) {
            throw std::out_of_range("Key not found in TimeSeriesDictOutput");
        }
        return it->second;
    }

    TimeSeriesDictOutputImpl::const_item_iterator TimeSeriesDictOutputImpl::begin() const {
        return _ts_values.begin();
    }

    TimeSeriesDictOutputImpl::item_iterator TimeSeriesDictOutputImpl::begin() {
        return _ts_values.begin();
    }

    TimeSeriesDictOutputImpl::const_item_iterator TimeSeriesDictOutputImpl::end() const {
        return _ts_values.end();
    }

    TimeSeriesDictOutputImpl::item_iterator TimeSeriesDictOutputImpl::end() {
        return _ts_values.end();
    }

    const TimeSeriesDictOutputImpl::map_type &TimeSeriesDictOutputImpl::valid_items() const {
        // Use cached result if cache was built at or after our last modification time
        // This avoids expensive iteration and key cloning when nothing changed
        auto lmt = last_modified_time();
        if (_valid_items_cache_time >= lmt && !_valid_items_cache.empty()) {
            return _valid_items_cache;
        }

        // Rebuild cache
        _valid_items_cache.clear();
        for (const auto &[pv_key, val] : _ts_values) {
            if (val->valid()) {
                _valid_items_cache.emplace(pv_key.view().clone(), val);
            }
        }
        _valid_items_cache_time = lmt;
        return _valid_items_cache;
    }

    const TimeSeriesDictOutputImpl::map_type &TimeSeriesDictOutputImpl::added_items() const {
        // Early return if no items were added (avoids iteration entirely)
        if (key_set().added_view().empty()) {
            _added_items_cache.clear();
            return _added_items_cache;
        }

        // Use cached result if key_set hasn't been modified since cache was built
        auto ks_lmt = key_set().last_modified_time();
        if (_added_items_cache_time >= ks_lmt && !_added_items_cache.empty()) {
            return _added_items_cache;
        }

        // Rebuild cache - uses instance member instead of static for thread safety
        _added_items_cache.clear();
        for (auto elem : key_set().added_view()) {
            auto it = _ts_values.find(elem);
            if (it != _ts_values.end()) {
                _added_items_cache.emplace(elem.clone(), it->second);
            }
        }
        _added_items_cache_time = ks_lmt;
        return _added_items_cache;
    }

    TimeSeriesSetOutput &TimeSeriesDictOutputImpl::key_set() { return *_key_set; }

    const TimeSeriesSetOutput &TimeSeriesDictOutputImpl::key_set() const { return *_key_set; }

    void TimeSeriesDictOutputImpl::py_set_item(const nb::object &key, const nb::object &value) {
        // Convert Python key to Value using TypeOps
        value::Value key_val(_key_type);
        key_val.emplace();
        _key_type->ops().from_python(key_val.data(), key, _key_type);
        auto ts{operator[](key_val.view())};
        ts->apply_result(value);
    }

    void TimeSeriesDictOutputImpl::py_del_item(const nb::object &key) {
        // Convert Python key to Value using TypeOps
        value::Value key_val(_key_type);
        key_val.emplace();
        _key_type->ops().from_python(key_val.data(), key, _key_type);
        erase(key_val.view());
    }

    void TimeSeriesDictOutputImpl::erase(const value::View &key) {
        remove_value(key, false);
    }

    nb::object TimeSeriesDictOutputImpl::py_pop(const nb::object &key, const nb::object &default_value) {
        nb::object result_value{};
        // Convert Python key to Value using TypeOps
        value::Value key_val(_key_type);
        key_val.emplace();
        _key_type->ops().from_python(key_val.data(), key, _key_type);
        auto key_view = key_val.view();
        if (auto it = _ts_values.find(key_view); it != _ts_values.end()) {
            result_value = it->second->py_value();
            remove_value(key_view, false);
        }
        if (!result_value.is_valid()) { result_value = default_value; }
        return result_value;
    }

    nb::object TimeSeriesDictOutputImpl::py_get_ref(const nb::object &key, const nb::object &requester) {
        return wrap_time_series(get_ref(key, static_cast<const void *>(requester.ptr())));
    }

    void TimeSeriesDictOutputImpl::py_release_ref(const nb::object &key, const nb::object &requester) {
        release_ref(key, static_cast<const void *>(requester.ptr()));
    }

    time_series_output_s_ptr& TimeSeriesDictOutputImpl::get_ref(const nb::object &key, const void *requester) {
        // Convert Python key to Value using TypeOps
        value::Value key_val(_key_type);
        key_val.emplace();
        _key_type->ops().from_python(key_val.data(), key, _key_type);
        return _ref_ts_feature.create_or_increment(key_val.view(), requester);
    }

    void TimeSeriesDictOutputImpl::release_ref(const nb::object &key, const void *requester) {
        // Convert Python key to Value using TypeOps
        value::Value key_val(_key_type);
        key_val.emplace();
        _key_type->ops().from_python(key_val.data(), key, _key_type);
        _ref_ts_feature.release(key_val.view(), requester);
    }

    void TimeSeriesDictOutputImpl::_dispose() {
        // Release all removed items first (now stores pair<value, was_valid>)
        for (auto &[_, pair] : _removed_items) { _ts_builder->release_instance(pair.first.get()); }
        _removed_items.clear();

        // Release all current values
        for (auto &[_, value] : _ts_values) { _ts_builder->release_instance(value.get()); }
        _ts_values.clear();
    }

    void TimeSeriesDictOutputImpl::_clear_key_changes() {
        // Release removed instances before clearing (now stores pair<value, was_valid>)
        for (auto &[_, pair] : _removed_items) { _ts_builder->release_instance(pair.first.get()); }
        _removed_items.clear();
    }

    TimeSeriesDictOutputImpl::value_type TimeSeriesDictOutputImpl::get_or_create(const value::View &key) {
        auto it = _ts_values.find(key);
        if (it != _ts_values.end()) {
            return it->second;
        }
        // Create returns the new item, avoiding a second hash lookup
        return create(key);
    }

    bool TimeSeriesDictOutputImpl::has_reference() const { return _ts_builder->has_reference(); }

    value::View TimeSeriesDictOutputImpl::key_from_ts(TimeSeriesOutput *ts) const {
        auto it = _ts_values_to_keys.find(ts);
        if (it != _ts_values_to_keys.end()) {
            return it->second.view();
        }
        throw std::out_of_range("Value not found in TimeSeriesDictOutput");
    }

    value::View TimeSeriesDictOutputImpl::key_from_ts(const TimeSeriesDictOutputImpl::value_type& ts) const {
        auto it = _ts_values_to_keys.find(const_cast<TimeSeriesOutput*>(ts.get()));
        if (it != _ts_values_to_keys.end()) {
            return it->second.view();
        }
        throw std::out_of_range("Value not found in TimeSeriesDictOutput");
    }

    TimeSeriesDictInputImpl::TimeSeriesDictInputImpl(const node_ptr &parent, input_builder_s_ptr ts_builder, const value::TypeMeta* key_type)
        : TimeSeriesDictInput(parent),
          _key_type{key_type},
          _key_set{arena_make_shared_as<TimeSeriesSetInput, TimeSeriesInput>(this)},
          _ts_builder{ts_builder} {}

    TimeSeriesDictInputImpl::TimeSeriesDictInputImpl(time_series_input_ptr parent, input_builder_s_ptr ts_builder, const value::TypeMeta* key_type)
        : TimeSeriesDictInput(parent),
          _key_type{key_type},
          _key_set{arena_make_shared_as<TimeSeriesSetInput, TimeSeriesInput>(this)},
          _ts_builder{ts_builder} {}

    bool TimeSeriesDictInputImpl::has_peer() const { return _has_peer; }

    TimeSeriesDictInputImpl::const_item_iterator TimeSeriesDictInputImpl::begin() const {
        return _ts_values.begin();
    }

    TimeSeriesDictInputImpl::item_iterator TimeSeriesDictInputImpl::begin() {
        return _ts_values.begin();
    }

    TimeSeriesDictInputImpl::const_item_iterator TimeSeriesDictInputImpl::end() const {
        return _ts_values.end();
    }

    TimeSeriesDictInputImpl::item_iterator TimeSeriesDictInputImpl::end() {
        return _ts_values.end();
    }

    size_t TimeSeriesDictInputImpl::size() const { return _ts_values.size(); }

    nb::object TimeSeriesDictInputImpl::py_value() const {
        auto v{nb::dict()};
        for (const auto &[pv_key, value] : _ts_values) {
            if (value->valid()) {
                // Convert key to Python using TypeOps
                nb::object py_key = _key_type->ops().to_python(pv_key.view().data(), _key_type);
                v[py_key] = value->py_value();
            }
        }
        // Return frozendict snapshot of all valid items to mirror Python `_tsd.py:value`
        return get_frozendict()(v);
    }

    nb::object TimeSeriesDictInputImpl::py_delta_value() const {
        auto delta{nb::dict()};
        // Build from currently modified and valid child inputs
        for (const auto &[pv_key, value] : _ts_values) {
            if (value->modified() && value->valid()) {
                // Convert key to Python using TypeOps
                nb::object py_key = _key_type->ops().to_python(pv_key.view().data(), _key_type);
                delta[py_key] = value->py_delta_value();
            }
        }
        // Handle removed items - use removed_items() which respects key_set's delta tracking
        // This ensures same-cycle add/remove doesn't show up as REMOVE
        const auto& removed_map = removed_items();

        // NOTE: Unlike OUTPUT, INPUT should NOT unconditionally emit REMOVE for all removed keys.
        // Python INPUT delta_value checks was_valid flag: only emit REMOVE if the item was valid
        // when it was removed. This is critical for test_mesh_removal where the mesh output
        // for key 7 was never valid (fib(7) didn't complete before removal).
        if (!removed_map.empty()) {
            auto removed{get_remove()};
            for (const auto &[pv_key, value] : removed_map) {
                // Check was_valid flag from _removed_items
                if (was_removed_valid(pv_key.view())) {
                    nb::object py_key = _key_type->ops().to_python(pv_key.view().data(), _key_type);
                    delta[py_key] = removed;
                }
            }
        }
        // Return frozendict to mirror Python `_tsd.py:delta_value`
        return get_frozendict()(delta);
    }

    TimeSeriesDictInputImpl::value_type TimeSeriesDictInputImpl::operator[](const value::View &key) const {
        auto it = _ts_values.find(key);
        if (it == _ts_values.end()) {
            throw std::out_of_range("Key not found in TimeSeriesDictInput");
        }
        return it->second;
    }

    TimeSeriesDictInputImpl::value_type TimeSeriesDictInputImpl::operator[](const value::View &key) {
        return get_or_create(key);
    }

    const TimeSeriesDictInputImpl::map_type &TimeSeriesDictInputImpl::valid_items() const {
        // Use cached result if cache was built at or after our last modification time
        // This avoids expensive iteration and key cloning when nothing changed
        auto lmt = last_modified_time();
        if (_valid_items_cache_time >= lmt && !_valid_items_cache.empty()) {
            return _valid_items_cache;
        }

        // Rebuild cache
        _valid_items_cache.clear();
        for (const auto &[pv_key, val] : _ts_values) {
            if (val->valid()) {
                _valid_items_cache.emplace(pv_key.view().clone(), val);
            }
        }
        _valid_items_cache_time = lmt;
        return _valid_items_cache;
    }

    const TimeSeriesDictInputImpl::map_type &TimeSeriesDictInputImpl::added_items() const {
        // Early return if no items were added (avoids collect_added() and iteration)
        if (!has_added()) {
            _added_items_cache.clear();
            return _added_items_cache;
        }

        // Use cached result if key_set hasn't been modified since cache was built
        auto ks_lmt = key_set().last_modified_time();
        if (_added_items_cache_time >= ks_lmt && !_added_items_cache.empty()) {
            return _added_items_cache;
        }

        // Rebuild cache using key_set's collect_added() which handles _prev_output
        _added_items_cache.clear();
        auto added_keys = key_set().collect_added();
        for (const auto& elem : added_keys) {
            auto it = _ts_values.find(elem);
            if (it != _ts_values.end()) {
                _added_items_cache.emplace(elem.clone(), it->second);
            }
        }
        _added_items_cache_time = ks_lmt;
        return _added_items_cache;
    }

    bool TimeSeriesDictInputImpl::has_added() const {
        return !key_set().collect_added().empty();
    }

    const TimeSeriesDictInputImpl::map_type &TimeSeriesDictInputImpl::removed_items() const {
        _removed_items_cache.clear();
        // Use key_set's collect_removed() which handles _prev_output for unbound inputs
        auto removed_keys = key_set().collect_removed();
        for (const auto& elem : removed_keys) {
            auto it = _removed_items.find(elem);
            if (it == _removed_items.end()) {
                // transplanted items stay in _ts_values
                auto it2 = _ts_values.find(elem);
                if (it2 == _ts_values.end()) continue;
                _removed_items_cache.emplace(elem.clone(), it2->second);
            } else {
                _removed_items_cache.emplace(elem.clone(), it->second.first);
            }
        }
        return _removed_items_cache;
    }

    bool TimeSeriesDictInputImpl::has_removed() const { return !removed_items().empty(); }

    const TimeSeriesDictInputImpl::map_type &TimeSeriesDictInputImpl::modified_items() const {
        // Python logic:
        // 1. If sampled: return valid items
        // 2. If has_peer: get keys from output's modified_items, map to input's values
        // 3. If active and last_modified_time == evaluation_time: return _modified_items
        // 4. Otherwise: iterate all items and return modified ones

        if (sampled()) {
            // Sampling returns all valid items as "modified" for this tick
            _modified_items_cache.clear();
            for (const auto& [pv_key, val] : _ts_values) {
                if (val->valid()) {
                    _modified_items_cache.emplace(pv_key.view().clone(), val);
                }
            }
            return _modified_items_cache;
        }

        if (has_peer()) {
            // Delegate to output's modified keys, map to input's values
            _modified_items_cache.clear();
            const auto& output_modified = output_t().modified_items();
            for (const auto& [pv_key, _] : output_modified) {
                auto it = _ts_values.find(pv_key.view());
                if (it != _ts_values.end()) {
                    _modified_items_cache.emplace(pv_key.view().clone(), it->second);
                }
            }
            return _modified_items_cache;
        } else if (active()) {
            auto et = owning_graph()->evaluation_time();
            if (_last_modified_time == et) {
                return _modified_items;
            } else {
                return empty_;
            }
        } else {
            // Non-active: iterate all items and return modified ones
            _modified_items_cache.clear();
            for (const auto& [pv_key, val] : _ts_values) {
                if (val->modified()) {
                    _modified_items_cache.emplace(pv_key.view().clone(), val);
                }
            }
            return _modified_items_cache;
        }
    }

    bool TimeSeriesDictInputImpl::was_modified(const value::View &key) const {
        if (has_peer()) {
            return output_t().was_modified(key);
        } else if (active()) {
            auto et = owning_graph()->evaluation_time();
            if (_last_modified_time == et) {
                return _modified_items.find(key) != _modified_items.end();
            } else {
                return false;
            }
        } else {
            auto it = _ts_values.find(key);
            return it != _ts_values.end() && it->second->modified();
        }
    }

    TimeSeriesSetInput &TimeSeriesDictInputImpl::key_set() { return *_key_set; }

    const TimeSeriesSetInput &TimeSeriesDictInputImpl::key_set() const { return *_key_set; }

    void TimeSeriesDictInputImpl::on_key_added(const value::View &key) {
        auto value{get_or_create(key)};
        value->bind_output(output_t()[key]);
    }

    void TimeSeriesDictInputImpl::on_key_removed(const value::View &key) {
        // Pop the value from _ts_values first
        auto it = _ts_values.find(key);
        if (it == _ts_values.end()) { return; }

        auto value{it->second};

        // Determine was_valid: whether the INPUT's bound value had a valid result before removal.
        //
        // This handles two cases:
        // 1. test_mesh_2: key 'e' produced a valid result before removal -> emit REMOVE
        // 2. test_mesh_removal: key 7 never produced a valid result (fib(7) didn't complete) -> NO REMOVE
        //
        // We use the TSD OUTPUT's stored was_valid (captured BEFORE clearing and before notifying
        // observers). This is the authoritative source because it captures validity before cascading.
        bool was_valid = value->valid();  // Current validity of INPUT's bound value
        if (!was_valid && has_output()) {
            // Value is currently invalid due to cascading. Query the TSD OUTPUT for the
            // authoritative was_valid that was captured before clearing.
            was_valid = output_t().was_removed_valid(key);
        }

        _ts_values.erase(it);
        _remove_key_value(key, value);

        register_clear_key_changes();

        if (value->parent_input() == this) {
            if (value->active()) { value->make_passive(); }
            // Use emplace instead of insert for move-only Value keys
            _removed_items.emplace(key.clone(), std::make_pair(value, was_valid));

            auto it_ = _modified_items.find(key);
            if (it_ != _modified_items.end()) {
                _modified_items.erase(it_);
            }
            // if (!has_peer()) { value->un_bind_output(false); }
        } else {
            // Transplanted input - put it back and unbind it
            // Use emplace instead of insert for move-only Value keys
            _ts_values.emplace(key.clone(), value);
            _add_key_value(key, value);
            value->un_bind_output(true);
        }
    }

    bool TimeSeriesDictInputImpl::was_removed_valid(const value::View &key) const {
        auto it = _removed_items.find(key);
        if (it == _removed_items.end()) { return false; }
        return it->second.second;
    }

    bool TimeSeriesDictInputImpl::do_bind_output(time_series_output_s_ptr value) {
        auto value_output{std::dynamic_pointer_cast<TimeSeriesDictOutputImpl>(value)};

        // Peer when types match AND neither has references (matching Python logic)
        bool  peer = (is_same_type(value_output.get()) || !(value_output->has_reference() || this->has_reference()));
        auto  output_key_set = std::dynamic_pointer_cast<TimeSeriesOutput>(value_output->key_set_s_ptr());

        key_set().bind_output(output_key_set);

        if (owning_node()->is_started() && has_output()) {
            output_t().remove_key_observer(this);
            _prev_output = {&output_t()};
            // TODO: check this will not enter again
            auto weak_self = weak_from_this();
            owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self]() {
                if (auto self = weak_self.lock()) {
                    static_cast<TimeSeriesDictInputImpl *>(self.get())->reset_prev();
                }
            });
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

        // Iterate INPUT's key_set values and removed (not output's) to get keys - matches Python behavior
        // The input's key_set was just bound to output's key_set above, so it now reflects
        // the delta from the old output to the new output
        for (auto elem : key_set().value_view()) {
            on_key_added(elem);
        }

        // Use key_set INPUT's collect_removed() which properly handles _prev_output case (computes delta when rebinding)
        // This returns items that were in the old output but aren't in the new one
        auto removed_keys = key_set().collect_removed();
        for (const auto& elem : removed_keys) {
            on_key_removed(elem);
        }

        value_output->add_key_observer(this);
        return peer;
    }

    void TimeSeriesDictInputImpl::do_un_bind_output(bool unbind_refs) {
        key_set().un_bind_output(unbind_refs);

        if (!_ts_values.empty()) {
            for (const auto &[pv_key, value] : _ts_values) {
                // Clone the Value key and use emplace (copy constructor is deleted)
                _removed_items.emplace(pv_key.view().clone(), std::make_pair(value, value->valid()));
            }
            _ts_values.clear();
            _clear_key_tracking();
            _modified_items.clear();
            register_clear_key_changes();

            removed_map_type to_keep{};
            for (auto &[pv_key, v] : _removed_items) {
                auto &[value, was_valid] = v;
                if (value->parent_input() != this) {
                    // Transplanted items - un-bind and put back
                    value->un_bind_output(unbind_refs);
                    // Use emplace instead of insert for move-only Value keys
                    _ts_values.emplace(pv_key.view().clone(), value);
                    _add_key_value(pv_key.view(), value);
                } else {
                    to_keep.emplace(pv_key.view().clone(), std::make_pair(value, was_valid));
                }
            }
            std::swap(_removed_items, to_keep);
        }
        output_t().remove_key_observer(this);
        if (has_peer()) {
            BaseTimeSeriesInput::do_un_bind_output(unbind_refs);
            _has_peer = false;
        } else {
            reset_output();
        }
    }


    TimeSeriesDictOutputImpl &TimeSeriesDictInputImpl::output_t() {
        return static_cast<TimeSeriesDictOutputImpl &>(*output());
    }

    const TimeSeriesDictOutputImpl &TimeSeriesDictInputImpl::output_t() const {
        return const_cast<TimeSeriesDictInputImpl *>(this)->output_t();
    }

    value::View TimeSeriesDictInputImpl::key_from_ts(TimeSeriesInput *ts) const {
        auto it = _ts_values_to_keys.find(ts);
        if (it != _ts_values_to_keys.end()) {
            return it->second.view();
        }
        throw std::runtime_error("key_from_ts: value not found in _ts_values_to_keys");
    }

    value::View TimeSeriesDictInputImpl::key_from_ts(value_type ts) const {
        return key_from_ts(ts.get());
    }

    void TimeSeriesDictInputImpl::reset_prev() { _prev_output = nullptr; }

    void TimeSeriesDictInputImpl::clear_key_changes() {
        _clear_key_changes_registered = false;

        // Guard against cleared node (matches Python: if self.owning_node is None)
        if (!has_owning_node()) { return; }

        // Release instances with deferred callback to ensure cleanup happens after all processing.
        // Note: Do NOT call un_bind_output here - by the time this deferred callback runs,
        // the reference outputs that this input was observing may have already been destroyed
        // (e.g., when a switch branch changes and the old graph is disposed).
        // The builder->release_instance will call builder_release_cleanup which safely handles
        // cleanup of any remaining bindings.
        for (auto &[key, value_pair] : _removed_items) {
            auto &[value, was_valid] = value_pair;
            // Capture by value to ensure the lambda has valid references
            auto builder  = _ts_builder;
            auto instance = value;
            owning_graph()->evaluation_engine_api()->add_after_evaluation_notification(
                [builder, instance]() { builder->release_instance(instance.get()); });
            instance->un_bind_output(true);
        }

        _removed_items.clear();
    }

    void TimeSeriesDictInputImpl::register_clear_key_changes() const {
        // This has side effects, but they are not directly impacting the behaviour of the class
        const_cast<TimeSeriesDictInputImpl *>(this)->register_clear_key_changes();
    }

    void TimeSeriesDictInputImpl::register_clear_key_changes() {
        if (!_clear_key_changes_registered) {
            _clear_key_changes_registered = true;
            auto weak_self = weak_from_this();
            owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self]() {
                if (auto self = weak_self.lock()) {
                    static_cast<TimeSeriesDictInputImpl *>(self.get())->clear_key_changes();
                }
            });
        }
    }

    void TimeSeriesDictInputImpl::_clear_key_tracking() { _ts_values_to_keys.clear(); }

    void TimeSeriesDictInputImpl::_add_key_value(const value::View &key, const value_type &value) {
        // Store Value key in reverse map - use emplace for move-only Value
        _ts_values_to_keys.emplace(const_cast<TimeSeriesInput *>(value.get()), key.clone());
    }

    void TimeSeriesDictInputImpl::_key_updated(const value::View &key) {
        auto it = _ts_values.find(key);
        if (it != _ts_values.end()) {
            // Use insert_or_assign for move-only Value keys
            _modified_items.insert_or_assign(key.clone(), it->second);
        } else {
            throw nb::key_error("Key not found in TSD");
        }
    }

    void TimeSeriesDictInputImpl::_remove_key_value(const value::View &key, const value_type &value) {
        _ts_values_to_keys.erase(const_cast<TimeSeriesInput *>(value.get()));
    }

    TimeSeriesDictInputImpl::value_type TimeSeriesDictInputImpl::get_or_create(const value::View &key) {
        auto it = _ts_values.find(key);
        if (it == _ts_values.end()) {
            create(key);
            it = _ts_values.find(key);
        }
        return it->second;
    }

    bool TimeSeriesDictInputImpl::is_same_type(const TimeSeriesType *other) const {
        // Single comparison checks both type (Dict) and direction (Input)
        if (other->kind() != kind()) { return false; }
        // Kind matches exactly, safe to use static_cast
        auto other_d = static_cast<const TimeSeriesDictInputImpl *>(other);
        return _ts_builder->is_same_type(*other_d->_ts_builder);
    }

    bool TimeSeriesDictInputImpl::has_reference() const { return _ts_builder->has_reference(); }

    void TimeSeriesDictInputImpl::make_active() {
        if (has_peer()) {
            TimeSeriesDictInput::make_active();
            // Reactivate transplanted inputs that might have been deactivated in make_passive()
            // This is an approximate solution but at this point the information about active state is lost
            for (auto &[_, value] : _ts_values) {
                // Check if this input was transplanted from another parent
                if (value->parent_input() != this) { value->make_active(); }
            }
        } else {
            set_active(true);
            key_set().make_active();
            for (auto &[_, value] : _ts_values) { value->make_active(); }
        }
    }

    void TimeSeriesDictInputImpl::make_passive() {
        if (has_peer()) {
            TimeSeriesDictInput::make_passive();
        } else {
            set_active(false);
            key_set().make_passive();
            for (auto &[_, value] : _ts_values) { value->make_passive(); }
        }
    }

    bool TimeSeriesDictInputImpl::modified() const {
        if (has_peer()) { return TimeSeriesDictInput::modified(); }
        if (active()) {
            auto et{owning_graph()->evaluation_time()};
            return _last_modified_time == et || key_set().modified() || sample_time() == et;
        }
        return key_set().modified() ||
               std::any_of(_ts_values.begin(), _ts_values.end(), [](const auto &pair) { return pair.second->modified(); });
    }

    bool TimeSeriesDictInputImpl::valid() const {
        // Match Python behavior: TSD input is valid if it was ever sampled (_sample_time > MIN_DT),
        // which allows reading data even when unbound.
        // Python: return self._sample_time > MIN_DT or (self.bound and self.output.valid)
        return sample_time() > MIN_DT || TimeSeriesDictInput::valid();
    }

    engine_time_t TimeSeriesDictInputImpl::last_modified_time() const {
        if (has_peer()) { return TimeSeriesDictInput::last_modified_time(); }
        if (active()) { return std::max(std::max(_last_modified_time, key_set().last_modified_time()), sample_time()); }
        auto max_e{std::max_element(_ts_values.begin(), _ts_values.end(), [](const auto &pair1, const auto &pair2) {
            return pair1.second->last_modified_time() < pair2.second->last_modified_time();
        })};
        return std::max(key_set().last_modified_time(), max_e == _ts_values.end() ? MIN_DT : max_e->second->last_modified_time());
    }

    void TimeSeriesDictInputImpl::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        if (_last_modified_time < modified_time) {
            _last_modified_time = modified_time;
            _modified_items.clear();
        }

        if (child != &key_set()) {
            // Child is not the key-set instance
            auto key{key_from_ts(child)};
            _key_updated(key);
        }

        BaseTimeSeriesInput::notify_parent(this, modified_time);
    }

    TimeSeriesDictInputImpl::value_type TimeSeriesDictInputImpl::create(const value::View &key_view) {
        auto item{_ts_builder->make_instance(this)};
        // For non-peered inputs that are active, make the newly created item active too
        // This ensures proper notification chain for fast non-peer TSD scenarios
        if (!has_peer() and active()) { item->make_active(); }
        // Use emplace with cloned key for move-only Value storage
        _ts_values.emplace(key_view.clone(), item);
        _add_key_value(key_view, item);
        return item;  // Return the created item
    }

    TimeSeriesDictOutputImpl::value_type TimeSeriesDictOutputImpl::create(const value::View &key_view) {
        // Add key to TSS (already Value-based)
        key_set().add(key_view);

        auto item{_ts_builder->make_instance(this)};
        // Use emplace with cloned key for move-only Value storage
        _ts_values.emplace(key_view.clone(), item);

        _add_key_value(key_view, item);

        // If the key was removed in this cycle, clean up the removed tracking
        if (auto it = _removed_items.find(key_view); it != _removed_items.end()) {
            _ts_builder->release_instance(it->second.first.get());  // pair<value, was_valid>.first
            _removed_items.erase(it);
        }

        // Update feature extension using Value-based key
        _ref_ts_feature.update(key_view);
        for (auto &observer : _key_observers) { observer->on_key_added(key_view); }

        auto et{owning_graph()->evaluation_time()};
        if (_last_cleanup_time < et) {
            _last_cleanup_time = et;
            auto weak_self = weak_from_this();
            owning_graph()->evaluation_engine_api()->add_after_evaluation_notification([weak_self]() {
                if (auto self = weak_self.lock()) {
                    static_cast<TimeSeriesDictOutputImpl *>(self.get())->_clear_key_changes();
                }
            });
        }

        return item;  // Return the created item to avoid second lookup in get_or_create
    }

    void TimeSeriesDictOutputImpl::add_key_observer(TSDKeyObserver *observer) {
        _key_observers.push_back(observer);
    }

    void TimeSeriesDictOutputImpl::remove_key_observer(TSDKeyObserver *observer) {
        auto it = std::find(_key_observers.begin(), _key_observers.end(), observer);
        if (it != _key_observers.end()) {
            *it = std::move(_key_observers.back());
            _key_observers.pop_back();
        }
    }

    bool TimeSeriesDictOutputImpl::is_same_type(const TimeSeriesType *other) const {
        // Single comparison checks both type (Dict) and direction (Output)
        if (other->kind() != kind()) { return false; }
        // Kind matches exactly, safe to use static_cast
        auto other_d = static_cast<const TimeSeriesDictOutputImpl *>(other);
        return _ts_builder->is_same_type(*other_d->_ts_builder);
    }

}  // namespace hgraph
