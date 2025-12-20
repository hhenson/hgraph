//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_TIME_SERIES_VALUE_H
#define HGRAPH_VALUE_TIME_SERIES_VALUE_H

#include <hgraph/types/value/value.h>
#include <hgraph/types/value/modification_tracker.h>
#include <hgraph/types/value/observer_storage.h>
#include <hgraph/types/value/bundle_type.h>
#include <hgraph/types/value/dict_type.h>
#include <hgraph/util/date_time.h>
#include <hgraph/util/string_utils.h>

namespace hgraph::value {

    // Forward declarations
    class TimeSeriesValue;
    class TimeSeriesValueView;
    class ObserverStorage;

    /**
     * TimeSeriesValueView - Mutable view with explicit time parameters
     *
     * Time is passed to mutation methods, not stored in the view.
     * This avoids stale time issues and enables explicit control over
     * when modifications are marked.
     *
     * Navigation (field/element) returns sub-views that propagate
     * modifications to the parent.
     *
     * Observer Support:
     * - Optionally holds a pointer to an ObserverStorage for notifications
     * - Modifications trigger notifications that propagate upward
     * - Subscribe/unsubscribe available for hierarchical subscriptions
     */
    class TimeSeriesValueView {
    public:
        TimeSeriesValueView() = default;

        TimeSeriesValueView(ValueView value_view, ModificationTracker tracker,
                            ObserverStorage* observer = nullptr)
            : _value_view(value_view), _tracker(tracker), _observer(observer) {}

        [[nodiscard]] bool valid() const { return _value_view.valid() && _tracker.valid(); }
        [[nodiscard]] const TypeMeta* schema() const { return _value_view.schema(); }
        [[nodiscard]] TypeKind kind() const { return _value_view.kind(); }

        // Raw access (without auto-tracking - use with caution)
        [[nodiscard]] ValueView value_view() { return _value_view; }
        [[nodiscard]] ConstValueView value_view() const {
            return static_cast<const ConstValueView&>(_value_view);
        }
        [[nodiscard]] ModificationTracker tracker() { return _tracker; }
        [[nodiscard]] ModificationTracker tracker() const { return _tracker; }
        [[nodiscard]] ObserverStorage* observer() { return _observer; }
        [[nodiscard]] const ObserverStorage* observer() const { return _observer; }

        // === Scalar access ===
        template<typename T>
        [[nodiscard]] T& as() {
            return _value_view.as<T>();
        }

        template<typename T>
        [[nodiscard]] const T& as() const {
            return static_cast<const ConstValueView&>(_value_view).as<T>();
        }

        // Time passed at mutation point
        template<typename T>
        void set(const T& val, engine_time_t time) {
            _value_view.as<T>() = val;
            _tracker.mark_modified(time);
            if (_observer) {
                _observer->notify(time);
            }
        }

        void mark_modified(engine_time_t time) {
            _tracker.mark_modified(time);
            if (_observer) {
                _observer->notify(time);
            }
        }

        void mark_invalid() {
            _tracker.mark_invalid();
        }

        // === Query methods (time as parameter) ===
        [[nodiscard]] bool modified_at(engine_time_t time) const {
            return _tracker.modified_at(time);
        }

        [[nodiscard]] bool has_value() const {
            return _tracker.valid_value();
        }

        [[nodiscard]] engine_time_t last_modified_time() const {
            return _tracker.last_modified_time();
        }

        // === Bundle field navigation ===
        // Returns sub-view. If no specific child observer exists, we pass the parent
        // observer so that notifications still propagate up through the hierarchy.
        [[nodiscard]] TimeSeriesValueView field(size_t index) {
            if (!valid() || kind() != TypeKind::Bundle) {
                return {};
            }
            ObserverStorage* child_observer = _observer ? _observer->child(index) : nullptr;
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            return {_value_view.field(index), _tracker.field(index), effective_observer};
        }

        [[nodiscard]] TimeSeriesValueView field(const std::string& name) {
            if (!valid() || kind() != TypeKind::Bundle) {
                return {};
            }
            auto field_view = _value_view.field(name);
            auto field_tracker = _tracker.field(name);
            ObserverStorage* child_observer = nullptr;
            if (_observer && field_view.valid()) {
                auto* bundle_meta = static_cast<const BundleTypeMeta*>(schema());
                auto it = bundle_meta->name_to_index.find(name);
                if (it != bundle_meta->name_to_index.end()) {
                    child_observer = _observer->child(it->second);
                }
            }
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            return {field_view, field_tracker, effective_observer};
        }

        [[nodiscard]] bool field_modified_at(size_t index, engine_time_t time) const {
            return _tracker.field_modified_at(index, time);
        }

        [[nodiscard]] size_t field_count() const {
            return _value_view.field_count();
        }

        // === List element navigation ===
        [[nodiscard]] TimeSeriesValueView element(size_t index) {
            if (!valid() || kind() != TypeKind::List) {
                return {};
            }
            ObserverStorage* child_observer = _observer ? _observer->child(index) : nullptr;
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            return {_value_view.element(index), _tracker.element(index), effective_observer};
        }

        [[nodiscard]] bool element_modified_at(size_t index, engine_time_t time) const {
            return _tracker.element_modified_at(index, time);
        }

        [[nodiscard]] size_t list_size() const {
            return _value_view.list_size();
        }

        // === Set operations (time as parameter) ===
        template<typename T>
        bool add(const T& element, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Set) return false;
            bool added = _value_view.set_add(element);
            if (added) {
                _tracker.mark_modified(time);
                if (_observer) {
                    _observer->notify(time);
                }
            }
            return added;
        }

        template<typename T>
        bool remove(const T& element, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Set) return false;
            bool removed = _value_view.set_remove(element);
            if (removed) {
                _tracker.mark_modified(time);
                if (_observer) {
                    _observer->notify(time);
                }
            }
            return removed;
        }

        template<typename T>
        [[nodiscard]] bool contains(const T& element) const {
            if (!valid() || kind() != TypeKind::Set) return false;
            return _value_view.set_contains(element);
        }

        [[nodiscard]] size_t set_size() const {
            return _value_view.set_size();
        }

        // === Dict operations (time as parameter) ===
        template<typename K, typename V>
        void insert(const K& key, const V& value, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Dict) return;

            bool is_new_key = !_value_view.dict_contains(key);
            _value_view.dict_insert(key, value);

            if (is_new_key) {
                _tracker.mark_modified(time);
            }
            if (_observer) {
                _observer->notify(time);
            }
        }

        template<typename K>
        [[nodiscard]] bool dict_contains(const K& key) const {
            if (!valid() || kind() != TypeKind::Dict) return false;
            return _value_view.dict_contains(key);
        }

        template<typename K>
        [[nodiscard]] ConstValueView dict_get(const K& key) const {
            if (!valid() || kind() != TypeKind::Dict) return {};
            return _value_view.dict_get(key);
        }

        // Dict entry navigation - returns sub-view for a specific entry
        template<typename K>
        [[nodiscard]] TimeSeriesValueView entry(const K& key) {
            if (!valid() || kind() != TypeKind::Dict) {
                return {};
            }
            auto entry_view = _value_view.dict_get(key);
            if (!entry_view.valid()) {
                return {};
            }
            auto* storage = static_cast<DictStorage*>(_value_view.data());
            auto idx = storage->find_index(&key);
            if (!idx) {
                return {};
            }
            ObserverStorage* child_observer = _observer ? _observer->child(*idx) : nullptr;
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            return {entry_view, _tracker, effective_observer};
        }

        template<typename K>
        bool dict_remove(const K& key, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Dict) return false;
            bool removed = _value_view.dict_remove(key);
            if (removed) {
                _tracker.mark_modified(time);
                if (_observer) {
                    _observer->notify(time);
                }
            }
            return removed;
        }

        [[nodiscard]] size_t dict_size() const {
            return _value_view.dict_size();
        }

        // Dict entry navigation using ConstValueView as key
        [[nodiscard]] TimeSeriesValueView entry(ConstValueView key) {
            if (!valid() || kind() != TypeKind::Dict || !key.valid()) {
                return {};
            }
            auto entry_view = _value_view.dict_get(key);
            if (!entry_view.valid()) {
                return {};
            }
            auto idx = _value_view.dict_find_index(key);
            if (!idx) {
                return {};
            }
            ObserverStorage* child_observer = _observer ? _observer->child(*idx) : nullptr;
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            return {entry_view, _tracker, effective_observer};
        }

        // === Child observer management ===

        /**
         * Ensure a child observer exists at the given index.
         *
         * This is used when you want to subscribe at a child level before navigation.
         * Returns the child observer, creating it if necessary.
         *
         * @param index The child index (field index for bundles, element index for lists, etc.)
         * @param child_meta Optional type metadata for the child (for proper initialization)
         * @return Pointer to the child observer, or nullptr if no observer exists at all
         */
        [[nodiscard]] ObserverStorage* ensure_child_observer(size_t index, const TypeMeta* child_meta = nullptr) {
            if (!_observer) return nullptr;
            return _observer->ensure_child(index, child_meta);
        }

        /**
         * Navigate to a field with child observer ensured.
         *
         * Unlike field(), this ensures a child observer exists for the field,
         * enabling subscriptions at this level.
         */
        [[nodiscard]] TimeSeriesValueView field_with_observer(size_t index) {
            if (!valid() || kind() != TypeKind::Bundle) {
                return {};
            }
            auto field_value = _value_view.field(index);
            if (!field_value.valid()) {
                return {};
            }
            ObserverStorage* child_observer = ensure_child_observer(index, field_value.schema());
            return {field_value, _tracker.field(index), child_observer};
        }

        [[nodiscard]] TimeSeriesValueView field_with_observer(const std::string& name) {
            if (!valid() || kind() != TypeKind::Bundle) {
                return {};
            }
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(schema());
            auto it = bundle_meta->name_to_index.find(name);
            if (it == bundle_meta->name_to_index.end()) {
                return {};
            }
            return field_with_observer(it->second);
        }

        /**
         * Navigate to an element with child observer ensured.
         */
        [[nodiscard]] TimeSeriesValueView element_with_observer(size_t index) {
            if (!valid() || kind() != TypeKind::List) {
                return {};
            }
            auto elem_value = _value_view.element(index);
            if (!elem_value.valid()) {
                return {};
            }
            ObserverStorage* child_observer = ensure_child_observer(index, elem_value.schema());
            return {elem_value, _tracker.element(index), child_observer};
        }

        /**
         * Navigate to a dict entry with child observer ensured.
         *
         * Uses ConstValueView for type-safe key passing.
         */
        [[nodiscard]] TimeSeriesValueView entry_with_observer(ConstValueView key) {
            if (!valid() || kind() != TypeKind::Dict || !key.valid()) {
                return {};
            }
            auto entry_view = _value_view.dict_get(key);
            if (!entry_view.valid()) {
                return {};
            }
            auto idx = _value_view.dict_find_index(key);
            if (!idx) {
                return {};
            }
            auto* dict_meta = static_cast<const DictTypeMeta*>(schema());
            ObserverStorage* child_observer = ensure_child_observer(*idx, dict_meta->value_type);
            return {entry_view, _tracker, child_observer};
        }

        // === Window operations (time as parameter) ===
        // Note: timestamp is the value's timestamp, eval_time is for modification tracking
        template<typename T>
        void window_push(const T& value, engine_time_t timestamp, engine_time_t eval_time) {
            if (!valid() || kind() != TypeKind::Window) return;
            _value_view.window_push(value, timestamp);
            _tracker.mark_modified(eval_time);
            if (_observer) {
                _observer->notify(eval_time);
            }
        }

        // Type-safe window push using ConstValueView
        void window_push(ConstValueView value, engine_time_t timestamp, engine_time_t eval_time) {
            if (!valid() || kind() != TypeKind::Window || !value.valid()) return;
            _value_view.window_push(value, timestamp);
            _tracker.mark_modified(eval_time);
            if (_observer) {
                _observer->notify(eval_time);
            }
        }

        [[nodiscard]] ConstValueView window_get(size_t index) const {
            if (!valid() || kind() != TypeKind::Window) return {};
            return _value_view.window_get(index);
        }

        [[nodiscard]] size_t window_size() const {
            return _value_view.window_size();
        }

        [[nodiscard]] bool window_empty() const {
            return _value_view.window_empty();
        }

        [[nodiscard]] bool window_full() const {
            return _value_view.window_full();
        }

        [[nodiscard]] engine_time_t window_timestamp(size_t index) const {
            return _value_view.window_timestamp(index);
        }

        [[nodiscard]] engine_time_t window_oldest_timestamp() const {
            return _value_view.window_oldest_timestamp();
        }

        [[nodiscard]] engine_time_t window_newest_timestamp() const {
            return _value_view.window_newest_timestamp();
        }

        [[nodiscard]] const TypeMeta* window_element_type() const {
            return _value_view.window_element_type();
        }

        [[nodiscard]] bool window_is_fixed_length() const {
            return _value_view.window_is_fixed_length();
        }

        [[nodiscard]] bool window_is_variable_length() const {
            return _value_view.window_is_variable_length();
        }

        void window_compact(engine_time_t eval_time) {
            if (!valid() || kind() != TypeKind::Window) return;
            _value_view.window_compact(eval_time);
        }

        void window_evict_expired(engine_time_t eval_time) {
            if (!valid() || kind() != TypeKind::Window) return;
            _value_view.window_evict_expired(eval_time);
        }

        void window_clear(engine_time_t time) {
            if (!valid() || kind() != TypeKind::Window) return;
            _value_view.window_clear();
            _tracker.mark_modified(time);
            if (_observer) {
                _observer->notify(time);
            }
        }

        // === Ref operations (time as parameter) ===

        [[nodiscard]] bool ref_is_empty() const {
            if (!valid() || kind() != TypeKind::Ref) return true;
            return _value_view.ref_is_empty();
        }

        [[nodiscard]] bool ref_is_bound() const {
            if (!valid() || kind() != TypeKind::Ref) return false;
            return _value_view.ref_is_bound();
        }

        [[nodiscard]] bool ref_is_unbound() const {
            if (!valid() || kind() != TypeKind::Ref) return false;
            return _value_view.ref_is_unbound();
        }

        [[nodiscard]] bool ref_is_valid() const {
            if (!valid() || kind() != TypeKind::Ref) return false;
            return _value_view.ref_is_valid();
        }

        [[nodiscard]] const ValueRef* ref_target() const {
            if (!valid() || kind() != TypeKind::Ref) return nullptr;
            return _value_view.ref_target();
        }

        [[nodiscard]] ValueRef* ref_target() {
            if (!valid() || kind() != TypeKind::Ref) return nullptr;
            return _value_view.ref_target();
        }

        [[nodiscard]] size_t ref_item_count() const {
            if (!valid() || kind() != TypeKind::Ref) return 0;
            return _value_view.ref_item_count();
        }

        [[nodiscard]] const TypeMeta* ref_value_type() const {
            if (!valid() || kind() != TypeKind::Ref) return nullptr;
            return _value_view.ref_value_type();
        }

        [[nodiscard]] bool ref_is_atomic() const {
            if (!valid() || kind() != TypeKind::Ref) return false;
            return _value_view.ref_is_atomic();
        }

        [[nodiscard]] bool ref_can_be_unbound() const {
            if (!valid() || kind() != TypeKind::Ref) return false;
            return _value_view.ref_can_be_unbound();
        }

        // Ref navigation for unbound refs
        [[nodiscard]] TimeSeriesValueView ref_item(size_t index) {
            if (!valid() || kind() != TypeKind::Ref || !ref_is_unbound()) {
                return {};
            }
            ObserverStorage* child_observer = _observer ? _observer->child(index) : nullptr;
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            return {_value_view.ref_item(index), _tracker.ref_item(index), effective_observer};
        }

        [[nodiscard]] bool ref_item_modified_at(size_t index, engine_time_t time) const {
            return _tracker.ref_item_modified_at(index, time);
        }

        // Mutable ref operations with tracking
        void ref_bind(ValueRef target, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Ref) return;
            _value_view.ref_bind(target);
            _tracker.mark_modified(time);
            if (_observer) {
                _observer->notify(time);
            }
        }

        void ref_clear(engine_time_t time) {
            if (!valid() || kind() != TypeKind::Ref) return;
            _value_view.ref_clear();
            _tracker.mark_modified(time);
            if (_observer) {
                _observer->notify(time);
            }
        }

        void ref_make_unbound(size_t count, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Ref) return;
            _value_view.ref_make_unbound(count);
            _tracker.mark_modified(time);
            if (_observer) {
                _observer->notify(time);
            }
        }

        void ref_set_item(size_t index, ValueRef target, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Ref || !ref_is_unbound()) return;
            _value_view.ref_set_item(index, target);
            auto item_tracker = _tracker.ref_item(index);
            if (item_tracker.valid()) {
                item_tracker.mark_modified(time);
            }
            if (_observer) {
                _observer->notify(time);
            }
        }

        // === String representation ===
        [[nodiscard]] std::string to_string() const {
            return _value_view.to_string();
        }

        // Debug string with modification status (requires time parameter)
        [[nodiscard]] std::string to_debug_string(engine_time_t time) const {
            std::string result = "TS[";
            result += _value_view.schema() ? (_value_view.schema()->name ? _value_view.schema()->name : "?") : "null";
            result += "]@";
            // Use const accessor for data pointer
            result += std::to_string(reinterpret_cast<uintptr_t>(
                static_cast<const ConstValueView&>(_value_view).data()));
            result += "(value=\"";
            result += to_string();
            result += "\", modified=";
            result += _tracker.modified_at(time) ? "true" : "false";
            result += ")";
            return result;
        }

    private:
        ValueView _value_view;
        ModificationTracker _tracker;
        ObserverStorage* _observer{nullptr};
        // No _current_time - passed as parameter to mutations
    };

    /**
     * TimeSeriesValue - Owning container for time-series value
     *
     * Combines Value (data storage) with ModificationTrackerStorage
     * (modification tracking) into a unified time-series value.
     *
     * Provides:
     * - Value storage and access
     * - Modification tracking at appropriate granularity per type
     * - Hierarchical propagation (field change → bundle modified)
     * - Observer/notification support (lazy allocation)
     */
    class TimeSeriesValue {
    public:
        TimeSeriesValue() = default;

        explicit TimeSeriesValue(const TypeMeta* schema)
            : _value(schema), _tracker(schema) {}

        // Move only
        TimeSeriesValue(TimeSeriesValue&&) noexcept = default;
        TimeSeriesValue& operator=(TimeSeriesValue&&) noexcept = default;
        TimeSeriesValue(const TimeSeriesValue&) = delete;
        TimeSeriesValue& operator=(const TimeSeriesValue&) = delete;

        // Schema access
        [[nodiscard]] const TypeMeta* schema() const { return _value.schema(); }
        [[nodiscard]] TypeKind kind() const { return _value.kind(); }
        [[nodiscard]] bool valid() const { return _value.valid() && _tracker.valid(); }

        // Value access (read-only)
        [[nodiscard]] ConstValueView value() const { return _value.const_view(); }

        // Modification state queries (time as parameter)
        [[nodiscard]] bool modified_at(engine_time_t time) const {
            return _tracker.tracker().modified_at(time);
        }

        [[nodiscard]] engine_time_t last_modified_time() const {
            return _tracker.tracker().last_modified_time();
        }

        [[nodiscard]] bool has_value() const {
            return _tracker.tracker().valid_value();
        }

        void mark_invalid() {
            _tracker.tracker().mark_invalid();
        }

        // Mutable access - returns view without stored time
        [[nodiscard]] TimeSeriesValueView view() {
            return {_value.view(), _tracker.tracker(), _observers.get()};
        }

        // Direct scalar access (convenience for simple TS values)
        template<typename T>
        void set_value(const T& val, engine_time_t time) {
            _value.view().as<T>() = val;
            _tracker.tracker().mark_modified(time);
            if (_observers) {
                _observers->notify(time);
            }
        }

        template<typename T>
        [[nodiscard]] const T& as() const {
            return _value.const_view().as<T>();
        }

        // Observer/subscription API (lazy allocation)
        void subscribe(hgraph::Notifiable* notifiable) {
            ensure_observers();
            _observers->subscribe(notifiable);
        }

        void unsubscribe(hgraph::Notifiable* notifiable) {
            if (_observers) {
                _observers->unsubscribe(notifiable);
            }
        }

        [[nodiscard]] bool has_observers() const {
            return _observers && _observers->has_subscribers();
        }

        // Access underlying storage (for advanced use)
        [[nodiscard]] Value& underlying_value() { return _value; }
        [[nodiscard]] const Value& underlying_value() const { return _value; }
        [[nodiscard]] ModificationTrackerStorage& underlying_tracker() { return _tracker; }
        [[nodiscard]] const ModificationTrackerStorage& underlying_tracker() const { return _tracker; }
        [[nodiscard]] ObserverStorage* underlying_observers() { return _observers.get(); }
        [[nodiscard]] const ObserverStorage* underlying_observers() const { return _observers.get(); }

        // String representation - value only
        [[nodiscard]] std::string to_string() const {
            return _value.to_string();
        }

        // Debug string with modification status (requires time parameter)
        [[nodiscard]] std::string to_debug_string(engine_time_t current_time) const {
            std::string result = "TS[";
            result += schema() ? (schema()->name ? schema()->name : "?") : "null";
            result += "]@";
            result += std::to_string(reinterpret_cast<uintptr_t>(_value.data()));
            result += "(value=\"";
            result += to_string();
            result += "\", modified=";
            result += modified_at(current_time) ? "true" : "false";
            result += ", last_modified=";
            result += hgraph::to_string(last_modified_time());
            result += ")";
            return result;
        }

        // Ensure observer storage exists (for subscription at nested levels)
        void ensure_observers() {
            if (!_observers) {
                _observers = std::make_unique<ObserverStorage>(_value.schema());
            }
        }

        // Access observer storage directly (may be null if never subscribed)
        [[nodiscard]] ObserverStorage* observers() { return _observers.get(); }
        [[nodiscard]] const ObserverStorage* observers() const { return _observers.get(); }

    private:
        Value _value;
        ModificationTrackerStorage _tracker;
        std::unique_ptr<ObserverStorage> _observers;  // Lazy: nullptr until first subscribe
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_TIME_SERIES_VALUE_H