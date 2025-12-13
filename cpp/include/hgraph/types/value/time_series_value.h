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

namespace hgraph::value {

    // Forward declarations
    class TimeSeriesValue;
    class TimeSeriesValueView;
    class ObserverStorage;

    /**
     * TimeSeriesValueView - Mutable view with automatic modification tracking
     *
     * Unlike raw ValueView, this view automatically marks modifications
     * when values are changed via set(). It also tracks the current
     * evaluation time for proper modification marking.
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

        TimeSeriesValueView(ValueView value_view, ModificationTracker tracker, engine_time_t current_time,
                            ObserverStorage* observer = nullptr)
            : _value_view(value_view), _tracker(tracker), _current_time(current_time), _observer(observer) {}

        [[nodiscard]] bool valid() const { return _value_view.valid() && _tracker.valid(); }
        [[nodiscard]] const TypeMeta* schema() const { return _value_view.schema(); }
        [[nodiscard]] TypeKind kind() const { return _value_view.kind(); }
        [[nodiscard]] engine_time_t current_time() const { return _current_time; }

        // Raw access (without auto-tracking - use with caution)
        [[nodiscard]] ValueView value_view() { return _value_view; }
        [[nodiscard]] ModificationTracker tracker() { return _tracker; }

        // Scalar access - auto-marks modified
        template<typename T>
        [[nodiscard]] T& as() {
            return _value_view.as<T>();
        }

        template<typename T>
        [[nodiscard]] const T& as() const {
            // Use ConstValueView::as<T>() for const access
            return static_cast<const ConstValueView&>(_value_view).as<T>();
        }

        template<typename T>
        void set(const T& val) {
            _value_view.as<T>() = val;
            _tracker.mark_modified(_current_time);
            if (_observer) {
                _observer->notify(_current_time);
            }
        }

        // Bundle field navigation - returns sub-view with parent tracking
        // Note: If no specific child observer exists, we pass the parent observer
        // so that notifications still propagate up through the hierarchy.
        [[nodiscard]] TimeSeriesValueView field(size_t index) {
            if (!valid() || kind() != TypeKind::Bundle) {
                return {};
            }
            // Try to get specific child observer, fall back to parent observer
            ObserverStorage* child_observer = _observer ? _observer->child(index) : nullptr;
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            return {_value_view.field(index), _tracker.field(index), _current_time, effective_observer};
        }

        [[nodiscard]] TimeSeriesValueView field(const std::string& name) {
            if (!valid() || kind() != TypeKind::Bundle) {
                return {};
            }
            auto field_view = _value_view.field(name);
            auto field_tracker = _tracker.field(name);
            // Get field index for observer lookup
            ObserverStorage* child_observer = nullptr;
            if (_observer && field_view.valid()) {
                auto* bundle_meta = static_cast<const BundleTypeMeta*>(schema());
                auto it = bundle_meta->name_to_index.find(name);
                if (it != bundle_meta->name_to_index.end()) {
                    child_observer = _observer->child(it->second);
                }
            }
            // If no specific child observer, use parent observer
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            return {field_view, field_tracker, _current_time, effective_observer};
        }

        [[nodiscard]] bool field_modified_at(size_t index, engine_time_t time) const {
            return _tracker.field_modified_at(index, time);
        }

        [[nodiscard]] size_t field_count() const {
            return _value_view.field_count();
        }

        // List element navigation - returns sub-view with parent tracking
        // Note: If no specific child observer exists, we pass the parent observer
        // so that notifications still propagate up through the hierarchy.
        [[nodiscard]] TimeSeriesValueView element(size_t index) {
            if (!valid() || kind() != TypeKind::List) {
                return {};
            }
            // Try to get specific child observer, fall back to parent observer
            ObserverStorage* child_observer = _observer ? _observer->child(index) : nullptr;
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            return {_value_view.element(index), _tracker.element(index), _current_time, effective_observer};
        }

        [[nodiscard]] bool element_modified_at(size_t index, engine_time_t time) const {
            return _tracker.element_modified_at(index, time);
        }

        [[nodiscard]] size_t list_size() const {
            return _value_view.list_size();
        }

        // Set operations - atomic tracking
        template<typename T>
        bool add(const T& element) {
            if (!valid() || kind() != TypeKind::Set) return false;
            bool added = _value_view.set_add(element);
            if (added) {
                _tracker.mark_modified(_current_time);
                if (_observer) {
                    _observer->notify(_current_time);
                }
            }
            return added;
        }

        template<typename T>
        bool remove(const T& element) {
            if (!valid() || kind() != TypeKind::Set) return false;
            bool removed = _value_view.set_remove(element);
            if (removed) {
                _tracker.mark_modified(_current_time);
                if (_observer) {
                    _observer->notify(_current_time);
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

        // Dict operations - structural + entry tracking
        template<typename K, typename V>
        void insert(const K& key, const V& value) {
            if (!valid() || kind() != TypeKind::Dict) return;

            // Check if key exists before insertion
            bool is_new_key = !_value_view.dict_contains(key);

            _value_view.dict_insert(key, value);

            // Mark structural modification if new key, always notify
            if (is_new_key) {
                _tracker.mark_modified(_current_time);
            }
            if (_observer) {
                _observer->notify(_current_time);
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
        // Note: Modifications to entries will notify at the dict level (structural notification)
        // For per-entry tracking, use the entry index with dict_entry_modified_at()
        template<typename K>
        [[nodiscard]] TimeSeriesValueView entry(const K& key) {
            if (!valid() || kind() != TypeKind::Dict) {
                return {};
            }
            // Get value view for this key
            auto entry_view = _value_view.dict_get(key);
            if (!entry_view.valid()) {
                return {};
            }
            // Get entry index for observer lookup
            auto* storage = static_cast<DictStorage*>(_value_view.data());
            auto idx = storage->find_index(&key);
            if (!idx) {
                return {};
            }
            // For dict entries, we use the dict's own tracker (modifications propagate to dict level)
            // The observer subtree is per-entry; fall back to parent observer if no child exists
            ObserverStorage* child_observer = _observer ? _observer->child(*idx) : nullptr;
            ObserverStorage* effective_observer = child_observer ? child_observer : _observer;
            return {entry_view, _tracker, _current_time, effective_observer};
        }

        template<typename K>
        bool dict_remove(const K& key) {
            if (!valid() || kind() != TypeKind::Dict) return false;
            bool removed = _value_view.dict_remove(key);
            if (removed) {
                _tracker.mark_modified(_current_time);
                if (_observer) {
                    _observer->notify(_current_time);
                }
            }
            return removed;
        }

        [[nodiscard]] size_t dict_size() const {
            return _value_view.dict_size();
        }

        // Observer access
        [[nodiscard]] ObserverStorage* observer() { return _observer; }
        [[nodiscard]] const ObserverStorage* observer() const { return _observer; }

    private:
        ValueView _value_view;
        ModificationTracker _tracker;
        engine_time_t _current_time{MIN_DT};
        ObserverStorage* _observer{nullptr};  // Non-owning pointer to observer at this level
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

        // Modification state queries
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

        // Mutable access with tracking and observer notification
        [[nodiscard]] TimeSeriesValueView view(engine_time_t current_time) {
            return {_value.view(), _tracker.tracker(), current_time, _observers.get()};
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
        void subscribe(Notifiable* notifiable) {
            ensure_observers();
            _observers->subscribe(notifiable);
        }

        void unsubscribe(Notifiable* notifiable) {
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

    private:
        void ensure_observers() {
            if (!_observers) {
                _observers = std::make_unique<ObserverStorage>(_value.schema());
            }
        }

        Value _value;
        ModificationTrackerStorage _tracker;
        std::unique_ptr<ObserverStorage> _observers;  // Lazy: nullptr until first subscribe
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_TIME_SERIES_VALUE_H
