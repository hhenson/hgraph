//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_TIME_SERIES_VALUE_H
#define HGRAPH_VALUE_TIME_SERIES_VALUE_H

#include <hgraph/types/value/value.h>
#include <hgraph/types/value/modification_tracker.h>
#include <hgraph/util/date_time.h>

namespace hgraph::value {

    // Forward declarations
    class TimeSeriesValue;
    class TimeSeriesValueView;

    /**
     * TimeSeriesValueView - Mutable view with automatic modification tracking
     *
     * Unlike raw ValueView, this view automatically marks modifications
     * when values are changed via set(). It also tracks the current
     * evaluation time for proper modification marking.
     *
     * Navigation (field/element) returns sub-views that propagate
     * modifications to the parent.
     */
    class TimeSeriesValueView {
    public:
        TimeSeriesValueView() = default;

        TimeSeriesValueView(ValueView value_view, ModificationTracker tracker, engine_time_t current_time)
            : _value_view(value_view), _tracker(tracker), _current_time(current_time) {}

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
        }

        // Bundle field navigation - returns sub-view with parent tracking
        [[nodiscard]] TimeSeriesValueView field(size_t index) {
            if (!valid() || kind() != TypeKind::Bundle) {
                return {};
            }
            return {_value_view.field(index), _tracker.field(index), _current_time};
        }

        [[nodiscard]] TimeSeriesValueView field(const std::string& name) {
            if (!valid() || kind() != TypeKind::Bundle) {
                return {};
            }
            return {_value_view.field(name), _tracker.field(name), _current_time};
        }

        [[nodiscard]] bool field_modified_at(size_t index, engine_time_t time) const {
            return _tracker.field_modified_at(index, time);
        }

        [[nodiscard]] size_t field_count() const {
            return _value_view.field_count();
        }

        // List element navigation - returns sub-view with parent tracking
        [[nodiscard]] TimeSeriesValueView element(size_t index) {
            if (!valid() || kind() != TypeKind::List) {
                return {};
            }
            return {_value_view.element(index), _tracker.element(index), _current_time};
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
            }
            return added;
        }

        template<typename T>
        bool remove(const T& element) {
            if (!valid() || kind() != TypeKind::Set) return false;
            bool removed = _value_view.set_remove(element);
            if (removed) {
                _tracker.mark_modified(_current_time);
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

            // Mark structural modification if new key
            if (is_new_key) {
                _tracker.mark_modified(_current_time);
            }

            // Note: For proper per-entry tracking, we'd need the entry index
            // from DictStorage. For now, we mark structural on any insert.
            // Future enhancement: track entry modifications separately.
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

        template<typename K>
        bool dict_remove(const K& key) {
            if (!valid() || kind() != TypeKind::Dict) return false;
            bool removed = _value_view.dict_remove(key);
            if (removed) {
                _tracker.mark_modified(_current_time);
            }
            return removed;
        }

        [[nodiscard]] size_t dict_size() const {
            return _value_view.dict_size();
        }

    private:
        ValueView _value_view;
        ModificationTracker _tracker;
        engine_time_t _current_time{MIN_DT};
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

        // Mutable access with tracking
        [[nodiscard]] TimeSeriesValueView view(engine_time_t current_time) {
            return {_value.view(), _tracker.tracker(), current_time};
        }

        // Direct scalar access (convenience for simple TS values)
        template<typename T>
        void set_value(const T& val, engine_time_t time) {
            _value.view().as<T>() = val;
            _tracker.tracker().mark_modified(time);
        }

        template<typename T>
        [[nodiscard]] const T& as() const {
            return _value.const_view().as<T>();
        }

        // Access underlying storage (for advanced use)
        [[nodiscard]] Value& underlying_value() { return _value; }
        [[nodiscard]] const Value& underlying_value() const { return _value; }
        [[nodiscard]] ModificationTrackerStorage& underlying_tracker() { return _tracker; }
        [[nodiscard]] const ModificationTrackerStorage& underlying_tracker() const { return _tracker; }

    private:
        Value _value;
        ModificationTrackerStorage _tracker;
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_TIME_SERIES_VALUE_H
