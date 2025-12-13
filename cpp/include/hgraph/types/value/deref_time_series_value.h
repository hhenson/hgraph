//
// Created by Howard Henson on 13/12/2025.
//
// DerefTimeSeriesValue - Wrapper that dereferences REF values transparently
//

#ifndef HGRAPH_VALUE_DEREF_TIME_SERIES_VALUE_H
#define HGRAPH_VALUE_DEREF_TIME_SERIES_VALUE_H

#include <hgraph/types/value/time_series_value.h>
#include <hgraph/types/value/ref_type.h>
#include <hgraph/types/value/modification_tracker.h>
#include <hgraph/util/date_time.h>

namespace hgraph::value {

    /**
     * DerefTimeSeriesValue - Wraps a REF value and provides transparent access
     *
     * This class manages dereferencing of REF types, providing:
     * - Transparent access to the referenced value
     * - Unified modification tracking (ref change OR underlying value change)
     * - Previous target retention for delta computation
     *
     * Lifecycle:
     * - begin_evaluation(): Update bindings, capture previous target if ref changed
     * - modified_at(): Check if ref changed OR underlying value modified
     * - previous_target(): Access previous binding for delta computation
     * - end_evaluation(): Clear previous target
     *
     * Usage:
     *   DerefTimeSeriesValue deref(ref_view, target_schema);
     *
     *   // Each evaluation cycle:
     *   deref.begin_evaluation(current_time);
     *   if (deref.modified_at(current_time)) {
     *       auto value = deref.target_value();
     *       // Use value...
     *       if (deref.has_previous()) {
     *           auto prev = deref.previous_target();
     *           // Compute delta...
     *       }
     *   }
     *   deref.end_evaluation();
     */
    class DerefTimeSeriesValue {
    public:
        DerefTimeSeriesValue() = default;

        /**
         * Construct a deref wrapper
         *
         * @param ref_view View to the REF value (TimeSeriesValueView pointing to RefStorage)
         * @param target_schema Schema of the dereferenced type (what the input expects)
         */
        DerefTimeSeriesValue(TimeSeriesValueView ref_view, const TypeMeta* target_schema)
            : _ref_view(ref_view), _target_schema(target_schema) {}

        // Move only (contains view references)
        DerefTimeSeriesValue(DerefTimeSeriesValue&&) noexcept = default;
        DerefTimeSeriesValue& operator=(DerefTimeSeriesValue&&) noexcept = default;
        DerefTimeSeriesValue(const DerefTimeSeriesValue&) = delete;
        DerefTimeSeriesValue& operator=(const DerefTimeSeriesValue&) = delete;

        [[nodiscard]] bool valid() const {
            return _ref_view.valid() && _target_schema != nullptr;
        }

        [[nodiscard]] const TypeMeta* target_schema() const { return _target_schema; }

        /**
         * Get the current target value (dereferenced)
         *
         * Returns a view to the value pointed to by the REF.
         * Returns invalid view if ref is empty/unbound.
         */
        [[nodiscard]] ConstValueView target_value() const {
            if (!_current_target.valid()) {
                return {};
            }
            return ConstValueView(_current_target.data, _current_target.schema);
        }

        /**
         * Get the current ValueRef
         */
        [[nodiscard]] const ValueRef& current_target() const { return _current_target; }

        /**
         * Unified modification tracking
         *
         * Returns true if:
         * - The reference binding changed at this time, OR
         * - The underlying value was modified at this time
         */
        [[nodiscard]] bool modified_at(engine_time_t time) const {
            // Check if ref changed at this time
            if (_current_target_bound_at == time) {
                return true;
            }

            // Check if underlying value modified at this time
            if (_current_target.valid() && _current_target.tracker) {
                ModificationTracker tracker(_current_target.tracker, _target_schema);
                return tracker.modified_at(time);
            }

            return false;
        }

        /**
         * Check if we have a previous target available for delta computation
         */
        [[nodiscard]] bool has_previous() const {
            return _previous_target.valid();
        }

        /**
         * Get the previous target (for delta computation)
         *
         * Only valid during the evaluation cycle where the ref changed.
         * After end_evaluation(), this returns an invalid ref.
         */
        [[nodiscard]] const ValueRef& previous_target() const { return _previous_target; }

        /**
         * Get previous value as a const view
         */
        [[nodiscard]] ConstValueView previous_value() const {
            if (!_previous_target.valid()) {
                return {};
            }
            return ConstValueView(_previous_target.data, _previous_target.schema);
        }

        /**
         * Check if reference changed at the given time
         */
        [[nodiscard]] bool ref_changed_at(engine_time_t time) const {
            return _current_target_bound_at == time;
        }

        /**
         * Begin evaluation cycle
         *
         * Updates the current binding if the ref value changed.
         * Captures the previous target if binding changed (for delta computation).
         */
        void begin_evaluation(engine_time_t time) {
            if (!valid()) return;

            // Clear previous target from last cycle
            _previous_target = ValueRef{};

            // Check if the REF itself was modified
            if (_ref_view.tracker().modified_at(time)) {
                // Get the new target from the ref
                auto* new_target = _ref_view.ref_target();

                if (new_target) {
                    // Check if target actually changed
                    if (*new_target != _current_target) {
                        // Capture previous for delta computation
                        _previous_target = _current_target;

                        // Update current binding
                        _current_target = *new_target;
                        _current_target_bound_at = time;
                    }
                } else {
                    // Ref cleared - capture previous and clear current
                    if (_current_target.valid()) {
                        _previous_target = _current_target;
                        _current_target = ValueRef{};
                        _current_target_bound_at = time;
                    }
                }
            }

            // If we don't have a current target yet (first evaluation), try to get one
            if (!_current_target.valid() && _ref_view.ref_is_bound()) {
                auto* target = _ref_view.ref_target();
                if (target && target->valid()) {
                    _current_target = *target;
                    _current_target_bound_at = time;
                }
            }
        }

        /**
         * End evaluation cycle
         *
         * Clears the previous target (no longer needed for delta computation).
         */
        void end_evaluation() {
            _previous_target = ValueRef{};
        }

    private:
        // Source REF value (view to RefStorage in a TimeSeriesValue)
        TimeSeriesValueView _ref_view;

        // Schema of the target type (what we're dereferencing to)
        const TypeMeta* _target_schema{nullptr};

        // Current binding state
        ValueRef _current_target;
        engine_time_t _current_target_bound_at{MIN_DT};

        // Previous target (shallow ref, for delta computation during one cycle)
        ValueRef _previous_target;
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_DEREF_TIME_SERIES_VALUE_H
