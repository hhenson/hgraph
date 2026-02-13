#pragma once

/**
 * @file ts_scalar_view.h
 * @brief TSScalarView - View for atomic time-series types (TS[T], TSW, SIGNAL).
 *
 * TSScalarView provides typed access to atomic time-series values with
 * proper modification tracking and observer notification on mutation.
 */

#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/types/value/indexed_view.h>
#include <hgraph/util/date_time.h>

namespace hgraph {

/**
 * @brief View for atomic time-series types.
 *
 * TSScalarView wraps a scalar time-series value and provides:
 * - Typed read access via value()
 * - Typed write access via set_value() with automatic modification tracking
 * - Observer notification on mutation
 *
 * Usage:
 * @code
 * TSScalarView<int> view(value_view, time_view, observer_view, current_time);
 *
 * // Read
 * int val = view.value();
 *
 * // Write (automatically marks modified and notifies observers)
 * view.set_value(42);
 * @endcode
 */
template<typename T>
class TSScalarView {
public:
    /**
     * @brief Construct a scalar view.
     *
     * @param value_view View of the value data
     * @param time_view View of the time data (engine_time_t)
     * @param observer_view View of the observer data (ObserverList)
     * @param current_time The current engine time
     */
    TSScalarView(value::View value_view,
                 value::View time_view,
                 value::View observer_view,
                 engine_time_t current_time) noexcept
        : value_view_(value_view)
        , time_view_(time_view)
        , observer_view_(observer_view)
        , current_time_(current_time)
    {}

    // ========== Read Access ==========

    /**
     * @brief Get the current value.
     * @return Const reference to the value
     */
    [[nodiscard]] const T& value() const {
        return value_view_.as<T>();
    }

    /**
     * @brief Get the last modification time.
     * @return The modification timestamp
     */
    [[nodiscard]] engine_time_t last_modified_time() const {
        return time_view_.as<engine_time_t>();
    }

    /**
     * @brief Check if modified at or after current_time.
     * @return true if last_modified_time >= current_time
     */
    [[nodiscard]] bool modified() const {
        return last_modified_time() >= current_time_;
    }

    /**
     * @brief Check if the value has ever been set.
     * @return true if last_modified_time != MIN_DT
     */
    [[nodiscard]] bool valid() const {
        return last_modified_time() != MIN_DT;
    }

    // ========== Write Access ==========

    /**
     * @brief Set the value with modification tracking.
     *
     * This method:
     * 1. Updates the value
     * 2. Marks the modification time as current_time
     * 3. Notifies all observers
     *
     * @param val The new value
     */
    void set_value(const T& val) {
        // Update value
        value_view_.as<T>() = val;

        // Mark modified
        time_view_.as<engine_time_t>() = current_time_;

        // Notify observers
        auto* observers = static_cast<ObserverList*>(observer_view_.data());
        observers->notify_modified(current_time_);
    }

    /**
     * @brief Set the value with move semantics.
     *
     * @param val The new value (moved)
     */
    void set_value(T&& val) {
        // Update value
        value_view_.as<T>() = std::move(val);

        // Mark modified
        time_view_.as<engine_time_t>() = current_time_;

        // Notify observers
        auto* observers = static_cast<ObserverList*>(observer_view_.data());
        observers->notify_modified(current_time_);
    }

    /**
     * @brief Get mutable reference for in-place modification.
     *
     * IMPORTANT: After modifying through this reference, you MUST call
     * mark_modified() to ensure proper tracking and notification.
     *
     * @return Mutable reference to the value
     */
    [[nodiscard]] T& value_mut() {
        return value_view_.as<T>();
    }

    /**
     * @brief Manually mark as modified and notify observers.
     *
     * Call this after modifying through value_mut().
     */
    void mark_modified() {
        time_view_.as<engine_time_t>() = current_time_;

        auto* observers = static_cast<ObserverList*>(observer_view_.data());
        observers->notify_modified(current_time_);
    }

    // ========== Observer Access ==========

    /**
     * @brief Add an observer.
     * @param obs The observer to add
     */
    void add_observer(Notifiable* obs) {
        auto* observers = static_cast<ObserverList*>(observer_view_.data());
        observers->add_observer(obs);
    }

    /**
     * @brief Remove an observer.
     * @param obs The observer to remove
     */
    void remove_observer(Notifiable* obs) {
        auto* observers = static_cast<ObserverList*>(observer_view_.data());
        observers->remove_observer(obs);
    }

private:
    value::View value_view_;
    value::View time_view_;
    value::View observer_view_;
    engine_time_t current_time_;
};

} // namespace hgraph
