#pragma once

/**
 * @file ts_bundle_view.h
 * @brief TSBView - View for time-series bundle (TSB) types.
 *
 * TSBView provides field-based access to bundle time-series with proper
 * modification tracking that bubbles up to the container level.
 */

#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_scalar_view.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/types/value/indexed_view.h>
#include <hgraph/util/date_time.h>

#include <string_view>

namespace hgraph {

// Forward declaration
class TSView;

/**
 * @brief View for time-series bundle (TSB) types.
 *
 * TSBView provides access to bundle fields as nested time-series views.
 * When a field is modified:
 * 1. The field's time is updated
 * 2. The field's observers are notified
 * 3. The container's time is updated (bubble up)
 * 4. The container's observers are notified (bubble up)
 *
 * Usage:
 * @code
 * TSBView bundle_view(meta, value_view, time_view, observer_view, current_time);
 *
 * // Access field as scalar
 * auto field = bundle_view.field<int>("price");
 * field.set_value(100);  // Automatically bubbles up
 *
 * // Check field status
 * if (bundle_view.field_modified("price")) { ... }
 * @endcode
 */
class TSBView {
public:
    /**
     * @brief Construct a bundle view.
     *
     * @param meta The TSMeta for this bundle
     * @param value_view View of the value (bundle type)
     * @param time_view View of the time (tuple[engine_time_t, ...])
     * @param observer_view View of the observer (tuple[ObserverList, ...])
     * @param current_time The current engine time
     */
    TSBView(const TSMeta* meta,
            value::View value_view,
            value::View time_view,
            value::View observer_view,
            engine_time_t current_time) noexcept
        : meta_(meta)
        , value_view_(value_view)
        , time_view_(time_view)
        , observer_view_(observer_view)
        , current_time_(current_time)
    {}

    // ========== Container-Level Access ==========

    /**
     * @brief Get the container's last modification time.
     * @return The container's modification timestamp
     */
    [[nodiscard]] engine_time_t last_modified_time() const {
        return time_view_.as_tuple().at(0).as<engine_time_t>();
    }

    /**
     * @brief Check if container is modified.
     * @return true if any field was modified at or after current_time
     */
    [[nodiscard]] bool modified() const {
        return last_modified_time() >= current_time_;
    }

    /**
     * @brief Check if the bundle has ever been set.
     * @return true if last_modified_time != MIN_ST
     */
    [[nodiscard]] bool valid() const {
        return last_modified_time() != MIN_ST;
    }

    /**
     * @brief Get the number of fields.
     */
    [[nodiscard]] size_t field_count() const {
        return meta_ ? meta_->field_count : 0;
    }

    // ========== Field Access by Index ==========

    /**
     * @brief Get a field's value view by index.
     * @param index The field index
     * @return View of the field's value
     */
    [[nodiscard]] value::View field_value(size_t index) const {
        return value_view_.as_bundle().at(index);
    }

    /**
     * @brief Get a field's time view by index.
     * @param index The field index
     * @return View of the field's time structure
     */
    [[nodiscard]] value::View field_time(size_t index) const {
        // Time is tuple[container_time, field0_time, field1_time, ...]
        return time_view_.as_tuple().at(index + 1);
    }

    /**
     * @brief Get a field's observer view by index.
     * @param index The field index
     * @return View of the field's observer structure
     */
    [[nodiscard]] value::View field_observer(size_t index) const {
        // Observer is tuple[container_obs, field0_obs, field1_obs, ...]
        return observer_view_.as_tuple().at(index + 1);
    }

    /**
     * @brief Check if a field is modified by index.
     * @param index The field index
     * @return true if the field was modified at or after current_time
     */
    [[nodiscard]] bool field_modified(size_t index) const {
        auto field_time = field_time_value(index);
        return field_time >= current_time_;
    }

    /**
     * @brief Check if a field is valid by index.
     * @param index The field index
     * @return true if the field has ever been set
     */
    [[nodiscard]] bool field_valid(size_t index) const {
        auto field_time = field_time_value(index);
        return field_time != MIN_ST;
    }

    // ========== Field Access by Name ==========

    /**
     * @brief Get a field's value view by name.
     * @param name The field name
     * @return View of the field's value
     */
    [[nodiscard]] value::View field_value(std::string_view name) const {
        return value_view_.as_bundle().at(name);
    }

    /**
     * @brief Check if a field is modified by name.
     * @param name The field name
     * @return true if the field was modified at or after current_time
     */
    [[nodiscard]] bool field_modified(std::string_view name) const {
        size_t index = find_field_index(name);
        return field_modified(index);
    }

    /**
     * @brief Check if a field is valid by name.
     * @param name The field name
     * @return true if the field has ever been set
     */
    [[nodiscard]] bool field_valid(std::string_view name) const {
        size_t index = find_field_index(name);
        return field_valid(index);
    }

    // ========== Typed Field Access ==========

    /**
     * @brief Get a scalar field view by index.
     *
     * @tparam T The scalar type
     * @param index The field index
     * @return TSScalarView for the field with bubbling support
     */
    template<typename T>
    [[nodiscard]] TSScalarView<T> field_scalar(size_t index) {
        return TSScalarView<T>(
            field_value(index),
            field_time(index),
            field_observer(index),
            current_time_
        );
    }

    /**
     * @brief Get a scalar field view by name.
     *
     * @tparam T The scalar type
     * @param name The field name
     * @return TSScalarView for the field with bubbling support
     */
    template<typename T>
    [[nodiscard]] TSScalarView<T> field_scalar(std::string_view name) {
        size_t index = find_field_index(name);
        return field_scalar<T>(index);
    }

    // ========== Mutation with Bubbling ==========

    /**
     * @brief Set a scalar field value with bubbling.
     *
     * This method:
     * 1. Updates the field value
     * 2. Marks the field time as current_time
     * 3. Notifies field observers
     * 4. Marks the container time as current_time (bubble up)
     * 5. Notifies container observers (bubble up)
     *
     * @tparam T The value type
     * @param index The field index
     * @param value The new value
     */
    template<typename T>
    void set_field(size_t index, const T& value) {
        // Update field value
        field_value(index).as<T>() = value;

        // Mark field modified
        mark_field_modified(index);

        // Bubble up to container
        mark_container_modified();
    }

    /**
     * @brief Set a scalar field value by name with bubbling.
     *
     * @tparam T The value type
     * @param name The field name
     * @param value The new value
     */
    template<typename T>
    void set_field(std::string_view name, const T& value) {
        size_t index = find_field_index(name);
        set_field<T>(index, value);
    }

    /**
     * @brief Mark a field as modified (without changing the value).
     *
     * Use this after modifying through field_value() directly.
     *
     * @param index The field index
     */
    void mark_field_modified(size_t index) {
        // Get field's TSMeta to determine time structure
        const TSMeta* field_meta = meta_->fields[index].ts_type;

        // Update field time based on its kind
        auto ft = field_time(index);
        if (field_meta->is_scalar_ts()) {
            // Scalar field: time is engine_time_t directly
            ft.as<engine_time_t>() = current_time_;
        } else {
            // Composite field: time is tuple[engine_time_t, ...]
            ft.as_tuple().at(0).as<engine_time_t>() = current_time_;
        }

        // Notify field observers
        auto fo = field_observer(index);
        if (field_meta->is_scalar_ts()) {
            auto* obs = static_cast<ObserverList*>(fo.data());
            obs->notify_modified(current_time_);
        } else {
            auto* obs = static_cast<ObserverList*>(fo.as_tuple().at(0).data());
            obs->notify_modified(current_time_);
        }
    }

    /**
     * @brief Mark the container as modified.
     *
     * Called automatically when fields are modified, or can be called
     * manually after bulk updates.
     */
    void mark_container_modified() {
        // Update container time (first element of time tuple)
        time_view_.as_tuple().at(0).as<engine_time_t>() = current_time_;

        // Notify container observers (first element of observer tuple)
        auto* container_obs = static_cast<ObserverList*>(
            observer_view_.as_tuple().at(0).data());
        container_obs->notify_modified(current_time_);
    }

    // ========== Observer Access ==========

    /**
     * @brief Add an observer to the container.
     * @param obs The observer to add
     */
    void add_observer(Notifiable* obs) {
        auto* container_obs = static_cast<ObserverList*>(
            observer_view_.as_tuple().at(0).data());
        container_obs->add_observer(obs);
    }

    /**
     * @brief Add an observer to a specific field.
     * @param index The field index
     * @param obs The observer to add
     */
    void add_field_observer(size_t index, Notifiable* obs) {
        const TSMeta* field_meta = meta_->fields[index].ts_type;
        auto fo = field_observer(index);

        if (field_meta->is_scalar_ts()) {
            auto* field_obs = static_cast<ObserverList*>(fo.data());
            field_obs->add_observer(obs);
        } else {
            auto* field_obs = static_cast<ObserverList*>(fo.as_tuple().at(0).data());
            field_obs->add_observer(obs);
        }
    }

private:
    /**
     * @brief Get the field's time value for comparison.
     */
    [[nodiscard]] engine_time_t field_time_value(size_t index) const {
        const TSMeta* field_meta = meta_->fields[index].ts_type;
        auto ft = field_time(index);

        if (field_meta->is_scalar_ts()) {
            return ft.as<engine_time_t>();
        } else {
            return ft.as_tuple().at(0).as<engine_time_t>();
        }
    }

    /**
     * @brief Find field index by name.
     */
    [[nodiscard]] size_t find_field_index(std::string_view name) const {
        for (size_t i = 0; i < meta_->field_count; ++i) {
            if (name == meta_->fields[i].name) {
                return i;
            }
        }
        throw std::runtime_error("Field not found: " + std::string(name));
    }

    const TSMeta* meta_;
    value::View value_view_;
    value::View time_view_;
    value::View observer_view_;
    engine_time_t current_time_;
};

} // namespace hgraph
