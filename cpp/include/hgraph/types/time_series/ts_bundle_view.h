#pragma once

/**
 * @file ts_bundle_view.h
 * @brief TSBView - View for time-series bundle (TSB) types.
 *
 * TSBView provides field-based access to bundle time-series with proper
 * modification tracking that bubbles up to the container level.
 */

#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/time_series/ts_scalar_view.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_view_range.h>
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
 * TSBView wraps a ViewData (containing all data pointers) plus current_time.
 *
 * Usage:
 * @code
 * TSBView bundle_view(view_data, current_time);
 *
 * // Access field as scalar
 * auto field = bundle_view.field_scalar<int>(0);
 * field.set_value(100);  // Automatically bubbles up
 *
 * // Check field status
 * if (bundle_view.field_modified(0)) { ... }
 * @endcode
 */
class TSBView {
public:
    /**
     * @brief Construct a bundle view from ViewData.
     *
     * @param view_data ViewData containing all data pointers and metadata
     * @param current_time The current engine time
     */
    TSBView(ViewData view_data, engine_time_t current_time) noexcept
        : view_data_(std::move(view_data))
        , current_time_(current_time)
    {}

    /**
     * @brief Default constructor - creates invalid view.
     */
    TSBView() noexcept = default;

    // ========== View Access ==========

    /**
     * @brief Get the value view.
     */
    [[nodiscard]] value::View value_view() const {
        return value::View(view_data_.value_data, meta()->value_type);
    }

    /**
     * @brief Get the time view.
     */
    [[nodiscard]] value::View time_view() const {
        return value::View(view_data_.time_data,
            TSMetaSchemaCache::instance().get_time_schema(meta()));
    }

    /**
     * @brief Get the observer view.
     */
    [[nodiscard]] value::View observer_view() const {
        return value::View(view_data_.observer_data,
            TSMetaSchemaCache::instance().get_observer_schema(meta()));
    }

    /**
     * @brief Get the TSMeta.
     */
    [[nodiscard]] const TSMeta* meta() const noexcept {
        return view_data_.meta;
    }

    /**
     * @brief Get the underlying ViewData.
     */
    [[nodiscard]] const ViewData& view_data() const noexcept {
        return view_data_;
    }

    // ========== TSView Navigation ==========

    /**
     * @brief Get a field as a TSView by index.
     *
     * Returns a proper TSView with full time-series semantics.
     *
     * @param index The field index
     * @return TSView for the field
     */
    [[nodiscard]] TSView field_ts(size_t index) const {
        if (!view_data_.ops) {
            throw std::runtime_error("field_ts requires valid ops");
        }
        return view_data_.ops->child_at(view_data_, index, current_time_);
    }

    /**
     * @brief Get a field as a TSView by name.
     *
     * Returns a proper TSView with full time-series semantics.
     *
     * @param name The field name
     * @return TSView for the field
     */
    [[nodiscard]] TSView field_ts(std::string_view name) const {
        if (!view_data_.ops) {
            throw std::runtime_error("field_ts requires valid ops");
        }
        return view_data_.ops->child_by_name(view_data_, std::string(name), current_time_);
    }

    /**
     * @brief Iterate over all fields as TSViews with names.
     *
     * Use it.name() to get field name, *it to get TSView.
     *
     * @return TSFieldRange for iteration
     */
    [[nodiscard]] TSFieldRange fields() const {
        if (!view_data_.valid()) {
            return TSFieldRange{};
        }
        return TSFieldRange(view_data_, meta(), 0, field_count(), current_time_);
    }

    // ========== Container-Level Access ==========

    /**
     * @brief Get the container's last modification time.
     * @return The container's modification timestamp
     */
    [[nodiscard]] engine_time_t last_modified_time() const {
        return time_view().as_tuple().at(0).as<engine_time_t>();
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
        return meta() ? meta()->field_count : 0;
    }

    // ========== Field Access by Index ==========

    /**
     * @brief Get a field's value view by index.
     * @param index The field index
     * @return View of the field's value
     */
    [[nodiscard]] value::View field_value(size_t index) const {
        return value_view().as_bundle().at(index);
    }

    /**
     * @brief Get a field's time view by index.
     * @param index The field index
     * @return View of the field's time structure
     */
    [[nodiscard]] value::View field_time(size_t index) const {
        // Time is tuple[container_time, field0_time, field1_time, ...]
        return time_view().as_tuple().at(index + 1);
    }

    /**
     * @brief Get a field's observer view by index.
     * @param index The field index
     * @return View of the field's observer structure
     */
    [[nodiscard]] value::View field_observer(size_t index) const {
        // Observer is tuple[container_obs, field0_obs, field1_obs, ...]
        return observer_view().as_tuple().at(index + 1);
    }

    /**
     * @brief Check if a field is modified by index.
     * @param index The field index
     * @return true if the field was modified at or after current_time
     */
    [[nodiscard]] bool field_modified(size_t index) const {
        auto ft = field_time_value(index);
        return ft >= current_time_;
    }

    /**
     * @brief Check if a field is valid by index.
     * @param index The field index
     * @return true if the field has ever been set
     */
    [[nodiscard]] bool field_valid(size_t index) const {
        auto ft = field_time_value(index);
        return ft != MIN_ST;
    }

    // ========== Field Access by Name ==========

    /**
     * @brief Get a field's value view by name.
     * @param name The field name
     * @return View of the field's value
     */
    [[nodiscard]] value::View field_value(std::string_view name) const {
        return value_view().as_bundle().at(name);
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
        const TSMeta* field_meta = meta()->fields[index].ts_type;

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
        time_view().as_tuple().at(0).as<engine_time_t>() = current_time_;

        // Notify container observers (first element of observer tuple)
        auto* container_obs = static_cast<ObserverList*>(
            observer_view().as_tuple().at(0).data());
        container_obs->notify_modified(current_time_);
    }

    // ========== Observer Access ==========

    /**
     * @brief Add an observer to the container.
     * @param obs The observer to add
     */
    void add_observer(Notifiable* obs) {
        auto* container_obs = static_cast<ObserverList*>(
            observer_view().as_tuple().at(0).data());
        container_obs->add_observer(obs);
    }

    /**
     * @brief Add an observer to a specific field.
     * @param index The field index
     * @param obs The observer to add
     */
    void add_field_observer(size_t index, Notifiable* obs) {
        const TSMeta* field_meta = meta()->fields[index].ts_type;
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
        const TSMeta* field_meta = meta()->fields[index].ts_type;
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
        for (size_t i = 0; i < meta()->field_count; ++i) {
            if (name == meta()->fields[i].name) {
                return i;
            }
        }
        throw std::runtime_error("Field not found: " + std::string(name));
    }

    ViewData view_data_;
    engine_time_t current_time_{MIN_ST};
};

} // namespace hgraph
