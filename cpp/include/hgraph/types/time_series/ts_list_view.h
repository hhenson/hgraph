#pragma once

/**
 * @file ts_list_view.h
 * @brief TSLView - View for time-series list (TSL) types.
 *
 * TSLView provides element-based access to list time-series with proper
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

namespace hgraph {

// Forward declaration
class TSView;

/**
 * @brief View for time-series list (TSL) types.
 *
 * TSLView provides access to list elements as nested time-series views.
 * When an element is modified:
 * 1. The element's time is updated
 * 2. The element's observers are notified
 * 3. The container's time is updated (bubble up)
 * 4. The container's observers are notified (bubble up)
 *
 * TSLView wraps a ViewData (containing all data pointers) plus current_time.
 *
 * Usage:
 * @code
 * TSLView list_view(view_data, current_time);
 *
 * // Access element as scalar
 * auto elem = list_view.element_scalar<int>(0);
 * elem.set_value(100);  // Automatically bubbles up
 *
 * // Check element status
 * if (list_view.element_modified(0)) { ... }
 * @endcode
 */
class TSLView {
public:
    /**
     * @brief Construct a list view from ViewData.
     *
     * @param view_data ViewData containing all data pointers and metadata
     * @param current_time The current engine time
     */
    TSLView(ViewData view_data, engine_time_t current_time) noexcept
        : view_data_(std::move(view_data))
        , current_time_(current_time)
    {}

    /**
     * @brief Default constructor - creates invalid view.
     */
    TSLView() noexcept = default;

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
     * @brief Get an element as a TSView by index.
     *
     * Returns a proper TSView with full time-series semantics.
     *
     * @param index The element index
     * @return TSView for the element
     */
    [[nodiscard]] TSView element_ts(size_t index) const {
        if (!view_data_.ops) {
            throw std::runtime_error("element_ts requires valid ops");
        }
        return view_data_.ops->child_at(view_data_, index, current_time_);
    }

    /**
     * @brief Iterate over all elements as TSViews.
     *
     * Use it.index() to get element index, *it to get TSView.
     *
     * @return TSViewRange for iteration
     */
    [[nodiscard]] TSViewRange values() const {
        if (!view_data_.valid()) {
            return TSViewRange{};
        }
        return TSViewRange(view_data_, 0, size(), current_time_);
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
     * @return true if any element was modified at or after current_time
     */
    [[nodiscard]] bool modified() const {
        return last_modified_time() >= current_time_;
    }

    /**
     * @brief Check if the list has ever been set.
     * @return true if last_modified_time != MIN_ST
     */
    [[nodiscard]] bool valid() const {
        return last_modified_time() != MIN_ST;
    }

    /**
     * @brief Get the number of elements.
     */
    [[nodiscard]] size_t size() const {
        if (meta()->fixed_size > 0) {
            return meta()->fixed_size;
        }
        return value_view().as_list().size();
    }

    // ========== Element Access ==========

    /**
     * @brief Get an element's value view by index.
     * @param index The element index
     * @return View of the element's value
     */
    [[nodiscard]] value::View element_value(size_t index) const {
        return value_view().as_list().at(index);
    }

    /**
     * @brief Get an element's time view by index.
     * @param index The element index
     * @return View of the element's time structure
     */
    [[nodiscard]] value::View element_time(size_t index) const {
        // Time is tuple[container_time, list[element_times]]
        return time_view().as_tuple().at(1).as_list().at(index);
    }

    /**
     * @brief Get an element's observer view by index.
     * @param index The element index
     * @return View of the element's observer structure
     */
    [[nodiscard]] value::View element_observer(size_t index) const {
        // Observer is tuple[container_obs, list[element_obs]]
        return observer_view().as_tuple().at(1).as_list().at(index);
    }

    /**
     * @brief Check if an element is modified.
     * @param index The element index
     * @return true if the element was modified at or after current_time
     */
    [[nodiscard]] bool element_modified(size_t index) const {
        auto elem_time = element_time_value(index);
        return elem_time >= current_time_;
    }

    /**
     * @brief Check if an element is valid.
     * @param index The element index
     * @return true if the element has ever been set
     */
    [[nodiscard]] bool element_valid(size_t index) const {
        auto elem_time = element_time_value(index);
        return elem_time != MIN_ST;
    }

    // ========== Typed Element Access ==========

    /**
     * @brief Get a scalar element view.
     *
     * @tparam T The scalar type
     * @param index The element index
     * @return TSScalarView for the element with bubbling support
     */
    template<typename T>
    [[nodiscard]] TSScalarView<T> element_scalar(size_t index) {
        return TSScalarView<T>(
            element_value(index),
            element_time(index),
            element_observer(index),
            current_time_
        );
    }

    // ========== Mutation with Bubbling ==========

    /**
     * @brief Set an element value with bubbling.
     *
     * This method:
     * 1. Updates the element value
     * 2. Marks the element time as current_time
     * 3. Notifies element observers
     * 4. Marks the container time as current_time (bubble up)
     * 5. Notifies container observers (bubble up)
     *
     * @tparam T The value type
     * @param index The element index
     * @param value The new value
     */
    template<typename T>
    void set_element(size_t index, const T& value) {
        // Update element value
        element_value(index).as<T>() = value;

        // Mark element modified
        mark_element_modified(index);

        // Bubble up to container
        mark_container_modified();
    }

    /**
     * @brief Mark an element as modified (without changing the value).
     *
     * Use this after modifying through element_value() directly.
     *
     * @param index The element index
     */
    void mark_element_modified(size_t index) {
        const TSMeta* elem_meta = meta()->element_ts;

        // Update element time
        auto et = element_time(index);
        if (elem_meta->is_scalar_ts()) {
            et.as<engine_time_t>() = current_time_;
        } else {
            et.as_tuple().at(0).as<engine_time_t>() = current_time_;
        }

        // Notify element observers
        auto eo = element_observer(index);
        if (elem_meta->is_scalar_ts()) {
            auto* obs = static_cast<ObserverList*>(eo.data());
            obs->notify_modified(current_time_);
        } else {
            auto* obs = static_cast<ObserverList*>(eo.as_tuple().at(0).data());
            obs->notify_modified(current_time_);
        }
    }

    /**
     * @brief Mark the container as modified.
     */
    void mark_container_modified() {
        // Update container time
        time_view().as_tuple().at(0).as<engine_time_t>() = current_time_;

        // Notify container observers
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
     * @brief Add an observer to a specific element.
     * @param index The element index
     * @param obs The observer to add
     */
    void add_element_observer(size_t index, Notifiable* obs) {
        const TSMeta* elem_meta = meta()->element_ts;
        auto eo = element_observer(index);

        if (elem_meta->is_scalar_ts()) {
            auto* elem_obs = static_cast<ObserverList*>(eo.data());
            elem_obs->add_observer(obs);
        } else {
            auto* elem_obs = static_cast<ObserverList*>(eo.as_tuple().at(0).data());
            elem_obs->add_observer(obs);
        }
    }

private:
    /**
     * @brief Get the element's time value for comparison.
     */
    [[nodiscard]] engine_time_t element_time_value(size_t index) const {
        const TSMeta* elem_meta = meta()->element_ts;
        auto et = element_time(index);

        if (elem_meta->is_scalar_ts()) {
            return et.as<engine_time_t>();
        } else {
            return et.as_tuple().at(0).as<engine_time_t>();
        }
    }

    ViewData view_data_;
    engine_time_t current_time_{MIN_ST};
};

} // namespace hgraph
