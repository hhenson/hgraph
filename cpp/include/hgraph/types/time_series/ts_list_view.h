#pragma once

/**
 * @file ts_list_view.h
 * @brief TSLView - View for time-series list (TSL) types.
 *
 * TSLView provides element-based access to list time-series.
 * Access elements via at(index) to get TSView.
 */

#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
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
 * Use at(index) to navigate to child TSViews.
 *
 * Usage:
 * @code
 * TSLView list = ts_view.as_list();
 *
 * // Access element by index
 * TSView elem = list.at(0);
 * double val = elem.value<double>();
 *
 * // Iterate over all elements
 * for (auto view : list.values()) {
 *     if (view.modified()) {
 *         std::cout << view.value<double>() << "\n";
 *     }
 * }
 *
 * // Iterate with index
 * for (auto it = list.items().begin(); it != list.items().end(); ++it) {
 *     std::cout << it.index() << ": " << (*it).value<double>() << "\n";
 * }
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

    // ========== Metadata ==========

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

    // ========== Element Navigation ==========

    /**
     * @brief Get an element as a TSView by index.
     *
     * @param index The element index
     * @return TSView for the element
     */
    [[nodiscard]] TSView at(size_t index) const {
        if (!view_data_.ops) {
            throw std::runtime_error("at() requires valid ops");
        }
        return view_data_.ops->child_at(view_data_, index, current_time_);
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

    // ========== Values Iteration ==========

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

    /**
     * @brief Iterate over valid elements only.
     *
     * @return TSViewRange that skips invalid elements
     */
    [[nodiscard]] TSViewRange valid_values() const {
        // TODO: Implement filtered iteration
        return values();
    }

    /**
     * @brief Iterate over modified elements only.
     *
     * @return TSViewRange that skips unmodified elements
     */
    [[nodiscard]] TSViewRange modified_values() const {
        // TODO: Implement filtered iteration
        return values();
    }

    // ========== Items Iteration (with index) ==========

    /**
     * @brief Iterate over all elements with index access.
     *
     * Use it.index() to get element index, *it to get TSView.
     *
     * @return TSViewRange for iteration
     */
    [[nodiscard]] TSViewRange items() const {
        return values();
    }

    /**
     * @brief Iterate over valid items only.
     *
     * @return TSViewRange that skips invalid items
     */
    [[nodiscard]] TSViewRange valid_items() const {
        return valid_values();
    }

    /**
     * @brief Iterate over modified items only.
     *
     * @return TSViewRange that skips unmodified items
     */
    [[nodiscard]] TSViewRange modified_items() const {
        return modified_values();
    }

    // ========== Container-Level Access ==========

    /**
     * @brief Get the container's last modification time.
     */
    [[nodiscard]] engine_time_t last_modified_time() const {
        return time_view().as_tuple().at(0).as<engine_time_t>();
    }

    /**
     * @brief Check if container is modified (any element changed).
     */
    [[nodiscard]] bool modified() const {
        return last_modified_time() >= current_time_;
    }

    /**
     * @brief Check if the list has ever been set.
     */
    [[nodiscard]] bool valid() const {
        return last_modified_time() != MIN_ST;
    }

private:
    /**
     * @brief Get the value view (internal).
     */
    [[nodiscard]] value::View value_view() const {
        return value::View(view_data_.value_data, meta()->value_type);
    }

    /**
     * @brief Get the time view (internal).
     */
    [[nodiscard]] value::View time_view() const {
        return value::View(view_data_.time_data,
            TSMetaSchemaCache::instance().get_time_schema(meta()));
    }

    ViewData view_data_;
    engine_time_t current_time_{MIN_ST};
};

} // namespace hgraph
