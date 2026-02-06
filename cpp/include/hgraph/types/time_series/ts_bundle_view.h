#pragma once

/**
 * @file ts_bundle_view.h
 * @brief TSBView - View for time-series bundle (TSB) types.
 *
 * TSBView provides field-based access to bundle time-series.
 * Access fields via field(name) or field(index) to get TSView.
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

#include <string_view>

namespace hgraph {

// Forward declaration
class TSView;

/**
 * @brief View for time-series bundle (TSB) types.
 *
 * TSBView provides access to bundle fields as nested time-series views.
 * Use field(name) or field(index) to navigate to child TSViews.
 *
 * Usage:
 * @code
 * TSBView bundle = ts_view.as_bundle();
 *
 * // Access field by name
 * TSView bid_ts = bundle.field("bid");
 * double bid = bid_ts.value<double>();
 *
 * // Access field by index
 * TSView first = bundle.field(0);
 *
 * // Iterate over all fields
 * for (auto it = bundle.items().begin(); it != bundle.items().end(); ++it) {
 *     std::cout << it.name() << ": " << (*it).value<double>() << "\n";
 * }
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

    // ========== Field Navigation ==========

    /**
     * @brief Get a field as a TSView by index.
     *
     * @param index The field index
     * @return TSView for the field
     */
    [[nodiscard]] TSView field(size_t index) const {
        if (!view_data_.ops) {
            throw std::runtime_error("field() requires valid ops");
        }
        return view_data_.ops->child_at(view_data_, index, current_time_);
    }

    /**
     * @brief Get a field as a TSView by name.
     *
     * @param name The field name
     * @return TSView for the field
     */
    [[nodiscard]] TSView field(std::string_view name) const {
        if (!view_data_.ops) {
            throw std::runtime_error("field() requires valid ops");
        }
        return view_data_.ops->child_by_name(view_data_, std::string(name), current_time_);
    }

    /**
     * @brief Access a field by index using [] operator.
     *
     * Equivalent to field(index).
     *
     * @param index The field index
     * @return TSView for the field
     *
     * Usage:
     * @code
     * TSBView quote = ...;
     * double bid = quote[0].value<double>();  // Same as quote.field(0)
     * @endcode
     */
    [[nodiscard]] TSView operator[](size_t index) const {
        return field(index);
    }

    /**
     * @brief Get the number of fields.
     */
    [[nodiscard]] size_t field_count() const {
        return meta() ? meta()->field_count : 0;
    }

    // ========== Iteration ==========

    /**
     * @brief Iterate over all fields.
     *
     * Use it.name() to get field name, *it to get TSView.
     *
     * @return TSFieldRange for iteration
     */
    [[nodiscard]] TSFieldRange items() const {
        if (!view_data_.valid()) {
            return TSFieldRange{};
        }
        return TSFieldRange(view_data_, meta(), 0, field_count(), current_time_);
    }

    /**
     * @brief Iterate over field names.
     *
     * Returns an iterator over field names without creating TSView objects.
     *
     * @return TSFieldNameRange for iteration
     *
     * Usage:
     * @code
     * for (const auto& field_name : bundle.keys()) {
     *     std::cout << field_name << "\n";
     * }
     * @endcode
     */
    [[nodiscard]] TSFieldNameRange keys() const {
        if (!meta()) {
            return TSFieldNameRange{};
        }
        return TSFieldNameRange(meta(), 0, field_count());
    }

    /**
     * @brief Iterate over valid fields only.
     *
     * @return FilteredTSFieldRange that skips invalid fields
     */
    [[nodiscard]] FilteredTSFieldRange<TSFilter::VALID> valid_items() const {
        if (!view_data_.valid()) {
            return FilteredTSFieldRange<TSFilter::VALID>{};
        }
        return FilteredTSFieldRange<TSFilter::VALID>(view_data_, meta(), 0, field_count(), current_time_);
    }

    /**
     * @brief Iterate over modified fields only.
     *
     * @return FilteredTSFieldRange that skips unmodified fields
     */
    [[nodiscard]] FilteredTSFieldRange<TSFilter::MODIFIED> modified_items() const {
        if (!view_data_.valid()) {
            return FilteredTSFieldRange<TSFilter::MODIFIED>{};
        }
        return FilteredTSFieldRange<TSFilter::MODIFIED>(view_data_, meta(), 0, field_count(), current_time_);
    }

    // ========== Container-Level Access ==========

    /**
     * @brief Get the container's last modification time.
     */
    [[nodiscard]] engine_time_t last_modified_time() const {
        return time_view().as_tuple().at(0).as<engine_time_t>();
    }

    /**
     * @brief Check if container is modified (any field changed).
     */
    [[nodiscard]] bool modified() const {
        return last_modified_time() >= current_time_;
    }

    /**
     * @brief Check if the bundle has ever been set.
     */
    [[nodiscard]] bool valid() const {
        return last_modified_time() != MIN_DT;
    }

private:
    /**
     * @brief Get the time view (internal).
     */
    [[nodiscard]] value::View time_view() const {
        return value::View(view_data_.time_data,
            TSMetaSchemaCache::instance().get_time_schema(meta()));
    }

    ViewData view_data_;
    engine_time_t current_time_{MIN_DT};
};

} // namespace hgraph
