#pragma once

/**
 * @file view_data.h
 * @brief ViewData - Core data structure for time-series views and links.
 *
 * ViewData is the shared structure between TSView and Link, containing:
 * - ShortPath: Graph-aware navigation path
 * - Data pointers: Access to value, time, observer, delta storage
 * - ts_ops: Operations vtable for polymorphic dispatch
 *
 * This structure enables:
 * - Converting Link to TSView by adding current_time
 * - Navigation that extends the path
 * - Efficient resolution without virtual dispatch overhead
 */

#include <hgraph/types/time_series/short_path.h>
#include <hgraph/types/time_series/ts_meta.h>

namespace hgraph {

// Forward declaration
struct ts_ops;

/**
 * @brief Core data structure for time-series views and links.
 *
 * ViewData contains all the information needed to access a time-series
 * value, including the graph-aware path for tracing back to the owning
 * node and the data pointers for value/time/observer/delta access.
 *
 * Key design points:
 * - ShortPath enables graph-level navigation (node, port, indices)
 * - Data pointers are raw for efficiency (lifetime managed externally)
 * - ts_ops enables polymorphic operations without virtual dispatch
 * - TSView = ViewData + current_time
 * - Link = ViewData (no current_time needed for binding)
 *
 * Usage:
 * @code
 * // ViewData is typically created by TSValue::make_view_data()
 * ViewData vd = ts_value.make_view_data(node, PortType::OUTPUT);
 *
 * // Access via ops table
 * bool mod = vd.ops->modified(vd, current_time);
 * value::View val = vd.ops->value(vd);
 *
 * // Navigation extends the path
 * ViewData child = vd.child_at(0);  // Creates new ViewData with path extended
 * @endcode
 */
struct ViewData {
    // ========== Graph Navigation ==========

    /**
     * @brief Graph-aware path to this view.
     *
     * Contains: Node*, PortType, vector<size_t> indices
     * Enables tracing back to owning node for scheduling/subscription.
     */
    ShortPath path;

    // ========== Data Pointers ==========

    /**
     * @brief Pointer to the value data.
     *
     * Points into TSValue::value_. Schema determined by TSMeta::value_type.
     */
    void* value_data{nullptr};

    /**
     * @brief Pointer to the time data.
     *
     * Points into TSValue::time_. Contains modification timestamps.
     * For scalars: engine_time_t*
     * For containers: Tuple with container time + child times
     */
    void* time_data{nullptr};

    /**
     * @brief Pointer to the observer data.
     *
     * Points into TSValue::observer_. Contains observer lists.
     * For scalars: ObserverList*
     * For containers: Tuple with container observers + child observers
     */
    void* observer_data{nullptr};

    /**
     * @brief Pointer to the delta data.
     *
     * Points into TSValue::delta_value_. Contains delta tracking.
     * May be nullptr if this TS kind doesn't track deltas.
     */
    void* delta_data{nullptr};

    /**
     * @brief Pointer to the link data.
     *
     * Points into TSValue::link_. Contains link flags for binding support.
     * May be nullptr if this TS kind doesn't support links (scalars).
     * For TSL/TSD: bool* indicating collection-level link.
     * For TSB: fixed_list[bool]* with one entry per field.
     */
    void* link_data{nullptr};

    // ========== Operations ==========

    /**
     * @brief Operations vtable for this time-series kind.
     *
     * Provides polymorphic operations (modified, value, set_value, etc.)
     * without virtual dispatch overhead.
     */
    const ts_ops* ops{nullptr};

    /**
     * @brief Time-series metadata.
     *
     * Contains kind, value_type, and generated schemas.
     */
    const TSMeta* meta{nullptr};

    // ========== Construction ==========

    /**
     * @brief Default constructor - creates invalid ViewData.
     */
    ViewData() noexcept = default;

    /**
     * @brief Full constructor.
     */
    ViewData(ShortPath path_,
             void* value_data_,
             void* time_data_,
             void* observer_data_,
             void* delta_data_,
             void* link_data_,
             const ts_ops* ops_,
             const TSMeta* meta_) noexcept
        : path(std::move(path_))
        , value_data(value_data_)
        , time_data(time_data_)
        , observer_data(observer_data_)
        , delta_data(delta_data_)
        , link_data(link_data_)
        , ops(ops_)
        , meta(meta_) {}

    /**
     * @brief Constructor without link_data (for backwards compatibility).
     */
    ViewData(ShortPath path_,
             void* value_data_,
             void* time_data_,
             void* observer_data_,
             void* delta_data_,
             const ts_ops* ops_,
             const TSMeta* meta_) noexcept
        : path(std::move(path_))
        , value_data(value_data_)
        , time_data(time_data_)
        , observer_data(observer_data_)
        , delta_data(delta_data_)
        , link_data(nullptr)
        , ops(ops_)
        , meta(meta_) {}

    // ========== Validity ==========

    /**
     * @brief Check if the ViewData is valid.
     *
     * Valid if has ops table and value_data pointer.
     */
    [[nodiscard]] bool valid() const noexcept {
        return ops != nullptr && value_data != nullptr;
    }

    /**
     * @brief Boolean conversion - returns validity.
     */
    explicit operator bool() const noexcept {
        return valid();
    }

    // ========== Navigation Helpers ==========

    /**
     * @brief Create a child ViewData by navigating to an index.
     *
     * This extends the path and adjusts the data pointers to point
     * to the child element's data.
     *
     * @param index The child index
     * @return ViewData for the child
     */
    [[nodiscard]] ViewData child_at(size_t index) const;

    /**
     * @brief Create a child ViewData by navigating to a field name.
     *
     * Only valid for bundle types.
     *
     * @param name The field name
     * @return ViewData for the field
     */
    [[nodiscard]] ViewData child_by_name(const std::string& name) const;
};

} // namespace hgraph
