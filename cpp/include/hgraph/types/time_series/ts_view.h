#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/value/value_view.h>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph {

struct TSInput;
struct TSOutput;
struct TSView;
struct TSBView;
struct TSDView;
struct TSLView;
struct TSSView;
struct TSWView;
struct SignalView;

/**
 * Identifies the endpoint object that owns or produced a `TSView`.
 *
 * A time-series view is intended to be rooted at either an input endpoint or
 * an output endpoint.
 */
using ParentValue = std::variant<TSInput *, TSOutput *>;

/**
 * Type-erased single-value range.
 *
 * This is intended to provide iterator-based access without materializing a
 * container of results.
 */
template <typename T>
struct HGRAPH_EXPORT Range
{
    struct iterator
    {
        [[nodiscard]] T operator*() const;
        iterator &operator++();
        [[nodiscard]] bool operator!=(const iterator &other) const;
    };

    [[nodiscard]] iterator begin() const;
    [[nodiscard]] iterator end() const;
};

/**
 * Type-erased key/value range.
 *
 * This is intended for iteration surfaces that yield a logical key together
 * with a value, such as `items()` on collection views.
 */
template <typename K, typename V>
struct HGRAPH_EXPORT KeyValueRange
{
    struct iterator
    {
        [[nodiscard]] std::pair<K, V> operator*() const;
        iterator &operator++();
        [[nodiscard]] bool operator!=(const iterator &other) const;
    };

    [[nodiscard]] iterator begin() const;
    [[nodiscard]] iterator end() const;
};

/**
 * Lightweight type-erased view over a logical time-series position.
 *
 * `TSView` defines the common inspection surface shared by scalar,
 * collection, and signal time-series values. Reference time-series are
 * intended to be exposed through this same interface because `REF[...]` is
 * logically equivalent to `TS[TimeSeriesReference]`, rather than through a
 * dedicated reference-view type. Concrete derived view types will refine this
 * wrapper with kind-specific operations.
 */
struct HGRAPH_EXPORT TSView
{
    /**
     * Destroy the view interface.
     */
    virtual ~TSView() = default;

    /**
     * Return the engine time at which this view is being evaluated.
     */
    [[nodiscard]] virtual engine_time_t evaluation_time() const noexcept;

    /**
     * Return the current point-in-time value for this view.
     */
    [[nodiscard]] virtual value::View value() const noexcept;

    /**
     * Return the current delta value for this view.
     */
    [[nodiscard]] virtual value::View delta_value() const noexcept;

    /**
     * Return whether this view was modified in the current engine cycle.
     */
    [[nodiscard]] virtual bool modified() const noexcept;

    /**
     * Return whether this view currently holds a valid value.
     */
    [[nodiscard]] virtual bool valid() const noexcept;

    /**
     * Return whether this view and all required descendants are valid.
     */
    [[nodiscard]] virtual bool all_valid() const noexcept;

    /**
     * Return the last modification time associated to this view.
     */
    [[nodiscard]] virtual engine_time_t last_modified_time() const noexcept;

    /**
     * Interpret this view as a bundle view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSBView as_bundle() noexcept;

    /**
     * Interpret this view as a bundle view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSBView as_bundle() const noexcept;

    /**
     * Interpret this view as a list view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSLView as_list() noexcept;

    /**
     * Interpret this view as a list view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSLView as_list() const noexcept;

    /**
     * Interpret this view as a dictionary view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSDView as_dict() noexcept;

    /**
     * Interpret this view as a dictionary view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSDView as_dict() const noexcept;

    /**
     * Interpret this view as a set view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSSView as_set() noexcept;

    /**
     * Interpret this view as a set view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSSView as_set() const noexcept;

    /**
     * Interpret this view as a window view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSWView as_window() noexcept;

    /**
     * Interpret this view as a window view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSWView as_window() const noexcept;

    /**
     * Interpret this view as a signal view when the runtime kind matches.
     */
    [[nodiscard]] virtual SignalView as_signal() noexcept;

    /**
     * Interpret this view as a signal view when the runtime kind matches.
     */
    [[nodiscard]] virtual SignalView as_signal() const noexcept;

protected:
    /**
     * Return the endpoint that owns the root of this view.
     */
    [[nodiscard]] virtual ParentValue parent() const noexcept;

    /**
     * Return the state node associated to the represented time-series
     * position.
     */
    [[nodiscard]] virtual TimeSeriesStatePtr state() const noexcept;
};

/**
 * Base for collection-oriented time-series views.
 *
 * Collection views extend `TSView` with navigation over child positions and
 * collection-level size queries.
 */
struct HGRAPH_EXPORT BaseCollectionView : TSView
{
    /**
     * Destroy the collection view interface.
     */
    ~BaseCollectionView() override = default;

    /**
     * Return the number of logical child positions in this collection.
     */
    [[nodiscard]] virtual size_t size() const noexcept;

    /**
     * Return the child view at the supplied collection index.
     */
    [[nodiscard]] virtual TSView at(size_t index) const noexcept;

    /**
     * Return the child view at the supplied collection index.
     */
    [[nodiscard]] virtual TSView operator[](size_t index) const noexcept;
};

/**
 * View over a time-series list position.
 */
struct HGRAPH_EXPORT TSLView : BaseCollectionView
{
    /**
     * Destroy the list view interface.
     */
    ~TSLView() override = default;

    /**
     * Return all child views in index order.
     */
    [[nodiscard]] virtual Range<TSView> values() const noexcept;

    /**
     * Return the child views that are currently valid.
     */
    [[nodiscard]] virtual Range<TSView> valid_values() const noexcept;

    /**
     * Return the child views modified in the current engine cycle.
     */
    [[nodiscard]] virtual Range<TSView> modified_values() const noexcept;

    /**
     * Return all indexed items in this list.
     */
    [[nodiscard]] virtual KeyValueRange<size_t, TSView> items() const noexcept;

    /**
     * Return all valid indexed items in this list.
     */
    [[nodiscard]] virtual KeyValueRange<size_t, TSView> valid_items() const noexcept;

    /**
     * Return all modified indexed items in this list.
     */
    [[nodiscard]] virtual KeyValueRange<size_t, TSView> modified_items() const noexcept;
};

/**
 * View over a time-series bundle position.
 */
struct HGRAPH_EXPORT TSBView : BaseCollectionView
{
    /**
     * Destroy the bundle view interface.
     */
    ~TSBView() override = default;

    /**
     * Return the child view for the supplied field name.
     */
    [[nodiscard]] virtual TSView field(std::string_view name) const noexcept;

    /**
     * Return the bundle field names in schema order.
     */
    [[nodiscard]] virtual Range<std::string_view> keys() const noexcept;

    /**
     * Return the child views in schema order.
     */
    [[nodiscard]] virtual Range<TSView> values() const noexcept;

    /**
     * Return the bundle items in schema order.
     */
    [[nodiscard]] virtual KeyValueRange<std::string_view, TSView> items() const noexcept;

    /**
     * Return the valid bundle items.
     */
    [[nodiscard]] virtual KeyValueRange<std::string_view, TSView> valid_items() const noexcept;

    /**
     * Return the modified bundle items.
     */
    [[nodiscard]] virtual KeyValueRange<std::string_view, TSView> modified_items() const noexcept;
};

/**
 * View over a time-series dictionary position.
 */
struct HGRAPH_EXPORT TSDView : BaseCollectionView
{
    /**
     * Destroy the dictionary view interface.
     */
    ~TSDView() override = default;

    using BaseCollectionView::at;
    using BaseCollectionView::operator[];

    /**
     * Return the child view for the supplied key.
     */
    [[nodiscard]] virtual TSView at(const value::View &key) const noexcept;

    /**
     * Return the child view for the supplied key.
     */
    [[nodiscard]] virtual TSView operator[](const value::View &key) const noexcept;

    /**
     * Return whether the supplied key is currently present.
     */
    [[nodiscard]] virtual bool contains(const value::View &key) const noexcept;

    /**
     * Return the current key set as a set view.
     */
    [[nodiscard]] virtual TSSView key_set() const noexcept;

    /**
     * Return the currently present keys.
     */
    [[nodiscard]] virtual Range<value::View> keys() const noexcept;

    /**
     * Return the currently present child views.
     */
    [[nodiscard]] virtual Range<TSView> values() const noexcept;

    /**
     * Return the currently present key/value pairs.
     */
    [[nodiscard]] virtual KeyValueRange<value::View, TSView> items() const noexcept;

    /**
     * Return the valid key/value pairs.
     */
    [[nodiscard]] virtual KeyValueRange<value::View, TSView> valid_items() const noexcept;

    /**
     * Return the modified key/value pairs.
     */
    [[nodiscard]] virtual KeyValueRange<value::View, TSView> modified_items() const noexcept;

    /**
     * Return the keys added in the current engine cycle.
     */
    [[nodiscard]] virtual Range<value::View> added_keys() const noexcept;

    /**
     * Return the keys removed in the current engine cycle.
     */
    [[nodiscard]] virtual Range<value::View> removed_keys() const noexcept;

    /**
     * Return the items added in the current engine cycle.
     */
    [[nodiscard]] virtual KeyValueRange<value::View, TSView> added_items() const noexcept;

    /**
     * Return the existing items updated in the current engine cycle.
     */
    [[nodiscard]] virtual KeyValueRange<value::View, TSView> updated_items() const noexcept;

    /**
     * Return the items removed in the current engine cycle.
     */
    [[nodiscard]] virtual KeyValueRange<value::View, TSView> removed_items() const noexcept;
};

/**
 * View over a time-series set position.
 */
struct HGRAPH_EXPORT TSSView : TSView
{
    /**
     * Destroy the set view interface.
     */
    ~TSSView() override = default;

    /**
     * Return the number of values currently present in the set.
     */
    [[nodiscard]] virtual size_t size() const noexcept;

    /**
     * Return whether the supplied element is currently present.
     */
    [[nodiscard]] virtual bool contains(const value::View &element) const noexcept;

    /**
     * Return the current set values.
     */
    [[nodiscard]] virtual Range<value::View> values() const noexcept;

    /**
     * Return the values added in the current engine cycle.
     */
    [[nodiscard]] virtual Range<value::View> added() const noexcept;

    /**
     * Return whether the supplied element was added in the current engine
     * cycle.
     */
    [[nodiscard]] virtual bool was_added(const value::View &element) const noexcept;

    /**
     * Return the values removed in the current engine cycle.
     */
    [[nodiscard]] virtual Range<value::View> removed() const noexcept;

    /**
     * Return whether the supplied element was removed in the current engine
     * cycle.
     */
    [[nodiscard]] virtual bool was_removed(const value::View &element) const noexcept;

    /**
     * Return the nested time-series view that tracks empty-state semantics.
     */
    [[nodiscard]] virtual TSView is_empty() const noexcept;
};

/**
 * View over a time-series window position.
 */
struct HGRAPH_EXPORT TSWView : TSView
{
    /**
     * Destroy the window view interface.
     */
    ~TSWView() override = default;

    /**
     * Return the current number of buffered values.
     */
    [[nodiscard]] virtual size_t size() const noexcept;

    /**
     * Return the minimum size or readiness threshold for the window.
     */
    [[nodiscard]] virtual size_t min_size() const noexcept;

    /**
     * Return the modification times associated with buffered values.
     */
    [[nodiscard]] virtual Range<engine_time_t> value_times() const noexcept;

    /**
     * Return the modification time of the oldest buffered value.
     */
    [[nodiscard]] virtual engine_time_t first_modified_time() const noexcept;

    /**
     * Return whether an evicted value is available for this engine cycle.
     */
    [[nodiscard]] virtual bool has_removed_value() const noexcept;

    /**
     * Return the value evicted from the window in the current engine cycle.
     */
    [[nodiscard]] virtual value::View removed_value() const noexcept;
};

/**
 * View over a signal time-series position.
 */
struct HGRAPH_EXPORT SignalView : TSView
{
    /**
     * Destroy the signal view interface.
     */
    ~SignalView() override = default;
};

}  // namespace hgraph
