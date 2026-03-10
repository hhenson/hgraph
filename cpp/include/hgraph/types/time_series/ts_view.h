#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/value/value_view.h>

#include <string_view>
#include <utility>

namespace hgraph {

struct TSInput;
struct TSOutput;

template <typename TView>
struct TSView;

template <typename TView>
struct BaseCollectionView;

template <typename TView>
struct TSBView;

template <typename TView>
struct TSDView;

template <typename TView>
struct TSLView;

template <typename TView>
struct TSSView;

template <typename TView>
struct TSWView;

template <typename TView>
struct SignalView;

/**
 * Identifies the endpoint object that owns or produced a time-series view.
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
struct Range
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
struct KeyValueRange
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
 * This is the common view surface shared by input and output endpoint views.
 * `REF[...]` is intended to be treated as `TS[TimeSeriesReference]`, so
 * reference semantics remain on this generic time-series view contract rather
 * than on a dedicated reference view type.
 */
template <typename TView>
struct TSView
{
    /**
     * Return the engine time at which this view is being evaluated.
     */
    [[nodiscard]] engine_time_t evaluation_time() const noexcept;

    /**
     * Return the current point-in-time value for this view.
     */
    [[nodiscard]] value::View value() const noexcept;

    /**
     * Return the current delta value for this view.
     */
    [[nodiscard]] value::View delta_value() const noexcept;

    /**
     * Return whether this view was modified in the current engine cycle.
     */
    [[nodiscard]] bool modified() const noexcept;

    /**
     * Return whether this view currently holds a valid value.
     */
    [[nodiscard]] bool valid() const noexcept;

    /**
     * Return whether this view and all required descendants are valid.
     */
    [[nodiscard]] bool all_valid() const noexcept;

    /**
     * Return the last modification time associated to this view.
     */
    [[nodiscard]] engine_time_t last_modified_time() const noexcept;

    /**
     * Interpret this view as a bundle view when the runtime kind matches.
     */
    [[nodiscard]] TSBView<TView> as_bundle() noexcept;

    /**
     * Interpret this view as a bundle view when the runtime kind matches.
     */
    [[nodiscard]] TSBView<TView> as_bundle() const noexcept;

    /**
     * Interpret this view as a list view when the runtime kind matches.
     */
    [[nodiscard]] TSLView<TView> as_list() noexcept;

    /**
     * Interpret this view as a list view when the runtime kind matches.
     */
    [[nodiscard]] TSLView<TView> as_list() const noexcept;

    /**
     * Interpret this view as a dictionary view when the runtime kind matches.
     */
    [[nodiscard]] TSDView<TView> as_dict() noexcept;

    /**
     * Interpret this view as a dictionary view when the runtime kind matches.
     */
    [[nodiscard]] TSDView<TView> as_dict() const noexcept;

    /**
     * Interpret this view as a set view when the runtime kind matches.
     */
    [[nodiscard]] TSSView<TView> as_set() noexcept;

    /**
     * Interpret this view as a set view when the runtime kind matches.
     */
    [[nodiscard]] TSSView<TView> as_set() const noexcept;

    /**
     * Interpret this view as a window view when the runtime kind matches.
     */
    [[nodiscard]] TSWView<TView> as_window() noexcept;

    /**
     * Interpret this view as a window view when the runtime kind matches.
     */
    [[nodiscard]] TSWView<TView> as_window() const noexcept;

    /**
     * Interpret this view as a signal view when the runtime kind matches.
     */
    [[nodiscard]] SignalView<TView> as_signal() noexcept;

    /**
     * Interpret this view as a signal view when the runtime kind matches.
     */
    [[nodiscard]] SignalView<TView> as_signal() const noexcept;

protected:
    /**
     * Return the endpoint that owns the root of this view.
     */
    [[nodiscard]] ParentValue parent() const noexcept;

    /**
     * Return the state node associated to the represented time-series
     * position.
     */
    [[nodiscard]] TimeSeriesStatePtr state() const noexcept;
};

/**
 * Base for collection-oriented time-series views.
 *
 * Collection views extend `TSView` with navigation over child positions and
 * collection-level size queries.
 */
template <typename TView>
struct BaseCollectionView : TSView<TView>
{
    /**
     * Return the number of logical child positions in this collection.
     */
    [[nodiscard]] size_t size() const noexcept;

    /**
     * Return the child view at the supplied collection index.
     */
    [[nodiscard]] TView at(size_t index) const noexcept;

    /**
     * Return the child view at the supplied collection index.
     */
    [[nodiscard]] TView operator[](size_t index) const noexcept;
};

/**
 * View over a time-series list position.
 */
template <typename TView>
struct TSLView : BaseCollectionView<TView>
{
    /**
     * Return all child views in index order.
     */
    [[nodiscard]] Range<TView> values() const noexcept;

    /**
     * Return the child views that are currently valid.
     */
    [[nodiscard]] Range<TView> valid_values() const noexcept;

    /**
     * Return the child views modified in the current engine cycle.
     */
    [[nodiscard]] Range<TView> modified_values() const noexcept;

    /**
     * Return all indexed items in this list.
     */
    [[nodiscard]] KeyValueRange<size_t, TView> items() const noexcept;

    /**
     * Return all valid indexed items in this list.
     */
    [[nodiscard]] KeyValueRange<size_t, TView> valid_items() const noexcept;

    /**
     * Return all modified indexed items in this list.
     */
    [[nodiscard]] KeyValueRange<size_t, TView> modified_items() const noexcept;
};

/**
 * View over a time-series bundle position.
 */
template <typename TView>
struct TSBView : BaseCollectionView<TView>
{
    /**
     * Return the child view for the supplied field name.
     */
    [[nodiscard]] TView field(std::string_view name) const noexcept;

    /**
     * Return the bundle field names in schema order.
     */
    [[nodiscard]] Range<std::string_view> keys() const noexcept;

    /**
     * Return the child views in schema order.
     */
    [[nodiscard]] Range<TView> values() const noexcept;

    /**
     * Return the bundle items in schema order.
     */
    [[nodiscard]] KeyValueRange<std::string_view, TView> items() const noexcept;

    /**
     * Return the valid bundle items.
     */
    [[nodiscard]] KeyValueRange<std::string_view, TView> valid_items() const noexcept;

    /**
     * Return the modified bundle items.
     */
    [[nodiscard]] KeyValueRange<std::string_view, TView> modified_items() const noexcept;
};

/**
 * View over a time-series dictionary position.
 */
template <typename TView>
struct TSDView : BaseCollectionView<TView>
{
    using BaseCollectionView<TView>::at;
    using BaseCollectionView<TView>::operator[];

    /**
     * Return the child view for the supplied key.
     */
    [[nodiscard]] TView at(const value::View &key) const noexcept;

    /**
     * Return the child view for the supplied key.
     */
    [[nodiscard]] TView operator[](const value::View &key) const noexcept;

    /**
     * Return whether the supplied key is currently present.
     */
    [[nodiscard]] bool contains(const value::View &key) const noexcept;

    /**
     * Return the current key set as a set view.
     */
    [[nodiscard]] TSSView<TView> key_set() const noexcept;

    /**
     * Return the currently present keys.
     */
    [[nodiscard]] Range<value::View> keys() const noexcept;

    /**
     * Return the currently present child views.
     */
    [[nodiscard]] Range<TView> values() const noexcept;

    /**
     * Return the currently present key/value pairs.
     */
    [[nodiscard]] KeyValueRange<value::View, TView> items() const noexcept;

    /**
     * Return the valid key/value pairs.
     */
    [[nodiscard]] KeyValueRange<value::View, TView> valid_items() const noexcept;

    /**
     * Return the modified key/value pairs.
     */
    [[nodiscard]] KeyValueRange<value::View, TView> modified_items() const noexcept;

    /**
     * Return the keys added in the current engine cycle.
     */
    [[nodiscard]] Range<value::View> added_keys() const noexcept;

    /**
     * Return the keys removed in the current engine cycle.
     */
    [[nodiscard]] Range<value::View> removed_keys() const noexcept;

    /**
     * Return the items added in the current engine cycle.
     */
    [[nodiscard]] KeyValueRange<value::View, TView> added_items() const noexcept;

    /**
     * Return the existing items updated in the current engine cycle.
     */
    [[nodiscard]] KeyValueRange<value::View, TView> updated_items() const noexcept;

    /**
     * Return the items removed in the current engine cycle.
     */
    [[nodiscard]] KeyValueRange<value::View, TView> removed_items() const noexcept;
};

/**
 * View over a time-series set position.
 */
template <typename TView>
struct TSSView : TSView<TView>
{
    /**
     * Return the number of values currently present in the set.
     */
    [[nodiscard]] size_t size() const noexcept;

    /**
     * Return whether the supplied element is currently present.
     */
    [[nodiscard]] bool contains(const value::View &element) const noexcept;

    /**
     * Return the current set values.
     */
    [[nodiscard]] Range<value::View> values() const noexcept;

    /**
     * Return the values added in the current engine cycle.
     */
    [[nodiscard]] Range<value::View> added() const noexcept;

    /**
     * Return whether the supplied element was added in the current engine
     * cycle.
     */
    [[nodiscard]] bool was_added(const value::View &element) const noexcept;

    /**
     * Return the values removed in the current engine cycle.
     */
    [[nodiscard]] Range<value::View> removed() const noexcept;

    /**
     * Return whether the supplied element was removed in the current engine
     * cycle.
     */
    [[nodiscard]] bool was_removed(const value::View &element) const noexcept;

    /**
     * Return the nested time-series view that tracks empty-state semantics.
     */
    [[nodiscard]] TView is_empty() const noexcept;
};

/**
 * View over a time-series window position.
 */
template <typename TView>
struct TSWView : TSView<TView>
{
    /**
     * Return the current number of buffered values.
     */
    [[nodiscard]] size_t size() const noexcept;

    /**
     * Return the minimum size or readiness threshold for the window.
     */
    [[nodiscard]] size_t min_size() const noexcept;

    /**
     * Return the modification times associated with buffered values.
     */
    [[nodiscard]] Range<engine_time_t> value_times() const noexcept;

    /**
     * Return the modification time of the oldest buffered value.
     */
    [[nodiscard]] engine_time_t first_modified_time() const noexcept;

    /**
     * Return whether an evicted value is available for this engine cycle.
     */
    [[nodiscard]] bool has_removed_value() const noexcept;

    /**
     * Return the value evicted from the window in the current engine cycle.
     */
    [[nodiscard]] value::View removed_value() const noexcept;
};

/**
 * View over a signal time-series position.
 */
template <typename TView>
struct SignalView : TSView<TView>
{};

}  // namespace hgraph
