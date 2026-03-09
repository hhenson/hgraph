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
 * Non-owning references to other views exposed by the time-series view API.
 *
 * The view layer is intended to be purely observational. Returned pointers
 * remain owned by the endpoint or runtime object that created the view graph.
 */
using TSViewRef = TSView *;
using ConstTSViewRef = const TSView *;
using TSViewList = std::vector<ConstTSViewRef>;
using IndexedTSViewList = std::vector<std::pair<size_t, ConstTSViewRef>>;
using NamedTSViewList = std::vector<std::pair<std::string, ConstTSViewRef>>;
using KeyedTSViewList = std::vector<std::pair<value::View, ConstTSViewRef>>;

/**
 * Abstract non-owning view over a logical time-series position.
 *
 * `TSView` defines the common inspection surface shared by scalar,
 * collection, and signal time-series values. Reference time-series are
 * intended to be exposed through this same interface because `REF[...]` is
 * logically equivalent to `TS[TimeSeriesReference]`, rather than through a
 * dedicated reference-view type. Concrete derived view types are expected to
 * provide the runtime-specific implementation details.
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
    [[nodiscard]] virtual engine_time_t evaluation_time() const noexcept = 0;

    /**
     * Return the current point-in-time value for this view.
     */
    [[nodiscard]] virtual value::View value() const noexcept = 0;

    /**
     * Return the current delta value for this view.
     */
    [[nodiscard]] virtual value::View delta_value() const noexcept = 0;

    /**
     * Return whether this view was modified in the current engine cycle.
     */
    [[nodiscard]] virtual bool modified() const noexcept = 0;

    /**
     * Return whether this view currently holds a valid value.
     */
    [[nodiscard]] virtual bool valid() const noexcept = 0;

    /**
     * Return whether this view and all required descendants are valid.
     */
    [[nodiscard]] virtual bool all_valid() const noexcept = 0;

    /**
     * Return the last modification time associated to this view.
     */
    [[nodiscard]] virtual engine_time_t last_modified_time() const noexcept = 0;

    /**
     * Return whether this view represents a reference-based position.
     *
     * Reference semantics are part of the generic `TSView` contract because
     * `REF[...]` is intended to behave as `TS[TimeSeriesReference]`.
     */
    [[nodiscard]] virtual bool is_reference() const noexcept = 0;

    /**
     * Interpret this view as a bundle view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSBView as_bundle() noexcept = 0;

    /**
     * Interpret this view as a bundle view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSBView as_bundle() const noexcept = 0;

    /**
     * Interpret this view as a list view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSLView as_list() noexcept = 0;

    /**
     * Interpret this view as a list view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSLView as_list() const noexcept = 0;

    /**
     * Interpret this view as a dictionary view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSDView as_dict() noexcept = 0;

    /**
     * Interpret this view as a dictionary view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSDView as_dict() const noexcept = 0;

    /**
     * Interpret this view as a set view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSSView as_set() noexcept = 0;

    /**
     * Interpret this view as a set view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSSView as_set() const noexcept = 0;

    /**
     * Interpret this view as a window view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSWView as_window() noexcept = 0;

    /**
     * Interpret this view as a window view when the runtime kind matches.
     */
    [[nodiscard]] virtual TSWView as_window() const noexcept = 0;

    /**
     * Interpret this view as a signal view when the runtime kind matches.
     */
    [[nodiscard]] virtual SignalView as_signal() noexcept = 0;

    /**
     * Interpret this view as a signal view when the runtime kind matches.
     */
    [[nodiscard]] virtual SignalView as_signal() const noexcept = 0;

protected:
    /**
     * Return the endpoint that owns the root of this view.
     */
    [[nodiscard]] virtual ParentValue parent() const noexcept = 0;

    /**
     * Return the state node associated to the represented time-series
     * position.
     */
    [[nodiscard]] virtual TimeSeriesStatePtr state() const noexcept = 0;
};

/**
 * Abstract base for collection-oriented time-series views.
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
    [[nodiscard]] virtual ConstTSViewRef at(size_t index) const noexcept;

    /**
     * Return the child view at the supplied collection index.
     */
    [[nodiscard]] virtual ConstTSViewRef operator[](size_t index) const noexcept;
};

/**
 * Abstract view over a time-series list position.
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
    [[nodiscard]] virtual TSViewList values() const noexcept;

    /**
     * Return the child views that are currently valid.
     */
    [[nodiscard]] virtual TSViewList valid_values() const noexcept;

    /**
     * Return the child views modified in the current engine cycle.
     */
    [[nodiscard]] virtual TSViewList modified_values() const noexcept;

    /**
     * Return all indexed items in this list.
     */
    [[nodiscard]] virtual IndexedTSViewList items() const noexcept;

    /**
     * Return all valid indexed items in this list.
     */
    [[nodiscard]] virtual IndexedTSViewList valid_items() const noexcept;

    /**
     * Return all modified indexed items in this list.
     */
    [[nodiscard]] virtual IndexedTSViewList modified_items() const noexcept;
};

/**
 * Abstract view over a time-series bundle position.
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
    [[nodiscard]] virtual ConstTSViewRef field(std::string_view name) const noexcept;

    /**
     * Return the bundle field names in schema order.
     */
    [[nodiscard]] virtual std::vector<std::string> keys() const noexcept;

    /**
     * Return the child views in schema order.
     */
    [[nodiscard]] virtual TSViewList values() const noexcept;

    /**
     * Return the bundle items in schema order.
     */
    [[nodiscard]] virtual NamedTSViewList items() const noexcept;

    /**
     * Return the valid bundle items.
     */
    [[nodiscard]] virtual NamedTSViewList valid_items() const noexcept;

    /**
     * Return the modified bundle items.
     */
    [[nodiscard]] virtual NamedTSViewList modified_items() const noexcept;
};

/**
 * Abstract view over a time-series dictionary position.
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
    [[nodiscard]] virtual ConstTSViewRef at(const value::View &key) const noexcept;

    /**
     * Return the child view for the supplied key.
     */
    [[nodiscard]] virtual ConstTSViewRef operator[](const value::View &key) const noexcept;

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
    [[nodiscard]] virtual std::vector<value::View> keys() const noexcept;

    /**
     * Return the currently present child views.
     */
    [[nodiscard]] virtual TSViewList values() const noexcept;

    /**
     * Return the currently present key/value pairs.
     */
    [[nodiscard]] virtual KeyedTSViewList items() const noexcept;

    /**
     * Return the valid key/value pairs.
     */
    [[nodiscard]] virtual KeyedTSViewList valid_items() const noexcept;

    /**
     * Return the modified key/value pairs.
     */
    [[nodiscard]] virtual KeyedTSViewList modified_items() const noexcept;

    /**
     * Return the keys added in the current engine cycle.
     */
    [[nodiscard]] virtual std::vector<value::View> added_keys() const noexcept;

    /**
     * Return the keys removed in the current engine cycle.
     */
    [[nodiscard]] virtual std::vector<value::View> removed_keys() const noexcept;

    /**
     * Return the items added in the current engine cycle.
     */
    [[nodiscard]] virtual KeyedTSViewList added_items() const noexcept;

    /**
     * Return the existing items updated in the current engine cycle.
     */
    [[nodiscard]] virtual KeyedTSViewList updated_items() const noexcept;

    /**
     * Return the items removed in the current engine cycle.
     */
    [[nodiscard]] virtual KeyedTSViewList removed_items() const noexcept;
};

/**
 * Abstract view over a time-series set position.
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
    [[nodiscard]] virtual std::vector<value::View> values() const noexcept;

    /**
     * Return the values added in the current engine cycle.
     */
    [[nodiscard]] virtual std::vector<value::View> added() const noexcept;

    /**
     * Return whether the supplied element was added in the current engine
     * cycle.
     */
    [[nodiscard]] virtual bool was_added(const value::View &element) const noexcept;

    /**
     * Return the values removed in the current engine cycle.
     */
    [[nodiscard]] virtual std::vector<value::View> removed() const noexcept;

    /**
     * Return whether the supplied element was removed in the current engine
     * cycle.
     */
    [[nodiscard]] virtual bool was_removed(const value::View &element) const noexcept;

    /**
     * Return the nested time-series view that tracks empty-state semantics.
     */
    [[nodiscard]] virtual ConstTSViewRef is_empty() const noexcept;
};

/**
 * Abstract view over a time-series window position.
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
    [[nodiscard]] virtual std::vector<engine_time_t> value_times() const noexcept;

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
 * Abstract view over a signal time-series position.
 */
struct HGRAPH_EXPORT SignalView : TSView
{
    /**
     * Destroy the signal view interface.
     */
    ~SignalView() override = default;
};

}  // namespace hgraph
