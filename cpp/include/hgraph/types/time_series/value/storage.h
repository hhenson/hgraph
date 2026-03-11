#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/value/set_storage.h>
#include <hgraph/types/value/slot_observer.h>
#include <hgraph/types/value/value.h>

#include <sul/dynamic_bitset.hpp>

namespace hgraph {

struct Value;
struct IndexedTimeSeriesValueStorage;
struct TimeSeriesMapStorage;
struct TimeSeriesWindowStorage;
struct LinkedValueState;
struct SignalValueState;

/**
 * Pointer variant covering all concrete time-series value-state nodes.
 *
 * Time-series value collections store child nodes by reference, rather than by
 * flattening them into a raw `value::Value`. The pointed-to node determines
 * whether the position owns data locally or delegates through a link.
 */
using TimeSeriesValueStatePtr =
    std::variant<Value *, IndexedTimeSeriesValueStorage *, TimeSeriesMapStorage *, value::SetStorage *,
                 TimeSeriesWindowStorage *, LinkedValueState *, SignalValueState *>;

/**
 * Indexed child storage for list-like time-series collections.
 *
 * This mirrors the value layer's indexed composite model: child entries are
 * stored in logical order and the storage tracks the number of active child
 * positions independently from capacity.
 *
 * Ownership:
 * This storage owns the ordered child references for the collection.
 *
 * Access:
 * Children are addressed by ordinal position.
 *
 * Exportability:
 * This is a good direct export candidate when all child branches resolve to a
 * common layout, for example fixed-size list or struct-like projections.
 */
struct HGRAPH_EXPORT IndexedTimeSeriesValueStorage
{
    /**
     * Child value-state references in logical child order.
     */
    std::vector<TimeSeriesValueStatePtr> values;
    /**
     * Number of active child positions.
     */
    size_t                              size{0};
};

/**
 * Parallel child storage synchronized to a `value::SetStorage` key set.
 *
 * This mirrors `value::ValueArray`: keys live in stable slots within the
 * backing key set, and the parallel child value-state storage keeps the child
 * branches aligned to those slots.
 *
 * Ownership:
 * This storage owns the slot-aligned child references but does not own the key
 * set that drives slot allocation.
 *
 * Access:
 * Children are addressed by stable slot index supplied by the paired key set.
 *
 * Exportability:
 * This is not directly export-friendly by itself. It becomes exportable once a
 * keyed layout decides how live slots should be materialized into dense output.
 */
struct HGRAPH_EXPORT TimeSeriesValueArray : value::SlotObserver
{
    TimeSeriesValueArray() = default;
    explicit TimeSeriesValueArray(const TSMeta *value_schema) noexcept : value_schema(value_schema) {}

    TimeSeriesValueArray(const TimeSeriesValueArray &) = delete;
    TimeSeriesValueArray &operator=(const TimeSeriesValueArray &) = delete;
    TimeSeriesValueArray(TimeSeriesValueArray &&) noexcept = default;
    TimeSeriesValueArray &operator=(TimeSeriesValueArray &&) noexcept = default;

    void on_capacity(size_t old_cap, size_t new_cap) override;
    void on_insert(size_t slot) override;
    void on_erase(size_t slot) override;
    void on_clear() override;

    /**
     * Child value-state references aligned to key slots.
     */
    std::vector<TimeSeriesValueStatePtr> values;
    /**
     * Slot occupancy for `values`.
     */
    sul::dynamic_bitset<>                occupied;
    /**
     * Time-series schema for the child branches.
     */
    const TSMeta *                       value_schema{nullptr};
    /**
     * Current slot capacity.
     */
    size_t                               capacity{0};
};

/**
 * Map-like storage for `TSD` value-state.
 *
 * This mirrors `value::MapStorage`: scalar keys are stored in `SetStorage`,
 * while child time-series branches are stored in a synchronized parallel
 * `TimeSeriesValueArray`.
 *
 * Ownership:
 * This storage owns both the scalar key storage and the parallel child branch
 * storage.
 *
 * Access:
 * Entries are addressed by scalar key, with stable slot semantics underneath.
 *
 * Exportability:
 * This is a conditional export candidate. It maps naturally to Arrow-style map
 * or struct-of-key/value layouts once a dense materialization rule is chosen.
 */
struct HGRAPH_EXPORT TimeSeriesMapStorage
{
    TimeSeriesMapStorage() = default;
    TimeSeriesMapStorage(const value::TypeMeta *key_type, const TSMeta *value_schema) noexcept;

    TimeSeriesMapStorage(const TimeSeriesMapStorage &) = delete;
    TimeSeriesMapStorage &operator=(const TimeSeriesMapStorage &) = delete;
    TimeSeriesMapStorage(TimeSeriesMapStorage &&) noexcept = default;
    TimeSeriesMapStorage &operator=(TimeSeriesMapStorage &&) noexcept = default;

    /**
     * Key storage with stable slots.
     */
    value::SetStorage keys;
    /**
     * Parallel child branches aligned to `keys`.
     */
    TimeSeriesValueArray values;
    /**
     * Scalar key schema.
     */
    const value::TypeMeta *key_type{nullptr};
    /**
     * Time-series schema for mapped child branches.
     */
    const TSMeta *         value_schema{nullptr};
};

/**
 * Storage for window samples.
 *
 * Windows carry scalar values plus their sample times.
 *
 * Ownership:
 * This storage owns both the sample payloads and the aligned sample times.
 *
 * Access:
 * Samples are addressed by ordinal position in storage order.
 *
 * Exportability:
 * This is a strong export candidate because it is already a pair of aligned
 * buffers that can be exposed as parallel arrays or a struct-of-arrays view.
 */
struct HGRAPH_EXPORT TimeSeriesWindowStorage
{
    /**
     * Sample payloads in storage order.
     */
    std::vector<value::Value> values;
    /**
     * Sample timestamps aligned to `values`.
     */
    std::vector<engine_time_t> sample_times;
};

}  // namespace hgraph
