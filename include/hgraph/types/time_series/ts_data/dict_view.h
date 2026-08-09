#ifndef HGRAPH_CPP_TS_DATA_DICT_VIEW_H
#define HGRAPH_CPP_TS_DATA_DICT_VIEW_H

#include <hgraph/types/time_series/ts_data/set_view.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/util/date_time.h>
#include <cstddef>
#include <utility>

namespace hgraph
{
    class Value;

    /** Read view over dictionary-shaped TSData. */
    class HGRAPH_EXPORT TSDDataView
    {
      public:
        explicit TSDDataView(TSDataView view);

        /** Transient generic TSData view over the same storage. */
        [[nodiscard]] TSDataView base() const noexcept;

        TSDDataView(const TSDDataView &) = delete;
        TSDDataView &operator=(const TSDDataView &) = delete;
        TSDDataView(TSDDataView &&) noexcept = default;
        TSDDataView &operator=(TSDDataView &&) noexcept = default;

        /** Binding, schema, layout, and value projections for the dictionary node. */
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;
        [[nodiscard]] const TSDDataLayout &layout() const;
        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value(DateTime evaluation_time) const;
        [[nodiscard]] DateTime last_modified_time() const;
        [[nodiscard]] bool modified(DateTime evaluation_time) const;
        void subscribe(Notifiable *observer) const;
        void unsubscribe(Notifiable *observer) const;
        [[nodiscard]] bool has_observers() const;
        [[nodiscard]] std::size_t observer_count() const;

        /** Number of live key/value entries. */
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;

        /** Allocated slot count and slot-level delta predicates. */
        [[nodiscard]] std::size_t slot_capacity() const;
        [[nodiscard]] bool slot_occupied(std::size_t slot) const;
        [[nodiscard]] bool slot_live(std::size_t slot) const;
        [[nodiscard]] bool slot_added(std::size_t slot) const;
        [[nodiscard]] bool slot_removed(std::size_t slot) const;
        [[nodiscard]] bool slot_modified(std::size_t slot) const;
        /** Sparse traversal of structural delta slots; pass ``TS_DATA_NO_CHILD_ID`` for the first slot. */
        [[nodiscard]] std::size_t next_added_slot(std::size_t previous = TS_DATA_NO_CHILD_ID) const;
        [[nodiscard]] std::size_t next_removed_slot(std::size_t previous = TS_DATA_NO_CHILD_ID) const;
        /** Sparse traversal of modified value slots; pass ``TS_DATA_NO_CHILD_ID`` for the first slot. */
        [[nodiscard]] std::size_t next_modified_slot(std::size_t previous = TS_DATA_NO_CHILD_ID) const;

        /** Key or child TSData view at ``slot``; throws if the slot is not occupied. */
        [[nodiscard]] ValueView key_at_slot(std::size_t slot) const;
        [[nodiscard]] TSDataView at_slot(std::size_t slot) const;
        /** Retained key at a sparse removed-delta slot; valid until that slot is erased. */
        [[nodiscard]] ValueView removed_key_at_slot(std::size_t slot) const;

        /** Key lookup helpers. Missing keys return an empty child view from ``at``. */
        [[nodiscard]] bool contains(const ValueView &key) const;
        [[nodiscard]] std::size_t find_slot(const ValueView &key) const;
        [[nodiscard]] TSDataView at(const ValueView &key) const;
        [[nodiscard]] TSDataView operator[](const ValueView &key) const;

        /** Live key, value, and item ranges. */
        [[nodiscard]] Range<ValueView> keys() const;
        [[nodiscard]] Range<TSDataView> values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> items() const;

        /** Entries whose child value is currently valid. */
        [[nodiscard]] Range<ValueView> valid_keys() const;
        [[nodiscard]] Range<TSDataView> valid_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> valid_items() const;

        /** Entries modified at ``evaluation_time``. */
        [[nodiscard]] Range<ValueView> modified_keys(DateTime evaluation_time) const;
        [[nodiscard]] Range<TSDataView> modified_values(DateTime evaluation_time) const;
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> modified_items(DateTime evaluation_time) const;

        /** Added and removed key/value delta ranges for the current delta surface. */
        [[nodiscard]] Range<ValueView> added_keys() const;
        [[nodiscard]] Range<TSDataView> added_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> added_items() const;
        [[nodiscard]] Range<ValueView> removed_keys() const;
        [[nodiscard]] Range<TSDataView> removed_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSDataView> removed_items() const;

        /** Set-shaped view over the dictionary keys. */
        [[nodiscard]] TSSDataView key_set() const;

        /** Begin a mutation view over this dictionary. */
        [[nodiscard]] TSDDataMutationView begin_mutation(DateTime evaluation_time) const;

      protected:
        [[nodiscard]] const TSDDataOps &dict_ops() const;

      private:
        [[nodiscard]] static Range<ValueView> empty_value_range() noexcept;
        [[nodiscard]] static Range<TSDataView> empty_ts_data_range() noexcept;
        [[nodiscard]] static KeyValueRange<ValueView, TSDataView> empty_ts_data_kv_range() noexcept;

        TSDDataStorageRef storage_{};
    };

    /** Mutation view over dictionary-shaped TSData. */
    class HGRAPH_EXPORT TSDDataMutationView : public TSDDataView
    {
      public:
        TSDDataMutationView(TSDataView view, DateTime evaluation_time);

        TSDDataMutationView(const TSDDataMutationView &) = delete;
        TSDDataMutationView &operator=(const TSDDataMutationView &) = delete;
        TSDDataMutationView(TSDDataMutationView &&) noexcept;
        TSDDataMutationView &operator=(TSDDataMutationView &&) = delete;
        ~TSDDataMutationView() noexcept;

        /** Read view for the same underlying dictionary. */
        [[nodiscard]] TSDDataView view();

        /** Engine time associated with this mutation scope. */
        [[nodiscard]] DateTime current_mutation_time() const;

        /** Reserve storage for at least ``capacity`` slots. */
        void reserve(std::size_t capacity);

        /** Mark the dictionary itself as touched for this mutation cycle. */
        void touch();

        /** Access or create the child TSData for ``key``. */
        [[nodiscard]] TSDataView at(const ValueView &key);
        [[nodiscard]] TSDataView operator[](const ValueView &key);

        /** Set the child value associated with ``key``. */
        void set(const ValueView &key, const ValueView &value);

        /** Remove ``key`` if present, returning true when the dictionary delta changes. */
        [[nodiscard]] bool erase(const ValueView &key);

        /** Remove all currently live entries. */
        void clear();

        /** Replace the dictionary from a value-layer map view. */
        [[nodiscard]] bool copy_value_from(const ValueView &source);

        /** Replace the dictionary from an owned value-layer map without copying keys or child values. */
        [[nodiscard]] bool move_value_from(Value &&source);

        /** Replace the dictionary from a writable map view without taking ownership. */
        [[nodiscard]] bool move_value_from(ValueView source);

      private:
        [[nodiscard]] TSDataView at_slot(std::size_t slot);

        TSDataMutationView mutation_;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_DICT_VIEW_H
