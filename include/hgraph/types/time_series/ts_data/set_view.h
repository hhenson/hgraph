#ifndef HGRAPH_CPP_TS_DATA_SET_VIEW_H
#define HGRAPH_CPP_TS_DATA_SET_VIEW_H

#include <hgraph/types/time_series/ts_data/base_view.h>
#include <hgraph/types/utils/slot_observer.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/util/date_time.h>
#include <cstddef>

namespace hgraph
{
    /** Read view over set-shaped TSData. */
    class TSSDataView
    {
      public:
        explicit TSSDataView(TSDataView view);

        /** Transient generic TSData view over the same storage. */
        [[nodiscard]] TSDataView base() const noexcept;

        TSSDataView(const TSSDataView &) = delete;
        TSSDataView &operator=(const TSSDataView &) = delete;
        TSSDataView(TSSDataView &&) noexcept = default;
        TSSDataView &operator=(TSSDataView &&) noexcept = default;

        /** Binding, schema, layout, and value projections for the set node. */
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;
        [[nodiscard]] const TSSDataLayout &layout() const;
        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value(DateTime evaluation_time) const;
        [[nodiscard]] DateTime last_modified_time() const;
        [[nodiscard]] bool modified(DateTime evaluation_time) const;
        void subscribe(Notifiable *observer) const;
        void unsubscribe(Notifiable *observer) const;
        [[nodiscard]] bool has_observers() const;
        [[nodiscard]] std::size_t observer_count() const;

        /** Register / remove a structural slot observer. */
        void subscribe_slot_observer(SlotObserver *observer) const;
        void unsubscribe_slot_observer(SlotObserver *observer) const;

        /** Number of live keys in the set. */
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;

        /** Allocated slot count; exposed for delta/slot level traversal. */
        [[nodiscard]] std::size_t slot_capacity() const;
        [[nodiscard]] bool slot_occupied(std::size_t slot) const;
        [[nodiscard]] bool slot_live(std::size_t slot) const;
        [[nodiscard]] bool slot_added(std::size_t slot) const;
        [[nodiscard]] bool slot_removed(std::size_t slot) const;
        /** Sparse traversal of structural delta slots; pass ``TS_DATA_NO_CHILD_ID`` for the first slot. */
        [[nodiscard]] std::size_t next_added_slot(std::size_t previous = TS_DATA_NO_CHILD_ID) const;
        [[nodiscard]] std::size_t next_removed_slot(std::size_t previous = TS_DATA_NO_CHILD_ID) const;

        /** Key stored at ``slot``; throws if the slot is not occupied. */
        [[nodiscard]] ValueView at_slot(std::size_t slot) const;

        /** Key lookup helpers using the set key binding. */
        [[nodiscard]] bool contains(const ValueView &key) const;
        [[nodiscard]] std::size_t find_slot(const ValueView &key) const;

        /** Live, added, and removed key ranges. */
        [[nodiscard]] Range<ValueView> values() const;
        [[nodiscard]] Range<ValueView> added() const;
        [[nodiscard]] Range<ValueView> removed() const;
        [[nodiscard]] Range<ValueView> added_values() const;
        [[nodiscard]] Range<ValueView> removed_values() const;
        [[nodiscard]] Range<ValueView>::iterator begin() const;
        [[nodiscard]] Range<ValueView>::iterator end() const;

        /** Begin a mutation view over this set. */
        [[nodiscard]] TSSDataMutationView begin_mutation(DateTime evaluation_time) const;

      protected:
        [[nodiscard]] const TSSDataOps &set_ops() const;

        TSSDataStorageRef storage_{};
    };

    /** Mutation view over set-shaped TSData. */
    class HGRAPH_EXPORT TSSDataMutationView : public TSSDataView
    {
      public:
        TSSDataMutationView(TSDataView view, DateTime evaluation_time);

        TSSDataMutationView(const TSSDataMutationView &) = delete;
        TSSDataMutationView &operator=(const TSSDataMutationView &) = delete;
        TSSDataMutationView(TSSDataMutationView &&) noexcept;
        TSSDataMutationView &operator=(TSSDataMutationView &&) = delete;
        ~TSSDataMutationView() noexcept;

        /** Read view for the same underlying set. */
        [[nodiscard]] TSSDataView view();

        /** Engine time associated with this mutation scope. */
        [[nodiscard]] DateTime current_mutation_time() const;

        /** Reserve storage for at least ``capacity`` slots. */
        void reserve(std::size_t capacity);

        /** Add or remove one key, returning true when the set delta changes. */
        [[nodiscard]] bool add(const ValueView &key);
        [[nodiscard]] bool remove(const ValueView &key);

        /** Remove all currently live keys. */
        void clear();

        /** Mark this set modified WITHOUT changing membership - an
            explicitly EMPTY tick still validates the set. */
        void touch();

        /** Replace the set from a value-layer set view. */
        [[nodiscard]] bool copy_value_from(const ValueView &source);

      private:
        TSDataMutationView mutation_;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_SET_VIEW_H
