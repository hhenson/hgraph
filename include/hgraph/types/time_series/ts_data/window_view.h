#ifndef HGRAPH_CPP_TS_DATA_WINDOW_VIEW_H
#define HGRAPH_CPP_TS_DATA_WINDOW_VIEW_H

#include <hgraph/types/time_series/ts_data/base_view.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/util/date_time.h>
#include <cstddef>

namespace hgraph
{
    /** Read view over window-shaped TSData. */
    class HGRAPH_CLASS_EXPORT TSWDataView
    {
      public:
        explicit TSWDataView(TSDataView view);

        /** Transient generic TSData view over the same storage. */
        [[nodiscard]] TSDataView base() const noexcept;

        TSWDataView(const TSWDataView &) = delete;
        TSWDataView &operator=(const TSWDataView &) = delete;
        TSWDataView(TSWDataView &&) noexcept = default;
        TSWDataView &operator=(TSWDataView &&) noexcept = default;

        /** Binding, schema, layout, and value projections for the window node. */
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;
        [[nodiscard]] const TSWDataLayout &layout() const;
        [[nodiscard]] const SizeTSWDataLayout &size_layout() const;
        [[nodiscard]] const TimeTSWDataLayout &time_layout() const;
        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value(DateTime evaluation_time) const;
        [[nodiscard]] DateTime last_modified_time() const;
        [[nodiscard]] bool modified(DateTime evaluation_time) const;
        void subscribe(Notifiable *observer) const;
        void unsubscribe(Notifiable *observer) const;
        [[nodiscard]] bool has_observers() const;
        [[nodiscard]] std::size_t observer_count() const;

        /** True for time-duration windows; false for fixed-size windows. */
        [[nodiscard]] bool duration_based() const noexcept;
        [[nodiscard]] bool size_based() const noexcept;
        [[nodiscard]] bool time_based() const noexcept;

        /** Fixed-size or duration configuration for the window. */
        [[nodiscard]] std::size_t period() const;
        [[nodiscard]] std::size_t min_period() const;
        [[nodiscard]] TimeDelta time_range() const;
        [[nodiscard]] TimeDelta min_time_range() const;

        /** Allocated and occupied window sizes. */
        [[nodiscard]] std::size_t capacity() const;
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] bool full() const;
        [[nodiscard]] bool all_valid() const;

        /** hgraph's removed_value: the element the supplied evaluation
            cycle's push evicted (a full tick window rolling, or a duration
            window's span drop). Data-level callers must supply the cycle;
            endpoint views provide their current evaluation time. */
        [[nodiscard]] bool has_removed_value(DateTime evaluation_time) const;
        [[nodiscard]] ValueView removed_value(DateTime evaluation_time) const;

        /** True when clear participated in the supplied evaluation cycle. */
        [[nodiscard]] bool cleared(DateTime evaluation_time) const;

        /** Time associated with the oldest element, or ``MIN_DT`` when empty. */
        [[nodiscard]] DateTime first_modified_time() const;

        /** Time metadata and value access by oldest-to-newest index. */
        [[nodiscard]] DateTime time_at(std::size_t index) const;
        [[nodiscard]] ValueView time_value_at(std::size_t index) const;
        [[nodiscard]] ValueView at(std::size_t index) const;
        [[nodiscard]] ValueView operator[](std::size_t index) const;
        [[nodiscard]] ValueView front() const;
        [[nodiscard]] ValueView back() const;

        /** Element, time-value, and raw evaluation-time ranges. */
        [[nodiscard]] Range<ValueView> values() const;
        [[nodiscard]] Range<ValueView> time_values() const;
        [[nodiscard]] Range<DateTime> value_times() const;
        [[nodiscard]] Range<ValueView>::iterator begin() const;
        [[nodiscard]] Range<ValueView>::iterator end() const;

        /** Begin a mutation view over this window. */
        [[nodiscard]] TSWDataMutationView begin_mutation(DateTime evaluation_time) const;

      protected:
        struct TrustedStorageTag
        {
        };

        TSWDataView(TSWDataStorageRef storage, TrustedStorageTag) noexcept : storage_(storage) {}
        [[nodiscard]] const TSWDataOps &window_ops() const;

      private:
        [[nodiscard]] static ValueView project_value(const void *context, const void *, std::size_t index);
        [[nodiscard]] static ValueView project_time_value(const void *context, const void *, std::size_t index);
        [[nodiscard]] static DateTime project_time(const void *context, const void *, std::size_t index);

        TSWDataStorageRef storage_{};
    };

    /** Mutation view over window-shaped TSData. */
    class HGRAPH_CLASS_EXPORT TSWDataMutationView : public TSWDataView
    {
      public:
        TSWDataMutationView(TSDataView view, DateTime evaluation_time);

        TSWDataMutationView(const TSWDataMutationView &) = delete;
        TSWDataMutationView &operator=(const TSWDataMutationView &) = delete;
        TSWDataMutationView(TSWDataMutationView &&) noexcept;
        TSWDataMutationView &operator=(TSWDataMutationView &&) = delete;
        ~TSWDataMutationView() noexcept;

        /** Read view for the same underlying window. */
        [[nodiscard]] TSWDataView view();

        /** Runtime time associated with this mutation scope. */
        [[nodiscard]] DateTime current_mutation_time() const;

        /** Append one window tick. Only one tick is accepted per evaluation time. */
        void push(const ValueView &source);

        /**
         * Remove every retained tick and mark the window modified.
         *
         * One push may follow through this same mutation scope, allowing
         * reset-before-current-value semantics without exposing an
         * intermediate window state.
         */
        void clear();

        /**
         * Replace the window from a value-layer window/list representation.
         *
         * A replacement is rejected after clear has participated in the same
         * evaluation cycle, including through a different mutation view.
         */
        [[nodiscard]] bool copy_value_from(const ValueView &source);

      private:
        friend class TSWDataView;

        TSWDataMutationView(TSWDataStorageRef storage, DateTime evaluation_time, TrustedStorageTag);

        TSDataMutationView mutation_;
        bool               cleared_{false};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_WINDOW_VIEW_H
