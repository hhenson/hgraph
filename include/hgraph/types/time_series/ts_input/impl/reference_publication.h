#ifndef HGRAPH_TYPES_TS_INPUT_IMPL_REFERENCE_PUBLICATION_H
#define HGRAPH_TYPES_TS_INPUT_IMPL_REFERENCE_PUBLICATION_H

#include <hgraph/types/time_series/ts_input/base_view.h>
#include <ankerl/unordered_dense.h>

namespace hgraph::detail
{
    /** Owner-private capacity cache for a finite declared designation tree.
        Opaque dynamic/recursive tokens retain their ordinary owning copy. */
    class HGRAPH_CLASS_EXPORT PreparedReferenceAssignment
    {
      public:
        PreparedReferenceAssignment() = default;
        explicit PreparedReferenceAssignment(const TSValueTypeMetaData *schema);
        bool assign(TimeSeriesReference &destination, const TimeSeriesReference &source) const;
        void recycle(TimeSeriesReference &destination) const;

      private:
        PreparedReferenceAssignment(const TSValueTypeMetaData *schema,
                                    ankerl::unordered_dense::set<const TSValueTypeMetaData *> &ancestors);
        using assign_fn = bool (*)(const PreparedReferenceAssignment &, TimeSeriesReference &, const TimeSeriesReference &);
        using recycle_fn = void (*)(const PreparedReferenceAssignment &, TimeSeriesReference &);
        static bool opaque_assign(const PreparedReferenceAssignment &, TimeSeriesReference &, const TimeSeriesReference &);
        static bool finite_assign(const PreparedReferenceAssignment &, TimeSeriesReference &, const TimeSeriesReference &);
        static void plain_recycle(const PreparedReferenceAssignment &, TimeSeriesReference &);
        static void finite_recycle(const PreparedReferenceAssignment &, TimeSeriesReference &);
        assign_fn assign_{&opaque_assign};
        recycle_fn recycle_{&plain_recycle};
        std::vector<PreparedReferenceAssignment> children_{};
        mutable std::vector<TimeSeriesReference> spare_{};
    };

    /** Implementation cache for the synthetic structural REF owner. Construct
        at start against its exact native output; no language or SDK API. */
    class HGRAPH_CLASS_EXPORT PreparedReferencePublication
    {
      public:
        PreparedReferencePublication() = default;
        PreparedReferencePublication(const PreparedReferencePublication &) = default;
        PreparedReferencePublication &operator=(const PreparedReferencePublication &) = default;
        PreparedReferencePublication(PreparedReferencePublication &&) noexcept;
        PreparedReferencePublication &operator=(PreparedReferencePublication &&) noexcept;
        PreparedReferencePublication(const TSInputView &source, const TSOutputView &output, DateTime time);
        bool publish(DateTime time) const;

      private:
        struct Update
        {
            using apply_fn = bool (*)(const Update &, const PreparedInputReferenceRoute &, TimeSeriesReference &, DateTime);
            using borrow_fn = const TimeSeriesReference *(*)(const PreparedInputReferenceRoute &);
            Update() = default;
            explicit Update(const PreparedInputReferenceRoute &route);
            void prime(const PreparedInputReferenceRoute &route, TimeSeriesReference &destination, DateTime time) const;
            bool apply(const PreparedInputReferenceRoute &route, TimeSeriesReference &destination, DateTime time) const;
            static bool empty(const Update &, const PreparedInputReferenceRoute &, TimeSeriesReference &, DateTime);
            static bool target(const Update &, const PreparedInputReferenceRoute &, TimeSeriesReference &, DateTime);
            static bool reference(const Update &, const PreparedInputReferenceRoute &, TimeSeriesReference &, DateTime);
            static bool children(const Update &, const PreparedInputReferenceRoute &, TimeSeriesReference &, DateTime);
            static const TimeSeriesReference *borrow_value(const PreparedInputReferenceRoute &);
            static const TimeSeriesReference *borrow_link(const PreparedInputReferenceRoute &);
            apply_fn apply_{&empty};
            borrow_fn borrow_{&borrow_value};
            PreparedReferenceAssignment assignment_{};
            std::vector<Update> children_{};
        };
        using publish_fn = bool (*)(const PreparedReferencePublication &, DateTime);
        static bool noop(const PreparedReferencePublication &, DateTime);
        static bool prepared(const PreparedReferencePublication &, DateTime);
        void swap(PreparedReferencePublication &) noexcept;
        publish_fn publish_{&noop};
        PreparedInputReferenceRoute route_{};
        Update update_{};
        mutable TimeSeriesReference primed_{};
        mutable bool primed_pending_{true};
        TimeSeriesReference *destination_{nullptr};
        TSDataTracking *tracking_{nullptr};
        const void *output_context_{nullptr};
        const void *output_memory_{nullptr};
        bool (*current_)(const void *, const void *){&ts_data_detail::missing_has_current_value};
    };
}

#endif
