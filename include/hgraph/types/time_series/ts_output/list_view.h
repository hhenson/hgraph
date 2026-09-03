#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_LIST_VIEW_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_LIST_VIEW_H

#include <hgraph/types/time_series/ts_output/typed_view.h>

namespace hgraph
{
    class TSOutput;
    class TSOutputView;
    class TSBOutputView;
    class TSLOutputView;
    class TSSOutputView;
    class TSDOutputView;
    class TSWOutputView;

    class HGRAPH_CLASS_EXPORT TSLOutputView : public TSOutputTypedView<TSLOutputView>
    {
      public:
        explicit TSLOutputView(TSOutputView view);

        [[nodiscard]] TSLDataView data_view() const;
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] Range<TSOutputView> values() const;
        [[nodiscard]] Range<TSOutputView> valid_values() const;
        [[nodiscard]] Range<TSOutputView> modified_values() const;
        [[nodiscard]] KeyValueRange<std::size_t, TSOutputView> items() const;
        [[nodiscard]] KeyValueRange<std::size_t, TSOutputView> valid_items() const;
        [[nodiscard]] KeyValueRange<std::size_t, TSOutputView> modified_items() const;

        /** Structural delta for this cycle (RFC 0031). A fixed TSL reports
            empty ranges rather than throwing. */
        [[nodiscard]] Range<std::size_t> added_indices() const;
        [[nodiscard]] Range<std::size_t> removed_indices() const;
        [[nodiscard]] Range<TSOutputView> added_values() const;
        [[nodiscard]] Range<TSOutputView> removed_values() const;
        [[nodiscard]] KeyValueRange<std::size_t, TSOutputView> added_items() const;
        [[nodiscard]] KeyValueRange<std::size_t, TSOutputView> removed_items() const;

        /** Child view for a live OR retained (removed this cycle) index. */
        [[nodiscard]] TSOutputView retained_at(std::size_t index) const;

        /**
         * Set the live list length, growing or truncating.
         *
         * Dynamic ``TSL`` only; a fixed ``TSL`` throws. Truncated children are
         * stopped now and destroyed when the delta window next rolls, so
         * ``removed_values()`` stays readable for the rest of the cycle. The
         * list is marked modified even when only its length changed.
         */
        void resize(std::size_t size) const;

        [[nodiscard]] TSOutputView at(std::size_t index) &;
        [[nodiscard]] TSOutputView at(std::size_t index) const &;
        TSOutputView at(std::size_t) && = delete;
        [[nodiscard]] TSOutputView operator[](std::size_t index) &;
        [[nodiscard]] TSOutputView operator[](std::size_t index) const &;
        TSOutputView operator[](std::size_t) && = delete;

      private:
        TSLOutputView(TSOutputView view, detail::TrustedTSEndpointKind)
            : TSOutputTypedView<TSLOutputView>(std::move(view))
        {
        }

        friend struct detail::TSEndpointVisitorAccess;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_LIST_VIEW_H
