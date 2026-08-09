#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_LIST_VIEW_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_LIST_VIEW_H

#include <hgraph/types/time_series/ts_output/typed_view.h>

namespace hgraph
{
    class TSOutput;
    class TSOutputView;
    class TSOutputMutationView;
    class TSBOutputView;
    class TSLOutputView;
    class TSSOutputView;
    class TSDOutputView;
    class TSWOutputView;

    class HGRAPH_EXPORT TSLOutputView : public TSOutputTypedView<TSLOutputView>
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
