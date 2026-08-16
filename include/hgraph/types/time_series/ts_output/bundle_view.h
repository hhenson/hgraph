#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_BUNDLE_VIEW_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_BUNDLE_VIEW_H

#include <hgraph/types/time_series/ts_output/typed_view.h>
#include <string_view>

namespace hgraph
{
    class TSOutput;
    class TSOutputView;
    class TSBOutputView;
    class TSLOutputView;
    class TSSOutputView;
    class TSDOutputView;
    class TSWOutputView;

    class HGRAPH_EXPORT TSBOutputView : public TSOutputTypedView<TSBOutputView>
    {
      public:
        explicit TSBOutputView(TSOutputView view);

        [[nodiscard]] TSBDataView data_view() const;
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] bool has_field(std::string_view name) const noexcept;
        [[nodiscard]] Range<std::string_view> keys() const;
        [[nodiscard]] Range<TSOutputView> values() const;
        [[nodiscard]] Range<TSOutputView> valid_values() const;
        [[nodiscard]] Range<TSOutputView> modified_values() const;
        [[nodiscard]] KeyValueRange<std::string_view, TSOutputView> items() const;
        [[nodiscard]] KeyValueRange<std::string_view, TSOutputView> valid_items() const;
        [[nodiscard]] KeyValueRange<std::string_view, TSOutputView> modified_items() const;

        [[nodiscard]] TSOutputView at(std::size_t index) &;
        [[nodiscard]] TSOutputView at(std::size_t index) const &;
        TSOutputView at(std::size_t) && = delete;
        [[nodiscard]] TSOutputView operator[](std::size_t index) &;
        [[nodiscard]] TSOutputView operator[](std::size_t index) const &;
        TSOutputView operator[](std::size_t) && = delete;

        [[nodiscard]] TSOutputView at(std::string_view name) &;
        [[nodiscard]] TSOutputView at(std::string_view name) const &;
        TSOutputView at(std::string_view) && = delete;
        [[nodiscard]] TSOutputView field(std::string_view name) &;
        [[nodiscard]] TSOutputView field(std::string_view name) const &;
        TSOutputView field(std::string_view) && = delete;
        [[nodiscard]] TSOutputView operator[](std::string_view name) &;
        [[nodiscard]] TSOutputView operator[](std::string_view name) const &;
        TSOutputView operator[](std::string_view) && = delete;

      private:
        TSBOutputView(TSOutputView view, detail::TrustedTSEndpointKind)
            : TSOutputTypedView<TSBOutputView>(std::move(view))
        {
        }

        friend struct detail::TSEndpointVisitorAccess;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_BUNDLE_VIEW_H
