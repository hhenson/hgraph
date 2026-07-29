#ifndef HGRAPH_CPP_ROOT_TS_INPUT_BUNDLE_VIEW_H
#define HGRAPH_CPP_ROOT_TS_INPUT_BUNDLE_VIEW_H

#include <hgraph/types/time_series/ts_input/typed_view.h>
#include <string_view>

namespace hgraph
{
    class TSInput;
    class TSInputView;
    class TSBInputView;
    class TSLInputView;
    class TSSInputView;
    class TSDInputView;
    class TSWInputView;

    class HGRAPH_EXPORT TSBInputView : public TSInputTypedView<TSBInputView>
    {
      public:
        explicit TSBInputView(TSInputView view);

        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] bool has_field(std::string_view name) const noexcept;
        [[nodiscard]] TSBDataView data_view() const;

        /** Field names and child views in schema order. */
        [[nodiscard]] Range<std::string_view> keys() const &;
        Range<std::string_view> keys() && = delete;
        [[nodiscard]] Range<TSInputView> values() const &;
        Range<TSInputView> values() && = delete;

        /** Child views filtered by current validity or modification time. */
        [[nodiscard]] Range<TSInputView> valid_values() const &;
        Range<TSInputView> valid_values() && = delete;
        [[nodiscard]] Range<TSInputView> modified_values() const &;
        Range<TSInputView> modified_values() && = delete;

        /** ``field name -> child`` pairs in schema order, optionally filtered. */
        [[nodiscard]] KeyValueRange<std::string_view, TSInputView> items() const &;
        KeyValueRange<std::string_view, TSInputView> items() && = delete;
        [[nodiscard]] KeyValueRange<std::string_view, TSInputView> valid_items() const &;
        KeyValueRange<std::string_view, TSInputView> valid_items() && = delete;
        [[nodiscard]] KeyValueRange<std::string_view, TSInputView> modified_items() const &;
        KeyValueRange<std::string_view, TSInputView> modified_items() && = delete;

        [[nodiscard]] TSInputView at(std::size_t index) &;
        [[nodiscard]] TSInputView at(std::size_t index) const &;
        TSInputView at(std::size_t) && = delete;
        [[nodiscard]] TSInputView operator[](std::size_t index) &;
        [[nodiscard]] TSInputView operator[](std::size_t index) const &;
        TSInputView operator[](std::size_t) && = delete;

        [[nodiscard]] TSInputView at(std::string_view name) &;
        [[nodiscard]] TSInputView at(std::string_view name) const &;
        TSInputView at(std::string_view) && = delete;
        [[nodiscard]] TSInputView field(std::string_view name) &;
        [[nodiscard]] TSInputView field(std::string_view name) const &;
        TSInputView field(std::string_view) && = delete;
        [[nodiscard]] TSInputView operator[](std::string_view name) &;
        [[nodiscard]] TSInputView operator[](std::string_view name) const &;
        TSInputView operator[](std::string_view) && = delete;

      private:
        TSBInputView(TSInputView view, detail::TrustedTSEndpointKind)
            : TSInputTypedView<TSBInputView>(std::move(view))
        {
        }

        static constexpr std::size_t npos = static_cast<std::size_t>(-1);

        [[nodiscard]] std::size_t field_index(std::string_view name) const;
        [[nodiscard]] std::size_t find_field_index(std::string_view name) const noexcept;
        [[nodiscard]] std::string_view key_at(std::size_t index) const noexcept;

        friend struct detail::TSEndpointVisitorAccess;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_BUNDLE_VIEW_H
