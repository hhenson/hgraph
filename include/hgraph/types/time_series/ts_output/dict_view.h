#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_DICT_VIEW_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_DICT_VIEW_H

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

    class HGRAPH_EXPORT TSDOutputView : public TSOutputTypedView<TSDOutputView>
    {
      public:
        explicit TSDOutputView(TSOutputView view);

        [[nodiscard]] TSDDataView data_view() const;
        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] std::size_t slot_capacity() const;
        [[nodiscard]] bool slot_occupied(std::size_t slot) const;
        [[nodiscard]] bool slot_live(std::size_t slot) const;
        [[nodiscard]] bool slot_added(std::size_t slot) const;
        [[nodiscard]] bool slot_removed(std::size_t slot) const;
        [[nodiscard]] bool slot_modified(std::size_t slot) const;
        [[nodiscard]] ValueView key_at_slot(std::size_t slot) const;
        [[nodiscard]] TSOutputView at_slot(std::size_t slot) const;
        [[nodiscard]] bool contains(const ValueView &key) const;
        [[nodiscard]] std::size_t find_slot(const ValueView &key) const;
        [[nodiscard]] TSOutputView at(const ValueView &key) const;
        [[nodiscard]] TSOutputView operator[](const ValueView &key) const;
        [[nodiscard]] Range<ValueView> keys() const;
        [[nodiscard]] Range<TSOutputView> values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSOutputView> items() const;
        [[nodiscard]] Range<ValueView> valid_keys() const;
        [[nodiscard]] Range<TSOutputView> valid_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSOutputView> valid_items() const;
        [[nodiscard]] Range<ValueView> modified_keys() const;
        [[nodiscard]] Range<TSOutputView> modified_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSOutputView> modified_items() const;
        [[nodiscard]] Range<ValueView> added_keys() const;
        [[nodiscard]] Range<TSOutputView> added_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSOutputView> added_items() const;
        [[nodiscard]] Range<ValueView> removed_keys() const;
        [[nodiscard]] Range<TSOutputView> removed_values() const;
        [[nodiscard]] KeyValueRange<ValueView, TSOutputView> removed_items() const;
        [[nodiscard]] TSDDataMutationView begin_mutation(DateTime evaluation_time) const;

        /**
         * The dictionary's **key set as a bindable output view** (``TSS[K]``):
         * a zero-copy projection over the same storage — inputs may bind to
         * it like any output (the runtime path component
         * ``ts_key_set_path_component`` addresses it in edges/bindings).
         */
        [[nodiscard]] TSOutputView key_set() const;

      private:
        TSDOutputView(TSOutputView view, detail::TrustedTSEndpointKind)
            : TSOutputTypedView<TSDOutputView>(std::move(view))
        {
        }

        friend struct detail::TSEndpointVisitorAccess;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_DICT_VIEW_H
