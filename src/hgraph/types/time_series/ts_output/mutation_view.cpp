#include <hgraph/types/time_series/ts_output/mutation_view.h>

#include <hgraph/types/time_series/ts_data/dict_view.h>
#include <hgraph/types/time_series/ts_output/view_common.h>
#include <hgraph/types/value/value.h>

#include <utility>

namespace hgraph
{
    TSDataMutationView TSOutputMutationView::begin_root_mutation(TSOutput &output, DateTime evaluation_time)
    {
        if (evaluation_time == MIN_DT) { throw std::invalid_argument("TSOutput mutation requires a concrete time"); }
        if (!output.has_value()) { throw std::logic_error("TSOutput mutation requires a bound output"); }
        return output.data_view().begin_mutation(evaluation_time);
    }

    TSOutputMutationView::TSOutputMutationView(TSOutput &output, DateTime evaluation_time)
        : mutation_(begin_root_mutation(output, evaluation_time))
    {
    }

    TSOutputMutationView::TSOutputMutationView(TSOutputMutationView &&) noexcept = default;

    TSOutputMutationView::~TSOutputMutationView() noexcept = default;

    TSDataMutationView &TSOutputMutationView::data_mutation() noexcept
    {
        return mutation_;
    }

    const TSDataMutationView &TSOutputMutationView::data_mutation() const noexcept
    {
        return mutation_;
    }

    ValueView TSOutputMutationView::value() const
    {
        return mutation_.value();
    }

    ValueView TSOutputMutationView::delta_value() const
    {
        return mutation_.delta_value(current_mutation_time());
    }

    DateTime TSOutputMutationView::current_mutation_time() const
    {
        return mutation_.current_mutation_time();
    }

    bool TSOutputMutationView::modified() const
    {
        return mutation_.modified(current_mutation_time());
    }

    void TSOutputMutationView::mark_modified()
    {
        mutation_.mark_modified();
    }

    bool TSOutputMutationView::copy_value_from(const ValueView &source)
    {
        return mutation_.copy_value_from(source);
    }

    bool TSOutputMutationView::move_value_from(Value &&source)
    {
        return move_value_from(source.view());
    }

    bool TSOutputMutationView::move_value_from(ValueView source)
    {
        auto root = mutation_.view();
        if (const auto *schema = root.schema(); schema != nullptr && schema->kind == TSTypeKind::TSD)
        {
            auto dict = root.as_dict();
            auto dict_mutation = dict.begin_mutation(current_mutation_time());
            return dict_mutation.move_value_from(std::move(source));
        }
        return mutation_.move_value_from(std::move(source));
    }

}  // namespace hgraph
