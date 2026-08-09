#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_TYPED_VIEW_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_TYPED_VIEW_H

#include <hgraph/types/time_series/ts_output/base_view.h>
#include <hgraph/types/time_series/typed_view.h>

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

    template <typename Derived>
    class TSOutputTypedView : public TSTypedTimeSeriesView<Derived, TSOutputView>
    {
      public:
        using base_type = TSTypedTimeSeriesView<Derived, TSOutputView>;

        void subscribe(Notifiable *observer) const { this->view_.subscribe(observer); }
        void unsubscribe(Notifiable *observer) const { this->view_.unsubscribe(observer); }
        [[nodiscard]] TSDataMutationView begin_mutation(DateTime evaluation_time) const
        {
            return this->view_.begin_mutation(evaluation_time);
        }

      protected:
        using base_type::base_type;
    };

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_TYPED_VIEW_H
