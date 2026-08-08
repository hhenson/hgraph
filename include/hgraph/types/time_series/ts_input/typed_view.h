#ifndef HGRAPH_CPP_ROOT_TS_INPUT_TYPED_VIEW_H
#define HGRAPH_CPP_ROOT_TS_INPUT_TYPED_VIEW_H

#include <hgraph/types/time_series/ts_input/base_view.h>
#include <hgraph/types/time_series/typed_view.h>

namespace hgraph
{
    class TSInput;
    class TSInputView;
    class TSBInputView;
    class TSLInputView;
    class TSSInputView;
    class TSDInputView;
    class TSWInputView;

    template <typename Derived>
    class TSInputTypedView : public TSTypedTimeSeriesView<Derived, TSInputView>
    {
      public:
        using base_type = TSTypedTimeSeriesView<Derived, TSInputView>;

        void bind_output(const TSOutputView &output) { this->view_.bind_output(output); }
        void unbind_output() { this->view_.unbind_output(); }
        [[nodiscard]] bool is_bindable() const noexcept { return this->view_.is_bindable(); }
        void make_active() { this->view_.make_active(); }
        void make_passive() { this->view_.make_passive(); }
        [[nodiscard]] bool active() const { return this->view_.active(); }

      protected:
        using base_type::base_type;
    };

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_TYPED_VIEW_H
