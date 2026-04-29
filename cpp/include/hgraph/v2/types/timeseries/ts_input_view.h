#ifndef HGRAPH_CPP_ROOT_TS_INPUT_VIEW_H
#define HGRAPH_CPP_ROOT_TS_INPUT_VIEW_H

#include <hgraph/v2/types/timeseries/ts_input.h>
#include <hgraph/v2/types/timeseries/ts_output_view.h>
#include <hgraph/v2/types/timeseries/ts_view.h>

#include <stdexcept>

namespace hgraph::v2
{
    /**
     * Non-owning input endpoint view.
     *
     * The first pass supports root bind/unbind semantics only. The view keeps
     * its cached root pointer in sync with the owning input when binding
     * changes, so `value()` reflects the currently linked output payload.
     */
    struct TsInputView : BasicTsView<TsInputTypeBinding>
    {
        using base_type    = BasicTsView<TsInputTypeBinding>;
        using context_type = typename base_type::context_type;

        TsInputView() = default;

        TsInputView(TsInput *input, engine_time_t evaluation_time = MIN_DT)
            : base_type(context_type{
                  input != nullptr ? detail::ts_state_view(input->binding(), input->state_data(), input->allocator())
                                   : TsInputStateHandle{},
                  input != nullptr ? input->data() : nullptr,
                  evaluation_time,
              }),
              m_input(input) {}

        [[nodiscard]] TsInput *owning_input() const noexcept { return m_input; }
        [[nodiscard]] bool     is_bound() const noexcept { return m_input != nullptr && m_input->is_bound(); }

        void bind_output(const TsOutputView &output) {
            if (m_input == nullptr) { throw std::logic_error("TsInputView::bind_output requires an owning input"); }
            if (output.binding() != nullptr) {
                m_input->bind_output(output);
                refresh(detail::ts_state_view(m_input->binding(), m_input->state_data(), m_input->allocator()), m_input->data());
                return;
            }
            throw std::logic_error("TsInputView::bind_output requires a bound output view");
        }

        void unbind_output() noexcept {
            if (m_input == nullptr) { return; }
            m_input->unbind_output();
            refresh(detail::ts_state_view(m_input->binding(), m_input->state_data(), m_input->allocator()), nullptr);
        }

      private:
        TsInput *m_input{nullptr};
    };

    inline void TsInput::bind_output(const TsOutput &output) {
        if (output.binding() == nullptr) { throw std::logic_error("TsInput::bind_output requires a bound output"); }
        ensure_state_binding_for(output.type());
        m_value_data = const_cast<void *>(output.data());
    }

    inline void TsInput::bind_output(const TsOutputView &output) {
        if (output.binding() == nullptr) { throw std::logic_error("TsInput::bind_output requires a bound output view"); }
        ensure_state_binding_for(output.type());
        m_value_data = output.value_data();
    }

    inline TsInputView TsInput::view(engine_time_t evaluation_time) { return TsInputView{this, evaluation_time}; }

    inline TsInputView TsInput::view(engine_time_t evaluation_time) const {
        return TsInputView{const_cast<TsInput *>(this), evaluation_time};
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_VIEW_H
