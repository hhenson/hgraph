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
                  input != nullptr ? detail::ts_storage_view(input->binding(), input->data(), input->allocator())
                                   : TsInputStorageHandle{},
                  evaluation_time,
              }),
              m_input(input) {}

        [[nodiscard]] TsInput *owning_input() const noexcept { return m_input; }
        [[nodiscard]] bool     is_bound() const noexcept { return m_input != nullptr && m_input->is_bound(); }

        void bind_output(const TsOutputView &output) {
            if (m_input == nullptr) { throw std::logic_error("TsInputView::bind_output requires an owning input"); }
            if (output.binding() != nullptr) {
                m_input->bind_output(output);
                refresh(detail::ts_storage_view(m_input->binding(), m_input->data(), m_input->allocator()));
                return;
            }
            throw std::logic_error("TsInputView::bind_output requires a bound output view");
        }

        void unbind_output() noexcept {
            if (m_input == nullptr) { return; }
            m_input->unbind_output();
            refresh(detail::ts_storage_view(m_input->binding(), nullptr, m_input->allocator()));
        }

      private:
        TsInput *m_input{nullptr};
    };

    inline void TsInput::bind_output(const TsOutput &output) {
        if (output.binding() == nullptr) { throw std::logic_error("TsInput::bind_output requires a bound output"); }
        if (type() != nullptr && type() != output.type()) {
            throw std::invalid_argument("TsInput::bind_output requires matching TS bindings");
        }

        const TsInputTypeBinding &input_binding =
            binding() != nullptr ? *binding() : TsInputBuilder::checked(output.type()).checked_binding();
        m_storage = storage_type::reference(input_binding, const_cast<void *>(output.data()), output.allocator());
    }

    inline void TsInput::bind_output(const TsOutputView &output) {
        if (output.binding() == nullptr) { throw std::logic_error("TsInput::bind_output requires a bound output view"); }
        if (type() != nullptr && type() != output.type()) {
            throw std::invalid_argument("TsInput::bind_output requires matching TS bindings");
        }

        const TsInputTypeBinding &input_binding =
            binding() != nullptr ? *binding() : TsInputBuilder::checked(output.type()).checked_binding();
        const auto &output_storage = output.storage();
        const auto *allocator_ops  = output_storage.allocator();
        m_storage                  = storage_type::reference(input_binding, const_cast<void *>(output_storage.data()),
                                                             allocator_ops != nullptr ? *allocator_ops : MemoryUtils::allocator());
    }

    inline TsInputView TsInput::view(engine_time_t evaluation_time) { return TsInputView{this, evaluation_time}; }

    inline TsInputView TsInput::view(engine_time_t evaluation_time) const {
        return TsInputView{const_cast<TsInput *>(this), evaluation_time};
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_VIEW_H
