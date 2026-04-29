#ifndef HGRAPH_CPP_ROOT_TS_INPUT_H
#define HGRAPH_CPP_ROOT_TS_INPUT_H

#include <hgraph/util/date_time.h>
#include <hgraph/v2/types/metadata/type_registry.h>
#include <hgraph/v2/types/timeseries/ts_input_builder.h>
#include <hgraph/v2/types/timeseries/ts_value.h>

namespace hgraph::v2
{
    struct TsInputView;
    struct TsOutput;
    struct TsOutputView;

    /**
     * Input-side TS endpoint state.
     *
     * Inputs own TS runtime state locally, but they do not own the current
     * value bytes. When linked, the input borrows a pointer into an output's
     * value region while keeping its own TS sidecar state.
     */
    struct TsInput
    {
        using state_storage_type = TsInputStateHandle;

        TsInput() noexcept = default;

        explicit TsInput(const TSValueTypeMetaData &type) : TsInput(TsValueBuilder::checked(&type)) {}

        explicit TsInput(const TsValueBuilder &builder) : TsInput(TsInputBuilder::checked(builder.type())) {}

        explicit TsInput(const TsInputBuilder &builder) : m_state_storage(builder.checked_binding()) {}

        explicit TsInput(const TsInputTypeBinding &binding) : m_state_storage(binding) {}

        TsInput(const TsInput &)                = default;
        TsInput(TsInput &&) noexcept            = default;
        TsInput &operator=(const TsInput &)     = default;
        TsInput &operator=(TsInput &&) noexcept = default;

        [[nodiscard]] bool     has_value() const noexcept { return m_value_data != nullptr; }
        [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }
        [[nodiscard]] bool     is_bound() const noexcept { return has_value(); }

        [[nodiscard]] const TsInputTypeBinding  *binding() const noexcept { return m_state_storage.binding(); }
        [[nodiscard]] const TSValueTypeMetaData *type() const noexcept {
            return binding() != nullptr ? binding()->type_meta : nullptr;
        }
        [[nodiscard]] const TsValueBuilder *builder() const noexcept {
            return type() != nullptr ? TsValueBuilder::find(type()) : nullptr;
        }
        [[nodiscard]] const ValueTypeBinding  *value_binding() const noexcept { return detail::ts_value_binding(binding()); }
        [[nodiscard]] const ValueTypeMetaData *value_type() const noexcept {
            return value_binding() != nullptr ? value_binding()->type_meta : nullptr;
        }
        [[nodiscard]] const MemoryUtils::StoragePlan *plan() const noexcept { return m_state_storage.plan(); }
        [[nodiscard]] const MemoryUtils::StoragePlan *state_plan() const noexcept { return m_state_storage.plan(); }
        [[nodiscard]] const MemoryUtils::StoragePlan *value_plan() const noexcept {
            return value_binding() != nullptr ? value_binding()->plan() : nullptr;
        }
        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept {
            if (const auto *allocator_ops = m_state_storage.allocator(); allocator_ops != nullptr) { return *allocator_ops; }
            return MemoryUtils::allocator();
        }

        [[nodiscard]] const ValueTypeBinding &checked_value_binding() const {
            if (const ValueTypeBinding *binding_ptr = value_binding(); binding_ptr != nullptr) { return *binding_ptr; }
            throw std::logic_error("TsInput is not bound to a value binding");
        }

        [[nodiscard]] void                     *data() noexcept { return m_value_data; }
        [[nodiscard]] const void               *data() const noexcept { return m_value_data; }
        [[nodiscard]] void                     *state_data() noexcept { return m_state_storage.data(); }
        [[nodiscard]] const void               *state_data() const noexcept { return m_state_storage.data(); }
        [[nodiscard]] state_storage_type       &state_storage() noexcept { return m_state_storage; }
        [[nodiscard]] const state_storage_type &state_storage() const noexcept { return m_state_storage; }
        [[nodiscard]] ValueView                 value() noexcept { return detail::ts_value_view(binding(), m_value_data); }
        [[nodiscard]] ValueView                 value() const noexcept {
            return detail::ts_value_view(binding(), const_cast<void *>(m_value_data));
        }

        void bind_output(const TsOutput &output);
        void bind_output(const TsOutputView &output);

        void bind_value(const ValueView &view) {
            if (!view.has_value()) { throw std::logic_error("TsInput::bind_value requires a live source value"); }
            ensure_state_binding_for(view.binding() != nullptr ? TypeRegistry::instance().ts(view.type()) : nullptr);
            if (view.binding() != value_binding()) {
                throw std::invalid_argument("TsInput::bind_value requires a matching value binding");
            }
            m_value_data = const_cast<void *>(view.data());
        }

        void unbind_output() noexcept { m_value_data = nullptr; }

        [[nodiscard]] TsInputView view(engine_time_t evaluation_time = MIN_DT);
        [[nodiscard]] TsInputView view(engine_time_t evaluation_time = MIN_DT) const;

      private:
        state_storage_type m_state_storage{};
        void              *m_value_data{nullptr};

        void ensure_state_binding_for(const TSValueTypeMetaData *type) {
            if (type == nullptr) { throw std::logic_error("TsInput requires a TS schema"); }
            if (binding() == nullptr) {
                m_state_storage = state_storage_type(TsInputBuilder::checked(type).checked_binding(), allocator());
                return;
            }
            if (binding()->type_meta != type) { throw std::invalid_argument("TsInput requires matching TS bindings"); }
        }
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_H
