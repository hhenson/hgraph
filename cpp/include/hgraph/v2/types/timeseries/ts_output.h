#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_H

#include <hgraph/util/date_time.h>
#include <hgraph/v2/types/timeseries/ts_ouptut_builder.h>
#include <hgraph/v2/types/timeseries/ts_value.h>

namespace hgraph::v2
{
    struct TsOutputView;

    /**
     * Owning output endpoint.
     *
     * Outputs own two parallel regions:
     * - the data-first value payload
     * - the TS runtime state sidecar
     *
     * `value()` projects directly onto the value region, while TS views borrow
     * both regions together.
     */
    struct TsOutput
    {
        using state_storage_type = TsOutputStateHandle;
        using value_storage_type = TsValueDataHandle;

        TsOutput() noexcept = default;

        explicit TsOutput(const TSValueTypeMetaData &type, const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : TsOutput(TsValueBuilder::checked(&type), allocator) {}

        explicit TsOutput(const TsValueBuilder &builder, const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : TsOutput(TsOutputBuilder::checked(builder.type()), allocator) {}

        explicit TsOutput(const TsOutputBuilder &builder, const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : m_value_storage(builder.checked_ts_value_builder().checked_value_binding(), allocator),
              m_state_storage(builder.checked_binding(), allocator) {}

        explicit TsOutput(const TsOutputTypeBinding &binding, const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : m_value_storage(binding.checked_ops().checked_value_binding(), allocator), m_state_storage(binding, allocator) {}

        TsOutput(const TsOutput &)                = default;
        TsOutput(TsOutput &&) noexcept            = default;
        TsOutput &operator=(const TsOutput &)     = default;
        TsOutput &operator=(TsOutput &&) noexcept = default;

        [[nodiscard]] bool     has_value() const noexcept { return m_value_storage.has_value(); }
        [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }

        [[nodiscard]] const TsOutputTypeBinding *binding() const noexcept { return m_state_storage.binding(); }
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
        [[nodiscard]] const MemoryUtils::StoragePlan  *plan() const noexcept { return m_state_storage.plan(); }
        [[nodiscard]] const MemoryUtils::StoragePlan  *state_plan() const noexcept { return m_state_storage.plan(); }
        [[nodiscard]] const MemoryUtils::StoragePlan  *value_plan() const noexcept { return m_value_storage.plan(); }
        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept {
            if (const auto *allocator_ops = m_value_storage.allocator(); allocator_ops != nullptr) { return *allocator_ops; }
            if (const auto *allocator_ops = m_state_storage.allocator(); allocator_ops != nullptr) { return *allocator_ops; }
            return MemoryUtils::allocator();
        }

        [[nodiscard]] const ValueTypeBinding &checked_value_binding() const {
            if (const ValueTypeBinding *binding_ptr = value_binding(); binding_ptr != nullptr) { return *binding_ptr; }
            throw std::logic_error("TsOutput is not bound to a value binding");
        }

        [[nodiscard]] void                     *data() noexcept { return m_value_storage.data(); }
        [[nodiscard]] const void               *data() const noexcept { return m_value_storage.data(); }
        [[nodiscard]] void                     *state_data() noexcept { return m_state_storage.data(); }
        [[nodiscard]] const void               *state_data() const noexcept { return m_state_storage.data(); }
        [[nodiscard]] state_storage_type       &state_storage() noexcept { return m_state_storage; }
        [[nodiscard]] const state_storage_type &state_storage() const noexcept { return m_state_storage; }
        [[nodiscard]] value_storage_type       &value_storage() noexcept { return m_value_storage; }
        [[nodiscard]] const value_storage_type &value_storage() const noexcept { return m_value_storage; }

        [[nodiscard]] ValueView value() noexcept { return detail::ts_value_view(binding(), m_value_storage.data()); }
        [[nodiscard]] ValueView value() const noexcept {
            return detail::ts_value_view(binding(), const_cast<void *>(m_value_storage.data()));
        }

        void reset() {
            if (m_state_storage.plan() != nullptr) { m_state_storage.reset_to_default(); }
            if (m_value_storage.plan() != nullptr) { m_value_storage.reset_to_default(); }
        }

        [[nodiscard]] TsOutputView view(engine_time_t evaluation_time = MIN_DT);
        [[nodiscard]] TsOutputView view(engine_time_t evaluation_time = MIN_DT) const;

      private:
        value_storage_type m_value_storage{};
        state_storage_type m_state_storage{};
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_H
