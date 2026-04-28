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
     * The output owns its TS payload directly through `StorageHandle`, mirroring
     * the `Value` design. Future output-local state such as alternatives or
     * dynamic projections can extend this wrapper without changing the
     * `value()` surface.
     */
    struct TsOutput
    {
        using storage_type = TsOutputStorageHandle;

        TsOutput() noexcept = default;

        explicit TsOutput(const TSValueTypeMetaData &type, const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : TsOutput(TsValueBuilder::checked(&type), allocator) {}

        explicit TsOutput(const TsValueBuilder &builder, const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : m_storage(TsOutputBuilder::checked(builder.type()).checked_binding(), allocator) {}

        explicit TsOutput(const TsOutputBuilder &builder, const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : m_storage(builder.checked_binding(), allocator) {}

        explicit TsOutput(const TsOutputTypeBinding &binding, const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator())
            : m_storage(binding, allocator) {}

        [[nodiscard]] bool     has_value() const noexcept { return m_storage.has_value(); }
        [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }

        [[nodiscard]] const TsOutputTypeBinding *binding() const noexcept { return m_storage.binding(); }
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
        [[nodiscard]] const MemoryUtils::StoragePlan  *plan() const noexcept { return m_storage.plan(); }
        [[nodiscard]] const MemoryUtils::StoragePlan  *value_plan() const noexcept { return plan(); }
        [[nodiscard]] const MemoryUtils::AllocatorOps &allocator() const noexcept {
            if (const auto *allocator_ops = m_storage.allocator(); allocator_ops != nullptr) { return *allocator_ops; }
            return MemoryUtils::allocator();
        }

        [[nodiscard]] const ValueTypeBinding &checked_value_binding() const {
            if (const ValueTypeBinding *binding_ptr = value_binding(); binding_ptr != nullptr) { return *binding_ptr; }
            throw std::logic_error("TsOutput is not bound to a value binding");
        }

        [[nodiscard]] void               *data() noexcept { return m_storage.data(); }
        [[nodiscard]] const void         *data() const noexcept { return m_storage.data(); }
        [[nodiscard]] storage_type       &storage() noexcept { return m_storage; }
        [[nodiscard]] const storage_type &storage() const noexcept { return m_storage; }

        [[nodiscard]] ValueView value() noexcept { return detail::ts_value_view(binding(), m_storage.data()); }
        [[nodiscard]] ValueView value() const noexcept {
            return detail::ts_value_view(binding(), const_cast<void *>(m_storage.data()));
        }

        void reset() {
            if (m_storage.plan() != nullptr) { m_storage.reset_to_default(); }
        }

        [[nodiscard]] TsOutputView view(engine_time_t evaluation_time = MIN_DT) noexcept;
        [[nodiscard]] TsOutputView view(engine_time_t evaluation_time = MIN_DT) const noexcept;

      private:
        storage_type m_storage{};
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_H
