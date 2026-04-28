#ifndef HGRAPH_CPP_ROOT_TS_INPUT_H
#define HGRAPH_CPP_ROOT_TS_INPUT_H

#include <hgraph/util/date_time.h>
#include <hgraph/v2/types/timeseries/ts_input_builder.h>
#include <hgraph/v2/types/timeseries/ts_value.h>

namespace hgraph::v2
{
    struct TsInputView;
    struct TsOutput;
    struct TsOutputView;

    /**
     * Input-side TS value reference.
     *
     * The input handle carries the TS binding directly. When the input is
     * unlinked it stays schema-bound through a borrowed handle with a null data
     * pointer, allowing `value()` to return an invalid `ValueView` for the
     * expected schema without storing duplicate binding state elsewhere.
     */
    struct TsInput
    {
        using storage_type = TsStorageHandle;

        TsInput() noexcept = default;

        explicit TsInput(const TSValueTypeMetaData &type) : TsInput(TsValueBuilder::checked(&type)) {}

        explicit TsInput(const TsValueBuilder &builder) : m_storage(storage_type::reference(builder.checked_binding(), nullptr)) {}

        explicit TsInput(const TsInputBuilder &builder) : TsInput(builder.checked_ts_value_builder()) {}

        explicit TsInput(const TsValueTypeBinding &binding) : m_storage(storage_type::reference(binding, nullptr)) {}

        TsInput(const TsInput &other) noexcept { bind_from_other(other); }

        TsInput(TsInput &&) noexcept = default;

        TsInput &operator=(const TsInput &other) noexcept {
            if (this != &other) { bind_from_other(other); }
            return *this;
        }

        TsInput &operator=(TsInput &&) noexcept = default;

        [[nodiscard]] bool     has_value() const noexcept { return m_storage.data() != nullptr; }
        [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }
        [[nodiscard]] bool     is_bound() const noexcept { return has_value(); }

        [[nodiscard]] const TsValueTypeBinding  *binding() const noexcept { return m_storage.binding(); }
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
            throw std::logic_error("TsInput is not bound to a value binding");
        }

        [[nodiscard]] void       *data() noexcept { return m_storage.data(); }
        [[nodiscard]] const void *data() const noexcept { return m_storage.data(); }
        [[nodiscard]] ValueView   value() noexcept { return detail::ts_value_view(binding(), m_storage.data()); }
        [[nodiscard]] ValueView   value() const noexcept {
            return detail::ts_value_view(binding(), const_cast<void *>(m_storage.data()));
        }

        void bind_output(const TsOutput &output);
        void bind_output(const TsOutputView &output);

        void bind_value(const ValueView &view) {
            if (!view.has_value()) { throw std::logic_error("TsInput::bind_value requires a live source value"); }
            if (binding() == nullptr) { throw std::logic_error("TsInput::bind_value requires a schema-bound input"); }
            if (view.binding() != value_binding()) {
                throw std::invalid_argument("TsInput::bind_value requires a matching value binding");
            }

            m_storage = storage_type::reference(*binding(), const_cast<void *>(view.data()), allocator());
        }

        void unbind_output() noexcept {
            if (const TsValueTypeBinding *ts_binding = binding(); ts_binding != nullptr) {
                m_storage = storage_type::reference(*ts_binding, nullptr, allocator());
            } else {
                m_storage.reset();
            }
        }

        [[nodiscard]] TsInputView view(engine_time_t evaluation_time = MIN_DT) noexcept;
        [[nodiscard]] TsInputView view(engine_time_t evaluation_time = MIN_DT) const noexcept;

      private:
        storage_type m_storage{};

        void bind_from_other(const TsInput &other) noexcept {
            m_storage.reset();
            if (const TsValueTypeBinding *ts_binding = other.binding(); ts_binding != nullptr) {
                m_storage = storage_type::reference(*ts_binding, const_cast<void *>(other.data()), other.allocator());
            }
        }
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_INPUT_H
