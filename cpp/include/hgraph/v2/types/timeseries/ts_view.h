#ifndef HGRAPH_CPP_ROOT_TS_VIEW_H
#define HGRAPH_CPP_ROOT_TS_VIEW_H

#include <hgraph/util/date_time.h>
#include <hgraph/v2/types/timeseries/ts_value.h>

#include <concepts>
#include <string_view>
#include <utility>

namespace hgraph::v2
{
    template <typename Binding = TsValueTypeBinding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    struct TsbView;
    template <typename Binding = TsValueTypeBinding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    struct TslView;
    template <typename Binding = TsValueTypeBinding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    struct TssView;
    template <typename Binding = TsValueTypeBinding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    struct TsdView;
    template <typename Binding = TsValueTypeBinding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    struct TswView;

    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    struct TsViewContextT
    {
        TsTypedStateHandle<Binding> state_storage{};
        void                       *value_data{nullptr};
        engine_time_t               evaluation_time{MIN_DT};

        TsViewContextT() = default;

        TsViewContextT(TsTypedStateHandle<Binding> state_storage, void *value_data, engine_time_t evaluation_time = MIN_DT) noexcept
            : state_storage(std::move(state_storage)), value_data(value_data), evaluation_time(evaluation_time) {}

        TsViewContextT(const TsViewContextT &)                = delete;
        TsViewContextT &operator=(const TsViewContextT &)     = delete;
        TsViewContextT(TsViewContextT &&) noexcept            = default;
        TsViewContextT &operator=(TsViewContextT &&) noexcept = default;
    };

    /**
     * Thin erased view over a logical time-series position.
     *
     * The TS runtime now treats state storage and value storage as two
     * parallel regions. The view therefore carries a borrowed TS-state handle
     * plus a raw value pointer. This keeps the cursor light while preserving
     * the data-first value layout needed for zero-cost `ValueView`
     * projection.
     */
    template <typename Binding>
        requires requires(const Binding &binding) {
            { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
        }
    struct BasicTsView
    {
        using binding_type = Binding;
        using storage_type = TsTypedStateHandle<Binding>;
        using context_type = TsViewContextT<Binding>;

        BasicTsView() = default;

        explicit BasicTsView(context_type context) : m_context(std::move(context)) { refresh_cached_bindings(); }

        BasicTsView(const BasicTsView &)                = delete;
        BasicTsView &operator=(const BasicTsView &)     = delete;
        BasicTsView(BasicTsView &&) noexcept            = default;
        BasicTsView &operator=(BasicTsView &&) noexcept = default;

        [[nodiscard]] const context_type &context() const noexcept { return m_context; }
        [[nodiscard]] const storage_type &state_storage() const noexcept { return m_context.state_storage; }
        [[nodiscard]] const storage_type &storage() const noexcept { return state_storage(); }
        [[nodiscard]] const Binding      *binding() const noexcept { return m_context.state_storage.binding(); }
        [[nodiscard]] const TsValueOps   *ops() const noexcept { return m_ops; }
        [[nodiscard]] const TsValueOps   &checked_ops() const {
            if (m_ops != nullptr) { return *m_ops; }
            throw std::logic_error("BasicTsView is missing TS runtime operations");
        }
        [[nodiscard]] const ValueTypeBinding *value_binding() const noexcept { return m_value_binding; }
        [[nodiscard]] const ValueTypeBinding &checked_value_binding() const {
            if (m_value_binding != nullptr) { return *m_value_binding; }
            throw std::logic_error("BasicTsView is missing an underlying value binding");
        }
        [[nodiscard]] const TsValueBuilder *builder() const noexcept {
            return type() != nullptr ? TsValueBuilder::find(type()) : nullptr;
        }
        [[nodiscard]] const TSValueTypeMetaData *type() const noexcept {
            return binding() != nullptr ? binding()->type_meta : nullptr;
        }
        [[nodiscard]] const ValueTypeMetaData *value_type() const noexcept {
            return m_value_binding != nullptr ? m_value_binding->type_meta : nullptr;
        }
        [[nodiscard]] const MemoryUtils::StoragePlan *state_plan() const noexcept { return state_storage().plan(); }
        [[nodiscard]] void         *state_data() const noexcept { return const_cast<void *>(state_storage().data()); }
        [[nodiscard]] void         *value_data() const noexcept { return m_context.value_data; }
        [[nodiscard]] bool          is_tsb() const noexcept { return ops() != nullptr && ops()->is_tsb(); }
        [[nodiscard]] bool          is_tsl() const noexcept { return ops() != nullptr && ops()->is_tsl(); }
        [[nodiscard]] bool          is_tss() const noexcept { return ops() != nullptr && ops()->is_tss(); }
        [[nodiscard]] bool          is_tsd() const noexcept { return ops() != nullptr && ops()->is_tsd(); }
        [[nodiscard]] bool          is_tsw() const noexcept { return ops() != nullptr && ops()->is_tsw(); }
        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return m_context.evaluation_time; }
        [[nodiscard]] bool          has_value() const noexcept { return m_context.value_data != nullptr; }
        [[nodiscard]] explicit      operator bool() const noexcept { return has_value(); }

        [[nodiscard]] ValueView value() const noexcept { return detail::ts_value_view(m_value_binding, m_context.value_data); }

        [[nodiscard]] TsbView<Binding> as_tsb() const;
        [[nodiscard]] TslView<Binding> as_tsl() const;
        [[nodiscard]] TssView<Binding> as_tss() const;
        [[nodiscard]] TsdView<Binding> as_tsd() const;
        [[nodiscard]] TswView<Binding> as_tsw() const;

      protected:
        void refresh(storage_type state_storage, void *value_data) {
            m_context.state_storage = std::move(state_storage);
            m_context.value_data    = value_data;
            refresh_cached_bindings();
        }

        [[nodiscard]] context_type cloned_context() const noexcept {
            return context_type{
                binding() != nullptr ? detail::ts_state_view(binding(), state_data(), state_allocator()) : storage_type{},
                value_data(),
                evaluation_time(),
            };
        }

        [[nodiscard]] const MemoryUtils::AllocatorOps &state_allocator() const noexcept {
            return detail::checked_allocator(state_storage().allocator());
        }

      private:
        void refresh_cached_bindings() {
            if (const Binding *current_binding = binding(); current_binding != nullptr) {
                m_ops           = &detail::checked_ts_ops(current_binding);
                m_value_binding = &detail::checked_ts_value_binding(m_ops);
            } else {
                m_ops           = nullptr;
                m_value_binding = nullptr;
            }
        }

        context_type            m_context{};
        const TsValueOps       *m_ops{nullptr};
        const ValueTypeBinding *m_value_binding{nullptr};
    };

    using TsViewContext = TsViewContextT<TsValueTypeBinding>;

    struct TsView : BasicTsView<TsValueTypeBinding>
    {
        using BasicTsView<TsValueTypeBinding>::BasicTsView;
        TsView() = default;
    };
}  // namespace hgraph::v2

#include <hgraph/v2/types/timeseries/ts_specialized_views.h>

#endif  // HGRAPH_CPP_ROOT_TS_VIEW_H
