#ifndef HGRAPH_CPP_ROOT_TS_VALUE_H
#define HGRAPH_CPP_ROOT_TS_VALUE_H

#include <hgraph/v2/types/timeseries/ts_value_builder.h>
#include <hgraph/v2/types/value/view.h>

#include <compare>
#include <concepts>
#include <stdexcept>

namespace hgraph::v2
{
    template <typename Binding>
    using TsTypedStorageHandle = MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, Binding>;

    using TsValueStorageHandle  = TsTypedStorageHandle<TsValueTypeBinding>;
    using TsInputStorageHandle  = TsTypedStorageHandle<TsInputTypeBinding>;
    using TsOutputStorageHandle = TsTypedStorageHandle<TsOutputTypeBinding>;

    namespace detail
    {
        template <typename Binding>
            requires requires(const Binding &binding) {
                { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
            }
        [[nodiscard]] inline const TsValueOps *ts_ops(const Binding *binding) noexcept {
            return binding != nullptr ? binding->ops : nullptr;
        }

        [[nodiscard]] inline const ValueTypeBinding *ts_value_binding(const TsValueOps *ops) noexcept {
            return ops != nullptr ? ops->value_binding : nullptr;
        }

        [[nodiscard]] inline const ValueTypeBinding &checked_ts_value_binding(const TsValueOps *ops) {
            if (const ValueTypeBinding *binding = ts_value_binding(ops); binding != nullptr) { return *binding; }
            throw std::logic_error("TS view is missing an underlying value binding");
        }

        template <typename Binding>
            requires requires(const Binding &binding) {
                { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
            }
        [[nodiscard]] inline const TsValueOps &checked_ts_ops(const Binding *binding) {
            if (const TsValueOps *ops = ts_ops(binding); ops != nullptr && ops->value_binding != nullptr) { return *ops; }
            throw std::logic_error("TS view is missing runtime operations");
        }

        template <typename Binding>
            requires requires(const Binding &binding) {
                { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
            }
        [[nodiscard]] inline TsTypedStorageHandle<Binding>
        ts_storage_view(const Binding *binding, void *data = nullptr,
                        const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator()) noexcept {
            return binding != nullptr ? TsTypedStorageHandle<Binding>::reference(*binding, data, allocator)
                                      : TsTypedStorageHandle<Binding>{};
        }

        template <typename Binding>
            requires requires(const Binding &binding) {
                { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
            }
        [[nodiscard]] inline const ValueTypeBinding *ts_value_binding(const Binding *binding) noexcept {
            return ts_value_binding(ts_ops(binding));
        }

        [[nodiscard]] inline ValueView ts_value_view(const ValueTypeBinding *value_binding, void *data) noexcept {
            if (value_binding != nullptr) {
                return data != nullptr ? ValueView{value_binding, data} : ValueView::invalid_for(*value_binding);
            }
            return {};
        }

        template <typename Binding>
            requires requires(const Binding &binding) {
                { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
            }
        [[nodiscard]] inline ValueView ts_value_view(const Binding *binding, void *data) noexcept {
            return ts_value_view(ts_value_binding(binding), data);
        }
    }  // namespace detail

}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_VALUE_H
