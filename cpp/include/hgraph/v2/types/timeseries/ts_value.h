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
            return binding != nullptr && binding->ops != nullptr ? &binding->checked_ops().checked_value_binding() : nullptr;
        }

        template <typename Binding>
            requires requires(const Binding &binding) {
                { binding.checked_ops().checked_value_binding() } -> std::same_as<const ValueTypeBinding &>;
            }
        [[nodiscard]] inline ValueView ts_value_view(const Binding *binding, void *data) noexcept {
            if (const ValueTypeBinding *value_binding = ts_value_binding(binding); value_binding != nullptr) {
                return data != nullptr ? ValueView{value_binding, data} : ValueView::invalid_for(*value_binding);
            }
            return {};
        }
    }  // namespace detail

}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_VALUE_H
