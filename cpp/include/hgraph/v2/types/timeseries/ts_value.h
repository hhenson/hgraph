#ifndef HGRAPH_CPP_ROOT_TS_VALUE_H
#define HGRAPH_CPP_ROOT_TS_VALUE_H

#include <hgraph/v2/types/timeseries/ts_value_builder.h>
#include <hgraph/v2/types/value/view.h>

#include <compare>
#include <stdexcept>

namespace hgraph::v2
{
    using TsStorageHandle     = MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, TsValueTypeBinding>;
    using TsStorageViewHandle = TsStorageHandle;

    namespace detail
    {
        [[nodiscard]] inline TsStorageViewHandle
        ts_storage_view(const TsValueTypeBinding *binding, void *data = nullptr,
                        const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator()) noexcept {
            return binding != nullptr ? TsStorageViewHandle::reference(*binding, data, allocator) : TsStorageViewHandle{};
        }

        [[nodiscard]] inline const ValueTypeBinding *ts_value_binding(const TsValueTypeBinding *binding) noexcept {
            return binding != nullptr && binding->ops != nullptr ? binding->ops->value_binding : nullptr;
        }

        [[nodiscard]] inline ValueView ts_value_view(const TsValueTypeBinding *binding, void *data) noexcept {
            if (const ValueTypeBinding *value_binding = ts_value_binding(binding); value_binding != nullptr) {
                return data != nullptr ? ValueView{value_binding, data} : ValueView::invalid_for(*value_binding);
            }
            return {};
        }
    }  // namespace detail

}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_VALUE_H
