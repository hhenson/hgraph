#ifndef HGRAPH_CPP_ROOT_TS_VALUE_OPS_H
#define HGRAPH_CPP_ROOT_TS_VALUE_OPS_H

#include <hgraph/v2/types/utils/intern_table.h>
#include <hgraph/v2/types/value/value_ops.h>

#include <stdexcept>

namespace hgraph::v2
{
    /**
     * Shared TS-schema operations and projection metadata.
     *
     * The first TS endpoint pass reuses the underlying value storage plan
     * directly. The TS binding therefore needs one extra piece of shared
     * metadata beyond the plan itself: the value-layer binding used to project
     * the same memory back to `ValueView`.
     *
     * Input/output-specific TS ops can extend this base record later without
     * changing the core idea that the bound handle carries the TS identity.
     */
    struct TsValueOps
    {
        const ValueTypeBinding *value_binding{nullptr};

        [[nodiscard]] constexpr bool valid() const noexcept { return value_binding != nullptr; }

        [[nodiscard]] const ValueTypeBinding &checked_value_binding() const {
            if (value_binding != nullptr) { return *value_binding; }
            throw std::logic_error("TsValueOps is missing an underlying value binding");
        }

        [[nodiscard]] const ValueTypeMetaData *value_type() const noexcept {
            return value_binding != nullptr ? value_binding->type_meta : nullptr;
        }
    };

    namespace detail
    {
        [[nodiscard]] inline InternTable<const ValueTypeBinding *, TsValueOps> &ts_value_ops_registry() noexcept {
            static InternTable<const ValueTypeBinding *, TsValueOps> registry;
            return registry;
        }
    }  // namespace detail

    [[nodiscard]] inline const TsValueOps &ts_value_ops(const ValueTypeBinding &value_binding) {
        return detail::ts_value_ops_registry().emplace(&value_binding, &value_binding);
    }
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TS_VALUE_OPS_H
