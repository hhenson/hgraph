#ifndef HGRAPH_CPP_ROOT_VALUE_BUILDER_OPS_H
#define HGRAPH_CPP_ROOT_VALUE_BUILDER_OPS_H

#include <hgraph/v2/types/metadata/type_binding.h>
#include <hgraph/v2/types/utils/memory_utils.h>
#include <hgraph/v2/types/value/value_ops.h>

#include <stdexcept>

namespace hgraph::v2
{
    /**
     * Bound storage plan and interned schema behavior for one concrete value.
     *
     * The binding is the lightweight shared record of:
     * - the interned value schema
     * - the bound storage plan and lifecycle hooks
     * - the observational `ValueOps`
     */
    struct ValueBuilderOps
    {
        const ValueTypeBinding *binding{nullptr};

        [[nodiscard]] constexpr bool valid() const noexcept { return binding != nullptr; }

        [[nodiscard]] const ValueTypeBinding &type_binding() const {
            if (binding == nullptr) { throw std::logic_error("ValueBuilderOps is missing a type binding"); }
            return *binding;
        }
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_VALUE_BUILDER_OPS_H
